"""行情数据本地 Parquet 缓存。

历史方案把每个 (源_代码) 的 DataFrame pickle 成 BLOB 存进 SQLite
（``daily.db`` / ``minute.db`` / ``5min.db``）：pickle 零压缩、增量补数时
整帧 ``INSERT OR REPLACE`` 重写，库文件膨胀到 GB 级且只增不减。现改为每个
缓存键一个 parquet 文件（``{root}/{freq}/{key}.parquet``，zstd 压缩），
与更早一版「每键一个 parquet 小文件」的布局一致，存量旧 parquet 文件直接可读。
旧 SQLite 库由 ``scripts/migrate_cache_to_parquet.py`` 一次性迁移。

回测预加载时，``get_all`` 用线程池并发读取全部 parquet（pyarrow 解压释放
GIL），避免大库串行 IO；增量写入走临时文件 + ``os.replace`` 原子替换，
同键最后写入者胜（与原 ``INSERT OR REPLACE`` 语义一致）。

parquet 本身不保存 ``df.attrs``（``adj``/``source`` 复权口径元数据），
写入时序列化为常量列 ``__attrs__``、读取时还原，与 pickle 往返语义等价。
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from ..config import CONFIG

_ATTRS_COL = "__attrs__"


def _try_read(path):
    """读取单个 parquet；损坏/半写文件返回 None（调用方按未命中/跳过处理）。"""
    try:
        return DataCache._read_parquet(path)
    except Exception:
        return None


def _read_keyed(item):
    """``(key, path)`` → ``(key, DataFrame|None)``，供 get_all 线程池 map。"""
    key, path = item
    return key, _try_read(path)


class DataCache:
    def __init__(self, root=None):
        self.root = root or CONFIG["DATA_DIR"]

    def _dir(self, freq):
        return os.path.join(self.root, freq)

    def _path(self, freq, code):
        return os.path.join(self._dir(freq), f"{code}.parquet")

    # ---- 序列化：df.attrs <-> __attrs__ 常量列 ----
    @staticmethod
    def _to_parquet(df, path):
        if df.attrs:
            df = df.copy()
            df[_ATTRS_COL] = json.dumps(df.attrs, ensure_ascii=False)
        tmp = f"{path}.tmp"
        df.to_parquet(tmp, compression="zstd")
        os.replace(tmp, path)

    @staticmethod
    def _read_parquet(path):
        df = pd.read_parquet(path)
        if _ATTRS_COL in df.columns:
            try:
                df.attrs = json.loads(df[_ATTRS_COL].iloc[0]) if len(df) else {}
            except (TypeError, ValueError):
                df.attrs = {}
            df = df.drop(columns=[_ATTRS_COL])
        return df

    @staticmethod
    def _covers(df, start=None, end=None):
        """缓存 DataFrame 是否覆盖 [start, end] 区间。

        不覆盖（末端早于 end，或起始晚于 start）即视为失效，需回源补齐。
        注意：``end`` 常是策略传入的「全集」哨兵（如 20300101），属未来日期，
        永远不可能被缓存覆盖，此时不应据此判失效，否则会触发全量回源并把
        mootdx 的分钟数据误当日线写入（见 ``put`` 的频率校验）。仅当 end 落在
        过去（≤ 今天）且确实未被覆盖时才判失效。
        """
        if df is None or (hasattr(df, "empty") and df.empty):
            return False
        last = first = None
        if "trade_date" in df.columns:
            last, first = str(df["trade_date"].max()), str(df["trade_date"].min())
        elif "date" in df.columns:
            last, first = str(df["date"].max()), str(df["date"].min())
        elif isinstance(getattr(df, "index", None), pd.DatetimeIndex):
            last, first = str(df.index.max().date()), str(df.index.min().date())
        else:
            return True  # 未知结构，保守命中，避免无限回源
        if end:
            try:
                end_ts = pd.Timestamp(end).date()
                # 未来哨兵/全集日期：缓存不可能覆盖，视作已覆盖，不据此失效
                if (end_ts <= pd.Timestamp.now().date()
                        and pd.Timestamp(last).date() < end_ts):
                    return False
            except Exception:
                return True
        if start:
            try:
                # 仅当数据起始晚于请求结束（数据完全在请求区间之后）才视为失效；
                # 请求起始早于数据起始（如回看窗口早于数据上市日）不失效——数据
                # 从 first 起已覆盖回测区间，回源会因 offline 失败导致基准缺失。
                if end and pd.Timestamp(first).date() > pd.Timestamp(end).date():
                    return False
            except Exception:
                return True
        return True

    def get(self, freq, code, loader, start=None, end=None):
        """命中缓存返回 DataFrame；未命中或覆盖不足时调用 loader 取数并写盘。

        关键修复：不再对“本地有但不完整”的缓存睁一只眼，覆盖不足即视为失效、
        回源补齐，避免回测使用被冻结的过期数据。
        """
        df = self.peek(freq, code)
        if df is not None and self._covers(df, start, end):
            return df
        df = loader()
        if df is not None and not df.empty:
            self.put(freq, code, df)
        return df

    def peek(self, freq, code):
        """仅查本地缓存，不触发 loader（无记录/文件损坏返回 None）。"""
        p = self._path(freq, code)
        if not os.path.exists(p):
            return None
        return _try_read(p)

    def put(self, freq, code, df):
        if df is None or df.empty:
            return
        os.makedirs(self._dir(freq), exist_ok=True)
        self._to_parquet(df, self._path(freq, code))

    def get_all(self, freq):
        """一次性读取某频率下全部缓存，返回 ``{key: DataFrame}``。"""
        d = self._dir(freq)
        if not os.path.isdir(d):
            return {}
        paths = [
            (os.path.splitext(f)[0], os.path.join(d, f))
            for f in os.listdir(d)
            if f.endswith(".parquet")
        ]
        out = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            for key, df in pool.map(_read_keyed, paths):
                if df is not None:
                    out[key] = df
        return out

    def keys(self, freq):
        d = self._dir(freq)
        if not os.path.isdir(d):
            return []
        return [
            os.path.splitext(f)[0] for f in os.listdir(d) if f.endswith(".parquet")
        ]

    def clear(self, freq=None):
        """清空缓存（测试/重置用）。"""
        for f in ([freq] if freq else ["daily", "minute"]):
            d = self._dir(f)
            if not os.path.isdir(d):
                continue
            for name in os.listdir(d):
                if name.endswith(".parquet"):
                    os.remove(os.path.join(d, name))
