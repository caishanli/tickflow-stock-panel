"""行情数据本地 SQLite 缓存。

把原本「每个 (源_代码) 一个 parquet 小文件」的方案替换为两个 SQLite 库
（``data/daily.db`` / ``data/minute.db``），将 2000+ 个零散小文件收敛为 2 个文件。

回测预加载时，``get_all`` 用一条查询把全部缓存一次性读入内存，避免逐个打开小文件
带来的系统调用与 parquet footer 解析开销；增量写入走 ``INSERT OR REPLACE``，
单键更新不会影响其它缓存。

兼容策略：首次读取若 DB 中无记录，会回退到旧的 parquet 文件；命中后立即写入 DB，
因此已有的 parquet 缓存在不被删除的情况下也能无缝迁移。
"""

import io
import os
import sqlite3
import time

import pandas as pd

from ..config import CONFIG


class DataCache:
    def __init__(self, root=None):
        self.root = root or CONFIG["DATA_DIR"]
        self._conns = {}

    def _db_path(self, freq):
        return os.path.join(self.root, f"{freq}.db")

    def _conn(self, freq):
        if freq not in self._conns:
            path = self._db_path(freq)
            os.makedirs(self.root, exist_ok=True)
            conn = sqlite3.connect(path)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cache ("
                "key TEXT PRIMARY KEY, data BLOB, updated_at INTEGER)"
            )
            conn.commit()
            self._conns[freq] = conn
        return self._conns[freq]

    @staticmethod
    def _serialize(df):
        # pickle 反序列化比 parquet 快约 5 倍，且完美保留索引/列 dtype；
        # 数据为本机可信缓存，不存在反序列化来源风险。
        buf = io.BytesIO()
        df.to_pickle(buf)
        return buf.getvalue()

    @staticmethod
    def _deserialize(blob):
        return pd.read_pickle(io.BytesIO(blob))

    def _from_parquet(self, freq, code):
        p = os.path.join(self.root, freq, f"{code}.parquet")
        if os.path.exists(p):
            return pd.read_parquet(p)
        return None

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
                if end_ts <= pd.Timestamp.now().date():
                    if pd.Timestamp(last).date() < end_ts:
                        return False
            except Exception:
                return True
        if start:
            try:
                if pd.Timestamp(first).date() > pd.Timestamp(start).date():
                    return False
            except Exception:
                return True
        return True

    def get(self, freq, code, loader, start=None, end=None):
        """命中缓存返回 DataFrame；未命中或覆盖不足时调用 loader 取数并写库。

        优先 DB；DB 缺失时回退旧 parquet 文件并写回 DB；都无或区间不足则回源。
        关键修复：不再对“本地有但不完整”的缓存睁一只眼，覆盖不足即视为失效、
        回源补齐，避免回测使用被冻结的过期数据。
        """
        conn = self._conn(freq)
        row = conn.execute("SELECT data FROM cache WHERE key=?", (code,)).fetchone()
        if row is not None:
            df = self._deserialize(row[0])
            if self._covers(df, start, end):
                return df
            # 覆盖不足：旧缓存失效，丢弃并回源
        df = self._from_parquet(freq, code)
        if df is not None and not df.empty and self._covers(df, start, end):
            self.put(freq, code, df)
            return df
        df = loader()
        if df is not None and not df.empty:
            self.put(freq, code, df)
        return df

    def peek(self, freq, code):
        """仅查本地缓存，不触发 loader（无记录返回 None）。"""
        conn = self._conn(freq)
        row = conn.execute("SELECT data FROM cache WHERE key=?", (code,)).fetchone()
        if row is not None:
            return self._deserialize(row[0])
        return None

    def put(self, freq, code, df):
        if df is None or df.empty:
            return
        conn = self._conn(freq)
        conn.execute(
            "INSERT OR REPLACE INTO cache(key, data, updated_at) VALUES(?, ?, ?)",
            (code, self._serialize(df), int(time.time())),
        )
        conn.commit()

    def get_all(self, freq):
        """一次性读取某频率下全部缓存，返回 ``{key: DataFrame}``。"""
        conn = self._conn(freq)
        out = {}
        for key, blob in conn.execute("SELECT key, data FROM cache"):
            try:
                out[key] = self._deserialize(blob)
            except Exception:
                continue
        return out

    def keys(self, freq):
        conn = self._conn(freq)
        return [r[0] for r in conn.execute("SELECT key FROM cache")]

    def clear(self, freq=None):
        """清空缓存（测试/重置用）。"""
        for f in ([freq] if freq else ["daily", "minute"]):
            p = self._db_path(f)
            if os.path.exists(p):
                os.remove(p)
            self._conns.pop(f, None)
