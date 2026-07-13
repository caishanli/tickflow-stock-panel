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

from app.quant.jqengine.config import CONFIG


class DataCache:
    def __init__(self, root=None):
        self.root = root or CONFIG["DATA_DIR"]
        self._conns = {}

    def _db_path(self, freq):
        return os.path.join(self.root, f"{freq}.db")

    def _conn(self, freq):
        if freq not in self._conns:
            path = self._db_path(freq)
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

    def get(self, freq, code, loader):
        """命中缓存返回 DataFrame；未命中调用 loader 取数并写库。

        优先 DB；DB 缺失时回退旧 parquet 文件并写回 DB；都无则调用 loader。
        """
        conn = self._conn(freq)
        row = conn.execute("SELECT data FROM cache WHERE key=?", (code,)).fetchone()
        if row is not None:
            return self._deserialize(row[0])
        df = self._from_parquet(freq, code)
        if df is not None and not df.empty:
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
