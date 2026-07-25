"""本地 Parquet 缓存（改编自 quant-daydayup datasource/cache.py）。"""
from __future__ import annotations
import os
import threading
import pandas as pd
import pyarrow.parquet as pq  # pyarrow 已在基础依赖

_LOCK = threading.Lock()


class DataCache:
    def __init__(self, root: str = ""):
        if not root:
            from ..config import CONFIG
            root = os.path.join(os.path.dirname(CONFIG.db_path), "quant_cache")
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _path(self, key: str) -> str:
        return os.path.join(self.root, f"{key}.parquet")

    def get(self, key: str):
        p = self._path(key)
        if os.path.exists(p):
            try:
                return pd.read_parquet(p)
            except Exception:
                return None
        return None

    def put(self, key: str, df):
        if df is None or getattr(df, "empty", True):
            return
        with _LOCK:
            df.to_parquet(self._path(key), index=False)
