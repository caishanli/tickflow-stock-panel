"""数据源优先级调度 + 自动降级（改编自 quant-daydayup datasource/manager.py）。"""
from __future__ import annotations

import pandas as pd

from .base import DataSourceError
from .cache import DataCache
from .tushare_src import TushareSource
from .mootdx_src import MootdxSource
from .astock_src import AStockSource
from .baostock_src import BaostockSource, interpolate_5min_to_1min
from .minute_synth import SyntheticMinuteSource

from ..config import CONFIG


def _lazy_tickflow():
    # tickflow 源依赖 duckdb/polars 等可选组件，缺失时返回 None 不阻塞其他数据源
    try:
        from .tickflow_src import TickflowSource
        return TickflowSource
    except Exception:
        return None


SOURCES = {
    "tickflow": None,
    "tushare": TushareSource,
    "mootdx": MootdxSource,
    "astock": AStockSource,
}


class QuantDataProvider:
    """按 QUANT_DATA_PRIORITY 依次尝试各源，失败自动降级。"""

    def __init__(self, priority=None, token=None, cache=None):
        self.cache = cache or DataCache()
        tok = token if token is not None else CONFIG.tushare_token
        self.sources = {}
        for k, v in SOURCES.items():
            if v is None:
                real = _lazy_tickflow() if k == "tickflow" else None
                if real is None:
                    continue
                v = real
            try:
                self.sources[k] = v(token=tok) if k == "tushare" else v()
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning("数据源 %s 初始化失败，跳过: %s", k, e)
                continue
        self.minute_source = SyntheticMinuteSource(
            lambda code, start, end: self.fetch("get_daily", code, start, end)
        )
        self.sources["baostock"] = BaostockSource()
        self.priority = priority or CONFIG.data_priority

    def fetch(self, method, code, *args):
        last = None
        for name in self.priority:
            src = self.sources.get(name)
            if src is None:
                continue
            try:
                return getattr(src, method)(code, *args)
            except DataSourceError as e:
                last = e
                continue
        raise last or DataSourceError(f"所有数据源均不可用: {method} {code}")

    def get_daily(self, code, start, end):
        key = f"daily_{code}_{start}_{end}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        df = self.fetch("get_daily", code, start, end)
        self.cache.put(key, df)
        return df

    def get_minute(self, code, date):
        return self.minute_source.get_minute(code, date)

    def get_stock_list(self):
        return self._fetch_noarg("get_stock_list")

    def get_etf_list(self):
        return self._fetch_noarg("get_etf_list")

    def _fetch_noarg(self, method):
        last = None
        for name in self.priority:
            src = self.sources.get(name)
            if src is None:
                continue
            try:
                return getattr(src, method)()
            except DataSourceError as e:
                last = e
                continue
            except TypeError:
                # 部分源的方法签名不带 code 参数，直接无参调用即可
                continue
        raise last or DataSourceError(f"所有数据源均不可用: {method}")
