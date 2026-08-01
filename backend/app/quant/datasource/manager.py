"""数据源优先级调度 + 自动降级（改编自 quant-daydayup datasource/manager.py）。"""
from __future__ import annotations

import pandas as pd

from .base import DataSourceError
from .cache import DataCache

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
}


class QuantDataProvider:
    """按 QUANT_DATA_PRIORITY 依次尝试各源，失败自动降级。"""

    def __init__(self, priority=None, token=None, cache=None):
        self.cache = cache or DataCache()
        self.sources = {}
        for k, v in SOURCES.items():
            if v is None:
                real = _lazy_tickflow() if k == "tickflow" else None
                if real is None:
                    continue
                v = real
            try:
                self.sources[k] = v()
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning("数据源 %s 初始化失败，跳过: %s", k, e)
                continue
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
        import logging
        log = logging.getLogger(__name__)
        key = f"daily_{code}_{start}_{end}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        # 按优先级遍历: tickflow(DuckDB) → mootdx → astock
        for name in self.priority:
            src = self.sources.get(name)
            if src is None:
                continue
            try:
                df = src.get_daily(code, start, end)
                if df is not None and not df.empty:
                    self.cache.put(key, df)
                    return df
            except Exception as e:
                log.warning("[QuantDataProvider] %s 日线失败 %s: %s", name, code, e)

        raise RuntimeError(
            f"[QuantDataProvider] 日线数据获取失败 {code}: "
            f"所有数据源均无数据"
        )

    def get_minute(self, code, date):
        import logging
        log = logging.getLogger(__name__)

        # 按优先级遍历: tickflow(DuckDB) → mootdx
        for name in self.priority:
            src = self.sources.get(name)
            if src is None:
                continue
            try:
                df = src.get_minute(code, date)
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                log.warning("[QuantDataProvider] %s 分钟数据失败 %s: %s", name, code, e)

        raise RuntimeError(
            f"[QuantDataProvider] 持仓 {code} 分钟数据获取失败: "
            f"所有数据源均无数据"
        )

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
                continue
        raise last or DataSourceError(f"所有数据源均不可用: {method}")
