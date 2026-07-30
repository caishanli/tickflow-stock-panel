"""数据源优先级调度 + 自动降级（改编自 quant-daydayup datasource/manager.py）。"""
from __future__ import annotations

import pandas as pd

from .base import DataSourceError
from .cache import DataCache
from .mootdx_src import MootdxSource
from .astock_src import AStockSource

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
    "mootdx": MootdxSource,
    "astock": AStockSource,
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
        key = f"daily_{code}_{start}_{end}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        df = self.fetch("get_daily", code, start, end)
        self.cache.put(key, df)
        return df

    def get_minute(self, code, date):
        import logging
        log = logging.getLogger(__name__)

        # 1) DuckDB 有就直接读
        tf = self.sources.get("tickflow")
        if tf is not None:
            try:
                df = tf.get_minute(code, date)
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                log.warning("[QuantDataProvider] tickflow 分钟数据失败 %s: %s", code, e)

        # 2) DuckDB 没有, mootdx 回源并落盘
        mx = self.sources.get("mootdx")
        if mx is not None:
            try:
                df = mx.get_minute(code, date)
                if df is not None and not df.empty:
                    self._persist_minute_to_duckdb(code, date, df)
                    return df
            except Exception as e:
                log.warning("[QuantDataProvider] mootdx 分钟数据失败 %s: %s", code, e)

        # 3) 都没有, 返回错误
        raise RuntimeError(
            f"[QuantDataProvider] 持仓 {code} 分钟数据获取失败: "
            f"DuckDB 和 mootdx 均无数据"
        )

    def _persist_minute_to_duckdb(self, code: str, date, df) -> None:
        """mootdx 回源数据落盘 DuckDB, 供后续读取。"""
        import logging
        try:
            import polars as pl
            pldf = pl.from_pandas(df)
            sym = code.split(".")[0]
            if "symbol" not in pldf.columns:
                pldf = pldf.with_columns(pl.lit(sym).alias("symbol"))
            keep = [c for c in ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
                    if c in pldf.columns]
            pldf = pldf.select(keep)
            tf = self.sources.get("tickflow")
            if tf is not None and hasattr(tf, "_repo"):
                tf._repo._upsert_daily(pldf, "kline_minute")
        except Exception as e:
            logging.getLogger(__name__).warning("[QuantDataProvider] 分钟数据落盘失败 %s: %s", code, e)

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
