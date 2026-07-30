"""TickFlow 本地数据适配器：复用原工程 repository / parquet，只读，不改动它们。

仅 READ-import 既有 ``app.tickflow.repository`` / ``app.parquet``，绝不修改原工程。
任何本地无数据 / 读取失败都统一抛 :class:`DataSourceError`，由上层降级，
绝不造伪数据。
"""
from __future__ import annotations

from datetime import date as _date
from pathlib import Path

import pandas as pd

from .base import DataSource, DataSourceError


def _to_tf_code(code: str) -> str:
    """聚宽代码 600000.XSHG -> TickFlow 6 位数字代码。"""
    return code.split(".")[0]


def _as_date(x) -> _date:
    if isinstance(x, _date):
        return x
    return _date.fromisoformat(str(x)[:10])


class TickflowSource(DataSource):
    name = "tickflow"

    def __init__(self, db_path: str | Path | None = None):
        from app.tickflow.repository import DataStore, KlineRepository
        from app.parquet import scan_enriched_parquet
        if db_path is not None:
            data_dir = Path(db_path).parent
            self._store = DataStore(data_dir=data_dir)
        else:
            self._store = DataStore()
        self._repo = KlineRepository(self._store)
        self._scan = scan_enriched_parquet
        self._enriched_glob = str(
            self._store.data_dir / "kline_daily_enriched" / "**" / "*.parquet"
        )

    def get_daily(self, code, start, end):
        sym = _to_tf_code(code)
        try:
            df = self._repo.get_daily(sym, _as_date(start), _as_date(end))
            if df is None or df.height == 0:
                raise DataSourceError(f"tickflow 无日线: {code}")
            out = df.to_pandas()
            keep = [c for c in ("date", "open", "high", "low", "close", "volume")
                    if c in out.columns]
            out = out[keep].copy()
            out["date"] = out["date"].astype(str)
            return out
        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"tickflow 日线失败: {e}")

    def get_minute(self, code, date):
        sym = _to_tf_code(code)
        try:
            df = self._repo.get_minute(sym, _as_date(date))
            if df is None or df.height == 0:
                raise DataSourceError(f"tickflow 无分钟: {code} {date}")
            return df.to_pandas()
        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"tickflow 分钟失败: {e}")

    def get_stock_list(self, code=None):
        df = self._repo.get_instruments()
        if df is None or df.height == 0 or "symbol" not in df.columns:
            raise DataSourceError("tickflow 无股票池")
        return df["symbol"].to_list()

    def get_etf_list(self, code=None):
        df = self._repo.get_etf_instruments()
        if df is None or df.height == 0 or "symbol" not in df.columns:
            raise DataSourceError("tickflow 无ETF池")
        return df["symbol"].to_list()

    def get_index_realtime(self, codes):
        raise DataSourceError("tickflow 源不提供实时指数")

    def get_us_index(self):
        raise DataSourceError("tickflow 源不提供美股")

    def test_connection(self):
        return True
