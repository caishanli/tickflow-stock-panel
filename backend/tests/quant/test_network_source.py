import datetime as _dt

import pandas as pd
import pytest

from app.quant.jqengine.datasource.manager import DataManager
from app.quant.jqengine.datasource.network_source import NetworkSource
from app.quant.datasource.cache import DataCache


def _df(code, closes):
    idx = pd.date_range(_dt.date.today() - pd.Timedelta(days=len(closes) - 1),
                        periods=len(closes))
    return pd.DataFrame({"open": closes, "high": closes, "low": closes,
                         "close": closes, "volume": 1000.0, "amount": 1e5,
                         "trade_dt": idx.normalize().values}, index=idx)


class FakeClient:
    def __init__(self):
        self.calls = []

    def preload_daily(self, lookback_days=400, asof=None):
        self.calls.append("preload_daily")
        return {"512670.XSHG": _df("512670.XSHG", [1.0, 1.1])}

    def get_price(self, security, start_date=None, end_date=None,
                  frequency="daily", fields=None):
        self.calls.append(("get_price", frequency, security))
        codes = security if isinstance(security, list) else [security]
        return {c: _df(c, [1.0, 1.1]) for c in codes}

    def get_adj_factors(self):
        return pd.DataFrame()


def test_network_source_get_daily():
    src = NetworkSource(FakeClient())
    df = src.get_daily("512670.XSHG", "2026-01-01", "2026-02-01")
    assert df["close"].iloc[-1] == 1.1


def test_datamanager_preload_via_client(tmp_path):
    dm = DataManager(cache=DataCache(root=str(tmp_path / "cache")),
                     client=FakeClient())
    dm.preload_daily()
    assert "get_daily_512670.XSHG" in dm._daily_mem
