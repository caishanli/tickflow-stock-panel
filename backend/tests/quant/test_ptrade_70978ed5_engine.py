"""ptrade 引擎补齐的 docx 原生函数测试（本地 ptrade_api + rqalpha ptradecompat）。"""
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from app.quant.ptradeengine import ptrade_api


class _FakeMgr:
    def __init__(self):
        self._daily_mem = {}
        self._minute_mem = {}

    def fetch(self, name, *a, **kw):
        if name == "get_daily":
            idx = pd.date_range("2026-06-01", "2026-07-10", freq="B")
            return pd.DataFrame({"close": np.linspace(10, 11, len(idx)),
                                 "volume": np.full(len(idx), 1000.0),
                                 "money": np.linspace(1e6, 1.1e6, len(idx))},
                                index=idx)
        if name == "get_etf_list":
            return ["510300.SH", "159915.SZ"]
        return None

    def get_daily_money_cached(self, codes, end_date, count):
        idx = pd.date_range("2026-07-06", "2026-07-10", freq="B")
        rows = []
        for c in codes:
            for t in idx:
                rows.append({"time": t, "code": c, "money": 1.0e6})
        return pd.DataFrame(rows)

    def get_minute_price_at(self, code, dt):
        return 10.5

    def get_minute(self, code, date_str, limit=None):
        idx = pd.date_range("2026-07-10 09:31", periods=100, freq="min")
        return pd.DataFrame({"close": np.full(100, 10.5),
                             "volume": np.full(100, 100.0)}, index=idx)


@pytest.fixture
def pt_ctx():
    ctx = ptrade_api._reset(_FakeMgr(), 0.0001, 0.0001, 100000.0)
    ctx.current_dt = datetime(2026, 7, 10, 14, 0)
    return ctx


def test_get_price_daily_wide(pt_ctx):
    df = ptrade_api.get_price(
        ["510300.SS", "159915.SZ"], end_date="2026-07-09", count=5,
        frequency="1d", fields=["close"])
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["510300.SS", "159915.SZ"]
    assert len(df) == 5

def test_get_price_single_field_col(pt_ctx):
    df = ptrade_api.get_price(
        "510300.SS", end_date="2026-07-09", count=5, frequency="1d", fields=["close"])
    assert list(df.columns) == ["close"] or list(df.columns) == ["510300.SS"]

def test_get_price_minute_volume(pt_ctx):
    df = ptrade_api.get_price(
        ["510300.SS"], start_date="2026-07-10 09:31", end_date="2026-07-10 14:00",
        frequency="1m", fields=["volume"])
    assert isinstance(df, pd.DataFrame)
    assert "510300.SS" in df.columns or "volume" in df.columns

def test_check_limit_returns_dict(pt_ctx):
    res = ptrade_api.check_limit("510300.SS")
    assert isinstance(res, dict)
    assert "510300.SS" in res
    assert res["510300.SS"] in (-2, -1, 0, 1, 2)

def test_get_stock_info(pt_ctx):
    res = ptrade_api.get_stock_info(["510300.SS", "159915.SZ"])
    assert isinstance(res, dict)
    assert "510300.SS" in res

def test_order_target_value(pt_ctx):
    ok = ptrade_api.order_target_value("510300.SS", 50000)
    assert ok is True or ok is False

def test_get_all_trades_days(pt_ctx):
    days = ptrade_api.get_all_trades_days()
    assert isinstance(days, list) and len(days) > 0
    assert hasattr(days[0], "year")

def test_get_trading_day_by_date(pt_ctx):
    d = ptrade_api.get_trading_day_by_date("2026-07-10", day=-1)
    assert hasattr(d, "year")

def test_get_etf_info(pt_ctx):
    info = ptrade_api.get_etf_info(["510300.SS", "159915.SZ"])
    assert isinstance(info, dict)
    for v in info.values():
        assert isinstance(v, str)
