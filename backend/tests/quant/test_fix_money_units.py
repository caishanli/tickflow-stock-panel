# -*- coding: utf-8 -*-
"""成交额单位归一与 get_all_securities types 过滤的修复回归测试。

背景：各数据源 amount 单位已统一为元，_ensure_money_yuan 直接将 amount
赋给 money，无需因子转换。get_all_securities(['etf']) 按 types 过滤，
指数不混入 ETF。
"""

import pandas as pd

from app.quant import jqcompat
from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.manager import DataManager, _ensure_money_yuan

DATES = pd.date_range("2026-07-25", periods=3)


def _astock_df():
    # astock schema：amount 单位元
    return pd.DataFrame({
        "trade_date": [d.strftime("%Y%m%d") for d in DATES],
        "open": [4.8, 4.85, 4.83],
        "high": [4.9, 4.9, 4.88],
        "low": [4.75, 4.8, 4.78],
        "close": [4.85, 4.83, 4.829],
        "vol": [8e6, 8.2e6, 8.24e6],
        "amount": [3.9e6, 4.0e6, 4039507.185],
    })


def _mootdx_df():
    # mootdx schema：volume 股（源内已 vol×100）、money 元（源内 amount→money）
    df = pd.DataFrame({
        "open": [1.0, 1.0, 1.0], "high": [1.0, 1.0, 1.0],
        "low": [1.0, 1.0, 1.0], "close": [1.0, 1.0, 1.0],
        "volume": [1e8, 1e8, 1e8], "money": [1.01e8, 1.0e8, 0.99e8],
        "amount": [1.01e8, 1.0e8, 0.99e8],
    }, index=DATES)
    return df


class _FakeClient:
    """网络客户端替身：preload_daily 返回合成日线帧（离线，不联网）。

    DataManager.preload_daily 已改走 network client，不再从本地分区/cache
    直接读日线；用替身把合成帧从 client 喂入，断言 money 单位归一口径不变。
    """

    def __init__(self, daily):
        self._daily = daily  # {jq_code: DatetimeIndex 日线帧}

    def preload_daily(self, lookback_days=400, asof=None):
        return self._daily

    def get_adj_factors(self):
        return pd.DataFrame()


def _make_dm(tmp_path, daily=None):
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)),
                     client=_FakeClient(daily or {}))
    dm._offline = True
    return dm


def test_ensure_money_yuan_astock_amount_is_yuan():
    df = _ensure_money_yuan(_astock_df(), "astock")
    # amount(元) 直接 → money(元)，无因子转换
    assert df["money"].iloc[-1] == 4039507.185
    assert "amount" in df.columns  # 原列保留


def test_ensure_money_yuan_non_astock_factor_1():
    df = _ensure_money_yuan(
        pd.DataFrame({"close": [1.0], "vol": [100], "amount": [123.0]}), "mootdx")
    assert df["money"].iloc[0] == 123.0


def test_ensure_money_yuan_keeps_existing_money():
    df = _mootdx_df()
    out = _ensure_money_yuan(df, "mootdx")
    assert out is df  # 已有 money(元) 不动


def test_preload_daily_normalizes_money_units(tmp_path):
    dm = _make_dm(tmp_path, daily={"510300.XSHG": _astock_df(),
                                   "512800.XSHG": _mootdx_df()})
    dm.preload_daily()
    ts = dm._daily_mem["get_daily_510300.XSHG"]
    assert ts["money"].iloc[-1] == 4039507.185
    mt = dm._daily_mem["get_daily_512800.XSHG"]
    assert mt["money"].iloc[0] == 1.01e8


def test_money_aggregate_excludes_unit_error(tmp_path):
    """策略口径全市场合计：amount 单位统一为元，直接可用。"""
    dm = _make_dm(tmp_path, daily={"510300.XSHG": _astock_df()})
    dm.preload_daily()
    m = dm.get_daily_money_cached(["510300.XSHG"], DATES[-1], count=1)
    assert m["money"].iloc[0] == 4039507.185


# ---------------------------------------------------------------------------
# get_all_securities types 过滤（指数不混入 etf）
# ---------------------------------------------------------------------------
def _install_universe():
    jqcompat._UNIVERSE = ["510300.XSHG", "512800.XSHG",
                          "000300.XSHG", "000510.XSHG", "399006.XSHE"]
    jqcompat._NAMES = {}
    jqcompat._LIST_DATES = {}


def test_get_all_securities_etf_excludes_index():
    _install_universe()
    df = jqcompat.get_all_securities(["etf"])
    assert set(df.index) == {"510300.XSHG", "512800.XSHG"}


def test_get_all_securities_index_only():
    _install_universe()
    df = jqcompat.get_all_securities(["index"])
    assert set(df.index) == {"000300.XSHG", "000510.XSHG", "399006.XSHE"}


def test_get_all_securities_none_returns_all():
    _install_universe()
    df = jqcompat.get_all_securities(None)
    assert len(df) == 5


def test_get_all_securities_etf_keeps_date_filter():
    _install_universe()
    jqcompat._LIST_DATES = {"510300.XSHG": ("2026-01-01", "2026-06-30")}
    df = jqcompat.get_all_securities(["etf"], date="2026-07-10")
    # 510300 已退市(过滤窗口外)，512800 无数据退化保留
    assert set(df.index) == {"512800.XSHG"}
    df = jqcompat.get_all_securities(["etf"], date="2026-03-01")
    assert set(df.index) == {"510300.XSHG", "512800.XSHG"}
