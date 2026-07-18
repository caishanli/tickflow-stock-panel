# -*- coding: utf-8 -*-
"""成交额单位归一与 get_all_securities types 过滤的修复回归测试。

背景（对账实测）：tushare pro.daily/fund_daily 的 amount 单位是**千元**，
被 _build_money_full 当元直接使用，全市场 ETF 成交额聚合小 ~1000 倍
（本地 41.7 亿 vs 聚宽 4967 亿）；同时 get_all_securities(['etf']) 未按
types 过滤，4 只指数（其 amount 为成分股全市场成交额、万亿级）混入 ETF
聚合。修复后 ETF 合计 5304 亿 vs 聚宽 4967 亿（+6.8%，宇宙差异）。
"""

import pandas as pd

from app.quant import jqcompat
from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.manager import DataManager, _ensure_money_yuan

DATES = pd.date_range("2026-07-08", periods=3)


def _tushare_df():
    # tushare schema：vol 单位手、amount 单位千元
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


def _make_dm(tmp_path):
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)))
    dm._offline = True
    return dm


def test_ensure_money_yuan_tushare_amount_is_qianyuan():
    df = _ensure_money_yuan(_tushare_df(), "tushare")
    # amount(千元) ×1000 → money(元)
    assert df["money"].iloc[-1] == 4039507.185 * 1000
    assert "amount" in df.columns  # 原列保留


def test_ensure_money_yuan_non_tushare_factor_1():
    df = _ensure_money_yuan(
        pd.DataFrame({"close": [1.0], "vol": [100], "amount": [123.0]}), "baostock")
    assert df["money"].iloc[0] == 123.0


def test_ensure_money_yuan_keeps_existing_money():
    df = _mootdx_df()
    out = _ensure_money_yuan(df, "mootdx")
    assert out is df  # 已有 money(元) 不动


def test_preload_daily_normalizes_money_units(tmp_path):
    dm = _make_dm(tmp_path)
    dm.cache.put("daily", "tushare_510300.XSHG", _tushare_df())
    dm.cache.put("daily", "mootdx_512800.XSHG", _mootdx_df())
    dm.preload_daily()
    ts = dm._daily_mem["get_daily_510300.XSHG"]
    assert ts["money"].iloc[-1] == 4039507.185 * 1000
    mt = dm._daily_mem["get_daily_512800.XSHG"]
    assert mt["money"].iloc[0] == 1.01e8


def test_money_aggregate_excludes_unit_error(tmp_path):
    """策略口径全市场合计：tushare 千元修正后与元单位帧同量级相加。"""
    dm = _make_dm(tmp_path)
    dm.cache.put("daily", "tushare_510300.XSHG", _tushare_df())
    dm.preload_daily()
    m = dm.get_daily_money_cached(["510300.XSHG"], DATES[-1], count=1)
    # 修正前 4,039,507（元），修正后 4,039,507,185（元）
    assert m["money"].iloc[0] == 4039507.185 * 1000


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
