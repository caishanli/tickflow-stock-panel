"""桥接层审查收尾修复的回归测试（不跑 rqalpha 事件循环，直接测数据源/shim）。

覆盖：
- M7  _fallback_volume_daily 盘中前视：当日分钟量缺失时，盘中（<15:00）不得把
      日线全天成交量贴到当前分钟（09:40 即得全天量 → 策略量比推算失真）；
      收盘后（>=15:00）当日日线已完整，兜底保持。
- M8  QuantRQAlphaDataSource 列名归一：tushare 风格（trade_date/vol）与
      mootdx 风格（datetime 索引 + vol）的日线 DataFrame 都能转 recarray，
      不再因硬编码 date/volume 列 KeyError 导致整个 run failed。
- M10 fq 复权缺口显式化：fq="pre" 被请求时每标的每次回测 logger.warning 一次
      （不刷屏）；install_jqcompat 重置；fq=None/"none" 不提示。
"""
import logging
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from app.quant import jqcompat as jq
from app.quant import rqalpha_bridge as bridge


# ---------------------------------------------------------------------------
# M7 _fallback_volume_daily 盘中防前视
# ---------------------------------------------------------------------------
def _daily_bars(day="2024-01-04", vol=12345.0):
    """单根日线 recarray（_fallback_volume_daily 只读 datetime/volume 字段）。"""
    arr = np.zeros(1, dtype=bridge._BAR_DTYPE)
    arr["datetime"] = int(day.replace("-", "")) * 1000000
    arr["volume"] = vol
    return arr


class _DayStore:
    def __init__(self, bars):
        self._bars = bars

    def get_bars(self, code):
        return self._bars


def _patch_env(monkeypatch, store):
    fake_ds = SimpleNamespace(_day_bar_store=store)
    monkeypatch.setattr(
        jq, "Environment",
        SimpleNamespace(get_instance=lambda: SimpleNamespace(data_source=fake_ds)),
    )


def test_m7_intraday_fallback_disabled(monkeypatch):
    """09:40 盘中调用：当日分钟量缺失时不得返回日线全天量（前视）。"""
    _patch_env(monkeypatch, _DayStore(_daily_bars(vol=12345.0)))
    out = pd.DataFrame(columns=["time", "code", "volume"])
    res = jq._fallback_volume_daily(out, ["510300.XSHG"], "1m", ["volume"],
                                    pd.Timestamp("2024-01-04 09:40"))
    assert res.empty  # 盘中不得贴全天量（修复前会给出 12345.0）


def test_m7_intraday_missing_code_not_backfilled(monkeypatch):
    """盘中：已有标的保留，缺失标的不得被贴全天量（量比推算不再失真）。"""
    _patch_env(monkeypatch, _DayStore(_daily_bars(vol=12345.0)))
    out = pd.DataFrame([{
        "time": pd.Timestamp("2024-01-04 09:40"),
        "code": "511880.XSHG",
        "volume": 100.0,
    }])
    res = jq._fallback_volume_daily(out, ["511880.XSHG", "510300.XSHG"],
                                    "1m", ["volume"], pd.Timestamp("2024-01-04 09:40"))
    assert set(res["code"]) == {"511880.XSHG"}  # 510300 不得被贴全天量


def test_m7_after_close_fallback_kept(monkeypatch):
    """15:00 起当日日线已完整：分钟量缺失时仍可兜底贴全天量（与聚宽口径等价）。"""
    _patch_env(monkeypatch, _DayStore(_daily_bars(vol=12345.0)))
    out = pd.DataFrame(columns=["time", "code", "volume"])
    res = jq._fallback_volume_daily(out, ["510300.XSHG"], "1m", ["volume"],
                                    pd.Timestamp("2024-01-04 15:00"))
    assert len(res) == 1
    assert res.iloc[0]["volume"] == 12345.0


# ---------------------------------------------------------------------------
# M8 _df_to_recarray 列名归一
# ---------------------------------------------------------------------------
def _tushare_style_df(closes=(10.0, 11.0, 12.0)):
    """tushare pro.daily 原生 schema：trade_date/vol（无 date/volume 列）。"""
    n = len(closes)
    return pd.DataFrame({
        "trade_date": ["20240102", "20240103", "20240104"][:n],
        "open": closes,
        "high": [c * 1.02 for c in closes],
        "low": [c * 0.98 for c in closes],
        "close": closes,
        "vol": [50.0] * n,      # tushare vol 单位: 手
        "amount": [50000.0] * n,
    })


def _mootdx_style_df(closes=(10.0, 11.0, 12.0)):
    """mootdx 原生 schema：datetime 索引 + vol（无 date 列）。"""
    idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"][: len(closes)])
    return pd.DataFrame({
        "open": closes,
        "high": [c * 1.02 for c in closes],
        "low": [c * 0.98 for c in closes],
        "close": closes,
        "vol": [50.0] * len(closes),
    }, index=idx)


def test_m8_tushare_style_df_converts():
    """tushare 风格 (trade_date/vol) 不再 KeyError；vol(手)×100 → volume(股)。"""
    arr = bridge.QuantRQAlphaDataSource._df_to_recarray(_tushare_style_df())
    # datetime 为 rqalpha 口径 YYYYMMDD000000（同 jqcompat _daily_to_recarray）
    assert arr["datetime"].tolist() == [20240102000000, 20240103000000, 20240104000000]
    assert arr["volume"].tolist() == [5000.0, 5000.0, 5000.0]  # 与 mootdx 源 vol×100 口径一致
    assert arr["close"].tolist() == [10.0, 11.0, 12.0]


def test_m8_mootdx_style_df_converts():
    """mootdx 风格 (datetime 索引 + vol) 不再 KeyError；索引 → date 列。"""
    arr = bridge.QuantRQAlphaDataSource._df_to_recarray(_mootdx_style_df())
    # datetime 为 rqalpha 口径 YYYYMMDD000000（同 jqcompat _daily_to_recarray）
    assert arr["datetime"].tolist() == [20240102000000, 20240103000000, 20240104000000]
    assert arr["volume"].tolist() == [5000.0, 5000.0, 5000.0]


def test_m8_canonical_df_untouched():
    """已有 date/volume 列的 df（bundle CSV 路径）原样通过，不做单位换算。"""
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
        "open": [10.0, 11.0], "high": [10.2, 11.2], "low": [9.8, 10.8],
        "close": [10.0, 11.0], "volume": [1000.0, 1000.0],
    })
    arr = bridge.QuantRQAlphaDataSource._df_to_recarray(df)
    assert arr["volume"].tolist() == [1000.0, 1000.0]


class _TushareLikeProvider:
    """返回 tushare 原生 schema 的最小 provider（无 cache 属性）。"""

    def get_daily(self, code, start, end):
        return _tushare_style_df()


def test_m8_datasource_constructs_from_tushare_schema():
    """端到端：tushare 风格日线不再让数据源构造 KeyError（修复前整个 run failed）。"""
    ds = bridge.QuantRQAlphaDataSource(
        _TushareLikeProvider(), bridge.CONFIG,
        {"symbols": ["600000.XSHG"], "start": "2024-01-02", "end": "2024-01-04"})
    ins = list(ds.get_instruments(["600000.XSHG"]))[0]
    bar = ds.get_bar(ins, pd.Timestamp("2024-01-03"), "1d")
    assert bar is not None
    assert float(bar["close"]) == 11.0
    # 交易日历同样从归一后的 date 列构建
    assert [d.isoformat() for d in ds._trading_dates] == [
        "2024-01-02", "2024-01-03", "2024-01-04"]


# ---------------------------------------------------------------------------
# M10 fq 口径显式化（新语义：本地日线统一前复权，pre/qfq/None 不提示）
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_fq_warned():
    jq._FQ_WARNED.clear()
    yield
    jq._FQ_WARNED.clear()


def test_m10_fq_warns_once_per_code(caplog):
    """fq="raw"（本地不支持的口径）被请求：每标的一次回测只 warning 一次，
    重复请求不刷屏。新语义：本地日线统一前复权，pre/qfq/None 不再提示，
    none/raw/post/hfq 逐标的提示一次（见 test_fix_compat2.py 的口径矩阵）。"""
    with caplog.at_level(logging.WARNING, logger="jqcompat"):
        jq._check_fq("raw", ["510300.XSHG"])
        jq._check_fq("raw", ["510300.XSHG"])  # 重复请求不再提示
        jq._check_fq("raw", ["159915.XSHE"])
    warns = [r for r in caplog.records if "复权" in r.message]
    assert len(warns) == 2
    assert {r.message.split(":")[0] for r in warns} == {"510300.XSHG", "159915.XSHE"}
    assert all("除息" in r.message for r in warns)  # 说明收益口径偏差


def test_m10_fq_none_no_warning(caplog):
    """fq=None（缺省即前复权）与 "pre"/"qfq" 不提示：本地日线统一前复权已满足。"""
    with caplog.at_level(logging.WARNING, logger="jqcompat"):
        jq._check_fq(None, ["510300.XSHG"])
        jq._check_fq("pre", ["510300.XSHG"])
        jq._check_fq("qfq", ["510300.XSHG"])
    assert not [r for r in caplog.records if "复权" in r.message]


def test_m10_install_resets_fq_warned():
    """install_jqcompat（每次回测）重置已提示集合：下一轮回测重新提示一次。"""
    jq._check_fq("raw", ["510300.XSHG"])
    assert jq._FQ_WARNED
    jq.install_jqcompat([])
    try:
        assert not jq._FQ_WARNED
    finally:
        jq.install_jqcompat([])  # 复位，避免污染同进程其他测试
