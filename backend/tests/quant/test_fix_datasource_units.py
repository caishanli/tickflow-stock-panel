# -*- coding: utf-8 -*-
"""数据源适配层单位/复权口径修复的回归测试（合成数据，不联网）。

覆盖修复点：
- volume 单位归一：manager._ensure_volume_shares（tushare vol(手)×100→
  volume(股)，其他源已有 volume(股) 不动），接入点与 _ensure_money_yuan
  相同（preload_daily / fetch get_daily 分支 / _build_money_full peek 兜底）
- _build_money_full peek 兜底顺序复用 _priority()（原硬编码
  ("mootdx","astock","tushare") 与 preload 顺序矛盾，价格帧与 money 帧
  可能来自不同复权口径的源）
- baostock 5 分钟 adjustflag "2"(后复权) → "1"(前复权)，与日线口径一致
- 各源 get_daily 返回帧 attrs 标注 source/adj 复权口径元数据；
  manager._pick_daily_frame 混源防护：同一代码多源缓存并存时优先选
  非 raw（前复权）帧，全 raw 才用 raw；旧缓存无 attrs 按源名推断
- mootdx 用 tdxpy get_xdxr_info 除权除息因子做前复权换算（失败保持 raw）
- astock get_daily 字符串帧数值化（OHLC/volume/amount 转 float、date 解析）
"""

import sys
import types

import pandas as pd
import pytest

from app.quant.jqengine.config import CONFIG
from app.quant.jqengine.datasource import astock_src, mootdx_src, tushare_src
from app.quant.jqengine.datasource.base import DataSourceError
from app.quant.jqengine.datasource.baostock_src import BaostockSource
from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.manager import (
    DataManager,
    _ensure_volume_shares,
    _infer_adj,
    _pick_daily_frame,
)
import app.quant.jqengine.datasource.manager as manager_mod

DATES = pd.date_range("2026-07-08", periods=3)
CODE = "510300.XSHG"


def _make_dm(tmp_path):
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)))
    dm._offline = True
    return dm


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


def _mootdx_df(close=1.0):
    # mootdx schema：datetime 索引，volume 股（源内已 vol×100）、money 元
    return pd.DataFrame({
        "open": [close] * 3, "high": [close] * 3,
        "low": [close] * 3, "close": [close] * 3,
        "volume": [1e8, 1e8, 1e8], "money": [1.01e8, 1.0e8, 0.99e8],
        "amount": [1.01e8, 1.0e8, 0.99e8],
    }, index=DATES)


# ---------------------------------------------------------------------------
# 1. volume 单位归一 _ensure_volume_shares
# ---------------------------------------------------------------------------

def test_ensure_volume_shares_tushare_vol_is_hands():
    """tushare vol(手) ×100 → volume(股)，原 vol 列保留。"""
    df = _ensure_volume_shares(_tushare_df(), "tushare")
    assert df["volume"].iloc[0] == 8e6 * 100
    assert df["vol"].iloc[0] == 8e6  # 原列保留


def test_ensure_volume_shares_keeps_existing_volume():
    """mootdx/baostock/astock 已有 volume(股) 的帧不动（原帧返回）。"""
    df = _mootdx_df()
    out = _ensure_volume_shares(df, "mootdx")
    assert out is df
    assert out["volume"].iloc[0] == 1e8


def test_ensure_volume_shares_non_tushare_factor_1():
    """非 tushare 源只有 vol 无 volume 时按股处理（factor=1，防御兜底）。"""
    df = _ensure_volume_shares(pd.DataFrame({"close": [1.0], "vol": [100.0]}),
                               "baostock")
    assert df["volume"].iloc[0] == 100.0


def test_ensure_volume_shares_no_vol_column_passthrough():
    """既无 volume 也无 vol 的帧原样返回（不造列）。"""
    df = pd.DataFrame({"close": [1.0]})
    assert _ensure_volume_shares(df, "tushare") is df
    assert _ensure_volume_shares(None, "tushare") is None


def test_preload_daily_normalizes_volume_units(tmp_path):
    """接入点 1：preload_daily 后 tushare 帧带 volume(股)、mootdx 帧不变。"""
    dm = _make_dm(tmp_path)
    dm.cache.put("daily", "tushare_510300.XSHG", _tushare_df())
    dm.cache.put("daily", "mootdx_512800.XSHG", _mootdx_df())
    dm.preload_daily()
    ts = dm._daily_mem["get_daily_510300.XSHG"]
    assert ts["volume"].iloc[0] == 8e6 * 100
    mt = dm._daily_mem["get_daily_512800.XSHG"]
    assert mt["volume"].iloc[0] == 1e8


def test_fetch_get_daily_normalizes_volume(tmp_path, monkeypatch):
    """接入点 2：fetch get_daily 分支在线回源帧同样归一 volume。"""
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)))
    fake = _tushare_df()
    monkeypatch.setattr(dm.sources["tushare"], "get_daily",
                        lambda *a, **k: fake.copy())
    df = dm.fetch("get_daily", CODE, "2026-07-08", "2026-07-10")
    assert df["volume"].iloc[0] == 8e6 * 100
    assert df["money"].iloc[-1] == 4039507.185 * 1000  # money 口径不受影响


def test_build_money_full_peek_normalizes_volume(tmp_path, monkeypatch):
    """接入点 3：_build_money_full 的 peek 兜底路径经过 volume 归一。"""
    dm = _make_dm(tmp_path)
    dm.cache.put("daily", f"tushare_{CODE}", _tushare_df())
    calls = []
    orig = manager_mod._ensure_volume_shares
    def _spy(df, src):
        calls.append(src)
        return orig(df, src)
    monkeypatch.setattr(manager_mod, "_ensure_volume_shares", _spy)
    out = dm.get_daily_money_cached([CODE], DATES[-1], count=1)
    assert "tushare" in calls  # peek 兜底调用了 volume 归一
    assert out["money"].iloc[0] == 4039507.185 * 1000


# ---------------------------------------------------------------------------
# 2. _build_money_full peek 兜底顺序复用 _priority()
# ---------------------------------------------------------------------------

def test_build_money_full_peek_follows_priority(tmp_path):
    """同一代码多源缓存：money 帧按 _priority() 顺序取（默认 tushare 优先）。

    旧硬编码顺序 ("mootdx","astock","tushare") 会取到 mootdx 帧，与 preload
    选出的价格帧（tushare 前复权）口径矛盾。
    """
    dm = _make_dm(tmp_path)
    mo = _mootdx_df()
    mo["trade_date"] = [d.strftime("%Y-%m-%d") for d in DATES]
    mo["money"] = [111.0, 111.0, 111.0]  # mootdx 标记值（元）
    dm.cache.put("daily", f"mootdx_{CODE}", mo)
    ts = _tushare_df()  # amount 千元 → money = amount×1000
    dm.cache.put("daily", f"tushare_{CODE}", ts)
    out = dm.get_daily_money_cached([CODE], DATES[-1], count=1)
    # _priority() 默认 tushare 在前 → 取 tushare 帧（旧实现取 mootdx 的 111）
    assert out["money"].iloc[0] == 4039507.185 * 1000


def test_build_money_full_peek_falls_to_next_source(tmp_path):
    """首优先级源无缓存时按 _priority() 顺序落到下一源。"""
    dm = _make_dm(tmp_path)
    mo = _mootdx_df()
    mo["trade_date"] = [d.strftime("%Y-%m-%d") for d in DATES]
    mo["money"] = [111.0, 111.0, 111.0]
    dm.cache.put("daily", f"mootdx_{CODE}", mo)
    out = dm.get_daily_money_cached([CODE], DATES[-1], count=1)
    assert out["money"].iloc[0] == 111.0  # tushare 无缓存 → mootdx


# ---------------------------------------------------------------------------
# 3. baostock 5 分钟 adjustflag 前复权 + attrs
# ---------------------------------------------------------------------------

class _FakeRS:
    """baostock query_history_k_data_plus 返回结果集的合成替身。"""

    def __init__(self, fields, rows, err="0"):
        self.error_code = err
        self.fields = fields
        self._rows = rows
        self._i = -1

    def next(self):
        self._i += 1
        return self._i < len(self._rows)

    def get_row_data(self):
        return self._rows[self._i]


def _install_fake_baostock(monkeypatch, rs):
    captured = {}

    def _query(symbol, fields, start_date=None, end_date=None,
               frequency=None, adjustflag=None):
        captured["adjustflag"] = adjustflag
        captured["frequency"] = frequency
        return rs

    fake = types.SimpleNamespace(query_history_k_data_plus=_query)
    monkeypatch.setitem(sys.modules, "baostock", fake)
    return captured


def test_baostock_5min_uses_qfq_adjustflag(monkeypatch):
    """5 分钟取数 adjustflag 由 "2"(后复权) 改为 "1"(前复权)，与日线一致。"""
    fields = ["date", "time", "open", "high", "low", "close", "volume", "amount"]
    rows = [["2026-01-05", "20260105093500000", "1.0", "1.1", "0.9", "1.05",
             "5000", "5250"]]
    rs = _FakeRS(fields, rows)
    src = BaostockSource()
    src._logged_in = True  # 跳过 login（离线）
    captured = _install_fake_baostock(monkeypatch, rs)
    df = src.get_5min(CODE, "2026-01-05", "2026-01-05")
    assert captured["adjustflag"] == "1"
    assert df.attrs["source"] == "baostock"
    assert df.attrs["adj"] == "qfq"
    assert df["close"].iloc[0] == 1.05  # 数值化生效


def test_baostock_daily_qfq_and_attrs(monkeypatch):
    """日线 adjustflag="1" 保持不变，返回帧带 qfq 口径元数据。"""
    fields = ["date", "open", "high", "low", "close", "volume", "amount"]
    rows = [["2026-01-05", "1.0", "1.1", "0.9", "1.05", "5000", "5250"]]
    rs = _FakeRS(fields, rows)
    src = BaostockSource()
    src._logged_in = True
    captured = _install_fake_baostock(monkeypatch, rs)
    df = src.get_daily(CODE, "20260105", "20260105")
    assert captured["adjustflag"] == "1"
    assert df.attrs["source"] == "baostock"
    assert df.attrs["adj"] == "qfq"
    assert df["trade_date"].iloc[0] == "2026-01-05"


# ---------------------------------------------------------------------------
# 4. 复权口径元数据 + _pick_daily_frame 混源防护
# ---------------------------------------------------------------------------

def test_detect_adj_qfq_chain_ok():
    """pre_close 链与 pct_chg 一致 → 复权生效，标 qfq。"""
    df = pd.DataFrame({
        "close": [10.0, 9.0, 9.9], "pre_close": [10.0, 10.0, 9.0],
        "pct_chg": [0.0, -10.0, 10.0]})
    assert tushare_src._detect_adj(df, "fund_daily") == "qfq"
    assert tushare_src._detect_adj(df, "daily") == "qfq"


def test_detect_adj_broken_chain_marks_raw():
    """除权跳变未修正（链与 pct_chg 显著偏离）→ 因子缺失，保守标 raw。"""
    # 除权日真实涨跌幅 +0.5%，但未复权 close/pre_close 链为 -10%
    df = pd.DataFrame({
        "close": [10.0, 9.0], "pre_close": [10.0, 10.0],
        "pct_chg": [0.0, 0.5]})
    assert tushare_src._detect_adj(df, "fund_daily") == "raw"


def test_detect_adj_index_is_raw():
    """index_daily 无复权参数，指数无复权概念 → raw。"""
    df = pd.DataFrame({
        "close": [10.0, 9.0], "pre_close": [10.0, 10.0],
        "pct_chg": [0.0, -10.0]})
    assert tushare_src._detect_adj(df, "index_daily") == "raw"


def test_detect_adj_missing_columns_defaults_qfq():
    """缺 pre_close/pct_chg 列无法校验 → 按声明口径 qfq。"""
    df = pd.DataFrame({"close": [10.0, 9.0]})
    assert tushare_src._detect_adj(df, "fund_daily") == "qfq"


def test_tushare_get_daily_marks_attrs():
    """tushare get_daily 返回帧带 source/adj 元数据（fund_daily adj=qfq）。"""
    src = tushare_src.TushareSource(token="x")
    resp = pd.DataFrame({
        "trade_date": ["20260708", "20260709"],
        "open": [4.8, 4.85], "high": [4.9, 4.9], "low": [4.75, 4.8],
        "close": [4.85, 4.83], "pre_close": [4.8, 4.85],
        "pct_chg": [1.0417, -0.4124], "vol": [8e6, 8.2e6],
        "amount": [3.9e6, 4.0e6],
    })

    class _Pro:
        def fund_daily(self, **kw):
            assert kw.get("adj") == "qfq"
            return resp

        def index_daily(self, **kw):
            return None

        def daily(self, **kw):
            return None

    src._pro = _Pro()
    df = src.get_daily(CODE, "2026-07-08", "2026-07-09")
    assert df.attrs["source"] == "tushare"
    assert df.attrs["adj"] == "qfq"


def test_infer_adj_fallback_by_source_name():
    """旧缓存帧无 attrs：按源名推断（tushare/baostock=qfq，mootdx=raw，
    其他=unknown）；有 attrs 时以 attrs 为准。"""
    df = pd.DataFrame({"close": [1.0]})
    assert _infer_adj(df, "tushare") == "qfq"
    assert _infer_adj(df, "mootdx") == "raw"
    assert _infer_adj(df, "baostock") == "qfq"
    assert _infer_adj(df, "astock") == "unknown"
    df.attrs["adj"] = "hfq"
    assert _infer_adj(df, "tushare") == "hfq"  # attrs 优先


def test_pick_daily_frame_prefers_qfq_over_raw():
    """高优先级 raw 帧让位低优先级 qfq 帧（mootdx raw vs tushare qfq）。"""
    raw = _mootdx_df(close=1.0)
    qfq = _tushare_df()
    qfq.attrs["adj"] = "qfq"
    cands = [(0, "mootdx", "mootdx_A", raw), (1, "tushare", "tushare_A", qfq)]
    picked = _pick_daily_frame(cands)
    assert picked[2] == "tushare_A"


def test_pick_daily_frame_unknown_beats_raw():
    """unknown（astock 复权口径不可判定）视为非 raw，优先于确定的 raw。"""
    raw = _mootdx_df(close=1.0)
    unk = pd.DataFrame({"close": [3.0]})
    unk.attrs["adj"] = "unknown"
    cands = [(0, "mootdx", "mootdx_A", raw), (1, "astock", "astock_A", unk)]
    assert _pick_daily_frame(cands)[2] == "astock_A"


def test_pick_daily_frame_all_raw_uses_first_raw():
    """全 raw 时按优先级取第一个 raw。"""
    r1 = _mootdx_df(close=1.0)
    r2 = _mootdx_df(close=2.0)
    cands = [(0, "mootdx", "mootdx_A", r1), (1, "mootdx", "mootdx_B", r2)]
    assert _pick_daily_frame(cands)[2] == "mootdx_A"
    assert _pick_daily_frame([]) is None


def test_preload_daily_prefers_qfq_frame(tmp_path, monkeypatch):
    """preload 混源防护：mootdx(raw) 优先级更高时仍选 tushare(qfq) 帧。"""
    monkeypatch.setitem(CONFIG, "DATASOURCE_PRIORITY",
                        ["mootdx", "tushare", "astock"])
    dm = _make_dm(tmp_path)
    raw = _mootdx_df(close=1.0)  # 无 attrs → mootdx 键推断 raw（兼容旧缓存）
    qfq = _tushare_df()          # close 4.8x，与 raw 帧区分
    qfq.attrs["adj"] = "qfq"
    dm.cache.put("daily", f"mootdx_{CODE}", raw)
    dm.cache.put("daily", f"tushare_{CODE}", qfq)
    dm.preload_daily()
    df = dm._daily_mem[f"get_daily_{CODE}"]
    assert df["close"].iloc[0] == 4.85  # 选中 tushare qfq 帧而非 mootdx raw


def test_preload_daily_all_raw_uses_priority(tmp_path, monkeypatch):
    """全部源均为 raw 时按优先级选第一个（mootdx 优先于 astock unknown? 否——
    unknown 非 raw；此处两帧均显式标 raw 验证全 raw 分支）。"""
    monkeypatch.setitem(CONFIG, "DATASOURCE_PRIORITY",
                        ["mootdx", "tushare", "astock"])
    dm = _make_dm(tmp_path)
    r1 = _mootdx_df(close=1.0)
    r1.attrs["adj"] = "raw"
    r2 = _tushare_df()
    r2.attrs["adj"] = "raw"  # tushare 因子缺失被 _detect_adj 标 raw 的情形
    dm.cache.put("daily", f"mootdx_{CODE}", r1)
    dm.cache.put("daily", f"tushare_{CODE}", r2)
    dm.preload_daily()
    df = dm._daily_mem[f"get_daily_{CODE}"]
    assert df["close"].iloc[0] == 1.0  # 全 raw → 优先级最高的 mootdx


# ---------------------------------------------------------------------------
# 5. mootdx xdxr 前复权换算
# ---------------------------------------------------------------------------

def _mootdx_price_df():
    """合成 mootdx 日线：01-07 除权（10派10元），除权后 10→9 元。"""
    idx = pd.to_datetime(["2026-01-05", "2026-01-06",
                          "2026-01-07", "2026-01-08"])
    df = pd.DataFrame({
        "open": [10.0, 10.0, 9.0, 9.0],
        "high": [10.0, 10.0, 9.0, 9.0],
        "low": [10.0, 10.0, 9.0, 9.0],
        "close": [10.0, 10.0, 9.0, 9.0],
        "vol": [1.0, 1.0, 2.0, 2.0], "amount": [1.0, 1.0, 2.0, 2.0],
    }, index=idx)
    df.attrs["source"] = "mootdx"
    df.attrs["adj"] = "raw"
    return df


def _xdxr_row(day, **kw):
    row = {"year": 2026, "month": 1, "day": day, "category": 1,
           "fenhong": 0.0, "peigujia": 0.0, "songzhuangu": 0.0, "peigu": 0.0}
    row.update(kw)
    return row


def test_mootdx_qfq_dividend_factor():
    """10派10元(每股1元)，昨收10 → 除权价9，除权日前价格 ×0.9。"""
    src = mootdx_src.MootdxSource()
    src._xdxr_cache["510300"] = [_xdxr_row(7, fenhong=10.0)]
    out = src._to_qfq(_mootdx_price_df(), "510300")
    assert out is not None and out.attrs["adj"] == "qfq"
    assert list(out["close"]) == [9.0, 9.0, 9.0, 9.0]
    # 只调价格：vol/amount 保持原始（与聚宽 fq="pre" 口径一致）
    assert list(out["vol"]) == [1.0, 1.0, 2.0, 2.0]
    assert out.attrs["source"] == "mootdx"  # copy 后 attrs 保留


def test_mootdx_qfq_split_factor():
    """10送10(每股送转1股)，昨收10 → 除权价5，除权日前价格 ×0.5。"""
    src = mootdx_src.MootdxSource()
    src._xdxr_cache["510300"] = [_xdxr_row(7, songzhuangu=10.0)]
    out = src._to_qfq(_mootdx_price_df(), "510300")
    assert out is not None
    assert list(out["close"]) == [5.0, 5.0, 9.0, 9.0]


def test_mootdx_qfq_multiple_events_compound():
    """多次除权因子累乘：01-06 与 01-08 各 10派10 → 首日价格 ×0.9×f2。"""
    idx = pd.to_datetime(["2026-01-05", "2026-01-06",
                          "2026-01-07", "2026-01-08"])
    df = pd.DataFrame({
        "open": [10.0] * 4, "high": [10.0] * 4, "low": [10.0] * 4,
        "close": [10.0, 9.0, 9.0, 8.1],
    }, index=idx)
    df.attrs["adj"] = "raw"
    src = mootdx_src.MootdxSource()
    src._xdxr_cache["510300"] = [_xdxr_row(6, fenhong=10.0),
                                 _xdxr_row(8, fenhong=10.0)]
    out = src._to_qfq(df, "510300")
    assert out is not None
    # 01-06 因子 0.9（昨收10派1）；01-08 因子 (9-1)/9（昨收9派1）
    f1, f2 = 0.9, 8.0 / 9.0
    assert abs(out["close"].iloc[0] - 10.0 * f1 * f2) < 1e-9
    assert abs(out["close"].iloc[1] - 9.0 * f2) < 1e-9
    assert abs(out["close"].iloc[2] - 9.0 * f2) < 1e-9
    assert abs(out["close"].iloc[3] - 8.1) < 1e-9  # 最新价不动


def test_mootdx_qfq_no_xdxr_keeps_raw():
    """xdxr 无记录/查询失败 → 返回 None，get_daily 保持 raw 口径。"""
    src = mootdx_src.MootdxSource()
    src._xdxr_cache["510300"] = []
    assert src._to_qfq(_mootdx_price_df(), "510300") is None
    src._xdxr_cache["510300"] = None
    assert src._to_qfq(_mootdx_price_df(), "510300") is None


def test_mootdx_qfq_ignores_non_category1():
    """category!=1（股本变动等不影响价格的类别）不参与换算。"""
    src = mootdx_src.MootdxSource()
    src._xdxr_cache["510300"] = [_xdxr_row(7, category=5)]
    assert src._to_qfq(_mootdx_price_df(), "510300") is None


def test_mootdx_qfq_skips_event_before_frame_start():
    """除权日早于帧内首个交易日（前收不在帧内）→ 该因子跳过；无可用
    因子时整体保持 raw。"""
    src = mootdx_src.MootdxSource()
    src._xdxr_cache["510300"] = [_xdxr_row(1, fenhong=10.0)]  # 2026-01-01
    assert src._to_qfq(_mootdx_price_df(), "510300") is None


def test_mootdx_get_daily_integration_qfq(monkeypatch):
    """get_daily 集成：bars + xdxr 均成功 → 返回 qfq 帧（attrs 标注）。"""
    src = mootdx_src.MootdxSource()
    bars_df = _mootdx_price_df().drop(columns=[])  # 含 vol/amount
    bars_df.attrs = {}

    class _Client:
        class client:
            @staticmethod
            def get_xdxr_info(market, code):
                assert market == 1 and code == "510300"  # SH 市场
                return [_xdxr_row(7, fenhong=10.0)]

        @staticmethod
        def bars(symbol, frequency):
            return bars_df.copy()

    monkeypatch.setattr(src, "_with_server_retry",
                        lambda fn, empty_ok=False: (fn(_Client()), None))
    df = src.get_daily(CODE, "2026-01-05", "2026-01-08")
    assert df.attrs["source"] == "mootdx"
    assert df.attrs["adj"] == "qfq"
    assert list(df["close"]) == [9.0, 9.0, 9.0, 9.0]
    # vol→volume ×100（源内既有口径）不受复权影响
    assert df["volume"].iloc[0] == 100.0


def test_mootdx_get_daily_xdxr_failure_keeps_raw(monkeypatch):
    """get_daily 集成：xdxr 查询异常 → 保持 raw 帧并标注，不中断取数。"""
    src = mootdx_src.MootdxSource()
    bars_df = _mootdx_price_df()
    bars_df.attrs = {}

    class _Client:
        class client:
            @staticmethod
            def get_xdxr_info(market, code):
                raise RuntimeError("xdxr boom")

        @staticmethod
        def bars(symbol, frequency):
            return bars_df.copy()

    monkeypatch.setattr(src, "_with_server_retry",
                        lambda fn, empty_ok=False: (fn(_Client()), None))
    df = src.get_daily(CODE, "2026-01-05", "2026-01-08")
    assert df.attrs["adj"] == "raw"
    assert list(df["close"]) == [10.0, 10.0, 9.0, 9.0]  # 原始价不变


def test_mootdx_get_daily_index_marks_raw(monkeypatch):
    """指数（000xxx.SH）无复权概念：不尝试 xdxr，直接标 raw。"""
    src = mootdx_src.MootdxSource()
    bars_df = _mootdx_price_df()
    bars_df.attrs = {}

    class _Client:
        @staticmethod
        def index_bars(symbol, frequency):
            return bars_df.copy()

    monkeypatch.setattr(src, "_with_server_retry",
                        lambda fn, empty_ok=False: (fn(_Client()), None))
    df = src.get_daily("000300.XSHG", "2026-01-05", "2026-01-08")
    assert df.attrs["source"] == "mootdx"
    assert df.attrs["adj"] == "raw"


# ---------------------------------------------------------------------------
# 6. astock 字符串帧数值化 + attrs
# ---------------------------------------------------------------------------

def _install_fake_baidu(monkeypatch, keys, rows):
    monkeypatch.setattr(astock_src.skill, "baidu_kline_with_ma",
                        lambda sym, start_time: {"keys": keys, "rows": rows})


def test_astock_get_daily_numeric_conversion(monkeypatch):
    """baidu kline 原始字符串帧数值化：OHLC/volume/amount 转 float，
    date 列解析为 Timestamp，attrs 标注 source/adj。"""
    keys = ["date", "open", "high", "low", "close", "volume", "amount"]
    rows = ["2026-01-05,1.0,1.1,0.9,1.05,50000000,52500000",
            "2026-01-06,1.05,1.2,1.0,1.15,51000000,58000000",
            ""]  # 尾部空行被过滤
    _install_fake_baidu(monkeypatch, keys, rows)
    df = astock_src.AStockSource().get_daily(CODE, "2026-01-01", "2026-01-31")
    assert len(df) == 2
    assert df["close"].dtype == float
    assert df["volume"].iloc[0] == 5e7  # volume 实测为股，数值化即可
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert df["date"].iloc[0] == pd.Timestamp("2026-01-05")
    assert df.attrs["source"] == "astock"
    assert df.attrs["adj"] == "unknown"


def test_astock_get_daily_bad_cells_become_nan(monkeypatch):
    """无法解析的单元格置 NaN，不整帧失败。"""
    keys = ["date", "open", "close"]
    rows = ["2026-01-05,abc,1.05"]
    _install_fake_baidu(monkeypatch, keys, rows)
    df = astock_src.AStockSource().get_daily(CODE, "2026-01-01", "2026-01-31")
    assert pd.isna(df["open"].iloc[0])
    assert df["close"].iloc[0] == 1.05


def test_astock_get_daily_empty_raises(monkeypatch):
    """无数据仍抛 DataSourceError（由上层降级，不造伪数据）。"""
    _install_fake_baidu(monkeypatch, ["date", "close"], ["", "  "])
    with pytest.raises(DataSourceError):
        astock_src.AStockSource().get_daily(CODE, "2026-01-01", "2026-01-31")
