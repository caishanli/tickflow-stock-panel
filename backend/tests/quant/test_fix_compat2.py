"""quant 兼容层/桥接第二轮修复的单元测试（不跑 rqalpha 事件循环）。

覆盖：
- fq 新语义：本地日线统一前复权，pre/qfq/None 不警告；none/raw/post/hfq
  每标的 warning 一次。
- 涨跌停幅度分档 _limit_rate：科创/创业 20cm（68/58/30/159）、ST 5%、
  主板 10%；日线/分钟/bridge 三处 recarray 同口径。
- is_st_stock（名称含 "ST"）与 is_suspended（日线 bar 缺失/volume==0；
  分钟缓存已加载时当天无分钟数据也判停牌），JqDataSource 与
  QuantRQAlphaDataSource 双侧。
- ETF 名录快照：新鲜快照直接用（不联网）、过期+离线不联网、在线刷新原子
  写回、网络失败回退旧快照、无快照回退缓存派生、缓存并集并入。
"""
import datetime as _dt
import json
import logging

import numpy as np
import pandas as pd
import pytest

from app.quant import jqcompat as jq
from app.quant import rqalpha_bridge as bridge


@pytest.fixture(autouse=True)
def _reset_fq_warned():
    jq._FQ_WARNED.clear()
    yield
    jq._FQ_WARNED.clear()


# ---------------------------------------------------------------------------
# 合成数据与 fake provider/DataManager（与 test_fix_bridge_unit.py 同风格）
# ---------------------------------------------------------------------------
def _daily_df(closes=(10.0, 11.0, 12.0), volumes=None, start="2024-01-02"):
    n = len(closes)
    dates = pd.date_range(start, periods=n, freq="B")
    volumes = volumes if volumes is not None else [1000.0] * n
    return pd.DataFrame({
        "date": dates,
        "open": closes,
        "high": [c * 1.02 for c in closes],
        "low": [c * 0.98 for c in closes],
        "close": closes,
        "volume": volumes,
    })


class _FakeCache:
    def __init__(self, df, code="510300.XSHG"):
        self._df = df
        self._code = code

    def get_all(self, kind):
        if kind == "daily":
            return {"astock_" + self._code: self._df}
        return {}


class _FakeDM:
    """JqDataSource 需要的最小 DataManager 鸭子类型。"""

    def __init__(self, df, minute_df=None, code="510300.XSHG"):
        self.cache = _FakeCache(df, code)
        self._df = df
        self._minute_df = minute_df
        self._code = code

    def fetch(self, kind, code, start, end):
        # 只认识构造时给的标的；未知 code 返回 None（模拟无数据）
        return self._df if code == self._code else None

    def get_minute_feed(self, code, start, end):
        return self._minute_df if code == self._code else None


def _minute_df(day="2024-01-02"):
    idx = pd.to_datetime([f"{day} 09:31", f"{day} 09:32", f"{day} 10:00"])
    return pd.DataFrame({
        "open": [11.0, 11.1, 11.2],
        "high": [11.1, 11.2, 11.3],
        "low": [10.9, 11.0, 11.1],
        "close": [11.05, 11.15, 11.25],
        "volume": [100.0, 100.0, 100.0],
        "money": [1105.0, 1115.0, 1125.0],
    }, index=idx)


def _make_jq_ds(df=None, minute_df=None, code="510300.XSHG"):
    df = df if df is not None else _daily_df()
    return jq.JqDataSource(_FakeDM(df, minute_df, code), [code],
                           "2024-01-02", "2024-01-05")


class _BundleLikeProvider:
    """QuantRQAlphaDataSource 需要的最小 provider（无 cache 属性）。"""

    def __init__(self, df):
        self._df = df

    def get_daily(self, code, start, end):
        return self._df


def _make_bridge_ds(df=None, code="600000.XSHG"):
    df = df if df is not None else _daily_df()
    return bridge.QuantRQAlphaDataSource(
        _BundleLikeProvider(df), bridge.CONFIG,
        {"symbols": [code], "start": "2024-01-02", "end": "2024-01-05"})


# ---------------------------------------------------------------------------
# fq 新语义：本地日线统一前复权
# ---------------------------------------------------------------------------
def test_fq_pre_qfq_none_no_warning(caplog):
    """fq="pre"/"qfq"/None：前复权口径已满足，不警告。"""
    with caplog.at_level(logging.WARNING, logger="jqcompat"):
        jq._check_fq("pre", ["510300.XSHG"])
        jq._check_fq("qfq", ["510300.XSHG"])
        jq._check_fq(None, ["510300.XSHG"])
        jq._check_fq("PRE", ["510300.XSHG"])  # 大小写不敏感
    assert not [r for r in caplog.records if "复权" in r.message]


@pytest.mark.parametrize("fq", ["none", "raw", "post", "hfq"])
def test_fq_unsupported_warns_once_per_code(caplog, fq):
    """fq=none/raw/post/hfq：本地只有前复权、不支持该口径，每标的警告一次。"""
    with caplog.at_level(logging.WARNING, logger="jqcompat"):
        jq._check_fq(fq, ["510300.XSHG"])
        jq._check_fq(fq, ["510300.XSHG"])  # 重复请求不再提示
        jq._check_fq(fq, ["159915.XSHE"])
    warns = [r for r in caplog.records if "复权" in r.message]
    assert len(warns) == 2
    assert {r.message.split(":")[0] for r in warns} == {"510300.XSHG", "159915.XSHE"}
    assert all("前复权" in r.message for r in warns)  # 明确告知本地口径


# ---------------------------------------------------------------------------
# 涨跌停幅度分档
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("code", [
    "688001.XSHG",  # 科创板股票
    "588000.XSHG",  # 科创板 ETF
    "300001.XSHE",  # 创业板股票
    "159915.XSHE",  # 创业板 ETF
])
def test_limit_rate_20cm(code):
    assert jq._limit_rate(code) == 0.20


def test_limit_rate_main_board_10pct():
    assert jq._limit_rate("510300.XSHG") == 0.10
    assert jq._limit_rate("600000.XSHG") == 0.10
    assert jq._limit_rate("000001.XSHE") == 0.10


def test_limit_rate_st_5pct_by_name():
    assert jq._limit_rate("600001.XSHG", name="ST测试") == 0.05
    assert jq._limit_rate("600001.XSHG", name="*ST测试") == 0.05
    # 取不到名称时按非 ST（不制造虚假 5% 档）
    assert jq._limit_rate("600001.XSHG", name="沪深300ETF") == 0.10
    assert jq._limit_rate("600001.XSHG", name=None) == 0.10


def test_limit_rate_st_from_names_map(monkeypatch):
    """name 缺省时取 jqcompat._NAMES 映射判定 ST。"""
    monkeypatch.setitem(jq._NAMES, "600002.XSHG", "ST某某")
    assert jq._limit_rate("600002.XSHG") == 0.05


def test_daily_recarray_limit_20cm():
    arr = jq._daily_to_recarray(_daily_df(), code="588000.XSHG")
    assert np.isnan(arr["limit_up"][0])  # 首日无前收
    assert arr["limit_up"][1] == pytest.approx(round(10.0 * 1.2, 2))
    assert arr["limit_down"][1] == pytest.approx(round(10.0 * 0.8, 2))


def test_daily_recarray_limit_st_5pct(monkeypatch):
    monkeypatch.setitem(jq._NAMES, "600001.XSHG", "ST测试")
    arr = jq._daily_to_recarray(_daily_df(), code="600001.XSHG")
    assert arr["limit_up"][1] == pytest.approx(round(10.0 * 1.05, 2))
    assert arr["limit_down"][1] == pytest.approx(round(10.0 * 0.95, 2))


def test_minute_recarray_limit_20cm():
    prev_close_map = {20240102: 10.0}
    arr = jq._minute_to_recarray(
        _minute_df("2024-01-02"), prev_close_map=prev_close_map, code="159915.XSHE")
    assert arr["limit_up"][0] == pytest.approx(12.0)
    assert arr["limit_down"][0] == pytest.approx(8.0)


def test_bridge_recarray_limit_20cm():
    arr = bridge.QuantRQAlphaDataSource._df_to_recarray(
        _daily_df(), code="159915.XSHE")
    assert np.isnan(arr["limit_up"][0])
    assert arr["limit_up"][1] == pytest.approx(12.0)
    assert arr["limit_down"][1] == pytest.approx(8.0)


def test_bridge_recarray_limit_default_10pct():
    """code 缺省（旧调用方式）保持主板 10% 兜底。"""
    arr = bridge.QuantRQAlphaDataSource._df_to_recarray(_daily_df())
    assert arr["limit_up"][1] == pytest.approx(11.0)
    assert arr["limit_down"][1] == pytest.approx(9.0)


# ---------------------------------------------------------------------------
# is_st_stock / is_suspended
# ---------------------------------------------------------------------------
def test_jq_is_st_stock_by_name(monkeypatch):
    ds = _make_jq_ds()
    monkeypatch.setitem(jq._NAMES, "510300.XSHG", "*ST测试")
    assert ds.is_st_stock("510300.XSHG", [pd.Timestamp("2024-01-02")]) == [True]
    # 取不到名称按非 ST
    assert ds.is_st_stock("999999.XSHG", [pd.Timestamp("2024-01-02")]) == [False]


def test_bridge_is_st_stock_by_name(monkeypatch):
    ds = _make_bridge_ds()
    monkeypatch.setitem(jq._NAMES, "600000.XSHG", "ST测试")
    dates = [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")]
    assert ds.is_st_stock("600000.XSHG", dates) == [True, True]
    assert ds.is_st_stock("600111.XSHG", dates) == [False, False]


def test_jq_is_suspended_daily_missing_or_zero_volume():
    # 2024-01-03 volume==0；2024-01-06 无日线 bar
    df = _daily_df(closes=(10.0, 11.0, 12.0), volumes=[1000.0, 0.0, 1000.0])
    ds = _make_jq_ds(df)
    dates = [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03"),
             pd.Timestamp("2024-01-06")]
    assert ds.is_suspended("510300.XSHG", dates) == [False, True, True]
    # 完全无数据的标的：逐日均停牌
    assert ds.is_suspended("999999.XSHG", dates) == [True, True, True]


def test_jq_is_suspended_minute_no_data_counts(monkeypatch):
    """分钟缓存已加载时，当天无任何分钟 bar 也视为停牌（日线有量也不行）。"""
    df = _daily_df(closes=(10.0, 11.0, 12.0))  # 三天日线均有量
    ds = _make_jq_ds(df, minute_df=_minute_df("2024-01-02"))
    ds._ensure_minute("510300.XSHG")  # 触发分钟缓存加载（仅 2024-01-02 有分钟数据）
    assert ds.is_suspended("510300.XSHG", [pd.Timestamp("2024-01-02")]) == [False]
    assert ds.is_suspended("510300.XSHG", [pd.Timestamp("2024-01-03")]) == [True]


def test_bridge_is_suspended_daily_missing_or_zero_volume():
    df = _daily_df(closes=(10.0, 11.0, 12.0), volumes=[1000.0, 0.0, 1000.0])
    ds = _make_bridge_ds(df)
    dates = [_dt.date(2024, 1, 2), _dt.date(2024, 1, 3), _dt.date(2024, 1, 6)]
    assert ds.is_suspended("600000.XSHG", dates) == [False, True, True]
    assert ds.is_suspended("999999.XSHG", dates) == [True, True, True]


# ---------------------------------------------------------------------------
# ETF 名录快照
# ---------------------------------------------------------------------------
class _FakeTushareSrc:
    def __init__(self, rows=None, fail=False):
        self._rows = rows or []
        self._fail = fail
        self.calls = 0

    def get_etf_list(self):
        self.calls += 1
        if self._fail:
            raise RuntimeError("network down")
        return self._rows


class _FakeMootdxSrc:
    def __init__(self, names=None, fail=True):
        self._names = names or {}
        self._fail = fail
        self.calls = 0

    def get_stock_names(self):
        self.calls += 1
        if self._fail:
            raise RuntimeError("no tdx")
        return self._names


class _UniverseDM:
    """_load_etf_universe 需要的最小 DataManager 鸭子类型。"""

    def __init__(self, mootdx=None, offline=False, cache_codes=()):
        self._offline = offline
        self._daily_mem = {"get_daily_" + c: object() for c in cache_codes}
        self.sources = {
            "mootdx": mootdx or _FakeMootdxSrc(),
        }


def _write_snapshot(path, codes=("510300.XSHG",), days_ago=0):
    payload = {
        "fetched_at": (_dt.datetime.now() - _dt.timedelta(days=days_ago)).isoformat(),
        "codes": list(codes),
        "names": {c: "ETF-" + c.split(".")[0] for c in codes},
        "list_dates": {c: ["2020-01-01", "2999-12-31"] for c in codes},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_snapshot_fresh_used_without_network(tmp_path, monkeypatch):
    """快照存在且 ≤7 天：直接用（在线也不联网），保证可复现。名称经清洗。"""
    snap = tmp_path / "etf_universe_snapshot.json"
    _write_snapshot(snap)
    monkeypatch.setattr(bridge, "_ETF_UNIVERSE_SNAPSHOT", str(snap))
    dm = _UniverseDM(offline=False)
    codes, names, list_dates = bridge._load_etf_universe(dm)
    assert codes == ["510300.XSHG"]
    assert names["510300.XSHG"] == "-510300"
    assert list_dates["510300.XSHG"] == ("2020-01-01", "2999-12-31")


def test_snapshot_stale_falls_back_to_cache(tmp_path, monkeypatch):
    """快照过期：从 _daily_mem 缓存推导 ETF 代码。"""
    snap = tmp_path / "etf_universe_snapshot.json"
    _write_snapshot(snap, days_ago=30)
    monkeypatch.setattr(bridge, "_ETF_UNIVERSE_SNAPSHOT", str(snap))
    dm = _UniverseDM(cache_codes=["510300.XSHG"])
    codes, names, list_dates = bridge._load_etf_universe(dm)
    assert codes == ["510300.XSHG"]


def test_snapshot_missing_falls_back_to_cache(tmp_path, monkeypatch):
    """无快照：从 _daily_mem 缓存推导 ETF 代码。"""
    monkeypatch.setattr(bridge, "_ETF_UNIVERSE_SNAPSHOT",
                        str(tmp_path / "nonexistent.json"))
    dm = _UniverseDM(cache_codes=["589720.XSHG"])
    codes, names, list_dates = bridge._load_etf_universe(dm)
    assert codes == ["589720.XSHG"]


def test_snapshot_stale_cache_derivation_with_names(tmp_path, monkeypatch):
    """快照过期 + 缓存有代码 + mootdx 有名称：从缓存推导并合并名称。"""
    snap = tmp_path / "etf_universe_snapshot.json"
    _write_snapshot(snap, codes=("OLD000.XSHG",), days_ago=30)
    monkeypatch.setattr(bridge, "_ETF_UNIVERSE_SNAPSHOT", str(snap))
    dm = _UniverseDM(mootdx=_FakeMootdxSrc({"510300": "300ETF"}, fail=False),
                     cache_codes=["510300.XSHG"])
    codes, names, list_dates = bridge._load_etf_universe(dm)
    assert codes == ["510300.XSHG"]


def test_snapshot_cache_union_merge(tmp_path, monkeypatch):
    """缓存并集保留：本地日线缓存代码并入宇宙。"""
    snap = tmp_path / "etf_universe_snapshot.json"
    _write_snapshot(snap)
    monkeypatch.setattr(bridge, "_ETF_UNIVERSE_SNAPSHOT", str(snap))
    dm = _UniverseDM(cache_codes=["589720.XSHG"])
    codes, names, list_dates = bridge._load_etf_universe(dm)
    assert set(codes) == {"510300.XSHG", "589720.XSHG"}
    # 缓存并入的代码无 tdx 名称时兜底为代码本身，list_dates 不造数据
    assert names["589720.XSHG"] == "589720.XSHG"
    assert "589720.XSHG" not in list_dates
