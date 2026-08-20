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
        self._last_close_override = None

    def set_last_close(self, close):
        """覆盖最后一个交易日 close（check_limit/get_snapshot 断言用）。
        默认末两日 close 为 [10.0, 10.5]：按昨收 10.0 + 主板 10% 分档，
        high_limit=round(10×1.1,2)=11.0、low_limit=round(10×0.9,2)=9.0。"""
        self._last_close_override = close

    def fetch(self, name, *a, **kw):
        if name == "get_daily":
            idx = pd.date_range("2026-06-01", "2026-07-09", freq="B")
            close = np.linspace(10, 11, len(idx))
            close[-2] = 10.0
            close[-1] = 10.5
            if self._last_close_override is not None:
                close[-1] = self._last_close_override
            # 真实数据层无 high_limit/low_limit/preclose 列：check_limit/get_snapshot
            # 必须按昨收+分档幅度自行推导涨跌停价，不得依赖本帧伪列。
            return pd.DataFrame({"close": close,
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


def test_check_limit_flat_when_close_between_limits(pt_ctx):
    mgr = ptrade_api._state["manager"]
    mgr.set_last_close(10.5)
    res = ptrade_api.check_limit("510300.SS")
    assert res["510300.SS"] == 0


def test_check_limit_limit_up(pt_ctx):
    mgr = ptrade_api._state["manager"]
    mgr.set_last_close(11.0)
    res = ptrade_api.check_limit("510300.SS")
    assert res["510300.SS"] == 1


def test_check_limit_limit_down(pt_ctx):
    mgr = ptrade_api._state["manager"]
    mgr.set_last_close(9.0)
    res = ptrade_api.check_limit("510300.SS")
    assert res["510300.SS"] == -1


def test_get_snapshot_populates_daily_fields(pt_ctx):
    mgr = ptrade_api._state["manager"]
    mgr.set_last_close(10.5)
    snap = ptrade_api.get_snapshot("510300.SS")["510300.SS"]
    assert snap["last_px"] == pytest.approx(10.5)
    assert snap["up_px"] == pytest.approx(11.0)
    assert snap["down_px"] == pytest.approx(9.0)
    assert snap["preclose_px"] == pytest.approx(10.0)


def test_get_price_start_date_end_date_filter_rows(pt_ctx):
    df = ptrade_api.get_price(
        "510300.SS", start_date="2026-07-01", end_date="2026-07-09",
        count=100, frequency="1d", fields=["close"])
    assert isinstance(df, pd.DataFrame) and len(df) > 0
    assert (df.index >= pd.Timestamp("2026-07-01")).all()
    assert (df.index < pd.Timestamp("2026-07-09")).all()

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


# ---------------------------------------------------------------------------
# rqalpha ptradecompat 引擎：check_limit/get_snapshot 按昨收+分档幅度计算
# ---------------------------------------------------------------------------
@pytest.fixture
def _ptradecompat(monkeypatch):
    """最小 rqalpha 桩：让 ptradecompat 可导入并运行（复用 test_ptradecompat 手法）。"""
    import sys
    import types
    rq = types.ModuleType("rqalpha")
    rq.__path__ = []  # namespace package，允许 from rqalpha.xxx import
    rq.api = types.ModuleType("rqalpha.api")
    rq.api.register_api = lambda *a, **k: None
    rq.core = types.ModuleType("rqalpha.core")
    rq.core.events = types.ModuleType("rqalpha.core.events")
    rq.core.events.EVENT = types.SimpleNamespace(BAR="bar", BEFORE_TRADING="bt", AFTER_TRADING="at")
    rq.const = types.ModuleType("rqalpha.const")
    rq.const.INSTRUMENT_TYPE = rq.const.MARKET = rq.const.TRADING_CALENDAR_TYPE = types.SimpleNamespace()
    rq.environment = types.ModuleType("rqalpha.environment")
    rq.environment.Environment = type("Env", (), {"get_instance": staticmethod(lambda: None)})
    rq.interface = types.ModuleType("rqalpha.interface")
    rq.interface.AbstractMod = type("AbstractMod", (), {})
    rq.model = types.ModuleType("rqalpha.model")
    rq.model.instrument = types.ModuleType("rqalpha.model.instrument")
    rq.model.instrument.Instrument = object
    monkeypatch.setitem(sys.modules, "rqalpha", rq)
    for _name, _mod in (
            ("rqalpha.api", rq.api), ("rqalpha.core", rq.core),
            ("rqalpha.core.events", rq.core.events), ("rqalpha.const", rq.const),
            ("rqalpha.environment", rq.environment), ("rqalpha.interface", rq.interface),
            ("rqalpha.model", rq.model), ("rqalpha.model.instrument", rq.model.instrument)):
        monkeypatch.setitem(sys.modules, _name, _mod)
    import app.quant.ptradecompat as pc
    return pc


def _ptradecompat_bars(closes):
    """单标的日线 bars：末两日 close 由调用方给定（昨收=closes[-2]，今收=closes[-1]）。"""
    days = ["20260708093000", "20260709093000"]
    arr = np.zeros(len(closes), dtype=np.dtype([("datetime", "S14"), ("close", "f8")]))
    for i, c in enumerate(closes):
        arr["datetime"][i] = days[i]
        arr["close"][i] = c
    return {"510300.XSHG": arr}


def test_ptradecompat_check_limit_flat_between_limits(_ptradecompat, monkeypatch):
    pc = _ptradecompat
    monkeypatch.setattr(pc, "_history_bars_batch",
                        lambda *a, **k: _ptradecompat_bars([10.0, 10.5]))
    res = pc.check_limit("510300.SS")
    assert res["510300.SS"] == 0


def test_ptradecompat_check_limit_limit_up(_ptradecompat, monkeypatch):
    pc = _ptradecompat
    monkeypatch.setattr(pc, "_history_bars_batch",
                        lambda *a, **k: _ptradecompat_bars([10.0, 11.0]))
    res = pc.check_limit("510300.SS")
    assert res["510300.SS"] == 1


def test_ptradecompat_check_limit_limit_down(_ptradecompat, monkeypatch):
    pc = _ptradecompat
    monkeypatch.setattr(pc, "_history_bars_batch",
                        lambda *a, **k: _ptradecompat_bars([10.0, 9.0]))
    res = pc.check_limit("510300.SS")
    assert res["510300.SS"] == -1


def test_ptradecompat_get_snapshot_computed(_ptradecompat, monkeypatch):
    pc = _ptradecompat
    monkeypatch.setattr(pc, "_history_bars_batch",
                        lambda *a, **k: _ptradecompat_bars([10.0, 10.5]))
    snap = pc.get_snapshot("510300.SS")["510300.SS"]
    assert snap["last_px"] == pytest.approx(10.5)
    assert snap["up_px"] == pytest.approx(11.0)
    assert snap["down_px"] == pytest.approx(9.0)
    assert snap["preclose_px"] == pytest.approx(10.0)


def test_ptradecompat_get_price_start_end_filter(_ptradecompat, monkeypatch):
    """get_price 透传 end_date 并按 start_date 过滤（真实-ish recarray 形状）。"""
    pc = _ptradecompat
    bars = {
        "510300.XSHG": np.array(
            [(20260701093000, 1.0), (20260702093000, 1.1),
             (20260703093000, 1.2), (20260706093000, 1.3)],
            dtype=[("datetime", "int64"), ("close", "f8")]),
    }
    monkeypatch.setattr(pc, "_history_bars_batch", lambda *a, **k: bars)
    df = pc.get_price("510300.SS", start_date="2026-07-02", end_date="2026-07-06",
                      count=100, frequency="1d", fields=["close"])
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["close"]
    assert (df.index >= pd.Timestamp("2026-07-02")).all()
    assert (df.index.normalize() <= pd.Timestamp("2026-07-06")).all()
