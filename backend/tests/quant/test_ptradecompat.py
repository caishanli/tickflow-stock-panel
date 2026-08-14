"""ptradecompat 纯逻辑单测：代码转换、get_history 宽表组装、调度注册、bar_dict 适配。"""
import sys
import types
from datetime import datetime

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def _fake_rqalpha(monkeypatch):
    """最小 rqalpha 桩：让 import 与 register_api 可用。"""
    rq = types.ModuleType("rqalpha")
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
    import app.quant.ptradecompat as pc
    return pc


def test_code_conversion(_fake_rqalpha):
    assert _fake_rqalpha._to_jq("510300.SS") == "510300.XSHG"
    assert _fake_rqalpha._to_jq("159915.SZ") == "159915.XSHE"
    assert _fake_rqalpha._to_pt("510300.XSHG") == "510300.SS"
    assert _fake_rqalpha._to_pt("159915.XSHE") == "159915.SZ"
    assert _fake_rqalpha._to_jq("510300.XSHG") == "510300.XSHG"  # 幂等


def test_get_history_wide(_fake_rqalpha, monkeypatch):
    bars = {
        "510300.XSHG": np.array(
            [(20260101100000, 3.0, 3.1), (20260102100000, 3.2, 3.3)],
            dtype=[("datetime", "int64"), ("close", "f8"), ("volume", "f8")]),
        "159915.XSHE": np.array(
            [(20260101100000, 2.0, 5.0), (20260102100000, 2.1, 5.5)],
            dtype=[("datetime", "int64"), ("close", "f8"), ("volume", "f8")]),
    }
    df = _fake_rqalpha._build_history_wide(bars, ["510300.XSHG", "159915.XSHE"], "close")
    assert list(df.columns) == ["510300.SS", "159915.SZ"]
    assert df.index[0] == pd.Timestamp("2026-01-01 10:00:00")
    assert float(df["510300.SS"].iloc[-1]) == 3.2


def test_run_daily_registers_order(_fake_rqalpha):
    _fake_rqalpha._DAILY_AT.clear()
    calls = []

    def cb(context):
        calls.append(1)

    _fake_rqalpha.run_daily(None, cb, time="13:10")
    assert _fake_rqalpha._DAILY_AT[(13, 10)] == [cb]


def test_adapt_bar_dict(_fake_rqalpha):
    from types import SimpleNamespace
    bd = {"510300.XSHG": SimpleNamespace(open=1.0, high=1.2, low=0.9, close=1.1,
                                          volume=100, total_turnover=1.1e5)}
    out = _fake_rqalpha._ptrade_adapt_bar_dict(bd)
    assert "510300.SS" in out
    assert out["510300.SS"].money == pytest.approx(1.1e5)
    assert out["510300.SS"].price == pytest.approx(1.1)
