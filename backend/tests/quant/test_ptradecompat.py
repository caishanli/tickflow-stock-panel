"""ptradecompat 纯逻辑单测：代码转换、get_history 宽表组装、调度注册、bar_dict 适配。"""
import sys
import types

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def _fake_rqalpha(monkeypatch):
    """最小 rqalpha 桩：让 import 与 register_api 可用。"""
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


def test_code_conversion_more(_fake_rqalpha):
    pc = _fake_rqalpha
    assert pc._to_jq("511880.SS") == "511880.XSHG"
    assert pc._to_pt("511880.XSHG") == "511880.SS"


def test_position_objects_ptrade_fields(_fake_rqalpha):
    pc = _fake_rqalpha
    from types import SimpleNamespace
    # 模拟 rqalpha PositionProxy 补丁后字段
    proxy = SimpleNamespace(amount=100, enable_amount=100, cost_basis=3.0, last_sale_price=3.1)
    pos = pc._position_view(proxy, "510300.SS")
    assert pos.amount == 100
    assert pos.enable_amount == 100
    assert pos.cost_basis == 3.0
    assert pos.last_sale_price == 3.1


def test_get_trading_day_prev(_fake_rqalpha, monkeypatch):
    pc = _fake_rqalpha
    calls = {}

    def fake_prev(date):
        calls["d"] = date
        return pd.Timestamp("2026-07-17")

    monkeypatch.setattr(pc, "_prev_trading_day", fake_prev)
    assert pc.get_trading_day(-1).strftime("%Y-%m-%d") == "2026-07-17"


def test_set_benchmark_stored(_fake_rqalpha):
    pc = _fake_rqalpha
    pc.set_benchmark("510300.SS")
    assert pc._BENCHMARK == "510300.SS"


def test_get_history_single_code_wide(monkeypatch):
    """单标的 get_history 返回宽表（非 Series），列名=标的码，可 df[code] 取值。"""
    import numpy as np
    import pandas as pd

    def _fake_batch(codes, count, freq, fields, end_dt):
        out = {}
        for c in codes:
            arr = np.zeros(count, dtype=np.dtype([("datetime", "S14"), ("close", "f8")]))
            for i in range(count):
                arr["datetime"][i] = "20260701093000"
                arr["close"][i] = 1.0 + i
            out[c] = arr
        return out

    import app.quant.ptradecompat as pc
    monkeypatch.setattr(pc, "_history_bars_batch", _fake_batch)
    df = pc.get_history(5, "1d", "close", security_list="510300.SS")
    assert isinstance(df, pd.DataFrame), "单标的必须返回 DataFrame"
    assert "510300.SS" in df.columns, "列名必须是标的码"
    assert len(df[df["510300.SS"] > 0]) > 0
