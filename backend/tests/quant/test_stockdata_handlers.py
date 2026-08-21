import datetime as _dt
import os

import pytest

from app.services.stockdata.handlers import HANDLERS, _norm_code, handle
from app.services.stockdata.sources import DataSources


@pytest.fixture
def src(tmp_path):
    import polars as pl
    day = _dt.date.today().isoformat()
    d = os.path.join(str(tmp_path), "kline_etf_minute", f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame({
        "symbol": ["512670.SH"], "datetime": [f"{day} 10:00:00"],
        "open": [1.0], "high": [1.1], "low": [0.9], "close": [1.05],
        "volume": [1000], "amount": [1050.0],
    }).write_parquet(os.path.join(d, "part.parquet"))
    return DataSources(data_root=str(tmp_path), mootdx_factory=None)


def test_handlers_registered():
    for m in ("ping", "status", "get_price", "current_snapshot", "preload_daily",
              "get_minute", "get_trade_days", "get_all_securities",
              "get_security_info", "get_index_stocks", "get_stock_names",
              "get_adj_factors", "trigger_sync"):
        assert m in HANDLERS, m


def test_get_price_minute(src):
    t, data = handle("get_price", {"security": "512670.XSHG",
                                   "frequency": "1m"}, src)
    assert t == "parquet"
    assert data["close"].to_list() == [1.05]


def test_ping(src):
    t, data = handle("ping", {}, src)
    assert t == "json" and data["pong"] is True


def test_h_status_includes_scheduler_state(monkeypatch):
    """status handler 应返回 scheduler 状态（含待办/最近任务），而非仅基础信息。"""
    from app.services.stockdata import handlers
    fake = {
        "last_backfill": "2026-08-20 10:00:00",
        "backfill_result": {"missing": {"kline_minute": True}},
        "active_tasks": ["backfill"],
        "ts": "2026-08-20 10:05:00",
    }
    monkeypatch.setattr("app.services.stockdata.scheduler.get_status", lambda: fake)
    t, data = handlers.h_status({}, None)
    assert t == "json"
    assert data["last_backfill"] == "2026-08-20 10:00:00"
    assert data["active_tasks"] == ["backfill"]


def test_norm_code_bare_6_digit():
    assert _norm_code("512670") == "512670.XSHG"   # 6 开头 → 沪市
    assert _norm_code("600000") == "600000.XSHG"   # 6 开头 → 沪市
    assert _norm_code("000001") == "000001.XSHE"   # 深市 000001 平安银行
    assert _norm_code("300750") == "300750.XSHE"   # 3 开头 → 深市


def test_norm_code_with_suffix():
    assert _norm_code("512670.SH") == "512670.XSHG"
    assert _norm_code("000300.XSHG") == "000300.XSHG"
    assert _norm_code("000001.SZ") == "000001.XSHE"


def test_h_trigger_sync_passes_params(monkeypatch):
    """trigger_sync handler 应透传 kind 之外的参数（如 day）。"""
    from app.services.stockdata import handlers
    got = {}

    def fake_trigger(kind, **params):
        got["kind"] = kind
        got["params"] = params

    monkeypatch.setattr("app.services.stockdata.scheduler.trigger_sync", fake_trigger)
    out = handlers.h_trigger_sync({"kind": "check_day", "day": "2026-08-05"}, None)
    assert got == {"kind": "check_day", "params": {"day": "2026-08-05"}}
    assert out == ("json", {"ok": True, "kind": "check_day"})
