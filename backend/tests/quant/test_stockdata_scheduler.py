"""stockdata scheduler：_run_sync 收盘后全量落盘路径。"""
import datetime as _dt
import threading

from app.services.stockdata import scheduler as sch


def test_run_sync_full_stock_minute_syncs_daily_and_index_daily(monkeypatch):
    """15:35 收盘路径(full_stock_minute=True)应同步当天日线 + 指数日线 +
    全量股票分钟(limit=None),而非增量慢跑。
    """
    calls = {}

    from app.services import mootdx_service

    def fake_etf_minute():
        return 100

    def fake_adj():
        return {"written_symbols": 0, "rows": 0, "total_symbols": 0}

    def fake_stock_minute(limit=None):
        calls["stock_limit"] = limit
        return 200

    def fake_nav():
        return 3

    def fake_daily(day):
        calls["daily_day"] = day
        return {"stock": 1, "etf": 1}

    def fake_index_daily(day):
        calls["index_day"] = day
        return {"written": 1}

    monkeypatch.setattr(sch, "_sync_lock", threading.Lock())
    monkeypatch.setattr(sch, "_lock", threading.Lock())
    monkeypatch.setattr(mootdx_service, "sync_etf_minute", fake_etf_minute)
    monkeypatch.setattr(mootdx_service, "sync_adj_factor", fake_adj)
    monkeypatch.setattr(mootdx_service, "sync_stock_minute", fake_stock_minute)
    monkeypatch.setattr(mootdx_service, "sync_daily", fake_daily)
    monkeypatch.setattr(mootdx_service, "sync_index_daily", fake_index_daily)

    from app.services import etf_nav_service
    monkeypatch.setattr(etf_nav_service, "sync_etf_nav", fake_nav)

    sch._run_sync(full_stock_minute=True)

    assert calls["stock_limit"] is None, "收盘后应全量拉股票分钟(limit=None)"
    assert calls["daily_day"] == _dt.date.today()
    assert calls["index_day"] == _dt.date.today()
    st = sch._scheduler_state["sync_result"]
    assert st["daily"] == {"stock": 1, "etf": 1}
    assert st["index_daily"] == {"written": 1}


def test_run_sync_incremental_keeps_limit(monkeypatch):
    """手动 trigger 路径(默认 full_stock_minute=False)保持增量慢跑且不落日线。"""
    calls = {}

    from app.services import mootdx_service

    def fake_stock_minute(limit=None):
        calls["stock_limit"] = limit
        return 0

    monkeypatch.setattr(sch, "_sync_lock", threading.Lock())
    monkeypatch.setattr(sch, "_lock", threading.Lock())
    monkeypatch.setattr(mootdx_service, "sync_etf_minute", lambda: 0)
    monkeypatch.setattr(mootdx_service, "sync_adj_factor", lambda: {})
    monkeypatch.setattr(mootdx_service, "sync_stock_minute", fake_stock_minute)
    monkeypatch.setattr(mootdx_service, "sync_daily", lambda day: (_ for _ in ()).throw(
        AssertionError("手动增量路径不应落日线")))
    monkeypatch.setattr(mootdx_service, "sync_index_daily", lambda day: (_ for _ in ()).throw(
        AssertionError("手动增量路径不应落指数日线")))

    from app.services import etf_nav_service
    monkeypatch.setattr(etf_nav_service, "sync_etf_nav", lambda: 0)

    sch._run_sync(full_stock_minute=False)

    assert calls["stock_limit"] == mootdx_service.STOCK_MINUTE_BATCH_LIMIT
