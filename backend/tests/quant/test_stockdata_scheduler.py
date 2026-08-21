"""stockdata scheduler：_run_sync 收盘后全量落盘路径。"""
import datetime as _dt
import threading

from app.services.stockdata import scheduler as sch


def test_get_status_returns_json_safe_snapshot(monkeypatch):
    """get_status 返回 scheduler 状态快照，date 等非 JSON 类型需转字符串。"""
    monkeypatch.setattr(sch, "_lock", threading.Lock())
    monkeypatch.setattr(sch, "_active_tasks", {"backfill"})
    monkeypatch.setattr(sch, "_scheduler_state", {
        "last_backfill": "2026-08-20 10:00:00",
        "backfill_result": {"missing": {"kline_minute": True},
                            "daily_days": [_dt.date(2026, 8, 19)]},
        "last_sync": None,
    })
    st = sch.get_status()
    # 非 JSON 类型已转字符串
    assert st["backfill_result"]["daily_days"] == ["2026-08-19"]
    assert st["active_tasks"] == ["backfill"]
    assert "ts" in st
    # 全部值可被 json 序列化
    import json
    json.dumps(st)


def test_run_sync_tracks_active_task(monkeypatch):
    """_run_sync 执行期间 active_tasks 含 'sync'，结束后移除。"""
    from app.services import mootdx_service

    monkeypatch.setattr(sch, "_lock", threading.Lock())
    monkeypatch.setattr(sch, "_sync_lock", threading.Lock())
    monkeypatch.setattr(sch, "_scheduler_state", {})
    observed = []

    def fake_stock_minute(limit=None):
        observed.append(sch.get_status()["active_tasks"])
        return 0

    monkeypatch.setattr(mootdx_service, "sync_etf_minute", lambda: 0)
    monkeypatch.setattr(mootdx_service, "sync_adj_factor", lambda: {})
    monkeypatch.setattr(mootdx_service, "sync_stock_minute", fake_stock_minute)

    sch._run_sync()
    assert observed == [["sync"]]
    assert "sync" not in sch.get_status()["active_tasks"]


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


def test_run_check_day_runs_repair(monkeypatch):
    """check_day 后台执行体：解析日期并调 mootdx_service.check_and_repair_day。"""
    from datetime import date as _d
    from app.services import mootdx_service
    calls = []
    monkeypatch.setattr(sch, "_sync_lock", threading.Lock())
    monkeypatch.setattr(sch, "_lock", threading.Lock())
    monkeypatch.setattr(mootdx_service, "check_and_repair_day",
                        lambda day: calls.append(day) or {"day": str(day), "results": {}})
    sch._run_check_day("2026-08-05")
    assert calls == [_d(2026, 8, 5)]
    assert sch._scheduler_state["check_day_result"]["day"] == "2026-08-05"


def test_run_check_full_runs_repair(monkeypatch):
    """check_full 后台执行体：调 check_and_repair_full 并记录汇总。"""
    from app.services import mootdx_service
    calls = []
    monkeypatch.setattr(sch, "_sync_lock", threading.Lock())
    monkeypatch.setattr(sch, "_lock", threading.Lock())
    monkeypatch.setattr(mootdx_service, "check_and_repair_full",
                        lambda content_recent=None: calls.append(content_recent)
                        or {"missing": {}, "backfilled": {}, "errors": []})
    sch._run_check_full()
    assert calls == [None]
    assert sch._scheduler_state["check_full_result"]["errors"] == []


def test_trigger_sync_check_kinds_spawn_thread(monkeypatch):
    """trigger_sync 的 check_day/check_full 走后台线程（start 不阻塞）。"""
    spawned = []

    class _T(threading.Thread):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            spawned.append(k.get("args", ()))

        def start(self):
            pass

    monkeypatch.setattr(threading, "Thread", _T)
    assert sch.trigger_sync("check_day", day="2026-08-05") == {"ok": True}
    assert sch.trigger_sync("check_full") == {"ok": True}
    assert len(spawned) == 2
