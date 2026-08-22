"""stockdata scheduler 00:00 全量缺失巡检测试。"""
import threading


class _FakeLock:
    """_sync_lock() 惰性解析的桩：call() 返回自身作上下文管理器。"""

    def __init__(self):
        self._lock = threading.Lock()

    def __call__(self):
        return self

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *exc):
        self._lock.release()
        return False


def test_midnight_scan_loop_triggers_full_scan(monkeypatch):
    import threading

    from app.services.stockdata import scheduler as sched

    fired = {"n": 0}
    monkeypatch.setattr(sched, "_sync_lock", lambda: _FakeLock())
    monkeypatch.setattr(sched, "_lock", threading.Lock())
    monkeypatch.setattr(sched, "_scheduler_state", {"last_full_scan": None, "full_scan_result": None})
    monkeypatch.setattr(sched, "_stop", threading.Event())
    monkeypatch.setattr(
        sched, "_dt",
        type("DT", (), {
            "datetime": type("DT2", (), {
                "now": staticmethod(lambda: __import__("datetime").datetime(2026, 8, 6, 0, 0, 10)),
            }),
            "date": __import__("datetime").date,
            "time": __import__("datetime").time,
        })())

    import app.services.mootdx_service as ms
    def _fake_full():
        fired["n"] += 1
        return {"missing": {}, "backfilled": {}, "errors": []}
    monkeypatch.setattr(ms, "scan_and_backfill_full", _fake_full)

    assert hasattr(sched, "_run_full_scan_once")
    sched._run_full_scan_once()
    assert fired["n"] == 1


def test_run_full_scan_records_completed_date(monkeypatch):
    """00:00 巡检完成后必须记录 full_scan_date，供 watchdog 判"已完成"。"""
    import threading

    from app.services.stockdata import scheduler as sched

    state = {"last_full_scan": None, "full_scan_result": None,
             "full_scan_date": None}
    monkeypatch.setattr(sched, "_sync_lock", lambda: _FakeLock())
    monkeypatch.setattr(sched, "_lock", threading.Lock())
    monkeypatch.setattr(sched, "_scheduler_state", state)
    monkeypatch.setattr(sched, "_stop", threading.Event())
    monkeypatch.setattr(
        sched, "_dt",
        type("DT", (), {
            "datetime": type("DT2", (), {
                "now": staticmethod(lambda: __import__("datetime").datetime(2026, 8, 6, 0, 0, 10)),
            }),
            "date": type("D3", (), {"today": staticmethod(lambda: __import__("datetime").date(2026, 8, 6))}),
            "time": __import__("datetime").time,
        })())

    import app.services.mootdx_service as ms
    monkeypatch.setattr(ms, "scan_and_backfill_full",
                        lambda: {"missing": {}, "backfilled": {}, "errors": []})

    sched._run_full_scan_once()
    assert state["full_scan_date"] == "2026-08-06"


def test_full_scan_started_today_logic():
    """watchdog 判定：full_scan_started 等于今天 → 已启动；缺失/昨天 → 未启动。"""
    import datetime as _dt

    from app.services.stockdata import scheduler as sched

    now = _dt.datetime(2026, 8, 6, 0, 5, 0)
    assert sched._full_scan_started_today({"full_scan_started": "2026-08-06"}, now)
    assert not sched._full_scan_started_today({"full_scan_started": "2026-08-05"}, now)
    assert not sched._full_scan_started_today({}, now)
    assert not sched._full_scan_started_today({"full_scan_started": None}, now)


def test_watchdog_warns_when_scan_not_completed(monkeypatch, caplog):
    """watchdog 检测到今日巡检未完成 → 打 WARNING 告警（08-09/08-10 无完成日志的回归）。"""
    import datetime as _dt
    import logging
    import threading

    from app.services.stockdata import scheduler as sched

    state = {"full_scan_date": None}  # 今天未完成
    monkeypatch.setattr(sched, "_scheduler_state", state)
    monkeypatch.setattr(sched, "_stop", threading.Event())
    monkeypatch.setattr(
        sched, "_dt",
        type("DT", (), {
            "datetime": type("DT2", (), {
                "now": staticmethod(lambda: __import__("datetime").datetime(2026, 8, 6, 0, 5, 10)),
            }),
            "date": __import__("datetime").date,
            "time": __import__("datetime").time,
        })())

    with caplog.at_level(logging.WARNING, logger="app.services.stockdata.scheduler"):
        # 手动执行一次 watchdog 判定体（单次迭代，不跑无限循环）
        now = _dt.datetime(2026, 8, 6, 0, 5, 10)
        assert not sched._full_scan_started_today(state, now)
        sched._warn_if_full_scan_incomplete(state, now)
    assert any("NOT started today" in r.message for r in caplog.records), \
        f"应告警巡检未完成，实际: {[r.message for r in caplog.records]}"


def test_watchdog_silent_when_scan_started_but_blocked(monkeypatch, caplog):
    """watchdog：今日已启动（full_scan_started==today）但被 _sync_lock 阻塞 → 不误告警。

    若 scan 已启动线程等待 _sync_lock，00:05 时尚未完成是正常的（15:35 sync
    长任务可能占用锁），不能当作"巡检缺席"告警。只有今天从未启动才告警。
    """
    import datetime as _dt
    import logging
    import threading

    from app.services.stockdata import scheduler as sched

    state = {"full_scan_started": "2026-08-06", "full_scan_date": None}
    monkeypatch.setattr(sched, "_scheduler_state", state)
    monkeypatch.setattr(sched, "_stop", threading.Event())

    with caplog.at_level(logging.WARNING, logger="app.services.stockdata.scheduler"):
        now = _dt.datetime(2026, 8, 6, 0, 5, 10)
        sched._warn_if_full_scan_incomplete(state, now)
    assert not any("NOT started today" in r.message for r in caplog.records), \
        f"不应告警（已启动仅被锁阻塞），实际: {[r.message for r in caplog.records]}"


def test_watchdog_silent_when_scan_completed(monkeypatch, caplog):
    """watchdog：今日巡检已完成 → 不告警。"""
    import datetime as _dt
    import logging
    import threading

    from app.services.stockdata import scheduler as sched

    state = {"full_scan_started": "2026-08-06", "full_scan_date": "2026-08-06"}  # 已完成
    monkeypatch.setattr(sched, "_scheduler_state", state)
    monkeypatch.setattr(sched, "_stop", threading.Event())

    with caplog.at_level(logging.WARNING, logger="app.services.stockdata.scheduler"):
        now = _dt.datetime(2026, 8, 6, 0, 5, 10)
        sched._warn_if_full_scan_incomplete(state, now)
    assert not any("NOT started today" in r.message for r in caplog.records), \
        f"不应告警，实际: {[r.message for r in caplog.records]}"
