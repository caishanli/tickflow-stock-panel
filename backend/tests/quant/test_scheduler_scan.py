"""stockdata scheduler 00:00 全量缺失巡检测试。"""


def test_midnight_scan_loop_triggers_full_scan(monkeypatch):
    import threading
    from app.services.stockdata import scheduler as sched

    fired = {"n": 0}
    monkeypatch.setattr(sched, "_sync_lock", threading.Lock())
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
