from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from app.jobs import daily_pipeline as pipeline

TZ = ZoneInfo("Asia/Shanghai")


def _clock(monkeypatch, now):
    monkeypatch.setattr(pipeline, "datetime", SimpleNamespace(now=lambda tz: now))


def _repo(tmp_path):
    return SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))


def _tracked(monkeypatch, outcomes, calls):
    def run(task, label):
        calls.append(label)
        if not outcomes.pop(0):
            return False
        task()
        return True

    monkeypatch.setattr(pipeline, "_run_tracked", run)
    monkeypatch.setattr(pipeline, "run_instruments_sync", lambda repo: {"instruments_rows": 5882})


def test_three_rounds_fire_at_0905_0910_0915_then_next_weekday():
    trigger = pipeline.instruments_schedule_trigger({"hour": 9, "minute": 5})
    now = datetime(2026, 10, 12, 9, 0, tzinfo=TZ)
    previous = None
    hits = []
    for _ in range(4):
        previous = trigger.get_next_fire_time(previous, now)
        hits.append(previous)
        now = previous + timedelta(seconds=1)
    assert hits == [datetime(2026, 10, 12, 9, m, tzinfo=TZ) for m in (5, 10, 15)] + [
        datetime(2026, 10, 13, 9, 5, tzinfo=TZ)]


def test_success_first_round_skips_later_rounds_using_disk_marker(monkeypatch, tmp_path):
    calls = []
    _tracked(monkeypatch, [True], calls)
    repo = _repo(tmp_path)
    for minute in (5, 10, 15):
        _clock(monkeypatch, datetime(2026, 10, 12, 9, minute, tzinfo=TZ))
        pipeline._run_pre_market_instruments(repo)
    assert calls == ["instruments_sync"]
    assert (tmp_path / "user_data/instruments_sync_success.json").exists()


def test_failure_then_success_stops_third_round(monkeypatch, tmp_path):
    calls = []
    _tracked(monkeypatch, [False, True], calls)
    repo = _repo(tmp_path)
    for minute in (5, 10, 15):
        _clock(monkeypatch, datetime(2026, 10, 12, 9, minute, tzinfo=TZ))
        pipeline._run_pre_market_instruments(repo)
    assert len(calls) == 2


def test_all_failures_or_busy_slot_do_not_mark_success(monkeypatch, tmp_path):
    calls = []
    _tracked(monkeypatch, [False, False, False], calls)
    repo = _repo(tmp_path)
    for minute in (5, 10, 15):
        _clock(monkeypatch, datetime(2026, 10, 12, 9, minute, tzinfo=TZ))
        pipeline._run_pre_market_instruments(repo)
    assert len(calls) == 3
    assert not (tmp_path / "user_data/instruments_sync_success.json").exists()


def test_next_day_retries_even_after_previous_day_success(monkeypatch, tmp_path):
    calls = []
    _tracked(monkeypatch, [True, True], calls)
    for day in (12, 13):
        _clock(monkeypatch, datetime(2026, 10, day, 9, 5, tzinfo=TZ))
        pipeline._run_pre_market_instruments(_repo(tmp_path))
    assert len(calls) == 2


def test_running_round_cannot_start_another_round(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline, "_run_tracked", lambda *args: pytest.fail("overlapping round"))
    pipeline._pre_market_instruments_lock.acquire()
    try:
        pipeline._run_pre_market_instruments(_repo(tmp_path))
    finally:
        pipeline._pre_market_instruments_lock.release()


def test_pre_market_sync_requests_strict_completeness(monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(pipeline.instrument_sync, "sync_instruments",
                        lambda path, **kwargs: seen.append(kwargs) or 10)
    monkeypatch.setattr(pipeline, "_refresh_instruments_view", lambda repo: None)
    monkeypatch.setattr(pipeline, "_invalidate", lambda table: None)
    repo = _repo(tmp_path)
    repo.clear_cache = lambda: None
    repo.refresh_cache = lambda: None
    assert pipeline.run_instruments_sync(repo) == {"instruments_rows": 10}
    assert seen == [{"require_complete": True}]


def test_schedule_update_uses_retry_trigger_and_returns_without_scheduler(monkeypatch):
    from app.api import settings as api
    from app.services import preferences

    monkeypatch.setattr(preferences, "set_instruments_schedule", lambda h, m: {"hour": h, "minute": m})
    seen = []
    scheduler = SimpleNamespace(reschedule_job=lambda *args, **kwargs: seen.append((args, kwargs)))
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(scheduler=scheduler)))
    result = api.update_instruments_schedule(api.PipelineScheduleIn(hour=9, minute=5), request)
    assert result == {"hour": 9, "minute": 5}
    assert seen[0][0] == ("pre_market_instruments",)
    assert len(seen[0][1]["trigger"].triggers) == 3
    request.app.state.scheduler = None
    assert api.update_instruments_schedule(api.PipelineScheduleIn(hour=9, minute=5), request) == result
