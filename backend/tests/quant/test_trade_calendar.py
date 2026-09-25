"""新浪交易日历解码器 + 校验单测（零网络，payload 为 2026-09-13 实测原文内联）。"""

import contextlib
import json
from datetime import date, timedelta

import pytest

from app.services import trade_calendar as tc

SINA_PAYLOAD_20260913 = 'LC/AAApNDXCw6mHbaPgkryxXv10eAJP1LW0SD39aT7+NV44Xba3PxCgTdrp5BkYVAc11hWvg0c/19UAc7jNtHQyWBAu2xmGuZI1NVAc3FepphjnTBw1X4hmGu+ypVAcvFenpBXPqCc6F4ZmGueLFwbIN8QTDXPsCc1FepphjvOoCc8FepphjvcgFO3CP00wxXXWhrkUdZrIJpw9X3ThrlEp6hlGc88Kcem0VeFpZM46VV4MrTC2KScKc811U4aLXUdlzINc9lTrwFW3T52KPj0mDueVFuUR1RtiEoCXfdgFOOSGRXnUhrXWhb0kt6Rk2pU44JV4SrTyU9wSDHPwCnXdP1FuiUM44r7qwdKqcYrIZpw1DqgrlU5IrHRawxjrwBaqcbrIt9gr3UhDtOpyVNjEnCHPnC3royNWvi0gjHXBXYdRlLbFpdJFueSFcqkK30sSDO+68K46IVOwVkaBX/'


def test_decode_real_payload():
    dates = tc.decode_sina_calendar(SINA_PAYLOAD_20260913)
    assert len(dates) == 8797  # 新浪原始 8796 + 补 1992-05-04
    assert dates[0] == "1990-12-19"
    assert dates[-1] == "2026-12-31"
    assert "1992-05-04" in dates
    assert "2026-09-10" in dates and "2026-09-11" in dates
    assert "2026-09-12" not in dates and "2026-10-01" not in dates


def test_decode_bad_payload_raises():
    with pytest.raises(ValueError):
        tc.decode_sina_calendar("!!!not-base64!!!___")


def test_validate_rejects():
    with pytest.raises(ValueError):
        tc.validate_calendar(["2026-09-11", "2026-09-10"])  # 非单调
    with pytest.raises(ValueError):
        tc.validate_calendar(["2020-01-02"], today=date(2026, 9, 13))  # 太短且过期


# --- Task 2: 抓取 + 探测 + 对账 + 钉钉周频上限（全部 monkeypatch，零真实网络） ---


def _write_cal(path, dates, fetched_at="2026-09-13T00:00:00", last_push=None):
    path.write_text(json.dumps({"dates": dates, "fetched_at": fetched_at,
                                "source": "sina", "meta": {"last_stale_push": last_push}}))


def test_refresh_uses_cache_when_fresh(tmp_path, monkeypatch):
    # fetched_at 取当天（相对日期）：缓存是否新鲜取决于与今天的差值，
    # 写死历史日期会随时间推移变 stale，导致本该 no-op 的用例去触网。
    p = tmp_path / "cal.json"
    p.write_text(json.dumps({"dates": ["2026-09-10", "2026-09-11"],
                             "fetched_at": date.today().isoformat() + "T00:00:00",
                             "source": "sina", "meta": {"last_stale_push": None}}))
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    called = []
    monkeypatch.setattr("requests.get", lambda *a, **k: called.append(1) or (_ for _ in ()).throw(AssertionError("must not fetch")))
    out = tc.refresh_calendar()
    assert out == {"ok": True, "reason": "fresh", "path": str(p), "count": 2}
    assert called == []


def test_check_drift_matrix():
    r = tc.check_drift("2026-09-09", "2026-09-10", "2026-09-10")
    assert r == {"ok": False, "level": "error", "reason": "引擎日历末端2026-09-09落后权威2026-09-10"}
    r = tc.check_drift("2026-09-10", "2026-09-10", "2026-09-09")
    assert r["ok"] is True and r["level"] == "warn"
    r = tc.check_drift("2026-09-10", "2026-09-10", "2026-09-10")
    assert r["ok"] is True and r["level"] == "ok"
    # 引擎超前权威（=权威文件陈旧）只报警告，不得判成"落后"
    r = tc.check_drift("2026-12-31", "2026-09-14", "2026-09-11")
    assert r["ok"] is True and r["level"] == "warn"
    assert "超前" in r["reason"]


def test_check_drift_probe_missing_skips_leg():
    """探测不可用（腾讯抖动/未启网络）时跳过 probe 腿，不得整体判错。"""
    assert tc.check_drift("2026-09-10", "2026-09-10")["level"] == "ok"
    assert tc.check_drift("2026-09-10", "2026-09-10", None)["level"] == "ok"
    assert tc.check_drift("2026-09-09", "2026-09-10", None)["ok"] is False


def test_check_drift_none_is_error():
    assert tc.check_drift("2026-09-10", "2026-09-10", None)["level"] == "ok"
    r = tc.check_drift(None, "2026-09-10", "2026-09-10")
    assert r["ok"] is False and r["level"] == "error"
    r = tc.check_drift("2026-09-10", None, "2026-09-10")
    assert r["ok"] is False and r["level"] == "error"


def test_load_calendar_file_rejects(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    with pytest.raises(LookupError):
        tc.load_calendar_file(str(bad))
    nonmono = tmp_path / "nm.json"
    nonmono.write_text(json.dumps({"dates": ["2026-09-11", "2026-09-10"],
                                   "fetched_at": "2026-09-13T00:00:00",
                                   "source": "sina", "meta": {}}))
    with pytest.raises(LookupError):
        tc.load_calendar_file(str(nonmono))
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"dates": [], "fetched_at": "x", "source": "sina"}))
    with pytest.raises(LookupError):
        tc.load_calendar_file(str(empty))
    with pytest.raises(LookupError):
        tc.load_calendar_file(str(tmp_path / "missing.json"))


def test_load_authoritative_dates_missing_both(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(tmp_path / "nope.json"))
    monkeypatch.setattr(tc, "_seed_path", lambda: str(tmp_path / "no-seed.json"))
    with pytest.raises(LookupError):
        tc.load_authoritative_dates()


def test_load_authoritative_dates_from_file(tmp_path, monkeypatch):
    p = tmp_path / "cal.json"
    _write_cal(p, ["2026-09-10", "2026-09-11"])
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    dates, origin = tc.load_authoritative_dates()
    assert dates == ["2026-09-10", "2026-09-11"] and origin == "file"


def _fake_resp(text=None, payload=None, status_exc=None):
    class R:
        def raise_for_status(self):
            if status_exc is not None:
                raise status_exc
        @property
        def text(self):
            return text
        def json(self):
            return payload
    return R()


def test_fetch_sina_payload_ok(monkeypatch):
    monkeypatch.setattr("requests.get",
                        lambda *a, **k: _fake_resp(text='foo var datelist="PAYLOAD123" bar'))
    assert tc.fetch_sina_payload() == "PAYLOAD123"


def test_fetch_sina_payload_retries_then_raises(monkeypatch):
    calls = []
    def _fail(*a, **k):
        calls.append(1)
        raise ConnectionError("down")
    monkeypatch.setattr("requests.get", _fail)
    monkeypatch.setattr(tc.time, "sleep", lambda s: None)
    with pytest.raises(ValueError):
        tc.fetch_sina_payload()
    assert len(calls) == tc._MAX_RETRIES


def test_fetch_sina_payload_no_datelist(monkeypatch):
    monkeypatch.setattr("requests.get", lambda *a, **k: _fake_resp(text="no marker here"))
    monkeypatch.setattr(tc.time, "sleep", lambda s: None)
    with pytest.raises(ValueError):
        tc.fetch_sina_payload()


def test_probe_recent_anchor_ok(monkeypatch):
    payload = {"data": {"sh000001": {"day": [["2026-09-10", 1], ["2026-09-11", 2]]}}}
    monkeypatch.setattr("requests.get", lambda *a, **k: _fake_resp(payload=payload))
    assert tc.probe_recent_anchor() == "2026-09-11"


def test_probe_recent_anchor_never_raises(monkeypatch):
    def _boom(*a, **k):
        raise ConnectionError("down")
    monkeypatch.setattr("requests.get", _boom)
    assert tc.probe_recent_anchor() is None
    monkeypatch.setattr("requests.get", lambda *a, **k: _fake_resp(payload={"data": {}}))
    assert tc.probe_recent_anchor() is None


def test_refresh_force_refetch_and_save(tmp_path, monkeypatch):
    p = tmp_path / "cal.json"
    _write_cal(p, ["2026-09-10"], fetched_at="2020-01-01T00:00:00")
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    monkeypatch.setattr(tc, "fetch_sina_payload", lambda: SINA_PAYLOAD_20260913)
    out = tc.refresh_calendar(force=True)
    assert out["ok"] is True and out["reason"] == "refreshed"
    assert out["path"] == str(p) and out["count"] == 8797
    data = json.loads(p.read_text())
    assert data["dates"][-1] == "2026-12-31" and data["meta"] == {"last_stale_push": None}


def test_refresh_failure_never_raises(tmp_path, monkeypatch):
    p = tmp_path / "cal.json"
    _write_cal(p, ["2026-09-10", "2026-09-11"])
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    def _boom():
        raise ConnectionError("down")
    monkeypatch.setattr(tc, "fetch_sina_payload", _boom)
    out = tc.refresh_calendar(force=True)
    assert out == {"ok": False, "reason": out["reason"], "path": str(p), "count": 2}
    assert json.loads(p.read_text())["dates"] == ["2026-09-10", "2026-09-11"]


def test_refresh_failure_alert_skipped_while_file_still_fresh(tmp_path, monkeypatch):
    """网络抖动不该天天告警：文件仍新鲜时刷新失败只记日志，不推钉钉。"""
    p = tmp_path / "cal.json"
    _write_cal(p, ["2026-09-10"], fetched_at=date.today().isoformat() + "T00:00:00")
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    monkeypatch.setattr(tc, "fetch_sina_payload",
                        lambda: (_ for _ in ()).throw(ConnectionError("down")))
    sent = []
    monkeypatch.setattr(tc, "push_stale_alert", lambda t: sent.append(t) or True)
    tc.refresh_calendar(force=True)
    assert sent == []


def test_refresh_failure_alerts_when_file_stale(tmp_path, monkeypatch):
    """设计 §6「任何降级都不静默」：文件已过期且刷新失败 → 必须告警。"""
    p = tmp_path / "cal.json"
    old = (date.today() - timedelta(days=30)).isoformat()
    _write_cal(p, ["2026-09-10"], fetched_at=old + "T00:00:00")
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    monkeypatch.setattr(tc, "fetch_sina_payload",
                        lambda: (_ for _ in ()).throw(ConnectionError("down")))
    sent = []
    monkeypatch.setattr(tc, "push_stale_alert", lambda t: sent.append(t) or True)
    tc.refresh_calendar(force=True)
    assert len(sent) == 1 and "过期" in sent[0]


def test_refresh_failure_alerts_when_file_missing(tmp_path, monkeypatch):
    """无可用日历文件（全新部署）也算降级，必须告警。"""
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(tmp_path / "missing.json"))
    monkeypatch.setattr(tc, "fetch_sina_payload",
                        lambda: (_ for _ in ()).throw(ConnectionError("down")))
    sent = []
    monkeypatch.setattr(tc, "push_stale_alert", lambda t: sent.append(t) or True)
    tc.refresh_calendar(force=True)
    assert len(sent) == 1


def test_save_preserves_last_stale_push(tmp_path):
    p = tmp_path / "cal.json"
    _write_cal(p, ["2026-09-10"], last_push="2026-09-07")
    tc.save_calendar_file(str(p), ["2026-09-10", "2026-09-11"])
    data = json.loads(p.read_text())
    assert data["dates"] == ["2026-09-10", "2026-09-11"]
    assert data["meta"] == {"last_stale_push": "2026-09-07"}
    assert data["source"] == "sina" and data["fetched_at"]


def test_last_trading_day():
    dates = ["2026-09-09", "2026-09-10", "2026-09-11"]
    assert tc.last_trading_day(dates, "2026-09-10") == "2026-09-10"
    assert tc.last_trading_day(dates, "2026-09-12") == "2026-09-11"
    assert tc.last_trading_day(dates, date(2026, 9, 8)) is None
    assert tc.last_trading_day([], "2026-09-10") is None


def test_refresh_calendar_path_failure_never_raises(monkeypatch):
    def _boom():
        raise RuntimeError("CONFIG exploded")
    monkeypatch.setattr(tc, "calendar_path", _boom)
    out = tc.refresh_calendar()
    assert out == {"ok": False, "reason": "CONFIG exploded", "path": "", "count": 0}


def test_push_stale_alert_no_config(tmp_path, monkeypatch):
    p = tmp_path / "cal.json"
    _write_cal(p, ["2026-09-10"])
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    monkeypatch.setattr("app.quant.db.get_quant_setting", lambda k: None)
    assert tc.push_stale_alert("stale") is False


def test_push_stale_alert_same_week_skips(tmp_path, monkeypatch):
    p = tmp_path / "cal.json"
    _write_cal(p, ["2026-09-10"], last_push=date.today().isoformat())
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    monkeypatch.setattr("app.quant.db.get_quant_setting",
                        lambda k: "http://hook" if k == "dingtalk_webhook_url" else "")
    called = []
    monkeypatch.setattr("app.quant.notify.send_dingtalk",
                        lambda *a, **k: called.append(1) or True)
    assert tc.push_stale_alert("stale") is False
    assert called == []


def test_push_stale_alert_sends_and_marks(tmp_path, monkeypatch):
    p = tmp_path / "cal.json"
    old_week = (date.today() - timedelta(days=7)).isoformat()
    _write_cal(p, ["2026-09-10"], last_push=old_week)
    monkeypatch.setenv("TRADE_CALENDAR_PATH", str(p))
    monkeypatch.setattr("app.quant.db.get_quant_setting",
                        lambda k: "http://hook" if k == "dingtalk_webhook_url" else "")
    sent = []
    monkeypatch.setattr("app.quant.notify.send_dingtalk",
                        lambda *a, **k: sent.append(a) or True)
    assert tc.push_stale_alert("stale text") is True
    assert len(sent) == 1 and sent[0][3] == "stale text"
    assert json.loads(p.read_text())["meta"] == {"last_stale_push": date.today().isoformat()}


# --- Task 4: 引擎接线（_CalendarStore.extend，零网络） ---


def test_calendar_store_extend():
    import pandas as pd

    from app.quant.jqcompat import _CalendarStore
    s = _CalendarStore([pd.Timestamp("2026-09-09").date()])
    assert s.extend(["2026-09-10", "2026-09-09"]) == 1
    assert s.get_trading_calendar()[-1].date().isoformat() == "2026-09-10"


# --- Task 5: runner 守卫 + 调度刷新钩子 ---


def test_refresh_never_raises(monkeypatch):
    import app.services.trade_calendar as m
    monkeypatch.setattr(m, "fetch_sina_payload", lambda: (_ for _ in ()).throw(ConnectionError("down")))
    out = m.refresh_calendar(force=True)
    assert out["ok"] is False and "down" in out["reason"]


def test_scheduler_hooks_calendar(monkeypatch):
    """收盘同步后必须刷新日历，且刷新失败不得中断同步（非阻断契约）。"""
    from app.services.stockdata import scheduler

    calls = []

    def _boom():
        calls.append(1)
        raise ConnectionError("sina down")

    monkeypatch.setattr("app.services.trade_calendar.refresh_calendar", _boom)
    monkeypatch.setattr(scheduler, "_mark_active", lambda *a, **k: None)
    monkeypatch.setattr(scheduler, "_mark_idle", lambda *a, **k: None)
    monkeypatch.setattr(scheduler, "_trim_memory", lambda *a, **k: None)
    monkeypatch.setattr(scheduler, "_sync_lock", contextlib.nullcontext)
    import app.services.mootdx_service as ms
    for name in ("sync_etf_minute", "sync_adj_factor", "sync_stock_minute",
                 "sync_daily", "sync_index_daily", "STOCK_MINUTE_BATCH_LIMIT"):
        monkeypatch.setattr(ms, name, (lambda *a, **k: {}) if name[0] == "s" else 1,
                            raising=False)
    monkeypatch.setattr("app.services.etf_nav_service.sync_etf_nav", lambda *a, **k: {})
    monkeypatch.setattr("app.services.tencent_minute.fill_recent_gaps", lambda *a, **k: None)
    monkeypatch.setattr("app.services.alt_daily.fill_recent_gaps_daily", lambda *a, **k: None)
    scheduler._run_sync(full_stock_minute=True)
    assert calls == [1]  # 刷新被调用；异常被吞，同步未中断
