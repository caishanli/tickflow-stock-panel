"""新浪交易日历解码器 + 校验单测（零网络，payload 为 2026-09-13 实测原文内联）。"""

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
    p = tmp_path / "cal.json"
    p.write_text(json.dumps({"dates": ["2026-09-10", "2026-09-11"],
                             "fetched_at": "2026-09-13T00:00:00",
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


def test_check_drift_none_is_error():
    r = tc.check_drift("2026-09-10", "2026-09-10", None)
    assert r == {"ok": False, "level": "error", "reason": r["reason"]}
    r = tc.check_drift(None, "2026-09-10", "2026-09-10")
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
