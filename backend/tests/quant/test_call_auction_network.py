"""get_call_auction 按需联网抓竞价的接线测试。"""

import datetime
from types import SimpleNamespace

from app.quant import auction_cache
from app.quant.jqengine.engine.jq import api as jq_api

DAY = datetime.date(2026, 10, 12)


def _setup(monkeypatch, tmp_path, calls):
    monkeypatch.setattr(auction_cache, "CACHE_ROOT", str(tmp_path / "ac"))
    auction_cache._memory_cache.clear()
    monkeypatch.setattr(auction_cache, "_now", lambda: datetime.datetime(2026, 10, 12, 9, 26))
    real = auction_cache.ensure_auction

    def _spy(codes, day, now=None):
        calls.append((list(codes), str(day)))
        return real(codes, day, now=datetime.datetime(2026, 10, 12, 9, 26))

    monkeypatch.setattr(auction_cache, "ensure_auction", _spy)
    monkeypatch.setattr(auction_cache, "fetch_tencent_auction", lambda codes: {
        c: {"price": 21.6, "volume_shares": 2305400.0,
            "prev_close": 20.94, "open": 21.6} for c in codes})
    jq_api._state["manager"] = None          # 缓存命中不会走到分钟切片
    jq_api._state["ctx"] = None


def test_cache_miss_today_triggers_network_fill(monkeypatch, tmp_path):
    """今日 + 缓存缺失 → ensure_auction 被调用，且返回抓到的竞价数据。"""
    calls = []
    _setup(monkeypatch, tmp_path, calls)
    out = jq_api.get_call_auction(
        "603906.XSHG", start_date=f"{DAY} 09:15:00",
        end_date=f"{DAY} 09:26:00",
        fields=["time", "volume", "current"])
    assert calls == [(["603906.XSHG"], str(DAY))]
    assert not out.empty
    row = out.iloc[0]
    assert row["volume"] == 2305400.0
    assert row["current"] == 21.6


def test_historical_day_never_fetches(monkeypatch, tmp_path):
    """历史日期不走网络（腾讯现价不是历史竞价）。"""
    calls = []
    _setup(monkeypatch, tmp_path, calls)
    out = jq_api.get_call_auction(
        "603906.XSHG", start_date="2026-10-08 09:15:00", end_date="2026-10-08 09:26:00",
        fields=["time", "volume", "current"])
    assert calls == []
    assert out.empty                      # 无缓存、无当日 bar（manager=None）


def test_second_call_served_from_cache(monkeypatch, tmp_path):
    """抓过一次后同标的再次查询命中缓存，不重复触网。"""
    calls = []
    _setup(monkeypatch, tmp_path, calls)
    today = DAY
    for _ in range(2):
        out = jq_api.get_call_auction(
            "603906.XSHG", start_date=f"{today} 09:15:00", end_date=f"{today} 09:26:00",
            fields=["time", "volume", "current"])
        assert not out.empty and out.iloc[0]["volume"] == 2305400.0
    # ensure 被调两次（每次 get_call_auction 都试），但第二次全部已缓存、
    # 内部 missing 为空直接返回 0，不触网
    assert len(calls) == 2
    assert auction_cache.lookup("603906.XSHG", today) is not None


def test_ensure_failure_does_not_break_lookup(monkeypatch, tmp_path):
    """联网失败不得炸 get_call_auction——静默回退分钟切片路径。"""
    _setup(monkeypatch, tmp_path, [])
    monkeypatch.setattr(auction_cache, "ensure_auction",
                       lambda codes, day, now=None: (_ for _ in ()).throw(
                            RuntimeError("net down")))
    out = jq_api.get_call_auction(
        "603906.XSHG", start_date=f"{DAY} 09:15:00",
        end_date=f"{DAY} 09:26:00",
        fields=["time", "volume", "current"])
    assert out.empty                      # 回退路径也没有数据，但不抛异常


def test_preopen_failure_never_uses_future_minute_bar(monkeypatch, tmp_path):
    import pytest

    _setup(monkeypatch, tmp_path, [])
    monkeypatch.setattr(auction_cache, "ensure_auction",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("offline")))
    monkeypatch.setattr(jq_api, "_minute_day_slice", lambda *a:
                        pytest.fail("live preopen must not read a future minute bar"))
    out = jq_api.get_call_auction("603906.XSHG", start_date=str(DAY), fields=["volume"])
    assert out.empty


def test_delayed_preopen_callback_still_rejects_future_minute(monkeypatch, tmp_path):
    import pytest

    _setup(monkeypatch, tmp_path, [])
    monkeypatch.setattr(auction_cache, "_now",
                        lambda: datetime.datetime(2026, 10, 12, 9, 31, 10))
    jq_api._state["ctx"] = SimpleNamespace(
        current_dt=datetime.datetime(2026, 10, 12, 9, 26))
    monkeypatch.setattr(auction_cache, "ensure_auction", lambda *a, **k: 0)
    monkeypatch.setattr(jq_api, "_minute_day_slice", lambda *a:
                        pytest.fail("delayed live preopen must not read a future minute bar"))
    out = jq_api.get_call_auction("603906.XSHG", start_date=str(DAY), fields=["volume"])
    assert out.empty
