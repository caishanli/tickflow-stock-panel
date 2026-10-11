"""Opening auction must remain valid across delayed quotes and multiple writers."""
import datetime as dt
import importlib.util
import threading
from types import SimpleNamespace

import pandas as pd
import pytest

from app.quant import auction_cache as auction
from app.quant.simulate import runner
from app.services.stockdata import scheduler

DAY = dt.date(2026, 10, 12)
NOW = dt.datetime(2026, 10, 12, 9, 26)


def _payload(stamp, price="10.5", volume="1234"):
    fields = [""] * 60
    fields[3:7] = [price, "10", "10.5", volume]
    fields[30] = stamp
    return ('v_sh600000="' + "~".join(fields) + '";').encode("gbk")


@pytest.mark.parametrize("stamp", ["20261009150000", "20261012092459",
                                  "20261012093000", "", "garbage"])
def test_reject_non_auction_quotes(monkeypatch, stamp):
    monkeypatch.setattr(auction, "_now", lambda: NOW, raising=False)
    monkeypatch.setattr(auction.urllib.request, "urlopen", lambda *a, **k:
                        SimpleNamespace(read=lambda: _payload(stamp), close=lambda: None))
    assert auction.fetch_tencent_auction(["600000.SH"]) == {}


def test_valid_quote_keeps_source_timestamp_and_share_units(monkeypatch):
    monkeypatch.setattr(auction, "_now", lambda: NOW, raising=False)
    monkeypatch.setattr(auction.urllib.request, "urlopen", lambda *a, **k:
                        SimpleNamespace(read=lambda: _payload("20261012092503"),
                                        close=lambda: None))
    row = auction.fetch_tencent_auction(["600000.SH"])["600000.XSHG"]
    assert row["price"] == 10.5
    assert row["volume_shares"] == 123400
    assert row["quote_time"] == "2026-10-12T09:25:03"


def test_batch_does_not_start_after_open(monkeypatch):
    clock = {"now": NOW}
    calls = []
    monkeypatch.setattr(auction, "_now", lambda: clock["now"], raising=False)

    def fetch(*args, **kwargs):
        calls.append(args)
        clock["now"] = NOW.replace(hour=9, minute=30)
        return SimpleNamespace(read=lambda: _payload("20261012093000"), close=lambda: None)

    monkeypatch.setattr(auction.urllib.request, "urlopen", fetch)
    assert auction.fetch_tencent_auction(["600000.SH"] * 56) == {}
    assert len(calls) == 1


@pytest.mark.parametrize("now", [NOW.replace(minute=24), NOW.replace(minute=30),
                                 dt.datetime(2026, 10, 7, 9, 26)])
def test_fetch_and_save_window_and_calendar_guard(monkeypatch, tmp_path, now):
    monkeypatch.setattr(auction, "CACHE_ROOT", str(tmp_path))
    monkeypatch.setattr(auction, "_now", lambda: now, raising=False)
    monkeypatch.setattr(auction, "fetch_tencent_auction", lambda codes:
                        pytest.fail("must not fetch outside today's auction window"))
    assert auction.fetch_and_save(codes=["600000.SH"]).empty


def _row(code):
    return {"symbol": code, "auction_price": 10.5, "auction_volume": 123400.,
            "prev_close": 10., "quote_time": "2026-10-12T09:25:05"}


def _second_reader(root):
    spec = importlib.util.spec_from_file_location("auction_other_process", auction.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.CACHE_ROOT = str(root)
    return module


def test_external_update_invalidates_cache_and_merge_preserves_all(monkeypatch, tmp_path):
    monkeypatch.setattr(auction, "CACHE_ROOT", str(tmp_path))
    auction._memory_cache.clear()
    other = _second_reader(tmp_path)
    auction.save_auction_day(DAY, pd.DataFrame([_row("600000.XSHG")]))
    assert auction.lookup("600000.XSHG", DAY)
    other.save_auction_day(DAY, pd.DataFrame([_row("000001.XSHE")]))
    assert auction.lookup("000001.XSHE", DAY) == (123400., 10.5)
    auction.save_auction_day(DAY, pd.DataFrame([_row("600519.XSHG")]))
    assert set(other.load_day(DAY).symbol) == {"600000.XSHG", "000001.XSHE", "600519.XSHG"}


def test_concurrent_writers_preserve_rows(monkeypatch, tmp_path):
    monkeypatch.setattr(auction, "CACHE_ROOT", str(tmp_path))
    writers = [auction, _second_reader(tmp_path)]
    barrier = threading.Barrier(2)
    errors = []

    def write(module, code):
        try:
            barrier.wait(timeout=3)
            module.save_auction_day(DAY, pd.DataFrame([_row(code)]))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=write, args=(module, code), daemon=True)
               for module, code in zip(writers, ["600000.XSHG", "000001.XSHE"], strict=True)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    assert not errors
    assert set(auction.load_day(DAY).symbol) == {"600000.XSHG", "000001.XSHE"}


def test_partial_collection_retries_only_missing_and_publishes_each_batch(monkeypatch, tmp_path):
    codes = [f"600{i:03}.XSHG" for i in range(56)]
    monkeypatch.setattr(auction, "CACHE_ROOT", str(tmp_path))
    monkeypatch.setattr(auction, "_now", lambda: NOW)
    calls = []

    def fetch(batch):
        calls.append(list(batch))
        if len(calls) == 2:
            # First batch is readable while the last batch is still missing.
            assert len(auction.load_day(DAY)) == 55
            return {}
        return {code: {"price": 10.5, "volume_shares": 123400., "prev_close": 10.,
                       "open": 10.5, "quote_time": "2026-10-12T09:25:05"} for code in batch}

    monkeypatch.setattr(auction, "fetch_tencent_auction", fetch)
    first = auction.fetch_and_save(codes=codes)
    assert len(first) == 55 and first.attrs["complete"] is False
    second = auction.fetch_and_save(codes=codes)
    assert len(second) == 56 and second.attrs["complete"] is True
    assert calls == [codes[:55], codes[55:], codes[55:]]


def test_failed_atomic_publish_preserves_previous_snapshot(monkeypatch, tmp_path):
    monkeypatch.setattr(auction, "CACHE_ROOT", str(tmp_path))
    auction.save_auction_day(DAY, pd.DataFrame([_row("600000.XSHG")]))

    def fail(*args):
        raise OSError("simulated publication failure")

    monkeypatch.setattr(auction.os, "replace", fail)
    with pytest.raises(OSError, match="publication failure"):
        auction.save_auction_day(DAY, pd.DataFrame([_row("000001.XSHE")]))
    assert set(auction.load_day(DAY).symbol) == {"600000.XSHG"}
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("first", [RuntimeError("network down"), pd.DataFrame()])
def test_cron_retries_failed_or_empty_collection(monkeypatch, first):
    calls = []
    clock = {"polls": 0}
    monkeypatch.setattr(auction, "_now", lambda: NOW, raising=False)
    monkeypatch.setattr(scheduler, "_scheduler_state", {})
    monkeypatch.setattr(scheduler, "_stop", SimpleNamespace(is_set=lambda: clock["polls"] >= 3))
    monkeypatch.setattr(scheduler.time, "sleep", lambda seconds:
                        clock.update(polls=clock["polls"] + 1))
    monkeypatch.setattr(scheduler, "_dt", SimpleNamespace(
        datetime=SimpleNamespace(now=lambda: NOW), time=dt.time))

    def fetch():
        calls.append(1)
        if len(calls) == 1:
            if isinstance(first, Exception):
                raise first
            return first
        return pd.DataFrame([_row("600000.XSHG")])

    monkeypatch.setattr(auction, "fetch_and_save", fetch)
    scheduler._auction_cron_loop()
    assert len(calls) == 2
    assert scheduler._scheduler_state["auction_job"] == DAY.isoformat()


def test_preopen_task_retries_missing_auction_only_before_any_fill(monkeypatch):
    calls = []
    api = SimpleNamespace(_state={"trades": [], "preopen_auction_missing": False})
    ctx = SimpleNamespace(current_dt=None)

    def task(_ctx):
        calls.append(1)
        api._state["preopen_auction_missing"] = len(calls) == 1

    bundle = SimpleNamespace(daily=[(task, "09:26")])
    fired = set()
    monkeypatch.setattr(runner, "_seed_universe", lambda *_: None)
    monkeypatch.setattr(runner, "_persist", lambda *_: None)
    now = dt.datetime(2026, 10, 12, 9, 26)
    aux = {"jq_api": api, "fired": fired}

    runner._fire_preopen_daily("test", bundle, ctx, {}, aux, now)
    assert not fired
    runner._fire_preopen_daily("test", bundle, ctx, {}, aux, now)
    assert fired == {(id(task), "09:26")}
    assert calls == [1, 1]
