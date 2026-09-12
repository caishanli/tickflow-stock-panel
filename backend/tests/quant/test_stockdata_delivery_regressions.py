"""行情交付回归：历史/实时隔离、回源写入可见性及时间窗口。"""

import datetime as dt
from unittest.mock import Mock

import polars as pl
import pytest

from app.services.stockdata import sources
from app.services.stockdata.handlers import handle


def minute(symbol, timestamp, close=1.0):
    return pl.DataFrame({
        "symbol": [symbol], "datetime": [timestamp],
        "open": [close], "high": [close], "low": [close], "close": [close],
        "volume": [100.0], "amount": [100.0 * close],
    })


def write_partition(root, subdir, day, frame):
    folder = root / subdir / f"date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    # 与回源一样原子替换，避免依赖 sleep 或文件系统 mtime 分辨率。
    temporary = folder / "part.tmp"
    frame.write_parquet(temporary)
    temporary.replace(folder / "part.parquet")


@pytest.fixture
def delivery(tmp_path, monkeypatch):
    now = dt.datetime.combine(dt.date.today(), dt.time(10, 5))
    monkeypatch.setattr(sources, "_now", lambda: now, raising=False)
    monkeypatch.setattr(sources, "_in_trading", lambda *_a, **_k: True)
    src = sources.DataSources(data_root=str(tmp_path), fetch_workers=1)
    fetch = Mock(return_value=[])
    monkeypatch.setattr(src.puller, "fetch_many", fetch)
    yield src, now, fetch
    src.puller.shutdown()


def test_historical_snapshot_preserves_live_memory(delivery):
    src, now, _fetch = delivery
    src.minute_store.update(str(now.date()), minute("600000.SH", now))
    src.get_realtime_snapshot(["600000.XSHG"], now - dt.timedelta(days=1))
    assert src.minute_store.day() == now.date()
    assert src.minute_store.get_slice({"600000.SH"}, str(now), str(now)).height == 1


@pytest.mark.parametrize("offset", [-1, 1])
def test_non_today_snapshot_never_pulls_live_data(delivery, offset):
    src, now, fetch = delivery
    assert src.get_realtime_snapshot(
        ["600000.XSHG"], now + dt.timedelta(days=offset)
    ).is_empty()
    fetch.assert_not_called()


@pytest.mark.parametrize("subdir,symbol", [
    ("kline_minute", "600000.SH"), ("kline_etf_minute", "512670.SH"),
])
def test_historical_snapshot_reads_both_asset_partitions(delivery, tmp_path, subdir, symbol):
    src, now, fetch = delivery
    then = now - dt.timedelta(days=1)
    write_partition(tmp_path, subdir, then.date(), minute(symbol, then))
    got = src.get_realtime_snapshot([symbol], then)
    assert got["close"].to_list() == [1.0]
    fetch.assert_not_called()


def test_snapshot_discards_other_dates_from_vendor(delivery):
    src, now, fetch = delivery
    fetch.return_value = [pl.concat([
        minute("600000.SH", now - dt.timedelta(days=1), 9.0),
        minute("600000.SH", now, 10.0),
    ])]
    got = src.get_realtime_snapshot(["600000.XSHG"], now)
    assert got["close"].to_list() == [10.0]
    stored = src.minute_store.get_slice(
        {"600000.SH"}, str(now - dt.timedelta(days=1)), str(now)
    )
    assert stored["close"].to_list() == [10.0]


def test_future_partition_bar_does_not_suppress_refresh(delivery, tmp_path):
    src, now, fetch = delivery
    write_partition(tmp_path, "kline_etf_minute", now.date(), pl.concat([
        minute("512670.SH", now - dt.timedelta(minutes=1)),
        minute("512670.SH", now + dt.timedelta(minutes=1), 99.0),
    ]))
    fetch.return_value = [minute("512670.SH", now, 2.0)]
    got = src.get_realtime_snapshot(["512670.XSHG"], now)
    fetch.assert_called_once()
    assert got["close"].to_list() == [1.0, 2.0]


@pytest.mark.parametrize("initial", [None, 1.0])
def test_minute_query_sees_refresh_without_waiting_for_ttl(delivery, initial):
    src, now, _fetch = delivery
    if initial is not None:
        src.minute_store.update(str(now.date()), minute("600000.SH", now, initial))
    src.get_minute(["600000.SH"], now, now)
    src.minute_store.update(str(now.date()), minute("600000.SH", now, 2.0))
    got = src.get_minute(["600000.SH"], now, now)
    assert got["close"].to_list() == [2.0]


def test_memory_rollover_discards_late_previous_day_write():
    store = sources.MinuteMemoryStore()
    yesterday = dt.datetime(2026, 9, 9, 10)
    today = yesterday + dt.timedelta(days=1)
    store.update(str(yesterday.date()), minute("600000.SH", yesterday))
    store.update(str(today.date()), minute("512670.SH", today, 2.0))
    store.update(str(yesterday.date()), minute("600000.SH", yesterday, 3.0))
    assert store.day() == today.date()
    got = store.get_slice({"600000.SH", "512670.SH"}, str(yesterday), str(today))
    assert got["close"].to_list() == [2.0]


def test_daily_cache_sees_atomic_backfill_repair(delivery, tmp_path):
    src, now, _fetch = delivery
    day = now.date()
    frame = minute("600000.SH", now).rename({"datetime": "date"}).with_columns(
        pl.col("date").cast(pl.Date)
    )
    write_partition(tmp_path, "kline_daily", day, frame)
    assert src.get_daily(["600000.SH"], str(day), str(day))["close"].to_list() == [1.0]
    write_partition(tmp_path, "kline_daily", day, frame.with_columns(pl.lit(2.0).alias("close")))
    assert src.get_daily(["600000.SH"], str(day), str(day))["close"].to_list() == [2.0]


def test_minute_get_price_preserves_intraday_bounds(delivery, tmp_path):
    src, now, _fetch = delivery
    write_partition(tmp_path, "kline_minute", now.date(), pl.concat([
        minute("600000.SH", now - dt.timedelta(minutes=1)),
        minute("600000.SH", now, 2.0),
        minute("600000.SH", now + dt.timedelta(minutes=1), 3.0),
    ]))
    kind, got = handle("get_price", {
        "security": "600000.SH", "frequency": "1m",
        "start_date": str(now), "end_date": str(now),
    }, src)
    assert kind == "parquet"
    assert got["close"].to_list() == [2.0]


def test_bootstrap_volume_baseline_excludes_previous_days(delivery):
    from app.services.stockdata.rt_sources import RTQuote

    src, now, _fetch = delivery
    symbol = "600000.SH"
    src.puller.synth.reset_if_new_day(now.date())
    frame = pl.concat([
        minute(symbol, now - dt.timedelta(days=1), 1.0),
        minute(symbol, now, 1.0),
    ])
    src.puller._seed_synth_from_frame(symbol, frame)
    quote = RTQuote(symbol=symbol, price=1.0, prev_close=1.0, open_=1.0,
                    high=1.0, low=1.0, cum_volume=150.0, cum_amount=150.0,
                    quote_time=now + dt.timedelta(seconds=1))
    got = src.puller.synth.update({symbol: quote})[0]
    assert got["volume"].to_list() == [150.0]
    assert got["amount"].to_list() == [150.0]


def test_memory_accepts_vendor_integer_volume_then_synthetic_float(delivery):
    src, now, _fetch = delivery
    initial = minute("600000.SH", now).with_columns(pl.col("volume").cast(pl.Int64))
    src.minute_store.update(str(now.date()), initial)
    src.minute_store.update(str(now.date()), minute("600000.SH", now, 2.0))
    got = src.get_realtime_snapshot(["600000.SH"], now)
    assert got["close"].to_list() == [2.0]
    assert got["volume"].to_list() == [100.0]


def test_snapshot_does_not_pull_outside_wall_clock_session(delivery, monkeypatch):
    src, now, fetch = delivery
    # 回放今日 10:05，但服务端已经收盘；按请求时间判断会错误触网。
    monkeypatch.setattr(sources, "_now", lambda: now.replace(hour=18))
    monkeypatch.setattr(sources, "_in_trading", lambda ts: ts.hour < 15)
    assert src.get_realtime_snapshot(["600000.SH"], now).is_empty()
    fetch.assert_not_called()


def test_empty_minute_request_does_not_scan_entire_market(delivery, monkeypatch):
    src, now, fetch = delivery
    scan = Mock(side_effect=AssertionError("空标的不应扫描全市场"))
    monkeypatch.setattr(src, "_scan_partitions", scan)
    assert src.get_minute([], now, now).is_empty()
    assert src.get_realtime_snapshot([], now).is_empty()
    scan.assert_not_called()
    fetch.assert_not_called()


def test_snapshot_cache_sees_repair_and_partition_removal(delivery, tmp_path):
    src, now, _fetch = delivery
    day = now.date()
    write_partition(tmp_path, "kline_minute", day, minute("600000.SH", now))
    assert src.get_realtime_snapshot(["600000.SH"], now)["close"].to_list() == [1.0]
    write_partition(tmp_path, "kline_minute", day, minute("600000.SH", now, 2.0))
    assert src.get_realtime_snapshot(["600000.SH"], now)["close"].to_list() == [2.0]
    (tmp_path / "kline_minute" / f"date={day}" / "part.parquet").unlink()
    assert src.get_realtime_snapshot(["600000.SH"], now).is_empty()
