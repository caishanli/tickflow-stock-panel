# backend/tests/quant/test_stockdata_sources.py
import datetime as _dt

import polars as pl
import pytest

from app.services.stockdata.sources import DataSources, MinuteMemoryStore


def _write_daily(root, day, rows):
    import os
    d = os.path.join(root, "kline_daily", f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame(rows).write_parquet(os.path.join(d, "part.parquet"))


def _write_minute(root, sub, day, rows):
    import os
    d = os.path.join(root, sub, f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame(rows).write_parquet(os.path.join(d, "part.parquet"))


@pytest.fixture
def src(tmp_path):
    import os
    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    day = _dt.date.today().isoformat()
    _write_daily(str(tmp_path), day, [
        {"symbol": "600000.SH", "date": day, "open": 10.0, "high": 11.0,
         "low": 9.0, "close": 10.5, "volume": 1000, "amount": 105000.0},
    ])
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=2)
    yield s
    os.environ.pop("PARTITION_DATA_ROOT", None)


def test_preload_daily_reads_partitions(src):
    df = src.preload_daily(lookback_days=400)
    assert not df.is_empty()
    assert "symbol" in df.columns and "close" in df.columns
    assert df["symbol"].to_list() == ["600000.SH"]
    # 股票日线 volume 手 → 股（×100）
    assert df["volume"].to_list() == [100000]


def test_minute_memory_store_lazy_and_clear():
    ms = MinuteMemoryStore()
    day = _dt.date.today()
    assert ms.day() is None  # 未请求标的：无日期、无内存
    df = pl.DataFrame({"symbol": ["600000.SH"], "datetime": [f"{day} 10:00:00"],
                       "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
                       "volume": [100], "amount": [100.0]})
    ms.update(f"{day} 00:00:00", df)
    assert ms.day() == day
    got = ms.get_slice({"600000.SH"}, "2000-01-01 00:00:00", f"{day} 15:00:00")
    assert not got.is_empty()
    # 换日 lazy 清空：ensure_day(次日) 后旧帧全部清空
    nxt = day + _dt.timedelta(days=1)
    ms.ensure_day(nxt)
    assert ms.day() == nxt
    assert ms.get_slice({"600000.SH"}, "2000-01-01 00:00:00", f"{day} 15:00:00").is_empty()
    # clear() 显式清空后回到初始态
    ms.clear()
    assert ms.day() is None


def test_realtime_snapshot_serves_from_memory(src, monkeypatch):
    day = _dt.date.today().isoformat()
    _write_minute(str(src.data_root), "kline_etf_minute", day, [
        {"symbol": "512670.SH", "datetime": f"{day} 09:31:00", "open": 1.0,
         "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1000, "amount": 1000.0},
    ])
    # 非交易时段：只读内存库，不触网
    monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: False)
    df = src.get_realtime_snapshot(["512670.XSHG"])
    assert not df.is_empty()
    assert df["close"].to_list() == [1.0]
