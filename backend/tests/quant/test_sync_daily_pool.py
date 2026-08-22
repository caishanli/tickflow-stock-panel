"""sync_daily/sync_index_daily/sync_etf_minute 并发池改造测试。"""
from __future__ import annotations

import datetime as _dt

import pandas as pd
import polars as pl

from app.services import mootdx_service as ms


class _FakeDailySrc:
    def get_daily(self, code, start, end):
        ts = pd.Timestamp(f"{end[:4]}-{end[4:6]}-{end[6:]} 15:00:00")
        return pd.DataFrame({"open": [1.0], "high": [2.0], "low": [0.5],
                             "close": [1.5], "volume": [100.0],
                             "amount": [1000.0]},
                            index=pd.DatetimeIndex([ts]))

    def get_minute_recent(self, code, pages=1):
        # 真实 get_minute_recent 契约：index.name == "datetime"
        idx = pd.DatetimeIndex([pd.Timestamp("2026-08-21 15:00:00")])
        idx.name = "datetime"
        return pd.DataFrame({"open": [1.0], "high": [1.0], "low": [1.0],
                             "close": [1.0], "volume": [1.0], "amount": [1.0]},
                            index=idx)


def test_sync_daily_concurrent_writes_partition(tmp_path, monkeypatch):
    day = _dt.date(2026, 8, 21)
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "_stock_universe", lambda: ["600000.SH"])
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["510300.XSHG"])
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeDailySrc())

    from tests.quant.test_sync_stock_minute_pool import _StubPool
    created = {}

    def fake_pool(workers=None, source_factory=None):
        created["pool"] = True
        return _StubPool(_FakeDailySrc())

    monkeypatch.setattr(ms, "BackfillPool", fake_pool)
    written = ms.sync_daily(day)
    assert created.get("pool")  # 走了池
    assert written["stock"] == 1 and written["etf"] == 1
    part = tmp_path / "kline_daily" / f"date={day}" / "part.parquet"
    assert part.exists()
    df = pl.read_parquet(part)
    assert df["symbol"].to_list() == ["600000.SH"]
    assert df["volume"].to_list() == [1.0]  # 股票 volume ÷100 换手


def test_sync_index_daily_concurrent(tmp_path, monkeypatch):
    day = _dt.date(2026, 8, 21)
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "_index_universe", lambda: ["000300.SH"])
    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeDailySrc())

    from tests.quant.test_sync_stock_minute_pool import _StubPool
    created = {}

    def fake_pool(workers=None, source_factory=None):
        created["pool"] = True
        return _StubPool(_FakeDailySrc())

    monkeypatch.setattr(ms, "BackfillPool", fake_pool)
    res = ms.sync_index_daily(day)
    assert created.get("pool")  # 走了池
    assert res["written"] == 1 and res["symbols"] == 1
    assert (tmp_path / "kline_index_daily" / f"date={day}" / "part.parquet").exists()


def test_sync_etf_minute_concurrent(tmp_path, monkeypatch):
    day = _dt.date(2026, 8, 21)
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["510300.XSHG"])
    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeDailySrc())

    from tests.quant.test_sync_stock_minute_pool import _StubPool
    created = {}

    def fake_pool(workers=None, source_factory=None):
        created["pool"] = True
        return _StubPool(_FakeDailySrc())

    monkeypatch.setattr(ms, "BackfillPool", fake_pool)
    res = ms.sync_etf_minute(day)
    assert created.get("pool")  # 走了池
    assert res["rows"] == 1 and res["query_failed"] == []
