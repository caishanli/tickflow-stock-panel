"""Tests for KlineRepository DuckDB write operations."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestDuckDBWrites:
    def test_upsert_daily_insert(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "date": [date(2026, 1, 15)],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "volume": [1000.0], "amount": [10000.0], "quote_ts": [1705305600],
        })
        repo.append_daily(df)
        result = repo.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1

    def test_upsert_daily_merge(self, repo: KlineRepository) -> None:
        df1 = pl.DataFrame({
            "symbol": ["000001"], "date": [date(2026, 1, 15)],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "volume": [1000.0], "amount": [10000.0], "quote_ts": [1705305600],
        })
        df2 = pl.DataFrame({
            "symbol": ["000001"], "date": [date(2026, 1, 15)],
            "open": [10.5], "high": [11.5], "low": [9.5], "close": [11.0],
            "volume": [1500.0], "amount": [15000.0], "quote_ts": [1705309200],
        })
        repo.append_daily(df1)
        repo.append_daily(df2)
        result = repo.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1  # Should be merged, not duplicated
        close = repo.db.execute("SELECT close FROM kline_daily").fetchone()
        assert close[0] == 11.0  # Should use latest value

    def test_flush_live_daily(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001", "000002"],
            "date": [date(2026, 1, 15), date(2026, 1, 15)],
            "open": [10.0, 20.0], "high": [11.0, 21.0], "low": [9.0, 19.0],
            "close": [10.5, 20.5], "volume": [1000.0, 2000.0],
            "amount": [10000.0, 40000.0], "quote_ts": [1705305600, 1705305600],
        })
        repo.flush_live_daily(df)
        result = repo.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 2
