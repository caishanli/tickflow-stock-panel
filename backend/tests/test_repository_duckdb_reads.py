"""Tests for KlineRepository DuckDB read operations."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo_with_data(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)

    # Insert test data
    df = pl.DataFrame({
        "symbol": ["000001", "000001", "000002"],
        "date": [date(2026, 1, 15), date(2026, 1, 16), date(2026, 1, 15)],
        "open": [10.0, 11.0, 20.0],
        "high": [11.0, 12.0, 21.0],
        "low": [9.0, 10.0, 19.0],
        "close": [10.5, 11.5, 20.5],
        "volume": [1000.0, 1200.0, 2000.0],
        "amount": [10000.0, 12000.0, 40000.0],
        "quote_ts": [1705305600, 1705305600, 1705305600],
    })
    repo.append_daily(df)

    yield repo
    store.db.close()


class TestDuckDBReads:
    def test_query_daily_all(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.db.execute("SELECT * FROM kline_daily").fetchall()
        assert len(result) == 3

    def test_query_daily_by_symbol(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.db.execute(
            "SELECT * FROM kline_daily WHERE symbol = ?", ["000001"]
        ).fetchall()
        assert len(result) == 2

    def test_query_daily_by_date_range(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.db.execute(
            "SELECT * FROM kline_daily WHERE date BETWEEN ? AND ?",
            [date(2026, 1, 15), date(2026, 1, 15)]
        ).fetchall()
        assert len(result) == 2

    def test_scan_daily_to_polars(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.db.execute("SELECT * FROM kline_daily").pl()
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3

    def test_scan_daily_with_filters(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data._scan_daily(
            "kline_daily",
            symbol="000001",
            start_date=date(2026, 1, 15),
            end_date=date(2026, 1, 16)
        )
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2
        assert result["symbol"].unique().to_list() == ["000001"]

    def test_scan_daily_no_filters(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data._scan_daily("kline_daily")
        assert len(result) == 3

    def test_get_latest_date(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.get_latest_date()
        assert result == date(2026, 1, 16)

    def test_get_date_range(self, repo_with_data: KlineRepository) -> None:
        min_date, max_date = repo_with_data.get_date_range()
        assert min_date == date(2026, 1, 15)
        assert max_date == date(2026, 1, 16)

    def test_symbols_lagging(self, repo_with_data: KlineRepository) -> None:
        # The existing symbols_lagging method uses min_gap_days parameter
        # With reference_date=2026-01-16 and min_gap_days=0, 000002 (latest=2026-01-15) should lag
        result = repo_with_data.symbols_lagging(date(2026, 1, 16), min_gap_days=0)
        assert "000002" in result
        assert "000001" not in result
