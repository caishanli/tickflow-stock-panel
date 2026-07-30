"""Tests for minute K DuckDB read/write."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo_with_minute(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    df = pl.DataFrame({
        "symbol": ["000001", "000001", "000002"],
        "datetime": [
            datetime(2026, 1, 15, 9, 30), datetime(2026, 1, 15, 9, 31),
            datetime(2026, 1, 15, 9, 30),
        ],
        "open": [10.0, 10.1, 20.0], "high": [10.5, 10.6, 20.5],
        "low": [9.9, 10.0, 19.9], "close": [10.1, 10.2, 20.1],
        "volume": [500.0, 600.0, 1000.0], "amount": [5000.0, 6000.0, 20000.0],
    })
    repo._upsert_daily(df, "kline_minute")
    yield repo
    store.db.close()


class TestMinuteDuckDB:
    def test_get_minute(self, repo_with_minute: KlineRepository) -> None:
        result = repo_with_minute.get_minute("000001", date(2026, 1, 15))
        assert len(result) == 2

    def test_get_minute_batch(self, repo_with_minute: KlineRepository) -> None:
        result = repo_with_minute.get_minute_batch(["000001", "000002"], date(2026, 1, 15))
        assert len(result) == 3

    def test_get_minute_range(self, repo_with_minute: KlineRepository) -> None:
        result = repo_with_minute.get_minute_range(
            ["000001"], date(2026, 1, 15), date(2026, 1, 15)
        )
        assert len(result) == 2

    def test_get_minute_by_dates(self, repo_with_minute: KlineRepository) -> None:
        result = repo_with_minute.get_minute_by_dates(
            ["000001"], [date(2026, 1, 15)]
        )
        assert len(result) == 2
