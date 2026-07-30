"""Tests for historical shares DuckDB read."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo_with_shares(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    df = pl.DataFrame({
        "symbol": ["000001", "000001"],
        "report_date": ["2025-06-30", "2025-12-31"],
        "total_shares": [1940000000.0, 1940000000.0],
        "float_shares": [1200000000.0, 1200000000.0],
    })
    repo.db.execute("CREATE OR REPLACE TABLE financials_shares AS SELECT * FROM df")
    yield repo
    store.db.close()


class TestHistoricalSharesDuckDB:
    def test_get_historical_shares(self, repo_with_shares: KlineRepository) -> None:
        result = repo_with_shares.get_historical_shares()
        assert len(result) > 0
