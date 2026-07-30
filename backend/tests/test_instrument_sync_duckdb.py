"""Tests for instrument sync service with DuckDB writes."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestInstrumentSyncDuckDB:
    def test_sync_instruments_writes_to_duckdb(self, repo: KlineRepository) -> None:
        # This test would need mocking of TickFlow API
        # For now, just verify the table exists and is writable
        df = pl.DataFrame({  # noqa: F841 — used by DuckDB SQL
            "symbol": ["000001"],
            "name": ["平安银行"],
            "exchange": ["SZSE"],
            "asset_type": ["stock"],
            "list_date": [None],
            "total_shares": [None],
            "float_shares": [None],
        })
        repo.db.execute("DELETE FROM instruments")
        repo.db.execute(
            "INSERT INTO instruments SELECT * FROM df"
        )
        result = repo.db.execute("SELECT count(*) FROM instruments").fetchone()
        assert result[0] == 1
