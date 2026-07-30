"""Tests for depth service with DuckDB writes."""
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


class TestDepthServiceDuckDB:
    def test_write_depth5(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "date": [date(2026, 1, 15)],
            "bid1_price": [10.0],
            "bid1_volume": [100.0],
            "ask1_price": [10.1],
            "ask1_volume": [200.0],
            "sealed": [True],
        })
        repo.db.execute("INSERT INTO depth5 SELECT * FROM df")
        result = repo.db.execute("SELECT count(*) FROM depth5").fetchone()
        assert result[0] == 1
