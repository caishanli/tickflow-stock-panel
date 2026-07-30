"""Tests for old pipeline path DuckDB writes."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository
from app.indicators.pipeline import run_pipeline


@pytest.fixture
def repo_with_daily(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    df = pl.DataFrame({
        "symbol": ["000001"] * 30,
        "date": [date(2026, 1, i + 1) for i in range(30)],
        "open": [10.0 + i * 0.1 for i in range(30)],
        "high": [11.0 + i * 0.1 for i in range(30)],
        "low": [9.0 + i * 0.1 for i in range(30)],
        "close": [10.5 + i * 0.1 for i in range(30)],
        "volume": [1000.0] * 30,
        "amount": [10000.0] * 30,
        "quote_ts": [1705305600] * 30,
    })
    repo.append_daily(df)
    yield repo
    store.db.close()


class TestPipelineOldPathDuckDB:
    def test_run_pipeline_writes_to_duckdb(self, repo_with_daily: KlineRepository) -> None:
        run_pipeline(repo_with_daily.store.data_dir, repo=repo_with_daily)
        result = repo_with_daily.db.execute("SELECT count(*) FROM kline_daily_enriched").fetchone()
        assert result[0] > 0
