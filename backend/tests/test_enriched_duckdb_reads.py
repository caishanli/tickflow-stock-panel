"""Tests for enriched data reads from DuckDB."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo_with_enriched(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    df = pl.DataFrame({
        "symbol": ["000001"] * 3,
        "date": [date(2026, 1, 13), date(2026, 1, 14), date(2026, 1, 15)],
        "open": [10.0, 10.5, 11.0], "high": [11.0, 11.5, 12.0],
        "low": [9.0, 9.5, 10.0], "close": [10.5, 11.0, 11.5],
        "volume": [1000.0, 1100.0, 1200.0], "amount": [10000.0, 11000.0, 12000.0],
        "raw_close": [10.5, 11.0, 11.5], "raw_high": [11.0, 11.5, 12.0],
        "raw_low": [9.0, 9.5, 10.0],
    })
    repo._upsert_daily(df, "kline_daily_enriched")
    yield repo
    store.db.close()


class TestEnrichedDuckDBReads:
    def test_refresh_enriched_from_duckdb(self, repo_with_enriched: KlineRepository) -> None:
        repo_with_enriched._refresh_enriched()
        cache = repo_with_enriched._enriched_cache
        assert cache is not None
        assert len(cache) > 0

    def test_build_live_agg_from_duckdb(self, repo_with_enriched: KlineRepository) -> None:
        repo_with_enriched._refresh_enriched()
        repo_with_enriched._build_live_agg(date(2026, 1, 15))
        live_agg = repo_with_enriched._live_agg_cache
        assert live_agg is not None
