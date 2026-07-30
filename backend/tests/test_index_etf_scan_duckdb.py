"""Tests for index/ETF daily scan from DuckDB."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo_with_index_etf(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    idx_df = pl.DataFrame({
        "symbol": ["000001"] * 2,
        "date": [date(2026, 1, 14), date(2026, 1, 15)],
        "open": [3000.0, 3010.0], "high": [3020.0, 3030.0],
        "low": [2990.0, 3000.0], "close": [3010.0, 3020.0],
        "volume": [1e9, 1.1e9], "amount": [1e10, 1.1e10],
    })
    repo._upsert_daily(idx_df, "kline_index_enriched")
    etf_df = pl.DataFrame({
        "symbol": ["510300"] * 2,
        "date": [date(2026, 1, 14), date(2026, 1, 15)],
        "open": [4.0, 4.05], "high": [4.1, 4.15],
        "low": [3.95, 4.0], "close": [4.05, 4.10],
        "volume": [1e7, 1.1e7], "amount": [4e7, 4.5e7],
    })
    repo._upsert_daily(etf_df, "kline_etf_enriched")
    yield repo
    store.db.close()


class TestIndexETFScanDuckDB:
    def test_scan_index_daily(self, repo_with_index_etf: KlineRepository) -> None:
        result = repo_with_index_etf._scan_index_daily_symbol(
            "000001", date(2026, 1, 14), date(2026, 1, 15), None
        )
        assert len(result) == 2

    def test_scan_etf_daily(self, repo_with_index_etf: KlineRepository) -> None:
        result = repo_with_index_etf._scan_etf_daily_symbol(
            "510300", date(2026, 1, 14), date(2026, 1, 15), None
        )
        assert len(result) == 2

    def test_scan_with_columns(self, repo_with_index_etf: KlineRepository) -> None:
        result = repo_with_index_etf._scan_index_daily_symbol(
            "000001", date(2026, 1, 14), date(2026, 1, 15), ["symbol", "close"]
        )
        assert result.columns == ["symbol", "close"]
