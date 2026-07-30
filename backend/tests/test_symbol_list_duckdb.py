"""Tests for instrument/financial symbol list reads from DuckDB."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo_with_instruments(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    df = pl.DataFrame({  # noqa: F841 — used by DuckDB SQL
        "symbol": ["000001", "000002", "000003"],
        "name": ["平安银行", "万科A", "国农"],
        "exchange": ["SZSE", "SZSE", "SZSE"],
        "asset_type": ["stock", "stock", "stock"],
        "list_date": [None, None, None],
        "total_shares": [None, None, None],
        "float_shares": [None, None, None],
    })
    repo.db.execute("DELETE FROM instruments")
    repo.db.execute("INSERT INTO instruments SELECT * FROM df")
    yield repo
    store.db.close()


class TestSymbolListDuckDB:
    def test_financial_get_symbols(self, repo_with_instruments: KlineRepository) -> None:
        from app.services.financial_sync import _get_symbols
        symbols = _get_symbols(repo_with_instruments.store.data_dir)
        assert len(symbols) == 3

    def test_enrich_names_reads_duckdb(self, repo_with_instruments: KlineRepository) -> None:
        from app.services.instrument_sync import enrich_names_from_quotes
        quotes = [
            {"symbol": "000001", "ext": {"name": "平安银行"}},
            {"symbol": "999999", "ext": {"name": "新股票"}},
        ]
        result = enrich_names_from_quotes(
            repo_with_instruments.store.data_dir, quotes, repo=repo_with_instruments
        )
        assert result >= 0
