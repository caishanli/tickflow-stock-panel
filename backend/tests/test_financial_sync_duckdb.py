"""Tests for financial sync service with DuckDB writes."""
from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository
from app.services import financial_sync


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestFinancialSyncDuckDB:
    def test_write_financial_table(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "report_date": ["2026-01-01"],
            "revenue": [1000000.0],
            "net_profit": [100000.0],
        })
        repo.db.execute("CREATE OR REPLACE TABLE financials_income AS SELECT * FROM df")
        result = repo.db.execute("SELECT count(*) FROM financials_income").fetchone()
        assert result[0] == 1

    def test_write_table_creates_duckdb_table(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "symbol": ["000001", "000002"],
            "report_date": ["2026-01-01", "2026-01-01"],
            "revenue": [1000000.0, 2000000.0],
        })
        rows = financial_sync._write_table("income", df, tmp_path)
        assert rows == 2
        db_path = tmp_path / "stock.duckdb"
        assert db_path.exists()
        conn = duckdb.connect(str(db_path), read_only=True)
        result = conn.execute("SELECT count(*) FROM financials_income").fetchone()
        conn.close()
        assert result[0] == 2

    def test_write_table_overwrites_existing(self, tmp_path: Path) -> None:
        df1 = pl.DataFrame({
            "symbol": ["000001"],
            "report_date": ["2026-01-01"],
            "revenue": [1000000.0],
        })
        financial_sync._write_table("income", df1, tmp_path)
        df2 = pl.DataFrame({
            "symbol": ["000002", "000003"],
            "report_date": ["2026-06-01", "2026-06-01"],
            "revenue": [3000000.0, 4000000.0],
        })
        rows = financial_sync._write_table("income", df2, tmp_path)
        assert rows == 2
        result = financial_sync.get_financial_df(tmp_path, "income")
        assert len(result) == 2
        assert set(result["symbol"].to_list()) == {"000002", "000003"}

    def test_write_table_empty_df_returns_zero(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"symbol": [], "report_date": []})
        rows = financial_sync._write_table("income", df, tmp_path)
        assert rows == 0

    def test_write_table_no_symbol_column_returns_zero(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"revenue": [1000000.0]})
        rows = financial_sync._write_table("income", df, tmp_path)
        assert rows == 0

    def test_get_financial_df_reads_from_duckdb(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "report_date": ["2026-01-01"],
            "revenue": [1000000.0],
        })
        financial_sync._write_table("income", df, tmp_path)
        result = financial_sync.get_financial_df(tmp_path, "income")
        assert len(result) == 1
        assert result["symbol"].to_list() == ["000001"]
        assert result["revenue"].to_list() == [1000000.0]

    def test_get_financial_df_nonexistent_table_returns_empty(self, tmp_path: Path) -> None:
        result = financial_sync.get_financial_df(tmp_path, "nonexistent")
        assert result.is_empty()

    def test_get_financial_df_no_duckdb_returns_empty(self, tmp_path: Path) -> None:
        result = financial_sync.get_financial_df(tmp_path, "income")
        assert result.is_empty()

    def test_all_financial_tables_writable(self, tmp_path: Path) -> None:
        for table in financial_sync.FINANCIAL_TABLES:
            df = pl.DataFrame({
                "symbol": ["000001"],
                "report_date": ["2026-01-01"],
            })
            rows = financial_sync._write_table(table, df, tmp_path)
            assert rows == 1
            result = financial_sync.get_financial_df(tmp_path, table)
            assert len(result) == 1
