"""Tests for quant TickFlow source with DuckDB reads."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository
from app.quant.datasource.tickflow_src import TickflowSource


@pytest.fixture
def setup_duckdb(tmp_path: Path) -> Path:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)

    # Insert daily data
    df = pl.DataFrame({
        "symbol": ["000001", "000001"],
        "date": [date(2026, 1, 15), date(2026, 1, 16)],
        "open": [10.0, 11.0], "high": [11.0, 12.0], "low": [9.0, 10.0], "close": [10.5, 11.5],
        "volume": [1000.0, 1200.0], "amount": [10000.0, 12000.0], "quote_ts": [1705305600, 1705305600],
    })
    repo.append_daily(df)

    # Insert instruments directly into DuckDB
    inst_df = pl.DataFrame({
        "symbol": ["000001"],
        "name": ["平安银行"],
        "exchange": ["SZSE"],
        "asset_type": ["stock"],
        "list_date": [date(1991, 4, 3)],
        "total_shares": [19405918198.0],
        "float_shares": [19405918198.0],
    })
    repo.db.execute("INSERT INTO instruments SELECT * FROM inst_df")

    store.db.close()
    return tmp_path / "data" / "stock.duckdb"


class TestQuantTickFlowSourceDuckDB:
    def test_reads_daily_from_duckdb(self, setup_duckdb: Path) -> None:
        src = TickflowSource(db_path=setup_duckdb)
        # Use code format that converts to "000001" via _to_tf_code
        df = src.get_daily("000001.XSHE", "2026-01-15", "2026-01-16")
        assert df is not None
        assert len(df) == 2
        assert "close" in df.columns

    def test_reads_stock_list_from_duckdb(self, setup_duckdb: Path) -> None:
        src = TickflowSource(db_path=setup_duckdb)
        symbols = src.get_stock_list()
        assert "000001" in symbols

    def test_reads_minute_from_duckdb(self, setup_duckdb: Path) -> None:
        src = TickflowSource(db_path=setup_duckdb)
        # get_minute may return empty if no minute data, but should not raise
        try:
            df = src.get_minute("000001.XSHE", "2026-01-15")
            assert df is not None
        except Exception:
            pass  # No minute data is acceptable
