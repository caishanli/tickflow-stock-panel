"""Tests for Parquet to DuckDB migration."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from scripts.migrate_parquet_to_duckdb import migrate_parquet_to_duckdb


@pytest.fixture
def parquet_data(tmp_path: Path) -> Path:
    """Create sample Parquet data for migration testing."""
    data_dir = tmp_path / "data"

    # Create kline_daily partition
    kline_dir = data_dir / "kline_daily" / "date=2026-01-15"
    kline_dir.mkdir(parents=True)
    df = pl.DataFrame(
        {
            "symbol": ["000001", "000002"],
            "date": [date(2026, 1, 15), date(2026, 1, 15)],
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.0, 19.0],
            "close": [10.5, 20.5],
            "volume": [1000.0, 2000.0],
            "amount": [10000.0, 40000.0],
            "quote_ts": [1705305600, 1705305600],
        }
    )
    df.write_parquet(kline_dir / "part.parquet")

    # Create instruments
    inst_dir = data_dir / "instruments"
    inst_dir.mkdir(parents=True)
    inst_df = pl.DataFrame(
        {
            "symbol": ["000001", "000002"],
            "name": ["平安银行", "万科A"],
            "exchange": ["SZSE", "SZSE"],
        }
    )
    inst_df.write_parquet(inst_dir / "instruments.parquet")

    return data_dir


class TestMigration:
    def test_migrate_creates_duckdb(self, parquet_data: Path, tmp_path: Path) -> None:
        db_path = tmp_path / "stock.duckdb"
        migrate_parquet_to_duckdb(parquet_data, db_path)
        assert db_path.exists()

    def test_migrate_kline_daily(self, parquet_data: Path, tmp_path: Path) -> None:
        db_path = tmp_path / "stock.duckdb"
        migrate_parquet_to_duckdb(parquet_data, db_path)

        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)
        result = conn.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 2
        conn.close()

    def test_migrate_instruments(self, parquet_data: Path, tmp_path: Path) -> None:
        db_path = tmp_path / "stock.duckdb"
        migrate_parquet_to_duckdb(parquet_data, db_path)

        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)
        result = conn.execute("SELECT count(*) FROM instruments").fetchone()
        assert result[0] == 2
        conn.close()

    def test_migrate_empty_dir(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        db_path = tmp_path / "stock.duckdb"
        migrate_parquet_to_duckdb(data_dir, db_path)
        assert db_path.exists()
