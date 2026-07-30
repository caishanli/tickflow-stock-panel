"""Tests for DataStore with persistent DuckDB."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from app.tickflow.repository import DataStore


@pytest.fixture
def store(tmp_path: Path) -> DataStore:
    """Create a DataStore with persistent DuckDB."""
    store = DataStore(data_dir=tmp_path / "data")
    yield store
    store.db.close()


class TestDataStoreDuckDB:
    def test_creates_duckdb_file(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        db_path = tmp_path / "data" / "stock.duckdb"
        assert db_path.exists()
        store.db.close()

    def test_creates_tables(self, store: DataStore) -> None:
        tables = store.db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "kline_daily" in table_names
        assert "instruments" in table_names

    def test_insert_and_query(self, store: DataStore) -> None:
        store.db.execute("""
            INSERT INTO kline_daily (symbol, date, open, high, low, close, volume, amount, quote_ts)
            VALUES ('000001', '2026-01-15', 10.0, 11.0, 9.0, 10.5, 1000.0, 10000.0, 1705305600)
        """)
        result = store.db.execute("SELECT * FROM kline_daily").fetchall()
        assert len(result) == 1

    def test_read_only_connection(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        store.db.execute("""
            INSERT INTO kline_daily (symbol, date, open, high, low, close, volume, amount, quote_ts)
            VALUES ('000001', '2026-01-15', 10.0, 11.0, 9.0, 10.5, 1000.0, 10000.0, 1705305600)
        """)
        store.db.close()

        read_conn = duckdb.connect(
            str(tmp_path / "data" / "stock.duckdb"),
            read_only=True,
        )
        result = read_conn.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1
        read_conn.close()
