"""Tests for DuckDB connection manager."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from app.tickflow.duckdb_manager import DuckDBManager


@pytest.fixture
def tmp_db(tmp_path: Path) -> DuckDBManager:
    db_path = tmp_path / "test.duckdb"
    mgr = DuckDBManager(db_path)
    yield mgr
    mgr.close()


class TestDuckDBManager:
    def test_creates_file(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        mgr = DuckDBManager(db_path)
        assert db_path.exists()
        mgr.close()

    def test_execute_create_table(self, tmp_db: DuckDBManager) -> None:
        tmp_db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
        tmp_db.execute("INSERT INTO test VALUES (1, 'hello')")
        result = tmp_db.execute("SELECT * FROM test").fetchall()
        assert result == [(1, "hello")]

    def test_execute_many(self, tmp_db: DuckDBManager) -> None:
        tmp_db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
        tmp_db.execute_many(
            "INSERT INTO test VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        result = tmp_db.execute("SELECT count(*) FROM test").fetchone()
        assert result[0] == 3

    def test_read_only_connection(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        mgr = DuckDBManager(db_path)
        mgr.execute("CREATE TABLE test (id INTEGER)")
        mgr.execute("INSERT INTO test VALUES (1)")
        mgr.close()

        read_mgr = DuckDBManager(db_path, read_only=True)
        result = read_mgr.execute("SELECT * FROM test").fetchall()
        assert result == [(1,)]

        with pytest.raises(duckdb.InvalidInputException):
            read_mgr.execute("INSERT INTO test VALUES (2)")

        read_mgr.close()

    def test_context_manager(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        with DuckDBManager(db_path) as mgr:
            mgr.execute("CREATE TABLE test (id INTEGER)")
            assert db_path.exists()
