"""Tests for pools DuckDB read/write."""
from __future__ import annotations

from pathlib import Path

import polars as pl

from app.tickflow.repository import DataStore


class TestPoolsDuckDB:
    def test_pools_table_exists(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        result = store.db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'pools'"
        ).fetchone()
        assert result is not None
        store.db.close()

    def test_write_and_read_pool(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        symbols = ["000001.SZ", "600519.SH"]
        df = pl.DataFrame({
            "pool_name": ["test_pool"] * len(symbols),
            "symbol": symbols,
        })
        store.db.execute("DELETE FROM pools WHERE pool_name = ?", ["test_pool"])
        store.db.execute("INSERT INTO pools SELECT * FROM df")

        result = store.db.execute(
            "SELECT symbol FROM pools WHERE pool_name = ?", ["test_pool"]
        ).fetchall()
        assert [r[0] for r in result] == symbols
        store.db.close()

    def test_pool_refresh_overwrites(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        old = ["000001.SZ"]
        new = ["600519.SH", "601318.SH"]

        for syms, pool in [(old, "r_pool"), (new, "r_pool")]:
            df = pl.DataFrame({"pool_name": [pool] * len(syms), "symbol": syms})
            store.db.execute("DELETE FROM pools WHERE pool_name = ?", [pool])
            store.db.execute("INSERT INTO pools SELECT * FROM df")

        result = store.db.execute(
            "SELECT symbol FROM pools WHERE pool_name = ?", ["r_pool"]
        ).fetchall()
        assert sorted(r[0] for r in result) == sorted(new)
        store.db.close()

    def test_empty_pool_returns_empty(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        result = store.db.execute(
            "SELECT symbol FROM pools WHERE pool_name = ?", ["nonexistent"]
        ).fetchall()
        assert result == []
        store.db.close()
