"""Tests for DuckDB views using real tables."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


class TestViewsDuckDB:
    def test_register_views_no_parquet(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        repo = KlineRepository(store=store)
        result = repo.db.execute(
            "SELECT table_name FROM information_schema.views WHERE table_schema = 'main'"
        ).fetchall()
        view_names = {r[0] for r in result}
        assert "kline_enriched" in view_names
        store.db.close()

    def test_rebuild_views(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        repo = KlineRepository(store=store)
        repo.rebuild_views()
        store.db.close()

    def test_refresh_index_views(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        repo = KlineRepository(store=store)
        repo.refresh_index_views()
        store.db.close()

    def test_register_views_does_not_overwrite_real_tables(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        repo = KlineRepository(store=store)
        store.db.execute("""
            INSERT INTO kline_daily (symbol, date, open, high, low, close, volume, amount, quote_ts)
            VALUES ('000001', '2026-01-15', 10.0, 11.0, 9.0, 10.5, 1000.0, 10000.0, 1705305600)
        """)
        store._register_views()
        result = store.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1
        store.db.close()
