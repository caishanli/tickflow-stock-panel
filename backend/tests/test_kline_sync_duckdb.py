"""Tests for kline sync service with DuckDB writes."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository
from app.services.kline_sync import sync_daily_batch, _normalize_adj_factor


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestKlineSyncDuckDB:
    def test_sync_daily_batch_writes_to_duckdb(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "date": [date(2026, 1, 15)],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "volume": [1000.0], "amount": [10000.0], "quote_ts": [1705305600],
        })
        repo.append_daily(df)
        result = repo.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1

    def test_append_adj_factor_writes_to_duckdb(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001", "000001"],
            "trade_date": [date(2026, 1, 15), date(2026, 1, 16)],
            "adj_factor": [1.0, 1.05],
        })
        repo.append_adj_factor(df, asset_type="stock")
        result = repo.db.execute("SELECT count(*) FROM adj_factor").fetchone()
        assert result[0] == 2

    def test_append_adj_factor_etf_writes_to_duckdb(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["510300"],
            "trade_date": [date(2026, 1, 15)],
            "adj_factor": [1.0],
        })
        repo.append_adj_factor(df, asset_type="etf")
        result = repo.db.execute("SELECT count(*) FROM adj_factor_etf").fetchone()
        assert result[0] == 1

    def test_append_adj_factor_ex_factor_rename(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001", "000001"],
            "trade_date": [date(2026, 1, 15), date(2026, 1, 16)],
            "ex_factor": [1.0, 1.05],
        })
        repo.append_adj_factor(df, asset_type="stock")
        result = repo.db.execute("SELECT count(*) FROM adj_factor").fetchone()
        assert result[0] == 2
        rows = repo.db.execute("SELECT adj_factor FROM adj_factor ORDER BY trade_date").fetchall()
        assert [r[0] for r in rows] == [1.0, 1.05]

    def test_normalize_adj_factor(self) -> None:
        raw = {
            "000001": [
                {"trade_date": 1705276800000, "adj_factor": 1.0},
                {"trade_date": 1705363200000, "adj_factor": 1.05},
            ]
        }
        result = _normalize_adj_factor(raw)
        assert result.height == 2
        assert result["symbol"].to_list() == ["000001", "000001"]
        assert result["ex_factor"].to_list() == [1.0, 1.05]
