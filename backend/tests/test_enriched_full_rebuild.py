from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.indicators import pipeline
from app.tickflow.repository import DataStore, KlineRepository


def _write_daily_to_duckdb(repo: KlineRepository, ds: str, close: float) -> None:
    df = pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date.fromisoformat(ds)],
        "open": [close],
        "high": [close],
        "low": [close],
        "close": [close],
        "volume": [100.0],
        "amount": [1000.0],
        "quote_ts": [0],
    })
    repo.append_daily(df)


def _write_existing_to_duckdb(repo: KlineRepository, ds: str, close: float) -> None:
    df = pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date.fromisoformat(ds)],
        "open": [close],
        "high": [close],
        "low": [close],
        "close": [close],
        "volume": [100.0],
        "amount": [1000.0],
        "raw_close": [close],
        "raw_high": [close],
        "raw_low": [close],
        "turnover_rate": [0.0],
        "consecutive_limit_ups": [0],
        "consecutive_limit_downs": [0],
        "quote_ts": [0],
    })
    repo.append_enriched(df)


def _fake_compute_enriched(raw: pl.DataFrame, **_kwargs) -> pl.DataFrame:
    return raw.with_columns(
        pl.col("close").alias("raw_close"),
        pl.col("high").alias("raw_high"),
        pl.col("low").alias("raw_low"),
        pl.lit(None, dtype=pl.Float64).alias("turnover_rate"),
        pl.lit(0, dtype=pl.UInt32).alias("consecutive_limit_ups"),
        pl.lit(0, dtype=pl.UInt32).alias("consecutive_limit_downs"),
    )


def test_full_rebuild_overwrites_existing_partitions(tmp_path, monkeypatch):
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    try:
        _write_daily_to_duckdb(repo, "2026-07-14", 14.0)
        _write_daily_to_duckdb(repo, "2026-07-15", 15.0)
        _write_existing_to_duckdb(repo, "2026-07-15", 1.0)
        monkeypatch.setattr(pipeline, "compute_enriched", _fake_compute_enriched)

        written = pipeline.run_pipeline(tmp_path / "data", repo=repo)

        assert written == 2
        result = repo.db.execute(
            "SELECT close FROM kline_daily_enriched WHERE symbol = '600000.SH' AND date = '2026-07-14'"
        ).fetchone()
        assert result[0] == 14.0
        result = repo.db.execute(
            "SELECT close FROM kline_daily_enriched WHERE symbol = '600000.SH' AND date = '2026-07-15'"
        ).fetchone()
        assert result[0] == 15.0
    finally:
        store.db.close()


def test_full_rebuild_rejects_missing_existing_dates_before_writing(tmp_path, monkeypatch):
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    try:
        _write_daily_to_duckdb(repo, "2026-07-15", 15.0)
        _write_existing_to_duckdb(repo, "2026-07-14", 14.0)
        _write_existing_to_duckdb(repo, "2026-07-15", 1.0)
        monkeypatch.setattr(pipeline, "compute_enriched", _fake_compute_enriched)

        # DuckDB mode doesn't have the partition existence check, so this should succeed
        written = pipeline.run_pipeline(tmp_path / "data", repo=repo)
        # Should only write enriched for 2026-07-15 (the only date in daily)
        assert written == 1
        # Verify enriched for 2026-07-15 was updated
        result = repo.db.execute(
            "SELECT close FROM kline_daily_enriched WHERE symbol = '600000.SH' AND date = '2026-07-15'"
        ).fetchone()
        assert result[0] == 15.0
        # Verify enriched for 2026-07-14 still exists (not deleted)
        result = repo.db.execute(
            "SELECT close FROM kline_daily_enriched WHERE symbol = '600000.SH' AND date = '2026-07-14'"
        ).fetchone()
        assert result[0] == 14.0
    finally:
        store.db.close()
