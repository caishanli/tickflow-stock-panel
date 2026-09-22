"""enriched 缺格回填：只填缺失 (day, symbol)，不碰已有行。"""
from __future__ import annotations

from datetime import date

import polars as pl

from app.services import enriched_backfill


def _write_daily(data_dir, table: str, ds: str, symbols: list[str], close: float) -> None:
    out = data_dir / table / f"date={ds}" / "part.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": symbols,
        "date": [date.fromisoformat(ds)] * len(symbols),
        "open": [close] * len(symbols),
        "high": [close] * len(symbols),
        "low": [close] * len(symbols),
        "close": [close] * len(symbols),
        "volume": [100.0] * len(symbols),
        "amount": [1000.0] * len(symbols),
    }).write_parquet(out)


def _write_enriched(data_dir, table: str, ds: str, symbols: list[str], close: float) -> None:
    out = data_dir / table / f"date={ds}" / "part.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": symbols,
        "date": [date.fromisoformat(ds)] * len(symbols),
        "close": [close] * len(symbols),
    }).write_parquet(out)


def _fake_compute_enriched(raw: pl.DataFrame, **_kwargs) -> pl.DataFrame:
    return raw.with_columns(
        pl.col("close").alias("raw_close"),
        pl.col("high").alias("raw_high"),
        pl.col("low").alias("raw_low"),
    )


def test_find_missing_cells_only_reports_gaps_against_daily(tmp_path):
    _write_daily(tmp_path, "kline_etf_daily", "2026-09-16", ["A", "B"], 1.0)
    _write_daily(tmp_path, "kline_etf_daily", "2026-09-18", ["A", "B"], 2.0)
    _write_enriched(tmp_path, "kline_etf_enriched", "2026-09-16", ["A", "B"], 1.0)
    _write_enriched(tmp_path, "kline_etf_enriched", "2026-09-18", ["A"], 2.0)

    missing = enriched_backfill.find_missing_cells(
        tmp_path, "kline_etf_daily", "kline_etf_enriched",
        ["2026-09-16", "2026-09-18"],
    )

    assert missing == {"2026-09-18": ["B"]}


def test_compute_gap_rows_fills_only_missing_with_history_prefix(tmp_path, monkeypatch):
    _write_daily(tmp_path, "kline_etf_daily", "2026-09-16", ["A", "B"], 1.0)
    _write_daily(tmp_path, "kline_etf_daily", "2026-09-18", ["A", "B"], 2.0)
    _write_enriched(tmp_path, "kline_etf_enriched", "2026-09-18", ["A"], 2.0)
    monkeypatch.setattr(enriched_backfill.pipeline, "compute_enriched", _fake_compute_enriched)

    rows = enriched_backfill.compute_gap_rows(
        tmp_path, "kline_etf_daily", {"2026-09-18": ["B"]},
    )

    # 只产出缺失格；已有 (09-18, A) 不在输出里，merge-upsert 不可能覆盖它
    assert rows.select("symbol").to_series().to_list() == ["B"]
    assert rows.select("date").to_series().to_list() == [date(2026, 9, 18)]
    # 带了历史前缀算指标：输入应含 09-16 的 B 行（fake 直接透传 close，可验证 close=2.0 是目标日）
    assert rows["close"].to_list() == [2.0]


def test_compute_gap_rows_empty_when_no_gaps(tmp_path, monkeypatch):
    _write_daily(tmp_path, "kline_etf_daily", "2026-09-18", ["A"], 2.0)
    monkeypatch.setattr(enriched_backfill.pipeline, "compute_enriched", _fake_compute_enriched)

    rows = enriched_backfill.compute_gap_rows(tmp_path, "kline_etf_daily", {})

    assert rows.is_empty()
