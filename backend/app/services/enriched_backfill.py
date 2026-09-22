"""enriched 缺格回填：以本地日线分区为基线，只计算缺失的 (day, symbol)。

只产出缺失格 —— 调用方用 repo.append_*_enriched (merge-upsert) 落盘时，
不可能覆盖已有行。
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.indicators import pipeline


def find_missing_cells(
    data_dir: Path,
    daily_table: str,
    enriched_table: str,
    days: list[str],
) -> dict[str, list[str]]:
    """对给定日期，以 daily 的 symbol 集合为基线，找出 enriched 缺失的标的。"""
    missing: dict[str, list[str]] = {}
    for ds in days:
        dpath = data_dir / daily_table / f"date={ds}" / "part.parquet"
        if not dpath.exists():
            continue
        daily_syms = set(
            pl.scan_parquet(dpath).select("symbol").collect()["symbol"].to_list()
        )
        epath = data_dir / enriched_table / f"date={ds}" / "part.parquet"
        enr_syms: set[str] = set()
        if epath.exists():
            enr_syms = set(
                pl.scan_parquet(epath).select("symbol").collect()["symbol"].to_list()
            )
        gap = sorted(daily_syms - enr_syms)
        if gap:
            missing[ds] = gap
    return missing


def compute_gap_rows(
    data_dir: Path,
    daily_table: str,
    missing: dict[str, list[str]],
    factors: pl.DataFrame | None = None,
    instruments: pl.DataFrame | None = None,
    history_days: int = 60,
) -> pl.DataFrame:
    """计算缺失格的 enriched 行（含历史前缀供指标窗口使用，输出只保留目标格）。"""
    if not missing:
        return pl.DataFrame()
    syms = sorted({s for cells in missing.values() for s in cells})
    target_days = sorted(missing)
    start = date.fromisoformat(target_days[0]) - timedelta(days=history_days)
    end = date.fromisoformat(target_days[-1])
    frames: list[pl.DataFrame] = []
    daily_base = data_dir / daily_table
    for part in sorted(daily_base.glob("date=*")):
        ds = part.name[5:]
        if not (start.isoformat() <= ds <= end.isoformat()):
            continue
        frames.append(
            pl.scan_parquet(part / "*.parquet")
            .filter(pl.col("symbol").is_in(syms))
            .collect()
        )
    if not frames:
        return pl.DataFrame()
    raw = pl.concat(frames, how="diagonal_relaxed").sort(["symbol", "date"])
    if raw.is_empty():
        return pl.DataFrame()
    out = pipeline.compute_enriched(raw, factors=factors, instruments=instruments)
    if out.is_empty():
        return out
    want = pl.DataFrame({
        "symbol": [s for ds in target_days for s in missing[ds]],
        "date": [
            date.fromisoformat(ds)
            for ds in target_days
            for _ in missing[ds]
        ],
    })
    return out.join(want, on=["symbol", "date"], how="semi").sort(["symbol", "date"])
