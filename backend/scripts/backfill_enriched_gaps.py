"""backfill_enriched_gaps.py — 本地补算 enriched 缺格，不碰网络/调度。

背景：2026-09-18 盘后管道先于备用链回补跑完，股票 enriched 缺 9-18 一天；
ETF enriched 历史上只在 9-14/15/16/18 落过分区且都不全（增量只算当轮拉到的 chunk）。

用法（backend/ 下）：
  uv run --extra dev python scripts/backfill_enriched_gaps.py <check|repair|verify>

  check  只打印三表缺口（不写数据）
  repair 股票走 run_pipeline(new_dates_only=True)；ETF 按日线基线找缺格、
         本地日线+60天前缀 compute_enriched 后 merge-upsert；指数只核对
  verify 修完后复查（应无缺口）
"""
from __future__ import annotations

import argparse
import os
import sys

BACKEND = os.path.dirname(os.path.abspath(__file__))
if os.path.isdir(os.path.join(BACKEND, "app")):
    sys.path.insert(0, BACKEND)


def _part_days(base, table: str) -> list[str]:
    root = base / table
    if not root.is_dir():
        return []
    return sorted(p.name[5:] for p in root.glob("date=*") if p.is_dir())


def _cover_days(base, daily_table: str, enriched_table: str) -> list[str]:
    """回填覆盖窗口：max(已有 enriched 最早日) 起的全部 daily 日。

    只往已有覆盖窗口内和之后填，不往从未建过 enriched 的远古历史重建。
    """
    daily = set(_part_days(base, daily_table))
    enriched = set(_part_days(base, enriched_table))
    if not daily:
        return []
    start = min(enriched) if enriched else sorted(daily)[-5]
    return sorted(d for d in daily if d >= start)


def check() -> int:
    import polars as pl

    from app.config import settings
    from app.services import enriched_backfill

    base = settings.data_dir
    issues = 0
    daily_days = set(_part_days(base, "kline_daily"))
    for ds in sorted(set(_part_days(base, "kline_daily_enriched")) | daily_days):
        dp = base / "kline_daily" / f"date={ds}" / "part.parquet"
        ep = base / "kline_daily_enriched" / f"date={ds}" / "part.parquet"
        if dp.exists() and not ep.exists():
            n = pl.scan_parquet(dp).select(pl.len()).collect().item()
            print(f"  stock {ds}: enriched 缺分区 (daily {n} 行)")
            issues += 1
    etf_days = _cover_days(base, "kline_etf_daily", "kline_etf_enriched")
    missing = enriched_backfill.find_missing_cells(
        base, "kline_etf_daily", "kline_etf_enriched", etf_days,
    )
    for ds, syms in missing.items():
        print(f"  etf {ds}: 缺 {len(syms)} 只")
        issues += len(syms)
    idx_days = _cover_days(base, "kline_index_daily", "kline_index_enriched")
    idx_missing = enriched_backfill.find_missing_cells(
        base, "kline_index_daily", "kline_index_enriched", idx_days,
    )
    for ds, syms in idx_missing.items():
        print(f"  index {ds}: 缺 {len(syms)} 只")
        issues += len(syms)
    if not issues:
        print("无缺口")
    return issues


def repair() -> None:
    import polars as pl

    from app.config import settings
    from app.indicators import pipeline as ind_pipeline
    from app.jobs import daily_pipeline
    from app.services import enriched_backfill, index_sync
    from app.tickflow.repository import DataStore, KlineRepository

    base = settings.data_dir
    repo = KlineRepository(DataStore())

    print("[repair] stock: run_pipeline(new_dates_only=True) ...", flush=True)
    written = ind_pipeline.run_pipeline(data_dir=base, new_dates_only=True)
    print(f"[repair] stock: +{written} 行")
    # 股票分区内部缺格（分区存在但比日线薄，如被误删后只合并了部分标的）：
    # 按日线基线找缺格，本地日线+60天前缀重算后 merge-upsert
    stock_days = sorted(set(_part_days(base, "kline_daily")) & set(_part_days(base, "kline_daily_enriched")))
    stock_missing = enriched_backfill.find_missing_cells(
        base, "kline_daily", "kline_daily_enriched", stock_days,
    )
    stock_total = sum(len(v) for v in stock_missing.values())
    print(f"[repair] stock: {len(stock_missing)} 天共缺 {stock_total} 格", flush=True)
    if stock_total:
        factors = ind_pipeline._load_factors(base / "adj_factor" / "all.parquet")
        try:
            instruments = pl.scan_parquet(
                str(base / "instruments" / "**" / "*.parquet")).collect()
        except Exception:
            instruments = pl.DataFrame()
        shares = ind_pipeline.load_share_history(base)
        rows = enriched_backfill.compute_gap_rows(
            base, "kline_daily", stock_missing,
            factors=factors, instruments=instruments if not instruments.is_empty() else None,
            historical_shares=shares,
        )
        print(f"[repair] stock: 算出 {rows.height} 行，落盘 ...", flush=True)
        repo.append_enriched(rows)
    daily_pipeline._refresh_single_view(repo, "kline_enriched")

    etf_days = _cover_days(base, "kline_etf_daily", "kline_etf_enriched")
    missing = enriched_backfill.find_missing_cells(
        base, "kline_etf_daily", "kline_etf_enriched", etf_days,
    )
    total = sum(len(v) for v in missing.values())
    print(f"[repair] etf: {len(missing)} 天共缺 {total} 格", flush=True)
    if total:
        factors = index_sync._load_etf_factors(repo)
        rows = enriched_backfill.compute_gap_rows(
            base, "kline_etf_daily", missing, factors=factors, instruments=None,
        )
        print(f"[repair] etf: 算出 {rows.height} 行，落盘 ...", flush=True)
        repo.append_etf_enriched(rows)
    idx_days = _cover_days(base, "kline_index_daily", "kline_index_enriched")
    idx_missing = enriched_backfill.find_missing_cells(
        base, "kline_index_daily", "kline_index_enriched", idx_days,
    )
    idx_total = sum(len(v) for v in idx_missing.values())
    print(f"[repair] index: {len(idx_missing)} 天共缺 {idx_total} 格", flush=True)
    if idx_total:
        idx_rows = enriched_backfill.compute_gap_rows(
            base, "kline_index_daily", idx_missing, factors=None, instruments=None,
        )
        print(f"[repair] index: 算出 {idx_rows.height} 行，落盘 ...", flush=True)
        repo.append_index_enriched(idx_rows)
    repo.refresh_index_views()
    print("[repair] 视图已刷新")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["check", "repair", "verify"])
    args = ap.parse_args()
    if args.action == "repair":
        repair()
    issues = check()
    if args.action == "verify":
        sys.exit(1 if issues else 0)


if __name__ == "__main__":
    main()
