"""backfill_bj_0922.py — 手动补 2026-09-22 北交所日线(344只)及 enriched 缺格。

背景：9-22 15:30 管线拉取时 TickFlow 北交所当日 bar 未发布，空帧被静默吞掉，
kline_daily/date=2026-09-22 只有 5208 行（沪深），BJ 全缺；enriched 9-22 也是
按残缺日线算的，同样缺 BJ。

用法（backend/ 下）：
  uv run --extra dev python scripts/backfill_bj_0922.py

幂等：落盘全是 merge-upsert，可重跑。
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

BACKEND = os.path.dirname(os.path.abspath(__file__))
if os.path.isdir(os.path.join(BACKEND, "app")):
    sys.path.insert(0, BACKEND)


def main() -> None:
    import polars as pl

    from app.config import settings
    from app.indicators import pipeline as ind_pipeline
    from app.jobs import daily_pipeline
    from app.services import kline_sync
    from app.tickflow.policy import detect_capabilities
    from app.tickflow.repository import DataStore, KlineRepository

    base = settings.data_dir
    repo = KlineRepository(DataStore())
    capset = detect_capabilities()

    print("[1/3] 北交所日K [2026-09-21 ~ 2026-09-22] ...", flush=True)
    n = kline_sync.complement_daily_bj(
        repo, capset, start_date=datetime(2026, 9, 21), end_date=datetime(2026, 9, 22, 23, 59),
    )
    print(f"[1/3] 写入 {n} 行", flush=True)

    day = base / "kline_daily" / "date=2026-09-22" / "part.parquet"
    syms = pl.scan_parquet(day).select("symbol").collect()["symbol"].to_list()
    bj = sorted(s for s in syms if s.endswith(".BJ"))
    print(f"[2/3] 9-22 日线共 {len(syms)} 只，其中 BJ {len(bj)} 只", flush=True)
    if bj:
        print(f"[2/3] enriched 重算 {len(bj)} 只 BJ（全日期合并）...", flush=True)
        w = ind_pipeline.run_pipeline(data_dir=base, symbols=bj)
        print(f"[2/3] +{w} 行", flush=True)
        daily_pipeline._refresh_single_view(repo, "kline_enriched")

    print("[3/3] 核对 ...", flush=True)
    d = set(pl.scan_parquet(day).select("symbol").collect()["symbol"].to_list())
    e = set(pl.scan_parquet(base / "kline_daily_enriched" / "date=2026-09-22" / "*.parquet")
            .select("symbol").collect()["symbol"].to_list())
    print(f"9-22 daily={len(d)} enriched={len(e)} 缺口={len(d - e)}")


if __name__ == "__main__":
    main()
