#!/usr/bin/env python3
"""用 mootdx 补齐分钟数据缺失的 ETF（最新日期 < 截止日）到按日分区 Parquet。

背景：data/kline_etf_minute/date=*/part.parquet 中约 20 只 ETF 的分钟数据
截止到 2026-04-23（历史回填时中断），后续日期缺失。这导致回测中这些标的在
缺失期被 is_temporarily_suspended 误判为停牌、无法交易（对齐 bug：159985 等）。

用 mootdx 拉历史分钟（分页回看），写入缺失日期的 date=*/part.parquet 分区。
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pandas as pd
import polars as pl

_REPO_ROOT = Path(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))
ROOT = Path(os.getenv("PARTITION_DATA_ROOT", _REPO_ROOT / "data")) / "kline_etf_minute"
CUTOFF = "2026-04-24"   # 只补齐这之后缺失的日期
END = "2026-07-31"      # 拉取上界


def find_incomplete() -> list[str]:
    """扫描分区，返回最新分钟日期 < CUTOFF 的 symbol（.SH/.SZ）。"""
    lf = pl.scan_parquet(str(ROOT / "**" / "*.parquet"), hive_partitioning=True)
    df = lf.select(["symbol", "datetime"]).collect()
    agg = df.group_by("symbol").agg(pl.col("datetime").max().alias("max_ts"))
    incomplete = agg.filter(pl.col("max_ts") < pd.Timestamp(CUTOFF))
    return sorted(incomplete["symbol"].to_list())


def fetch_and_write(sym: str) -> int:
    from app.quant.jqengine.datasource.mootdx_src import MootdxSource
    src = MootdxSource()
    df = src.get_minute(sym, max_bars=30000)
    if df is None or df.empty:
        return 0
    # 转标准列：mootdx 返回含 year/month/day 等冗余列，只保留 OHLCV+amount
    out = df.reset_index()
    out = out.rename(columns={"index": "datetime"})
    out["symbol"] = sym
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    for c in keep:
        if c not in out.columns:
            # mootdx 用 vol，无 volume
            if c == "volume" and "vol" in out.columns:
                out["volume"] = out["vol"]
            elif c == "amount" and "money" in out.columns:
                out["amount"] = out["money"]
    out = out[[c for c in keep if c in out.columns]]
    out["datetime"] = pd.to_datetime(out["datetime"])
    out = out[out["datetime"] >= pd.Timestamp(CUTOFF)]
    if out.empty:
        return 0
    # 按日期分组写分区（pandas 分组）
    out = out.assign(_date=pd.to_datetime(out["datetime"]).dt.date)
    written = 0
    for day, sub in out.groupby("_date"):
        d = str(day)
        pdir = ROOT / f"date={d}"
        pdir.mkdir(parents=True, exist_ok=True)
        part = pdir / "part.parquet"
        sub = sub.drop(columns=["_date"]).sort_values("datetime")
        sub_pl = pl.from_pandas(sub)
        # 统一 datetime 为 us（与现有分区一致），否则 concat vstack 失败
        sub_pl = sub_pl.with_columns(pl.col("datetime").cast(pl.Datetime("us")))
        if part.exists():
            old = pl.read_parquet(part)
            merged = pl.concat([old, sub_pl]).unique(
                subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
            tmp = pdir / "part.tmp"
            merged.write_parquet(tmp)
            tmp.rename(part)
        else:
            tmp = pdir / "part.tmp"
            sub_pl.write_parquet(tmp)
            tmp.rename(part)
        written += len(sub)
    return written


def main() -> int:
    syms = find_incomplete()
    print(f"缺失标的: {len(syms)} 只")
    total = 0
    for sym in syms:
        try:
            n = fetch_and_write(sym)
            print(f"  {sym}: 补 {n} 行")
            total += n
        except Exception as e:
            print(f"  {sym}: 失败 {e}")
    print(f"完成: 共补 {total} 行")
    return 0


if __name__ == "__main__":
    sys.exit(main())
