#!/usr/bin/env python3
"""Normalize kline_etf_daily amount/volume units to 元/股.

背景：ETF 日线历史曾由多个入口写入 DuckDB，单位不统一——
- mootdx_src.get_daily 正规化为 volume=股、amount/money=元；
- fetch_etf_daily.py / backfill_etf_daily.py 直接存通达信原始单位
  volume=手、amount=千元（部分还混入正确帧），导致 amount/(close*volume)
  的中位数在同一张表里分成 ~0.1（千元）与 ~100（元）两组。

本脚本按 symbol 计算成交额/价格×量 比率的中位数来判组：
- m < 0.5          → amount 单位千元，×1000 归一为元
- 归一后 m~100     → volume 单位手，×100 归一为股
规则阈值经全表抽样验证（589700 等 0.5<m<1 的噪点帧不会被误判）。

用法：uv run python scripts/normalize_etf_daily_units.py [--dry-run]
"""
import os
import sys

import duckdb

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "stock.duckdb")


def main():
    dry = "--dry-run" in sys.argv
    conn = duckdb.connect(DB)
    syms = conn.execute(
        """
        SELECT symbol, MEDIAN(amount / NULLIF(close * volume, 0)) AS m
        FROM kline_etf_daily
        WHERE close > 0 AND volume > 0 AND amount > 0
        GROUP BY symbol
        """
    ).fetchall()
    plan = {}
    for sym, m in syms:
        if m is None:
            continue
        amount_scale = 1000.0 if m < 0.5 else 1.0
        adj = m * amount_scale
        vol_scale = 100.0 if 50 < adj < 200 else 1.0
        if amount_scale != 1.0 or vol_scale != 1.0:
            plan[sym] = (amount_scale, vol_scale)
    print(f"symbols to normalize: {len(plan)} / {len(syms)}")
    if dry:
        for sym, (a, v) in sorted(plan.items())[:10]:
            print(f"  {sym}: amount x{a:.0f}, volume x{v:.0f}")
        print("  ...")
        conn.close()
        return
    n_rows = 0
    for sym, (a, v) in plan.items():
        cur = conn.execute(
            "UPDATE kline_etf_daily SET amount = amount * ?, volume = volume * ? "
            "WHERE symbol = ?",
            [a, v, sym],
        )
        n_rows += cur.rowcount
    conn.execute("CHECKPOINT")
    conn.close()
    print(f"updated rows: {n_rows}")


if __name__ == "__main__":
    main()
