#!/usr/bin/env python3
"""补齐本地日线缓存中截断到 2026-01-30 的 ETF 日线到 2026-07-08。

根因：本地日线缓存多数 ETF 日线只拉到 2026-01-30，导致 2-7 月回测时
候选池几乎为空、选股严重错位。本脚本用 tushare 回源补齐并落盘（符合 C3）。

用法:
  python scripts/backfill_daily.py [--end 20260708] [--sleep 0.25]
"""
import argparse
import os
import sys
import time
import logging

logging.disable(logging.CRITICAL)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

from app.quant.jqengine.datasource.manager import get_data_manager


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", default="20260708")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--start", default="20260101")
    ap.add_argument("--force", action="store_true", help="忽略末端检查，全量重补")
    args = ap.parse_args()

    dm = get_data_manager()
    tok = os.environ.get("TUSHARE_TOKEN") or dm.sources["tushare"].token
    dm.sources["tushare"].token = tok
    import tushare as ts
    ts.set_token(tok)

    all_d = dm.cache.get_all("daily")
    trunc = []
    for k, df in all_d.items():
        if df is None or getattr(df, "empty", True):
            continue
        col = "trade_date" if "trade_date" in df.columns else ("date" if "date" in df.columns else None)
        if col is None:
            continue
        last = str(df[col].max())
        if args.force or last < args.end:
            # key 形如 tushare_588710.XSHG
            src, raw = k.split("_", 1)
            trunc.append((raw, last))
    print(f"[backfill] 截断 ETF 共 {len(trunc)} 只，目标末端 >= {args.end}")

    ok = fail = 0
    for i, (code, last) in enumerate(trunc):
        for attempt in range(3):
            try:
                df = dm.sources["tushare"].get_daily(code, args.start, args.end)
                if df is not None and not df.empty:
                    # 写库：cache.put 走 daily 频率校验
                    dm.cache.put("daily", f"tushare_{code}", df)
                    ok += 1
                    break
            except Exception:
                time.sleep(args.sleep * (attempt + 1))
        else:
            fail += 1
        if args.sleep:
            time.sleep(args.sleep)
        if (i + 1) % 100 == 0:
            print(f"[backfill] 进度 {i+1}/{len(trunc)} ok={ok} fail={fail}")
    print(f"[backfill] 完成 ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
