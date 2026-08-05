#!/usr/bin/env python
"""baostock 全市场近 3 年回源 CLI（股票 5min + ETF/指数日线 + 复权因子 + 分红送转）。

用法:
  python scripts/backfill_baostock_3y.py                         # 全部 stage
  python scripts/backfill_baostock_3y.py --stage minute          # 只回源股票 5min
  python scripts/backfill_baostock_3y.py --stage daily           # ETF/指数日线
  python scripts/backfill_baostock_3y.py --stage corporate       # 因子+分红
  python scripts/backfill_baostock_3y.py --limit 3               # 冒烟（各 stage 只处理 3 只）

断点续传：data/baostock_backfill_state.json，中断后重跑自动跳过已完成标的；
--retry-failed 重试上次失败标的，--reset-state 清空状态重跑。
"""
import argparse
import logging
import os
import sys
from datetime import date as _date
from datetime import timedelta as _td

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import baostock_backfill as bb


def _default_start() -> _date:
    return _date.today() - _td(days=365 * 3)


def main() -> None:
    ap = argparse.ArgumentParser(description="baostock 全市场近 3 年回源")
    ap.add_argument("--start", type=_date.fromisoformat, default=None,
                    help="起始日期 YYYY-MM-DD（默认 3 年前）")
    ap.add_argument("--end", type=_date.fromisoformat, default=None,
                    help="结束日期 YYYY-MM-DD（默认今天）")
    ap.add_argument("--stage", choices=["minute", "daily", "corporate", "all"],
                    default="all")
    ap.add_argument("--reset-state", action="store_true", help="清空断点状态重跑")
    ap.add_argument("--retry-failed", action="store_true", help="重试失败标的")
    ap.add_argument("--timeout", type=float, default=300.0, help="单请求墙钟超时秒")
    ap.add_argument("--flush-batch", type=int, default=100, help="攒满多少只批量写分区")
    ap.add_argument("--limit", type=int, default=None, help="每 stage 最多处理标的数（冒烟用）")
    args = ap.parse_args()

    start = args.start or _default_start()
    end = args.end or _date.today()
    if start >= end:
        print(f"start({start}) 必须早于 end({end})")
        sys.exit(1)

    if args.reset_state and bb.STATE_PATH.exists():
        bb.STATE_PATH.unlink()
        print("已清空断点状态")
    state = bb.load_state()
    state["start"] = start.isoformat()
    state["end"] = end.isoformat()
    bb.save_state(state)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    import baostock as bs

    res = bs.login()
    if res is None or getattr(res, "error_code", "0") != "0":
        print(f"[baostock-backfill] login 失败: {getattr(res, 'error_msg', '')}", flush=True)
        sys.exit(1)
    progress = bb.make_progress_printer()
    print(f"[baostock-backfill] {start} ~ {end} stage={args.stage} ...", flush=True)

    if args.stage in ("minute", "all"):
        bb.sync_minute(start, end, state, timeout=args.timeout,
                       flush_batch=args.flush_batch, retry_failed=args.retry_failed,
                       limit=args.limit, progress=progress)
    if args.stage in ("daily", "all"):
        bb.sync_daily(start, end, state, timeout=args.timeout,
                      retry_failed=args.retry_failed, limit=args.limit,
                      progress=progress)
    if args.stage in ("corporate", "all"):
        bb.sync_corporate(start, end, state, timeout=args.timeout,
                          retry_failed=args.retry_failed, limit=args.limit,
                          progress=progress)
    print("[baostock-backfill] 完成", flush=True)


if __name__ == "__main__":
    main()
