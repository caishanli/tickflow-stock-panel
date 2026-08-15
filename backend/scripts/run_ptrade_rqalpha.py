#!/usr/bin/env python3
"""跑 rqalpha 版 wufu v5.4 双持仓 ptrade 策略, 导出 trades/equity。

用法:
  python scripts/run_ptrade_rqalpha.py [--start 2026-04-01] [--end 2026-08-11] \
      [--strategy .../wufu-v5.4-dual-adapt.ptrade.py] [--out data/quant_sim/ptradedual] \
      [--cash 100000] [--fee 0.0001] [--slippage 0.0001]
"""
import argparse
import os
import sys
import warnings

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from app.quant.rqalpha_bridge import run_ptrade_backtest  # noqa: E402

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.join(BACKEND, "tests", "fixtures", "dual_v54")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-04-01")
    ap.add_argument("--end", default="2026-08-11")
    ap.add_argument("--strategy", default=os.path.join(REPO, "wufu-v5.4-dual-adapt.ptrade.py"))
    ap.add_argument("--out", default=os.path.join("data", "quant_sim", "ptradedual"))
    ap.add_argument("--cash", type=float, default=100000.0)
    ap.add_argument("--fee", type=float, default=0.0001)
    ap.add_argument("--slippage", type=float, default=0.0001)
    ap.add_argument("--minute_cache_cap", type=int, default=800)
    ap.add_argument("--log_level", default="error")
    args = ap.parse_args()

    params = {
        "start": args.start,
        "end": args.end,
        "capital": args.cash,
        "fee": args.fee,
        "slippage": args.slippage,
        "minute_cache_cap": args.minute_cache_cap,
        "log_level": args.log_level,
        "out_dir": os.path.abspath(args.out),
        "strategy_id": "wufu-v5.4-dual-adapt-ptrade",
    }
    res = run_ptrade_backtest(args.strategy, params)
    if "error" in res:
        print("ERROR:", res["error"])
        sys.exit(1)
    print("trades_csv:", res.get("trades_csv"))
    print("equity_csv:", res.get("equity_csv"))
    print("n_trades:", res.get("n_trades"))
    print("final_equity:", res.get("final_equity"))
    print("metrics:", res.get("metrics"))


if __name__ == "__main__":
    main()
