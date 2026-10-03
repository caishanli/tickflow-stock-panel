"""连板策略本地回测 runner：run_jq_backtest + 全市场股票宇宙。

用法（在 backend/ 下）：
  uv run python scripts/xsb_opt/run_lb_bt.py --start 2026-07-10 --end 2026-09-04 \
      --strategy scripts/xsb_opt/xsb_lb_base.py --out /tmp/xsb_opt/base --tag base

调优参数经环境变量 P_* 传入（见策略文件头部）。
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--strategy", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--cash", type=float, default=100000.0)
    ap.add_argument("--stop-loss", type=float, default=0.0,
                    help="账户级止损注入（<=0 关闭，与模拟盘口径对齐时传 0.02）")
    ap.add_argument("--benchmark", default="000300.XSHG")
    ap.add_argument("--etf", action="store_true",
                    help="ETF 策略模式：不传 universe（走引擎默认全市场 ETF 宇宙）")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    params = {
        "start": args.start,
        "end": args.end,
        "capital": args.cash,
        "fee": 0.0003,
        "slippage": float(os.getenv("BT_SLIPPAGE", "0.0001")),
        "benchmark": args.benchmark,
        "minute_cache_cap": 800,
        "log_level": "info",
        "out_dir": os.path.abspath(args.out),
        "strategy_id": "xsb_lb",
        "stop_loss": args.stop_loss,
    }
    if not args.etf:
        params["universe"] = "all_stocks"
    from app.quant.rqalpha_bridge import run_jq_backtest

    res = run_jq_backtest(os.path.abspath(args.strategy), params)
    if "error" in res:
        print("ERROR:", res["error"])
        sys.exit(1)
    tag = args.tag or os.path.basename(args.out)
    print(f"[{tag}] trades_csv:", res.get("trades_csv"))
    print(f"[{tag}] equity_csv:", res.get("equity_csv"))
    print(f"[{tag}] n_trades:", res.get("n_trades"))
    print(f"[{tag}] final_equity:", res.get("final_equity"))
    print(f"[{tag}] metrics:", res.get("metrics"))


if __name__ == "__main__":
    main()
