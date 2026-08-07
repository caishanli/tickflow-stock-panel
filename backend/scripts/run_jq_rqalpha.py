#!/usr/bin/env python3
"""跑 rqalpha 版 五福闹新春-v5.2（包裹 DataManager 原始缓存），导出 trades/equity。

用法:
  python scripts/run_jq_rqalpha.py [--start 2026-01-01] [--end 2026-07-08] \
      [--strategy .../wufu-v5.2.py] [--out data/quant_sim/jqwufu] \
      [--cash 100000] [--fee 0.0001] [--slippage 0.0001] [--benchmark 510300.XSHG]
"""
import argparse
import os
import sys
import warnings

# 抑制 pandas/numpy 等第三方库在热循环（每日×全池动量计算）中触发的大量
# warning（每次格式化 + 读源码行极慢），避免回测多耗数分钟。
warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from app.quant.rqalpha_bridge import run_jq_backtest
from app.quant import jqcompat as _jqcompat

_BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.environ.get(
    "WUFU_REPO",
    os.path.join(_BACKEND, "tests", "fixtures", "wufu_v52"),
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-07-08")
    ap.add_argument(
        "--strategy",
        default=os.path.join(REPO, "wufu-v5.2.py"),
    )
    ap.add_argument("--out", default=os.path.join("data", "quant_sim", "jqwufu"))
    ap.add_argument("--cash", type=float, default=100000.0)
    ap.add_argument("--fee", type=float, default=0.0001)
    ap.add_argument("--slippage", type=float, default=0.0001)
    ap.add_argument("--benchmark", default="510300.XSHG")
    ap.add_argument("--minute_cache_cap", type=int, default=800)
    ap.add_argument("--log_level", default="error")
    ap.add_argument(
        "--prev-close",
        action="store_true",
        help="current_price 用昨收（方案A：对齐聚宽 13:10 last_price≈昨收）",
    )
    args = ap.parse_args()

    _jqcompat.set_use_prev_close(args.prev_close)

    params = {
        "start": args.start,
        "end": args.end,
        "capital": args.cash,
        "fee": args.fee,
        "slippage": args.slippage,
        "benchmark": args.benchmark,
        "minute_cache_cap": args.minute_cache_cap,
        "log_level": args.log_level,
        "out_dir": os.path.abspath(args.out),
        "strategy_id": "wufu-v5.2",
    }
    res = run_jq_backtest(args.strategy, params)
    if "error" in res:
        print("ERROR:", res["error"])
        sys.exit(1)
    print("trades_csv:", res.get("trades_csv"))
    print("equity_csv:", res.get("equity_csv"))
    print("n_trades:", res.get("n_trades"))
    print("final_equity:", res.get("final_equity"))
    print("metrics:", res.get("metrics"))
    print("universe_size:", res.get("universe_size"))


if __name__ == "__main__":
    main()
