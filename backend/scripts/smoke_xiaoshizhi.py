"""进程内冒烟：f51e08f9 小市值策略短窗回测，快速暴露兼容层问题。"""
import sys
import time

sys.path.insert(0, ".")

from app.quant.rqalpha_bridge import run_jq_backtest

STRATEGY = "../data/quant_strategies/f51e08f9.py"
start = sys.argv[1] if len(sys.argv) > 1 else "2026-07-30"
end = sys.argv[2] if len(sys.argv) > 2 else "2026-08-05"

params = {
    "run_id": "smoke_xs",
    "strategy_id": "f51e08f9",
    "name": "smoke",
    "start": start,
    "end": end,
    "frequency": "1m",
    "capital": 1000000.0,
    "fee": 0.0003,
    "min_commission": 5,
    "slippage": 0,
    "stop_loss": 0,
    "benchmark": "000300.XSHG",
    "symbols": ["000300.XSHG", "399101.XSHE"],
}
t0 = time.time()
try:
    res = run_jq_backtest(STRATEGY, params, db_path=None)
    print("RESULT:", res)
except Exception as e:
    import traceback
    traceback.print_exc()
finally:
    print(f"elapsed: {time.time() - t0:.1f}s")
