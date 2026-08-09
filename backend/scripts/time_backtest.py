"""计时回测：跑五福策略，输出各阶段耗时。"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from app.quant.rqalpha_bridge import run_jq_backtest
from app.quant.config import CONFIG

strategy_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..",
    "app", "quant", "strategies", "samples", "wufu_etf_rotation.py"
)

params = {
    "start": "2026-01-01",
    "end": "2026-07-08",
    "benchmark": "510300.XSHG",
    "minute_cache_cap": 800,
}

t0 = time.time()
result = run_jq_backtest(strategy_path, params, db_path=CONFIG.db_path)
elapsed = time.time() - t0
print(f"\n===== 回测总耗时: {elapsed:.1f}s =====")
if isinstance(result, dict):
    for k, v in result.items():
        if k != "trades":
            print(f"  {k}: {v}")
