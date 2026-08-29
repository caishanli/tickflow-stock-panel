"""f51e08f9 小市值策略对齐回测：写入 quant.db 并派生官方 worker 子进程。"""
import json
import subprocess
import sys
import uuid

from app.quant import db

PARAMS = {
    "name": "大小外择时小市值3.0-本地对齐",
    "strategy_id": "f51e08f9",
    "start": sys.argv[1] if len(sys.argv) > 1 else "2026-04-01",
    "end": sys.argv[2] if len(sys.argv) > 2 else "2026-08-21",
    "frequency": "1m",
    "capital": 1000000.0,
    "fee": 0.0003,
    "min_commission": 5,
    "slippage": 0,
    "stop_loss": 0,          # 关闭账户级止损注入（聚宽原始策略口径）
    "benchmark": "000300.XSHG",
    "symbols": ["000300.XSHG", "518880.XSHG", "513030.XSHG", "513100.XSHG",
                "164824.XSHE", "159866.XSHE", "399101.XSHE", "511010.XSHG"],
}

run_id = uuid.uuid4().hex[:8]
params = dict(PARAMS, run_id=run_id)
db.init_db()
db.upsert_run(run_id, PARAMS["strategy_id"], PARAMS["name"],
              json.dumps(params, ensure_ascii=False), "running")
print("run_id:", run_id)
subprocess.run([sys.executable, "scripts/run_quant_backtest.py", run_id])
print("worker exited")
