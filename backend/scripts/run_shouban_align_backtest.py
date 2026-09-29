"""一次性对齐驱动：建 5313ae33 回测 run 行并直接拉起子进程（stdout 落盘）。

用法（backend/ 下）：
    uv run python scripts/run_shouban_align_backtest.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import uuid

from dotenv import load_dotenv

load_dotenv()

from app.quant import db  # noqa: E402
from app.quant.strategies.store import get_strategy  # noqa: E402

RUN_ID = "sb" + uuid.uuid4().hex[:6]
STRATEGY_ID = "5313ae33"
START = "2026-07-10"
END = "2026-09-28"

s = get_strategy(STRATEGY_ID)
params = {
    "name": s["name"],
    "strategy_id": STRATEGY_ID,
    "strategy_code": s["code"],
    "symbols": [],
    "start": START,
    "end": END,
    "frequency": "1m",
    "capital": 100000.0,
    "fee": 0.0003,
    "slippage": 0.0001,
    "universe": "all_stocks",
    # 对齐聚宽 fixture：无账户级止损层（策略自带 -5% 硬止损）
    "stop_loss": 0,
    "run_id": RUN_ID,
    "out_dir": "data/quant_sim/jqwufu",
}
payload = json.dumps(params, ensure_ascii=False)
db.upsert_run(RUN_ID, STRATEGY_ID, s["name"], payload, "queued")
print("run_id:", RUN_ID)

log_path = f"/tmp/backtest_{RUN_ID}.log"
with open(log_path, "w") as f:
    proc = subprocess.Popen(
        [sys.executable, "scripts/run_quant_backtest.py", RUN_ID],
        stdout=f, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
db.set_run_pid(RUN_ID, proc.pid)
print("pid:", proc.pid, "log:", log_path)
