"""回测独立进程：由 FastAPI 派生子进程启动，经 quant.db 与前端通信。"""
from __future__ import annotations

import json
import sys

from app.quant import db
from app.quant.config import CONFIG
from app.quant.rqalpha_bridge import run_backtest
from app.quant.datasource.manager import QuantDataProvider
from app.quant.strategies.store import get_strategy


def main():
    if len(sys.argv) < 2:
        print("usage: run_quant_backtest.py <run_id>", file=sys.stderr)
        sys.exit(1)
    run_id = sys.argv[1]
    run = db.get_run(run_id)
    if not run:
        print(f"run not found: {run_id}", file=sys.stderr)
        sys.exit(1)
    params = json.loads(run["params_json"])
    strategy_id = params.get("strategy_id", "")
    code = ""
    if strategy_id:
        s = get_strategy(strategy_id)
        code = s["code"] if s else ""
    if not code:
        code = params.get("strategy_code", "")
    provider = QuantDataProvider()
    run_backtest(code, params, provider=provider, db_path=CONFIG.db_path)


if __name__ == "__main__":
    main()
