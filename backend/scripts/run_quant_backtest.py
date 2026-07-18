"""回测独立进程：由 FastAPI 派生子进程启动，经 quant.db 与前端通信。"""
from __future__ import annotations

import json
import os
import sys

from app.quant import db
from app.quant.config import CONFIG
from app.quant.datasource.manager import QuantDataProvider
from app.quant.strategies.store import get_strategy


def _looks_like_jq(code: str) -> bool:
    """聚宽(jq)策略用 def initialize(...) 与 jqcompat API（log/order_target/
    get_price/get_all_securities），rqalpha 原生策略用 def init(...)。据此路由。
    """
    if not code:
        return False
    if "def initialize(" in code:
        return True
    for kw in ("get_all_securities", "order_target", "get_price(", "update_universe", "run_daily"):
        if kw in code:
            return True
    return False


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

    if _looks_like_jq(code):
        # 聚宽(jq)策略走 jqcompat 引擎（正确的日志/成交捕获与 ETF 池解析）
        from app.quant.rqalpha_bridge import run_jq_backtest
        # run_jq_backtest 需要 strategy 文本；通过临时文件传入（与 scripts/
        # run_jq_rqalpha.py 同口径），避免把整段代码塞进 params 造成歧义。
        tmp = os.path.join(CONFIG.runtime_dir, f"jqstrat_{run_id}.py")
        os.makedirs(CONFIG.runtime_dir, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(code)
        params = dict(params, run_id=run_id, strategy_id=strategy_id or "jq",
                      name=params.get("name", ""), out_dir=os.path.join(CONFIG.runtime_dir, "jqwufu"))
        run_jq_backtest(tmp, params, db_path=CONFIG.db_path)
    else:
        from app.quant.rqalpha_bridge import run_backtest
        provider = QuantDataProvider()
        run_backtest(code, params, provider=provider, db_path=CONFIG.db_path)


if __name__ == "__main__":
    main()
