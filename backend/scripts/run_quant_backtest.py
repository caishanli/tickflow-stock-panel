"""回测独立进程：由 FastAPI 派生子进程启动，经 quant.db 与前端通信。"""
from __future__ import annotations

import json
import os
import sys
import traceback

from dotenv import load_dotenv

# 与 scripts/run_jq_rqalpha.py 等独立脚本同口径：先把 .env 加载进环境变量，
# 再导入 app.quant（其 config 在 import 时读 TUSHARE_TOKEN 等环境变量）。
# UI 链路不经过 pydantic-settings，缺了这一步子进程拿不到 token。
load_dotenv()

from app.quant import db
from app.quant.config import CONFIG
from app.quant.datasource.manager import QuantDataProvider
from app.quant.strategies.store import get_strategy


def _now() -> str:
    import datetime as _dt

    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _progress(run_id: str, msg: str) -> None:
    """运行期进度日志实时落库（SSE 即刻推送）：预加载阶段没有 quantlive 钩子，
    不写的话前端长达数十秒只看到 running 徽章、看不到任何运行情况。"""
    try:
        db.insert_log(run_id, _now(), "INFO", msg)
    except Exception:  # noqa: BLE001
        pass


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


def _looks_like_ptrade(code: str) -> bool:
    """PTrade 策略用 .SS/.SZ 代码 + run_daily(context, func, time)，走 ptradecompat 引擎。"""
    return bool(code) and (".SS" in code or ".SZ" in code)


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
    _progress(run_id, "回测子进程已启动，策略代码就绪，正在初始化数据与引擎…")

    if _looks_like_ptrade(code):
        # PTrade 策略（.SS/.SZ + run_daily(context, func, time)）走 ptradecompat 引擎
        _progress(run_id, "检测到 PTrade 策略，路由到 ptradecompat 引擎（1m 逐 bar）")
        from app.quant.rqalpha_bridge import run_ptrade_backtest
        tmp = os.path.join(CONFIG.runtime_dir, f"ptradestrat_{run_id}.py")
        os.makedirs(CONFIG.runtime_dir, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(code)
        params = dict(params, run_id=run_id, strategy_id=strategy_id or "ptrade",
                      name=params.get("name", ""), out_dir=os.path.join(CONFIG.runtime_dir, "ptradewufu"))
        run_ptrade_backtest(tmp, params, db_path=CONFIG.db_path)
    elif _looks_like_jq(code):
        # 聚宽(jq)策略走 jqcompat 引擎（正确的日志/成交捕获与 ETF 池解析）
        _progress(run_id, "检测到聚宽式策略，路由到 jqcompat 引擎（1m 逐 bar）")
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
    # 兜底：任何未捕获异常落 quant.db 并把 run 置 failed。子进程 stdout/stderr
    # 被父进程 DEVNULL，不写库则前端永远停在 queued、看不到任何失败原因。
    _run_id = sys.argv[1] if len(sys.argv) > 1 else None
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        if _run_id:
            try:
                import datetime as _dt

                _now = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                db.insert_log(_run_id, _now, "ERROR", traceback.format_exc()[-2000:])
                db.update_run(_run_id, "failed", error=str(e)[:500], finished_at=_now)
            except Exception:  # noqa: BLE001
                pass
        sys.exit(1)
