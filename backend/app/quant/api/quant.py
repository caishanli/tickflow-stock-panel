"""量化回测/模拟盘 API（FastAPI 薄层，仅提交任务 + 读 quant.db）。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import db
from ..config import CONFIG
from ..service import (
    submit_backtest, account_create, account_start, account_pause, account_reset,
)
from ..strategies.store import (
    list_strategies, get_strategy, save_strategy, delete_strategy,
    export_strategy, import_strategy,
)
from ..datasource.manager import QuantDataProvider

router = APIRouter(prefix="/api/quant", tags=["quant"])


class StrategyIn(BaseModel):
    name: str
    code: str


class BacktestIn(BaseModel):
    strategy_id: str = ""
    strategy_code: str = ""
    symbols: list[str] = []
    start: str
    end: str
    frequency: str = "daily"
    capital: float = 100000.0
    fee: float = 0.0003
    slippage: float = 0.001


class AccountIn(BaseModel):
    name: str
    capital: float
    stop_loss: float = 0.03


# ---- 策略 ----
@router.get("/strategies")
def get_strategies():
    return {"data": list_strategies()}


@router.post("/strategies")
def post_strategy(body: StrategyIn):
    import uuid

    return {"data": save_strategy(uuid.uuid4().hex[:8], body.name, body.code)}


@router.get("/strategies/{sid}")
def get_one_strategy(sid: str):
    s = get_strategy(sid)
    if not s:
        raise HTTPException(404, "not found")
    return {"data": s}


@router.put("/strategies/{sid}")
def put_strategy(sid: str, body: StrategyIn):
    return {"data": save_strategy(sid, body.name, body.code)}


@router.delete("/strategies/{sid}")
def del_strategy(sid: str):
    delete_strategy(sid)
    return {"data": None}


@router.get("/strategies/{sid}/export")
def export_one_strategy(sid: str):
    return {"data": export_strategy(sid)}


@router.post("/strategies/import")
def import_one_strategy(body: StrategyIn):
    return {"data": import_strategy(body.name, body.code)}


# ---- 回测 ----
@router.post("/backtest/run")
def run_backtest(body: BacktestIn):
    params = body.model_dump()
    run_id = submit_backtest(params)
    return {"data": {"run_id": run_id, "status": "queued"}}


@router.get("/backtest/{run_id}/status")
def backtest_status(run_id: str):
    r = db.get_run(run_id)
    if not r:
        raise HTTPException(404, "not found")
    return {"data": r}


@router.get("/backtest/{run_id}/equity")
def backtest_equity(run_id: str):
    return {"data": db.get_equity(run_id)}


@router.get("/backtest/{run_id}/trades")
def backtest_trades(run_id: str):
    return {"data": db.get_trades(run_id)}


@router.get("/backtest/{run_id}/logs")
def backtest_logs(run_id: str):
    return {"data": db.get_logs(run_id)}


@router.get("/backtest/{run_id}/trades.csv")
def backtest_trades_csv(run_id: str):
    from fastapi.responses import StreamingResponse

    rows = db.get_trades(run_id)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["ts", "code", "action", "price", "amount", "pnl", "pnl_pct", "commission"])
    w.writeheader()
    w.writerows(rows)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={run_id}.csv"})


@router.post("/backtest/{run_id}/terminate")
def backtest_terminate(run_id: str):
    db.update_run(run_id, "failed", error="terminated")
    return {"data": None}


@router.delete("/backtest/{run_id}")
def backtest_delete(run_id: str):
    if not db.get_run(run_id):
        raise HTTPException(404, "not found")
    db.delete_run(run_id)
    return {"data": None}


# ---- 模拟盘 ----
@router.get("/sim/accounts")
def sim_accounts():
    return {"data": db.list_sim_accounts()}


@router.post("/sim/accounts")
def sim_accounts_post(body: AccountIn):
    aid = account_create(body.name, body.capital, body.stop_loss)
    return {"data": db.get_sim_account(aid)}


@router.post("/sim/accounts/{aid}/start")
def sim_start(aid: str):
    account_start(aid)
    return {"data": None}


@router.post("/sim/accounts/{aid}/pause")
def sim_pause(aid: str):
    account_pause(aid)
    return {"data": None}


@router.post("/sim/accounts/{aid}/reset")
def sim_reset(aid: str):
    account_reset(aid)
    return {"data": None}


@router.get("/sim/accounts/{aid}/status")
def sim_status(aid: str):
    acct = db.get_sim_account(aid)
    if not acct:
        raise HTTPException(404, "not found")
    return {"data": {"account": acct, "state": db.read_sim_state(aid),
                     "stop_loss": db.get_sim_stoploss(aid)}}


@router.get("/sim/accounts/{aid}/equity")
def sim_equity(aid: str):
    return {"data": db.get_sim_snapshots(aid)}


@router.get("/sim/accounts/{aid}/trades")
def sim_trades(aid: str):
    return {"data": db.get_sim_trades(aid)}


# ---- 数据源 ----
@router.get("/datasource")
def datasource_get():
    return {"data": {"priority": CONFIG.data_priority, "tushare_set": bool(CONFIG.tushare_token)}}


@router.post("/datasource/priority")
def datasource_priority(body: dict):
    # 仅内存生效；落 .env 由设置页处理（见原工程设置机制）
    CONFIG.data_priority = body.get("priority", CONFIG.data_priority)
    return {"data": None}


@router.post("/datasource/token")
def datasource_token(body: dict):
    CONFIG.tushare_token = body.get("token", "")
    return {"data": None}


@router.post("/datasource/verify")
def datasource_verify():
    try:
        QuantDataProvider().get_stock_list()
        return {"data": {"ok": True}}
    except Exception as e:  # noqa: BLE001
        return {"data": {"ok": False, "error": str(e)}}
