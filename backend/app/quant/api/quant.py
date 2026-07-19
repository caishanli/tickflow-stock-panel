"""量化回测/模拟盘 API（FastAPI 薄层，仅提交任务 + 读 quant.db）。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import db
from ..config import CONFIG
from ..service import (
    submit_backtest, terminate_backtest, account_create, account_start, account_pause,
    account_reset,
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
    name: str = ""
    strategy_id: str = ""
    strategy_code: str = ""
    symbols: list[str] = []
    # 前端「编译运行」可不选日期：留空由桥接层按默认数据窗口执行
    start: str = ""
    end: str = ""
    frequency: str = "daily"
    capital: float = 100000.0
    fee: float = 0.0003
    slippage: float = 0.001


class AccountIn(BaseModel):
    name: str
    capital: float
    stop_loss: float = 0.03
    strategy_id: str = ""
    start_date: str = ""  # YYYY-MM-DD；早于今天则从该日起历史补跑后续实时，晚于今天则到日前空转
    frequency: str = "minute"  # 运行频率：minute 逐分钟 / daily 每日一次（开盘首 bar 全量触发）


# ---- 策略 ----
@router.get("/strategies")
def get_strategies():
    return {"data": list_strategies()}


@router.post("/strategies")
def post_strategy(body: StrategyIn):
    import uuid

    return {"data": save_strategy(uuid.uuid4().hex[:8], body.name, body.code)}


@router.get("/strategies/with-latest")
def strategies_with_latest():
    return {"data": db.list_strategies_with_latest()}


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
    # 空日期不下发（前端「编译运行」可不选日期）：键存在但为空串会让
    # 原生 rqalpha 路径把 "" 当日期解析，桥接层拿不到键时才会走默认窗口
    params = {k: v for k, v in params.items() if k not in ("start", "end") or str(v).strip()}
    # frequency 显式透传给桥接层（rqalpha_bridge 侧消费，按其支持的取值执行）
    params["frequency"] = body.frequency or "daily"
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


@router.get("/backtest/runs")
def backtest_runs(limit: int = 50, strategy_id: str | None = None):
    rows = db.list_runs(limit)
    if strategy_id:
        rows = [r for r in rows if r.get("strategy_id") == strategy_id]
    return {"data": rows}


@router.get("/backtest/{run_id}/stream")
async def backtest_stream(run_id: str, since_id: int | None = None,
                          last_id: int | None = None):
    """SSE：按事件类型增量推送 log/trade/equity/status。

    查 quant.db 增量（按 rowid 偏移），避免跨进程队列。前端用 EventSource
    接收；断线后凭 /equity /logs /trades 轮询恢复历史。
    M12：前端"先轮询历史、再开 SSE"时可传 since_id/last_id（快照的 max rowid），
    从断点续推，避免建连间隙写入的行（rowid ≤ 建连 max）既不在快照也不推送而丢失；
    不传时保持现行为（只推建连后的新行）。
    """
    import asyncio
    import json as _json

    from fastapi.responses import StreamingResponse

    if not db.get_run(run_id):
        raise HTTPException(404, "not found")

    async def gen():
        start = since_id if since_id is not None else last_id
        off_log = db.get_max_log_id(run_id) if start is None else start
        off_trade = db.get_max_trade_id(run_id) if start is None else start
        off_equity = db.get_max_equity_id(run_id) if start is None else start
        while True:
            r = db.get_run(run_id)
            status = r["status"] if r else "unknown"
            # status 事件（每次都推，便于前端感知结束）
            yield f"event: status\ndata: {_json.dumps({'status': status, 'metrics': (r or {}).get('metrics_json')}, ensure_ascii=False)}\n\n"
            for row in db.get_equity_after(run_id, off_equity):
                off_equity = row["rowid"]
                d = {k: row[k] for k in ("dt", "value", "benchmark", "cash", "positions_value")}
                yield f"event: equity\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
            for row in db.get_trades_after(run_id, off_trade):
                off_trade = row["rowid"]
                d = {k: row[k] for k in ("ts", "code", "action", "price", "amount", "pnl", "pnl_pct", "commission")}
                yield f"event: trade\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
            for row in db.get_logs_after(run_id, off_log):
                off_log = row["rowid"]
                yield f"event: log\ndata: {_json.dumps({'ts': row['ts'], 'level': row['level'], 'message': row['message']}, ensure_ascii=False)}\n\n"
            # 终态（含 run 行被删的 unknown）：推完剩余增量后正常关闭流
            if status in ("done", "failed", "unknown"):
                return
            await asyncio.sleep(0.5)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


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
    if not db.get_run(run_id):
        raise HTTPException(404, "not found")
    # M5：先按 pid 杀子进程组，再把 run 置 failed（不再只改 DB 状态）
    terminate_backtest(run_id)
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
    if body.strategy_id and not get_strategy(body.strategy_id):
        raise HTTPException(400, f"strategy not found: {body.strategy_id}")
    if body.start_date:
        import datetime as _dt
        try:
            _dt.date.fromisoformat(body.start_date)
        except ValueError:
            raise HTTPException(400, f"invalid start_date (expect YYYY-MM-DD): {body.start_date}")
    if body.frequency not in ("minute", "daily"):
        raise HTTPException(400, f"invalid frequency (expect minute/daily): {body.frequency}")
    aid = account_create(body.name, body.capital, body.stop_loss, body.strategy_id,
                         body.start_date, body.frequency)
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
    sid = acct.get("strategy_id") or ""
    strat = get_strategy(sid) if sid else None
    return {"data": {"account": acct, "strategy_name": (strat or {}).get("name", ""),
                     "state": db.read_sim_state(aid),
                     "stop_loss": db.get_sim_stoploss(aid)}}


@router.get("/sim/accounts/{aid}/stream")
def sim_stream(aid: str, since_id: int | None = None):
    """模拟盘实时增量推送（SSE）：status/log/trade/equity 事件。

    与 backtest_stream 同机制：按 rowid 偏移增量推送，避免前端每 4s 全量轮询。
    """
    from fastapi.responses import StreamingResponse
    import asyncio
    import json as _json

    if not db.get_sim_account(aid):
        raise HTTPException(404, "not found")

    async def gen():
        start = since_id
        off_log = db.get_max_sim_log_id(aid) if start is None else start
        off_trade = db.get_max_sim_trade_id(aid) if start is None else start
        off_eq = db.get_max_sim_snapshot_id(aid) if start is None else start
        last_status = None
        while True:
            acct = db.get_sim_account(aid)
            status = acct.get("status") if acct else "unknown"
            # 终态（停止/删除/未知）推完增量后关闭流
            terminal = status in ("stopped", "cancelled", "deleted", "unknown")
            # 仅取轻量的 max(rowid) 判断是否有增量，避免每轮全量查库
            max_log = db.get_max_sim_log_id(aid)
            max_trade = db.get_max_sim_trade_id(aid)
            max_eq = db.get_max_sim_snapshot_id(aid)
            has_new = (max_log > off_log or max_trade > off_trade
                       or max_eq > off_eq or status != last_status)
            if has_new:
                if status != last_status:
                    yield f"event: status\ndata: {_json.dumps({'status': status, 'state': db.read_sim_state(aid)}, ensure_ascii=False)}\n\n"
                    last_status = status
                for row in db.get_sim_snapshots_after(aid, off_eq):
                    off_eq = row["rowid"]
                    d = {k: row[k] for k in ("dt", "net_value", "cash", "positions_value", "pnl", "pnl_pct")}
                    yield f"event: equity\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
                for row in db.get_sim_trades_after(aid, off_trade):
                    off_trade = row["rowid"]
                    d = {k: row[k] for k in ("ts", "code", "action", "price", "amount", "pnl", "pnl_pct", "commission")}
                    yield f"event: trade\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
                for row in db.get_sim_logs_after(aid, off_log):
                    off_log = row["rowid"]
                    yield f"event: log\ndata: {_json.dumps({'ts': row['ts'], 'level': row['level'], 'message': row['message']}, ensure_ascii=False)}\n\n"
            if terminal:
                return
            # 空闲（无增量）时拉长间隔，避免非交易时段空转烧 CPU
            await asyncio.sleep(0.5 if has_new else 3)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/sim/accounts/{aid}/equity")
def sim_equity(aid: str):
    return {"data": db.get_sim_snapshots(aid)}


@router.get("/sim/accounts/{aid}/trades")
def sim_trades(aid: str):
    return {"data": db.get_sim_trades(aid)}


@router.get("/sim/accounts/{aid}/logs")
def sim_logs(aid: str, limit: int = 500):
    return {"data": db.get_sim_logs(aid, limit)}


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
