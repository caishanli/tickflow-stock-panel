"""量化回测/模拟盘 API（FastAPI 薄层，仅提交任务 + 读 quant.db）。"""
from __future__ import annotations

import csv
import datetime
import io

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import db
from ..config import CONFIG
from ..service import (
    submit_backtest, terminate_backtest, account_create, account_start, account_pause,
    account_reset, account_delete,
)
from ..strategies.store import (
    list_strategies, get_strategy, save_strategy, delete_strategy,
    export_strategy, import_strategy,
)
from ..datasource.manager import QuantDataProvider
from ..simulate.names import resolve_name

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
    record: bool = True


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
    # record 仅控制落库位置（compile → /tmp 独立库），不下发 worker、不写 params_json
    params.pop("record")
    # 空日期不下发（前端「编译运行」可不选日期）：键存在但为空串会让
    # 原生 rqalpha 路径把 "" 当日期解析，桥接层拿不到键时才会走默认窗口
    params = {k: v for k, v in params.items() if k not in ("start", "end") or str(v).strip()}
    # frequency 显式透传给桥接层（rqalpha_bridge 侧消费，按其支持的取值执行）
    params["frequency"] = body.frequency or "daily"
    run_id = submit_backtest(params, compile_mode=not body.record)
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
    rows = db.get_trades(run_id)
    for row in rows:
        row["name"] = resolve_name(row["code"])
    return {"data": rows}


@router.get("/backtest/{run_id}/logs")
def backtest_logs(run_id: str, before: int | None = None, limit: int = 200):
    """回测日志（增量/分页）。

    - 不带 before：返回最近 limit 条（最新在尾部），含 total / min_rowid。
    - 带 before（已加载批次的最小 rowid）：返回该 rowid 之前最多 limit 条，
      用于前端「向上滚动加载更早」。has_more 表示是否还有更早日志可加载。
    """
    if before is None:
        rows, min_rid, total = db.get_logs_tail(run_id, limit)
        return {"data": rows, "total": total, "min_rowid": min_rid,
                "has_more": total > len(rows)}
    rows, min_rid = db.get_logs_before(run_id, before, limit)
    return {"data": rows, "min_rowid": min_rid, "has_more": len(rows) >= limit}


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
                d["name"] = resolve_name(row["code"])
                yield f"event: trade\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
            for row in db.get_logs_after(run_id, off_log):
                off_log = row["rowid"]
                yield f"event: log\ndata: {_json.dumps({'rowid': row['rowid'], 'ts': row['ts'], 'level': row['level'], 'message': row['message']}, ensure_ascii=False)}\n\n"
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


class BatchDeleteIn(BaseModel):
    ids: list[str]


@router.delete("/backtest")
def backtest_batch_delete(body: BatchDeleteIn):
    """批量删除回测（含各自日志/净值/成交）。"""
    removed = []
    for rid in body.ids:
        if db.get_run(rid):
            db.delete_run(rid)
            removed.append(rid)
    return {"data": {"removed": removed, "count": len(removed)}}


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


@router.delete("/sim/accounts/{aid}")
def sim_delete(aid: str):
    account_delete(aid)
    return {"data": None}


@router.get("/sim/accounts/{aid}/status")
def sim_status(aid: str):
    acct = db.get_sim_account(aid)
    if not acct:
        raise HTTPException(404, "not found")
    sid = acct.get("strategy_id") or ""
    strat = get_strategy(sid) if sid else None
    trades = db.get_sim_trades(aid)
    return {"data": {"account": acct, "strategy_name": (strat or {}).get("name", ""),
                     "state": db.read_sim_state(aid),
                     "stop_loss": db.get_sim_stoploss(aid),
                     "trade_days": _build_trade_days(acct, trades)}}


def _build_trade_days(account: dict, trades: list[dict]) -> list[str]:
    """账户区间 [start_date, 今天] 的真实交易日列表（YYYY-MM-DD）。

    start_date 为空时取最早成交日，仍为空则取今天。真实日历取数失败/异常
    时降级为工作日（周一~周五）日历，保证接口不挂。
    """
    today = datetime.date.today().isoformat()
    start = (account.get("start_date") or "").strip()[:10]
    if not start and trades:
        start = str(trades[0].get("ts") or "")[:10]
    if not start or start > today:
        start = today
    try:
        from ..datasource.network_client import StockDataClient
        days = StockDataClient().get_trade_days(start, today)
        if days:
            return sorted({str(d)[:10] for d in days})
    except Exception:  # noqa: BLE001
        pass
    try:
        import pandas as _pd
        return [d.strftime("%Y-%m-%d") for d in _pd.bdate_range(start, today)]
    except Exception:  # noqa: BLE001
        return [start, today]


def _build_benchmark_map(first_day: str, last_day: str) -> dict:
    """按日聚合沪深300 收盘，返回 {YYYYMMDD: 相对启动前一交易日涨跌%}。

    基准日 = 模拟盘启动日前一个交易日（用前日收盘做基准，首日即反映当天涨跌）。
    网络源日线日期列名为 trade_dt（非 trade_date/date/datetime），找不到日期列会
    导致 day_close 恒空 → 基准收益恒 0 → 沪深300 基线平直。取数失败返回空 dict。
    """
    import pandas as _pd

    if not first_day:
        return {}
    try:
        from ..jqengine.datasource.manager import get_data_manager
        dm = get_data_manager()
        start_ts = _pd.Timestamp(first_day) - _pd.Timedelta(days=10)
        df = dm.fetch("get_daily", "000300.XSHG", str(start_ts)[:10], last_day)
        if df is None or (hasattr(df, "empty") and df.empty):
            return {}
        df = df.copy()
        dcol = next((c for c in ("trade_date", "date", "datetime", "trade_dt")
                     if c in df.columns), None)
        if dcol is None and isinstance(df.index, _pd.DatetimeIndex):
            dcol = df.index
        if dcol is None:
            return {}
        if isinstance(dcol, str):
            df["_day"] = df[dcol].astype(str).str.replace("-", "").str[:8]
        else:
            df["_day"] = dcol.strftime("%Y-%m-%d").str.replace("-", "").str[:8]
        close_col = "close" if "close" in df.columns else df.columns[-1]
        day_close = df.groupby("_day")[close_col].last().to_dict()
        if not day_close:
            return {}
        base_day = first_day.replace("-", "")
        sorted_days = sorted(day_close.keys())
        base_idx = sorted_days.index(base_day) if base_day in sorted_days else 0
        if base_idx > 0:
            base_day = sorted_days[base_idx - 1]  # 前一个交易日
        base = day_close.get(base_day) or (sorted(day_close.values())[0] if day_close else 0)
        if not base:
            return {}
        return {d: round((day_close[d] / base - 1) * 100, 2) for d in day_close}
    except Exception:
        return {}


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
        # 连接期取一次基准映射，增量 equity 行也附 benchmark_pct（否则当天末点掉回 0）
        _bench = {}
        try:
            _snaps = db.get_sim_snapshots(aid)
            if _snaps:
                _bench = _build_benchmark_map(str(_snaps[0].get("dt", ""))[:10],
                                              str(_snaps[-1].get("dt", ""))[:10])
        except Exception:  # noqa: BLE001
            _bench = {}
        last_status = None
        last_state_sig = None
        while True:
            acct = db.get_sim_account(aid)
            status = acct.get("status") if acct else "unknown"
            # 终态（停止/删除/未知）推完增量后关闭流
            terminal = status in ("stopped", "cancelled", "deleted", "unknown")
            # 仅取轻量的 max(rowid) 判断是否有增量，避免每轮全量查库
            max_log = db.get_max_sim_log_id(aid)
            max_trade = db.get_max_sim_trade_id(aid)
            max_eq = db.get_max_sim_snapshot_id(aid)
            # sim_state 是 upsert（rowid 不变），补跑期间 account.status 恒为 running，
            # 只按 rowid/状态变化判断会漏推状态 → 卡片（净值/收益率/持仓）不更新。
            # 每轮读一次 state 做签名比较，净额/现金/时间戳变了就推 status 事件。
            state = db.read_sim_state(aid)
            state_sig = (state.get("net_value"), state.get("cash"), state.get("pnl"),
                         state.get("dt"))
            state_changed = state_sig != last_state_sig
            has_new = (max_log > off_log or max_trade > off_trade
                       or max_eq > off_eq or status != last_status or state_changed)
            if has_new:
                if status != last_status or state_changed:
                    yield f"event: status\ndata: {_json.dumps({'status': status, 'state': state}, ensure_ascii=False)}\n\n"
                    last_status = status
                    last_state_sig = state_sig
                for row in db.get_sim_snapshots_after(aid, off_eq):
                    off_eq = row["rowid"]
                    d = {k: row[k] for k in ("dt", "net_value", "cash", "positions_value", "pnl", "pnl_pct")}
                    day = str(row["dt"]).replace("-", "")[:8]
                    d["benchmark_pct"] = _bench.get(day, 0)
                    yield f"event: equity\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
                for row in db.get_sim_trades_after(aid, off_trade):
                    off_trade = row["rowid"]
                    d = {k: row[k] for k in ("ts", "code", "name", "action", "price",
                                             "amount", "pnl", "pnl_pct", "commission")}
                    yield f"event: trade\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
                for row in db.get_sim_logs_after(aid, off_log):
                    off_log = row["rowid"]
                    yield f"event: log\ndata: {_json.dumps({'ts': row['ts'], 'level': row['level'], 'message': row['message']}, ensure_ascii=False)}\n\n"
            if terminal:
                return
            # 空闲（无增量）时拉长间隔，避免非交易时段空转烧 CPU；
            # 状态在变（补跑逐 bar 推进）时保持短轮询，卡片及时跟上
            await asyncio.sleep(0.5 if (has_new or state_changed) else 3)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/sim/accounts/{aid}/equity")
def sim_equity(aid: str):
    snaps = db.get_sim_snapshots(aid)
    if not snaps:
        return {"data": []}
    first_day = str(snaps[0].get("dt", ""))[:10]
    last_day = str(snaps[-1].get("dt", ""))[:10]
    bench = _build_benchmark_map(first_day, last_day)
    # 前端净值曲线/指标只消费每日首末点：按天保留首尾两条，盘中明细不传输
    # （分钟快照 5000+ 行 → 每交易日 2 行，payload 由 ~800KB 降到 ~7KB）
    per_day: dict[str, list] = {}
    for s in snaps:
        per_day.setdefault(str(s.get("dt", ""))[:10], []).append(s)
    out: list[dict] = []
    for rows in per_day.values():
        out.append(rows[0])
        if len(rows) > 1:
            out.append(rows[-1])
    for s in out:
        day = str(s.get("dt", ""))[:10].replace("-", "")
        s["benchmark_pct"] = bench.get(day, 0)
    return {"data": out}


@router.get("/sim/accounts/{aid}/trades")
def sim_trades(aid: str):
    return {"data": db.get_sim_trades(aid)}


@router.get("/sim/accounts/{aid}/logs")
def sim_logs(aid: str, limit: int = 0):
    return {"data": db.get_sim_logs(aid, limit)}


# ---- 数据源 ----
@router.get("/datasource")
def datasource_get():
    return {"data": {"priority": CONFIG.data_priority}}


@router.post("/datasource/priority")
def datasource_priority(body: dict):
    # 仅内存生效；落 .env 由设置页处理（见原工程设置机制）
    CONFIG.data_priority = body.get("priority", CONFIG.data_priority)
    return {"data": None}


@router.post("/datasource/verify")
def datasource_verify():
    try:
        from ..jqengine.datasource.mootdx_src import MootdxSource
        src = MootdxSource()
        ok, msg = src.test_connection()
        return {"data": {"ok": ok, "error": msg if not ok else None}}
    except Exception as e:  # noqa: BLE001
        return {"data": {"ok": False, "error": str(e)}}


# ---- 策略侧名称源 ----
@router.get("/sim/settings/name-source")
def sim_name_source_get():
    return {"data": {"source": db.get_quant_setting("sim_strategy_name_source") or "jq"}}


@router.put("/sim/settings/name-source")
def sim_name_source_put(body: dict):
    src = body.get("source", "jq")
    if src not in ("jq", "tdx"):
        raise HTTPException(400, "source must be 'jq' or 'tdx'")
    db.set_quant_setting("sim_strategy_name_source", src)
    return {"data": {"source": src}}


# ---- 钉钉推送 ----
@router.get("/settings/dingtalk")
def get_dingtalk_config():
    return {"data": {
        "webhook_url": db.get_quant_setting("dingtalk_webhook_url") or "",
        "secret": db.get_quant_setting("dingtalk_secret") or "",
    }}


@router.put("/settings/dingtalk")
def save_dingtalk_config(body: dict):
    db.set_quant_setting("dingtalk_webhook_url", body.get("webhook_url", ""))
    db.set_quant_setting("dingtalk_secret", body.get("secret", ""))
    return {"data": "ok"}


@router.put("/sim/accounts/{aid}/dingtalk")
def toggle_dingtalk(aid: str, body: dict):
    db.update_sim_account(aid, dingtalk_enabled=1 if body.get("enabled") else 0)
    return {"data": "ok"}


@router.post("/settings/dingtalk/test")
def test_dingtalk(body: dict):
    from ..notify import send_dingtalk
    body = body or {}
    url = body.get("webhook_url") or db.get_quant_setting("dingtalk_webhook_url") or ""
    secret = body.get("secret") or db.get_quant_setting("dingtalk_secret") or ""
    if not url:
        return {"data": {"success": False, "message": "未配置 webhook URL"}}
    ok = send_dingtalk(url, secret, "测试通知", "## 测试通知\n\n这是一条来自量化模拟盘的测试消息")
    return {"data": {"success": ok, "message": "发送成功" if ok else "发送失败"}}
