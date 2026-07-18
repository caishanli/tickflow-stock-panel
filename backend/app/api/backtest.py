"""回测 API — 信号回测 + 因子回测 + 策略回测。"""
from __future__ import annotations

import asyncio
import json
import queue
import threading
from dataclasses import asdict
from datetime import date, timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.services.backtest import (
    BacktestConfig,
    BacktestService,
    VectorbtUnavailable,
    is_available,
)

router = APIRouter(prefix="/api/backtest", tags=["backtest"])

# 默认回测窗口: 因子回测 3 年 (IC/分层需要长样本), 策略回测 180 天 (全周期逐日模拟更重)。
# 历史遗留: 两个常量曾名实互换 (因子侧用了 3 年常量、策略侧用了 180 天常量),
# 行为保持现状 (因子 3 年、策略 180 天), 仅修正命名使其名实相符。
FACTOR_DEFAULT_DAYS = 365 * 3
STRATEGY_DEFAULT_DAYS = 180
BACKTEST_MAX_SERVER_DAYS = 186
# 单次回测指定标的上限 (因子/信号回测共用), 防止一次拉全市场撑爆内存。
BACKTEST_MAX_SYMBOLS = 1000
# 优化器并行 worker 绝对上限: 用户传再大的值也钳到这里, 避免并行回测吃满内存 (服务器约 1.8GB)。
OPTIMIZE_MAX_WORKERS = 8
BACKTEST_SERVER_GUARD_MESSAGE = (
    "当前服务器内存约 1.8GB，回测区间最多支持 6 个月；"
    "更长周期容易触发 OOM，建议在 8GB 以上内存环境或本机运行。"
)


def _get_engine(request: Request):
    """获取或创建 BacktestEngine (单例，PanelCache 跨请求生效)。"""
    from app.backtest.engine import BacktestEngine
    engine = getattr(request.app.state, "backtest_engine", None)
    if engine is None:
        engine = BacktestEngine(request.app.state.repo)
        request.app.state.backtest_engine = engine
    return engine


def _resolve_start(req: BaseModel, end: date, default_days: int) -> date:
    """未传 start 使用默认区间；显式传 null/空值表示全部历史。"""
    start = getattr(req, "start")
    if start is not None:
        return start
    if "start" in req.model_fields_set:
        return date(1900, 1, 1)
    return end - timedelta(days=default_days)


def _guard_server_backtest_range(start: date, end: date):
    if not settings.backtest_range_guard:
        return
    days = (end - start).days + 1
    if days > BACKTEST_MAX_SERVER_DAYS:
        raise HTTPException(status_code=400, detail=BACKTEST_SERVER_GUARD_MESSAGE)


# ================================================================
# 状态
# ================================================================

@router.get("/status")
def status():
    """前端可用此接口判断回测页是否要灰显 (vectorbt 缺失时 available=False)。"""
    return {"available": is_available()}


# ================================================================
# 信号回测 (现有接口，保持不变)
# ================================================================

class BacktestRequest(BaseModel):
    symbols: list[str] = Field(..., min_length=1)
    start: date | None = None
    end: date | None = None
    entries: list[str] = []
    exits: list[str] = []
    stop_loss_pct: float | None = None
    max_hold_days: int | None = Field(None, gt=0)  # <=0 会静默失效, 直接拦下
    fees_pct: float = Field(0.0002, ge=0)          # 负费用 = 凭空造钱
    slippage_bps: float = Field(5, ge=0)
    matching: Literal["close_t", "open_t+1"] = "close_t"
    asset_type: str = "stock"


@router.post("/run")
def run(req: BacktestRequest, request: Request):
    """信号回测 — 现有接口，向后兼容。"""
    repo = request.app.state.repo
    svc = BacktestService(repo)
    end = req.end or date.today()
    start = req.start or (end - timedelta(days=365 * 3))
    # 与因子/策略端点对齐: 服务端范围保护 + 标的上限, 防止大批量长区间回测撑爆内存。
    _guard_server_backtest_range(start, end)
    if len(req.symbols) > BACKTEST_MAX_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"指定标的最多支持 {BACKTEST_MAX_SYMBOLS} 只，请缩小标的范围。",
        )

    cfg = BacktestConfig(
        symbols=req.symbols,
        start=start,
        end=end,
        entries=req.entries,
        exits=req.exits,
        stop_loss_pct=req.stop_loss_pct,
        max_hold_days=req.max_hold_days,
        fees_pct=req.fees_pct,
        slippage_bps=req.slippage_bps,
        matching=req.matching,
        asset_type=req.asset_type,
    )
    try:
        result = svc.run(cfg)
    except VectorbtUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    return asdict(result)


# ================================================================
# 因子回测
# ================================================================

class FactorColumnsResponse(BaseModel):
    columns: list[dict]


@router.get("/factor/columns")
def factor_columns():
    """返回可用的因子列列表。"""
    from app.backtest.factor import FACTOR_COLUMNS
    return {"columns": FACTOR_COLUMNS}


class FactorBacktestRequest(BaseModel):
    factor_name: str
    symbols: list[str] | None = None
    start: date | None = None
    end: date | None = None
    n_groups: int = Field(5, ge=1)
    rebalance: Literal["daily", "weekly", "monthly"] = "monthly"
    weight: Literal["equal", "factor_weight"] = "equal"
    fees_pct: float = Field(0.0002, ge=0)
    slippage_bps: float = Field(5.0, ge=0)
    asset_type: str = "stock"


@router.post("/factor/run")
def factor_run(req: FactorBacktestRequest, request: Request):
    """因子回测 — IC/IR 分析 + 分层回测。"""
    from app.backtest.factor import FactorBacktestService, FactorConfig

    engine = _get_engine(request)
    svc = FactorBacktestService(engine)

    end = req.end or date.today()
    start = _resolve_start(req, end, FACTOR_DEFAULT_DAYS)
    _guard_server_backtest_range(start, end)
    symbols = req.symbols if req.symbols else None
    if symbols is not None and len(symbols) > BACKTEST_MAX_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"指定标的最多支持 {BACKTEST_MAX_SYMBOLS} 只，请缩小标的范围。",
        )

    cfg = FactorConfig(
        factor_name=req.factor_name,
        symbols=symbols,
        start=start,
        end=end,
        n_groups=req.n_groups,
        rebalance=req.rebalance,
        weight=req.weight,
        fees_pct=req.fees_pct,
        slippage_bps=req.slippage_bps,
        asset_type=req.asset_type,
    )
    result = svc.run(cfg)
    return asdict(result)


# ================================================================
# 策略回测
# ================================================================

class StrategyBacktestRequest(BaseModel):
    strategy_id: str
    symbols: list[str] | None = None
    start: date | None = None
    end: date | None = None
    params: dict | None = None
    overrides: dict | None = None
    # matching 向后兼容; 显式传 entry_fill/exit_fill 时以二者为准。
    matching: Literal["close_t", "open_t+1"] = "open_t+1"
    entry_fill: Literal["close_t", "open_t+1"] | None = None
    exit_fill: Literal["close_t", "open_t+1"] | None = None
    fees_pct: float = Field(0.0002, ge=0)
    commission_pct: float | None = Field(None, ge=0)
    stamp_tax_pct: float | None = Field(None, ge=0)
    slippage_bps: float = Field(5.0, ge=0)
    max_positions: int = Field(10, ge=1)
    max_exposure_pct: float = Field(1.0, gt=0)
    initial_capital: float = Field(1_000_000.0, gt=0)
    position_sizing: Literal["equal", "score_weight"] = "equal"
    mode: Literal["position", "full"] = "position"
    holding_days: int = Field(5, ge=1)
    asset_type: str = "stock"


@router.post("/strategy/run")
def strategy_run(req: StrategyBacktestRequest, request: Request):
    """策略回测 — 复用 StrategyDef 体系做全周期回测。"""
    from app.backtest.strategy import StrategyBacktestService, StrategyBacktestConfig

    engine = _get_engine(request)
    strategy_engine = request.app.state.strategy_engine
    svc = StrategyBacktestService(engine, strategy_engine)

    end = req.end or date.today()
    start = _resolve_start(req, end, STRATEGY_DEFAULT_DAYS)
    _guard_server_backtest_range(start, end)

    cfg = StrategyBacktestConfig(
        strategy_id=req.strategy_id,
        symbols=req.symbols if req.symbols else None,
        start=start,
        end=end,
        params=req.params,
        overrides=req.overrides,
        matching=req.matching,
        entry_fill=req.entry_fill,
        exit_fill=req.exit_fill,
        fees_pct=req.fees_pct,
        commission_pct=req.commission_pct,
        stamp_tax_pct=req.stamp_tax_pct,
        slippage_bps=req.slippage_bps,
        max_positions=req.max_positions,
        max_exposure_pct=req.max_exposure_pct,
        initial_capital=req.initial_capital,
        position_sizing=req.position_sizing,
        mode=req.mode,
        holding_days=req.holding_days,
        asset_type=req.asset_type,
    )
    result = svc.run(cfg)
    return asdict(result)


# ── SSE 流式回测 (实时进度 + 可取消 + 支持重连) ───────────────────

import time
import hashlib


class _BacktestJob:
    """单个回测任务的状态, 存模块级供重连使用。"""
    __slots__ = ("key", "cancel_event", "progress", "result", "error", "done", "finish_ts")

    def __init__(self, key: str):
        self.key = key
        self.cancel_event = threading.Event()
        self.progress: list[dict] = []   # 进度历史 (新连接可回放)
        self.result = None               # 完成后的结果
        self.error: str | None = None
        self.done = False
        self.finish_ts: float = 0.0


# 模块级任务表: key -> _BacktestJob
_running_jobs: dict[str, _BacktestJob] = {}
_jobs_lock = threading.Lock()
_JOB_TTL = 300  # 完成后保留 5 分钟

# 并发回测上限: 多个重回测同时跑会 OOM (服务器内存约 1.8GB)。用信号量限并发,
# 超出的任务在 _run_backtest 里排队, SSE 连接照常保持, run 一开始就有进度。
_backtest_semaphore = threading.Semaphore(2)


def _cleanup_stale_jobs():
    """清理过期任务 (完成超过 TTL 的)。全程持 _jobs_lock: 迭代+pop 与其他访问互斥。"""
    now = time.time()
    with _jobs_lock:
        stale = [k for k, j in _running_jobs.items() if j.done and now - j.finish_ts > _JOB_TTL]
        for k in stale:
            _running_jobs.pop(k, None)


def _fail_job(job: "_BacktestJob", message: str) -> None:
    """确定性失败路径 (guard/权限/数据范围等只依赖请求参数的检查) 统一收尾:
    置 error/done 并记完成时间。否则 job 永不 done 变僵尸 — _cleanup_stale_jobs 只清
    done 任务, 且同参重连 is_new=False 会跳过线程启动, SSE 无限空转、参数组合被毒化。"""
    job.error = message
    job.done = True
    job.finish_ts = time.time()


def _check_minute_fill_guard(request: Request, start_date: date) -> str | None:
    """minute_fill 门控: Pro+ 权限 + 本地分钟K历史覆盖。返回错误消息, 通过返回 None。

    strategy/stream 与 optimize/stream 共用同一检查, 保证两条 SSE 路径口径一致
    (此前 optimize 侧缺门控, 无权限/数据不足时会静默跑完一组错误的优化)。
    """
    capset = request.app.state.capabilities
    from app.tickflow.capabilities import Cap
    if not capset.has(Cap.KLINE_MINUTE_BATCH):
        return '分钟K精确回测需要 Pro+ 权限 (kline.minute.batch)'
    # 检查本地分钟K历史是否覆盖回测区间
    repo = request.app.state.repo
    earliest_minute = repo.earliest_minute_date() if hasattr(repo, "earliest_minute_date") else None
    if earliest_minute is not None and start_date < earliest_minute:
        return (f"本地分钟K历史最早到 {earliest_minute}, 无法覆盖回测起始日 {start_date}。"
                f"请先用「扩展分钟K历史」功能拉取更多数据, 或缩小回测区间。")
    return None


def _make_job_key(
    strategy_id: str, symbols: str | None, start: str | None, end: str | None,
    matching: str, entry_fill: str | None, exit_fill: str | None,
    fees_pct: float, slippage_bps: float,
    max_positions: int, max_exposure_pct: float, initial_capital: float, position_sizing: str,
    params: str | None, overrides: str | None,
    mode: str = "position", holding_days: int = 5,
    commission_pct: float | None = None, stamp_tax_pct: float | None = None,
    asset_type: str = "stock",
    minute_fill: bool = False,
) -> str:
    raw = f"{strategy_id}|{symbols}|{start}|{end}|{matching}|{entry_fill}|{exit_fill}|{fees_pct}|{slippage_bps}|{max_positions}|{max_exposure_pct}|{initial_capital}|{position_sizing}|{params}|{overrides}|{mode}|{holding_days}|{commission_pct}|{stamp_tax_pct}|{asset_type}|{minute_fill}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _resolve_stream_range(request: Request, start: str | None, end: str | None) -> tuple[date, date]:
    """解析流式端点的日期区间 (stream/cancel 共用同一口径, 保证 job_key 一致)。

    非法日期抛 400 (原生 date.fromisoformat 的 ValueError 会漏成 500);
    空 start = 全部历史: 用本地最早日K日期, 查不到再回退到默认窗口。
    """
    try:
        end_date = date.fromisoformat(end) if end else date.today()
        start_date = date.fromisoformat(start) if start else None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式非法 (需要 YYYY-MM-DD): {e}") from e
    if start_date is None:
        earliest = request.app.state.repo.earliest_daily_date()
        start_date = earliest or (end_date - timedelta(days=STRATEGY_DEFAULT_DAYS))
    return start_date, end_date


@router.get("/strategy/stream")
async def strategy_stream(
    request: Request,
    strategy_id: str,
    symbols: str | None = None,
    start: str | None = None,
    end: str | None = None,
    # 枚举/数值校验与 POST 端对齐: GET 裸 str 无校验时非法值会静默跑偏; 非法值 422。
    matching: Literal["close_t", "open_t+1"] = "open_t+1",
    entry_fill: Literal["close_t", "open_t+1"] | None = None,
    exit_fill: Literal["close_t", "open_t+1"] | None = None,
    fees_pct: float = Query(0.0002, ge=0),
    commission_pct: float | None = Query(None, ge=0),
    stamp_tax_pct: float | None = Query(None, ge=0),
    slippage_bps: float = Query(5.0, ge=0),
    max_positions: int = Query(10, ge=1),
    max_exposure_pct: float = Query(1.0, gt=0),
    initial_capital: float = Query(1_000_000.0, gt=0),
    position_sizing: Literal["equal", "score_weight"] = "equal",
    params: str | None = None,
    overrides: str | None = None,
    mode: Literal["position", "full"] = "position",
    holding_days: int = Query(5, ge=1),
    asset_type: str = "stock",
    minute_fill: bool = False,
):
    """SSE 流式策略回测: 实时推送进度, 完成后推送结果, 支持重连 (刷新/切页后恢复)。

    - 相同参数的任务只启动一次, 多次连接订阅同一个任务
    - 断开连接不会取消任务 (除非显式调用 cancel)
    - 结果保留 5 分钟供重连

    事件类型:
      - progress: {day, total, date, equity}
      - done: {result} (完整回测结果)
      - error: {message}
    """
    from app.backtest.strategy import StrategyBacktestService, StrategyBacktestConfig

    engine = _get_engine(request)
    strategy_engine = request.app.state.strategy_engine
    svc = StrategyBacktestService(engine, strategy_engine)

    start_date, end_date = _resolve_stream_range(request, start, end)

    # params/overrides 是 JSON 字符串: 在流开始前解析, 非法 JSON 直接 400。
    # (原先在 generator 内 json.loads, SSE 已返回 200 后中途断裂, 前端拿不到明确错误。)
    try:
        parsed_params = json.loads(params) if params else None
        parsed_overrides = json.loads(overrides) if overrides else None
    except (json.JSONDecodeError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"params/overrides 必须是合法 JSON: {e}") from e

    # 服务端范围保护
    guard_violated = False
    if settings.backtest_range_guard:
        days = (end_date - start_date).days + 1
        if days > BACKTEST_MAX_SERVER_DAYS:
            guard_violated = True

    # key 用解析后的日期 (而非原始参数, 缺省会被固化成 "None"): 避免跨零点/数据入库后
    # 同 key 命中陈旧任务; cancel 侧走同一解析 (_resolve_stream_range) 保证 key 一致。
    job_key = _make_job_key(
        strategy_id, symbols, start_date.isoformat(), end_date.isoformat(),
        matching, entry_fill, exit_fill,
        fees_pct, slippage_bps, max_positions, max_exposure_pct, initial_capital, position_sizing,
        params, overrides,
        mode, holding_days,
        commission_pct, stamp_tax_pct,
        asset_type=asset_type,
        minute_fill=minute_fill,
    )

    _cleanup_stale_jobs()

    # 获取或创建任务
    with _jobs_lock:
        job = _running_jobs.get(job_key)
        if job is None:
            job = _BacktestJob(job_key)
            _running_jobs[job_key] = job
            is_new = True
        else:
            is_new = False

    async def event_generator():
        # 范围保护: 直接报错。确定性失败 (只依赖请求参数) 先 _fail_job 收尾,
        # 否则 job 永不 done 变僵尸, 同参重连 is_new=False 还会跳过线程启动、SSE 空转。
        if guard_violated:
            _fail_job(job, BACKTEST_SERVER_GUARD_MESSAGE)
            yield f"event: error\ndata: {json.dumps({'message': BACKTEST_SERVER_GUARD_MESSAGE}, ensure_ascii=False)}\n\n"
            return

        # 分钟K精确回测: Pro+ 门控 + 数据范围检查 (与 optimize/stream 共用 _check_minute_fill_guard)
        if minute_fill:
            msg = _check_minute_fill_guard(request, start_date)
            if msg:
                _fail_job(job, msg)
                yield f"event: error\ndata: {json.dumps({'message': msg}, ensure_ascii=False)}\n\n"
                return

        # 如果是新任务, 启动回测线程
        if is_new and not job.done:
            cfg = StrategyBacktestConfig(
                strategy_id=strategy_id,
                symbols=[s.strip() for s in symbols.split(",") if s.strip()] if symbols else None,
                start=start_date,
                end=end_date,
                params=parsed_params,
                overrides=parsed_overrides,
                matching=matching,
                entry_fill=entry_fill,
                exit_fill=exit_fill,
                fees_pct=fees_pct,
                commission_pct=commission_pct,
                stamp_tax_pct=stamp_tax_pct,
                slippage_bps=slippage_bps,
                max_positions=int(max_positions),
                max_exposure_pct=float(max_exposure_pct),
                initial_capital=float(initial_capital),
                position_sizing=position_sizing,
                mode=mode,
                holding_days=int(holding_days),
                asset_type=asset_type,
                minute_fill=minute_fill,
            )

            def _run_backtest():
                # 信号量限并发: 超额任务在此阻塞排队, 不并发吃满内存 (等待期间 cancel_event
                # 仍可置位, svc.run 会据此提前返回 cancelled)。持槽跑完在 finally 释放。
                _backtest_semaphore.acquire()
                try:
                    result = svc.run(cfg, lambda d: job.progress.append(d), job.cancel_event)
                    job.result = result
                    job.done = True
                    job.finish_ts = time.time()
                except Exception as e:
                    job.error = str(e)
                    job.done = True
                    job.finish_ts = time.time()
                finally:
                    _backtest_semaphore.release()

            # 启动后台线程 (不阻塞事件循环)
            threading.Thread(target=_run_backtest, daemon=True).start()

        # 订阅进度: 用读指针读 job.progress 列表 (多连接互不干扰)
        cursor = 0
        tick = 0

        try:
            while True:
                # 已完成: 推送最终结果/错误并退出
                if job.done:
                    if job.error:
                        yield f"event: error\ndata: {json.dumps({'message': job.error}, ensure_ascii=False)}\n\n"
                    elif job.result is not None:
                        r = job.result
                        if hasattr(r, "error") and r.error == "cancelled":
                            yield f"event: error\ndata: {json.dumps({'message': '回测已取消'}, ensure_ascii=False)}\n\n"
                        elif hasattr(r, "error") and r.error:
                            yield f"event: error\ndata: {json.dumps({'message': r.error}, ensure_ascii=False)}\n\n"
                        else:
                            yield f"event: done\ndata: {json.dumps(asdict(r), ensure_ascii=False, default=str)}\n\n"
                    return

                # 断开检测: 每 4 轮检查一次 (降低 GIL 抢占频率)
                tick += 1
                if tick % 4 == 0 and await request.is_disconnected():
                    break

                # 推送新进度 (从 cursor 开始读)
                prog_list = job.progress
                while cursor < len(prog_list):
                    msg = prog_list[cursor]
                    cursor += 1
                    yield f"event: progress\ndata: {json.dumps(msg, ensure_ascii=False, default=str)}\n\n"

                await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            raise

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/strategy/cancel")
async def strategy_cancel(request: Request):
    """取消正在运行的回测任务 (前端传 query string, 后端算 job_key)。"""
    body = await request.json()
    qs = body.get("qs", "")
    # 解析 qs 得到参数
    from urllib.parse import parse_qs
    p = parse_qs(qs)
    def _get(key: str, default: str = "") -> str:
        return p.get(key, [default])[0]
    def _get_opt_float(key: str) -> float | None:
        # 可选成本参数: 缺省或空串 → None (与 stream 侧 float | None 口径一致, 保证 job_key 对齐)。
        v = _get(key)
        return float(v) if v else None
    def _get_float(key: str, default: float) -> float:
        # 必填数值: 空串回落默认 (容错同 _get_opt_float, 避免 float("") 抛 ValueError 变 500)。
        v = _get(key)
        return float(v) if v else default
    def _get_int(key: str, default: int) -> int:
        v = _get(key)
        return int(v) if v else default
    # 日期与 stream 侧同一解析口径 (解析后才入 key): 否则两侧 key 对不上、任务取消不掉。
    start_date, end_date = _resolve_stream_range(request, _get("start") or None, _get("end") or None)
    try:
        job_key = _make_job_key(
            _get("strategy_id"),
            _get("symbols") or None,
            start_date.isoformat(),
            end_date.isoformat(),
            _get("matching", "open_t+1"),
            _get("entry_fill") or None,
            _get("exit_fill") or None,
            _get_float("fees_pct", 0.0002),
            _get_float("slippage_bps", 5.0),
            _get_int("max_positions", 10),
            _get_float("max_exposure_pct", 1.0),
            _get_float("initial_capital", 1_000_000.0),
            _get("position_sizing", "equal"),
            _get("params") or None,
            _get("overrides") or None,
            _get("mode", "position"),
            _get_int("holding_days", 5),
            commission_pct=_get_opt_float("commission_pct"),
            stamp_tax_pct=_get_opt_float("stamp_tax_pct"),
            asset_type=_get("asset_type", "stock"),
            # minute_fill 参与 job_key: 漏传时分钟K任务的 key 对不上, 永远取消不掉。
            minute_fill=_get("minute_fill").lower() in ("1", "true", "yes", "on"),
        )
    except (ValueError, TypeError):
        return {"ok": False, "message": "参数格式非法"}
    # 持锁读任务表: 与 _cleanup_stale_jobs 的 pop、stream 的写入互斥
    with _jobs_lock:
        job = _running_jobs.get(job_key)
    if job and not job.done:
        job.cancel_event.set()
        return {"ok": True}
    return {"ok": False, "message": "任务不存在或已完成"}


# ══════════════════════════════════════════════════════════════
# 参数网格优化器 — 复用 _BacktestJob SSE 框架 (多组参数并行回测 + 排序)
# ══════════════════════════════════════════════════════════════

# 透传给每组回测的 StrategyBacktestConfig 字段 (作为 backtest_kwargs)。
# 成交口径 (entry_fill/exit_fill)、asset_type、minute_fill 必须在内: 缺了会导致
# 用户用非默认口径跑回测时, 优化器实际优化的是另一套配置。
_OPT_BT_FIELDS = [
    "matching", "entry_fill", "exit_fill", "fees_pct", "commission_pct", "stamp_tax_pct",
    "slippage_bps", "max_positions", "max_exposure_pct", "initial_capital", "position_sizing",
    "mode", "holding_days", "asset_type", "minute_fill",
]


def _make_opt_job_key(strategy_id, symbols, start, end, param_grid, objective, direction, bt_sig, params=None, overrides=None) -> str:
    raw = f"OPT|{strategy_id}|{symbols}|{start}|{end}|{param_grid}|{objective}|{direction}|{bt_sig}|{params}|{overrides}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _opt_backtest_kwargs(
    matching, fees_pct, commission_pct, stamp_tax_pct, slippage_bps,
    max_positions, max_exposure_pct, initial_capital, position_sizing, mode, holding_days,
    entry_fill=None, exit_fill=None, asset_type="stock", minute_fill=False,
) -> dict:
    return {
        "matching": matching,
        "entry_fill": entry_fill,
        "exit_fill": exit_fill,
        "fees_pct": fees_pct,
        "commission_pct": commission_pct,
        "stamp_tax_pct": stamp_tax_pct,
        "slippage_bps": slippage_bps,
        "max_positions": int(max_positions),
        "max_exposure_pct": float(max_exposure_pct),
        "initial_capital": float(initial_capital),
        "position_sizing": position_sizing,
        "mode": mode,
        "holding_days": int(holding_days),
        "asset_type": asset_type,
        "minute_fill": minute_fill,
    }


@router.get("/optimize/stream")
async def optimize_stream(
    request: Request,
    strategy_id: str,
    param_grid: str,                 # JSON: {param_id: [values] | {min,max,step}}
    objective: str = "sortino",
    direction: str | None = None,
    max_workers: int = Query(4, ge=1),
    params: str | None = None,       # JSON: 未扫描参数固定为用户当前值 (base_params)
    overrides: str | None = None,    # JSON: 策略当前的 basic_filter/signals/风控等覆盖
    symbols: str | None = None,
    start: str | None = None,
    end: str | None = None,
    # 口径/校验与 strategy/stream 对齐: 优化必须跑和用户回测同一套配置。
    matching: Literal["close_t", "open_t+1"] = "open_t+1",
    entry_fill: Literal["close_t", "open_t+1"] | None = None,
    exit_fill: Literal["close_t", "open_t+1"] | None = None,
    fees_pct: float = Query(0.0002, ge=0),
    commission_pct: float | None = Query(None, ge=0),
    stamp_tax_pct: float | None = Query(None, ge=0),
    slippage_bps: float = Query(5.0, ge=0),
    max_positions: int = Query(10, ge=1),
    max_exposure_pct: float = Query(1.0, gt=0),
    initial_capital: float = Query(1_000_000.0, gt=0),
    position_sizing: Literal["equal", "score_weight"] = "equal",
    mode: Literal["position", "full"] = "position",
    holding_days: int = Query(5, ge=1),
    asset_type: str = "stock",
    minute_fill: bool = False,
):
    """SSE 流式参数优化: 并行跑各参数组回测, 按 objective 排序。

    事件类型:
      - progress: {type: "optimizer_progress", done, total, best_score}
      - done: {result} (含 best_params / results 排名)
      - error: {message}
    """
    from app.backtest.optimizer import OptimizeConfig, StrategyOptimizer
    from app.backtest.strategy import StrategyBacktestService

    engine = _get_engine(request)
    strategy_engine = request.app.state.strategy_engine
    svc = StrategyBacktestService(engine, strategy_engine)

    start_date, end_date = _resolve_stream_range(request, start, end)

    guard_violated = False
    if settings.backtest_range_guard and (end_date - start_date).days + 1 > BACKTEST_MAX_SERVER_DAYS:
        guard_violated = True

    # 空串归一为 None, 与 cancel 侧 `_get("direction") or None` 口径一致, 避免 job_key 失配。
    direction = direction or None
    bt_kwargs = _opt_backtest_kwargs(
        matching, fees_pct, commission_pct, stamp_tax_pct, slippage_bps,
        max_positions, max_exposure_pct, initial_capital, position_sizing, mode, holding_days,
        entry_fill=entry_fill, exit_fill=exit_fill, asset_type=asset_type, minute_fill=minute_fill,
    )
    bt_sig = "|".join(f"{k}={bt_kwargs[k]}" for k in _OPT_BT_FIELDS)
    # key 用解析后的日期 (同 strategy/stream 的修复): 原始 None 固化成 "None" 会命中陈旧任务。
    job_key = _make_opt_job_key(strategy_id, symbols, start_date.isoformat(), end_date.isoformat(), param_grid, objective, direction, bt_sig, params, overrides)

    _cleanup_stale_jobs()
    with _jobs_lock:
        job = _running_jobs.get(job_key)
        if job is None:
            job = _BacktestJob(job_key)
            _running_jobs[job_key] = job
            is_new = True
        else:
            is_new = False

    async def event_generator():
        # 首个事件回吐 job_key, 前端存下供 cancel 直接引用 (消除两侧重算契约)。
        yield f"event: job\ndata: {json.dumps({'key': job_key}, ensure_ascii=False)}\n\n"

        if guard_violated:
            # 确定性失败先 _fail_job 收尾 (同 strategy/stream): 否则僵尸 job 毒化该参数组合。
            _fail_job(job, BACKTEST_SERVER_GUARD_MESSAGE)
            yield f"event: error\ndata: {json.dumps({'message': BACKTEST_SERVER_GUARD_MESSAGE}, ensure_ascii=False)}\n\n"
            return

        # 分钟K精确回测: 与 strategy/stream 同一门控 (Pro+ 权限 + 分钟K数据覆盖),
        # 失败走 _fail_job, 避免无权限/数据不足时静默跑完整组优化。
        if minute_fill:
            msg = _check_minute_fill_guard(request, start_date)
            if msg:
                _fail_job(job, msg)
                yield f"event: error\ndata: {json.dumps({'message': msg}, ensure_ascii=False)}\n\n"
                return

        if is_new and not job.done:
            try:
                grid = json.loads(param_grid)
            except (json.JSONDecodeError, TypeError):
                grid = None
            # grid 必须是非空 dict; null/[]/"" 等合法 JSON 但结构错误也在此拦下,
            # 否则会跳过线程启动却不置 done -> event_generator 永久空转、job 挂死。
            if not isinstance(grid, dict) or not grid:
                job.error = "param_grid 必须是非空的参数网格对象"
                job.done = True
                job.finish_ts = time.time()
                grid = None

            if grid is not None:
                # 未扫描参数固定为用户当前值 (base_params); overrides 让策略的 basic_filter/
                # 信号/风控按用户当前配置参与, 保证优化的就是用户实际回测的策略。
                try:
                    base_params = json.loads(params) if params else {}
                except (json.JSONDecodeError, TypeError):
                    base_params = {}
                try:
                    ov = json.loads(overrides) if overrides else None
                except (json.JSONDecodeError, TypeError):
                    ov = None
                ocfg = OptimizeConfig(
                    strategy_id=strategy_id,
                    symbols=[s.strip() for s in symbols.split(",") if s.strip()] if symbols else None,
                    start=start_date,
                    end=end_date,
                    param_grid=grid,
                    objective=objective,
                    direction=direction,
                    max_workers=max(1, min(int(max_workers), OPTIMIZE_MAX_WORKERS)),
                    base_params=base_params if isinstance(base_params, dict) else {},
                    overrides=ov if isinstance(ov, dict) else None,
                    backtest_kwargs=bt_kwargs,
                )

                def _run_opt():
                    # 与 _run_backtest 共用信号量限并发: 优化内部多 worker 并行回测,
                    # 不吃槽直接起线程更容易并发 OOM。持槽跑完在 finally 释放。
                    _backtest_semaphore.acquire()
                    try:
                        opt = StrategyOptimizer(svc, strategy_engine)
                        job.result = opt.optimize(ocfg, lambda d: job.progress.append(d), job.cancel_event)
                        job.done = True
                        job.finish_ts = time.time()
                    except Exception as e:
                        job.error = str(e)
                        job.done = True
                        job.finish_ts = time.time()
                    finally:
                        _backtest_semaphore.release()

                threading.Thread(target=_run_opt, daemon=True).start()

        cursor = 0
        tick = 0
        try:
            while True:
                if job.done:
                    if job.error:
                        yield f"event: error\ndata: {json.dumps({'message': job.error}, ensure_ascii=False)}\n\n"
                    elif job.cancel_event.is_set():
                        # 取消时优化器把每组记为 cancelled 并正常返回, 需在此分流为取消提示而非"完成"。
                        yield f"event: error\ndata: {json.dumps({'message': '优化已取消'}, ensure_ascii=False)}\n\n"
                    elif job.result is not None:
                        yield f"event: done\ndata: {json.dumps(job.result, ensure_ascii=False, default=str)}\n\n"
                    return
                tick += 1
                if tick % 4 == 0 and await request.is_disconnected():
                    break
                while cursor < len(job.progress):
                    msg = job.progress[cursor]
                    cursor += 1
                    yield f"event: progress\ndata: {json.dumps(msg, ensure_ascii=False, default=str)}\n\n"
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            raise

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/optimize/cancel")
async def optimize_cancel(request: Request):
    """取消优化任务 — 前端传 stream 首事件回吐的 job_key, 后端直接查表。

    不再让 cancel 侧重算 job_key: 两侧重算必须逐字段一致的脆弱契约(PR3 C1 / direction
    空串失配都源于此)在此彻底消除。stream 首个 SSE 事件把后端算出的 key 回吐给前端,
    cancel 原样传回即可。
    """
    body = await request.json()
    job_key = body.get("job_key", "")
    # 持锁读任务表: 与 _cleanup_stale_jobs 的 pop、stream 的写入互斥 (同 strategy/cancel 纪律)
    with _jobs_lock:
        job = _running_jobs.get(job_key)
    if job and not job.done:
        job.cancel_event.set()
        return {"ok": True}
    return {"ok": False, "message": "任务不存在或已完成"}

