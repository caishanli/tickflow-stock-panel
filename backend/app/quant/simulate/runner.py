"""模拟盘实时主循环（独立进程逻辑；由 scripts/run_quant_sim.py 调用）。

两种模式（按账户是否绑定 strategy_id 分派）：
- 策略驱动：加载聚宽式策略，交易时段每分钟喂实时行情并触发
  run_daily / run_minute / handle_data，订单经 jqengine 单机引擎本地撮合
  （``engine/jq/api.py``），成交/净值/日志实时落 quant.db；止损 Matcher 每轮巡检。
- 看护（未绑策略）：仅对既有持仓做分钟级止损巡检（旧行为保留）。
"""
from __future__ import annotations

import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from . import live_feed
from .protocol import read_state, save_state, is_paused
from .matcher import Matcher
from .. import db
from ..config import CONFIG
from ..datasource.manager import QuantDataProvider
from ..jqengine.engine.jq.context import Position
from ..strategies.store import get_strategy

log = logging.getLogger("app.quant.simulate.runner")

POLL_INTERVAL = 60  # 看护模式巡检间隔（秒）：分钟级止损，同时避免猛打数据源
IDLE_INTERVAL = 30  # 非交易时段空转间隔（秒）
LIMIT_DOWN_PCT = -0.098  # 跌停判定阈值（主板 10% 留容差；科创/创业 20% 简化不细分）
LIMIT_UP_PCT = 0.098     # 涨停判定阈值（禁买，与跌停同口径）
TICK_OFFSET = 8          # 交易时段每分钟第 N 秒后触发，等刚收的 bar 可读
SESSION_END_GRACE = datetime.time(15, 2)  # 15:00 收市 bar 的处理宽限（之后进收盘钩子）


def in_trading(now=None):
    now = now or datetime.datetime.now()
    t = now.time()
    return (datetime.time(9, 30) <= t <= datetime.time(11, 30)
            or datetime.time(13, 0) <= t <= datetime.time(15, 0)) \
        and now.weekday() < 5  # M2：weekday 用传入的 now，而非真实当前时间


# ---------------------------------------------------------------------------
# 看护模式（无策略账户，旧行为保留）
# ---------------------------------------------------------------------------

def _last_price(df) -> float:
    col = "close" if "close" in df.columns else df.columns[-1]
    return float(df[col].iloc[-1])


def _prev_close(provider: QuantDataProvider, code: str, today: str):
    """昨收（跌停判定用，get_daily 有进程内缓存）。取不到则返回 None → 不做跌停判定。"""
    try:
        start = str(datetime.date.fromisoformat(str(today)) - datetime.timedelta(days=45))
        df = provider.get_daily(code, start, str(today))
    except Exception as e:  # noqa: BLE001
        log.warning("[runner] %s 日线获取失败，本轮跳过跌停判定: %s", code, e)
        return None
    if df is None or df.empty:
        return None
    col = "close" if "close" in df.columns else df.columns[-1]
    # 有日期列时取日期 < today 的最后一根；无日期列退化为倒数第二根（末根通常是当日）
    dcol = next((c for c in ("date", "datetime", "dt", "day") if c in df.columns), None)
    if dcol:
        hist = df[df[dcol].astype(str).str[:10] < str(today)]
        return float(hist[col].iloc[-1]) if not hist.empty else None
    return float(df[col].iloc[-2]) if len(df) >= 2 else None


def _step_once(account_id: str, provider: QuantDataProvider, matcher: Matcher,
               state: dict) -> None:
    """单轮巡检：取价 → 跌停判定 → 撮合止损 → 存状态 + 快照。"""
    codes = list(state.get("positions", {}).keys())
    today = str(datetime.date.today())
    prices, no_sell = {}, set()
    for c in codes:
        try:
            df = provider.get_minute(c, today)
            if df is None or df.empty:
                raise RuntimeError("get_minute返回空")
        except Exception as e:  # noqa: BLE001
            # M2：停牌/节假日/数据源抖动 → 跳过该持仓并告警，不再 raise 搞死进程
            log.warning("[runner] 持仓 %s 分钟数据缺失，本轮跳过: %s", c, e)
            continue
        prices[c] = _last_price(df)
        prev = _prev_close(provider, c, today)
        if prev and prices[c] <= prev * (1 + LIMIT_DOWN_PCT):
            no_sell.add(c)  # 跌停禁止卖出，止损顺延（主引擎 sell_limit_down 口径）
    state["dt"] = str(datetime.datetime.now())
    matcher.step(state, prices, no_sell=no_sell)
    save_state(account_id, state)
    # M15：快照第 4 参写真实持仓市值（matcher 已算好挂到 state["positions_value"]）
    db.insert_sim_snapshot(account_id, state["dt"], state["net_value"],
                           state["cash"], float(state.get("positions_value", 0.0)),
                           state["pnl"],
                           (state["net_value"] / state["start_cash"] - 1) if state["start_cash"] else 0.0)


def _run_watcher_loop(account_id: str, acct: dict, provider, matcher: Matcher,
                      poll_interval: float, idle_interval: float):
    """看护主循环：仅对既有持仓做分钟级止损巡检。"""
    provider = provider or QuantDataProvider()
    state = read_state(account_id)
    if not state.get("start_cash"):
        state["start_cash"] = float(acct.get("capital", 0.0))
        state["cash"] = float(acct.get("capital", 0.0))
        state["net_value"] = float(acct.get("capital", 0.0))
    try:
        while not is_paused(account_id):
            if in_trading():
                _step_once(account_id, provider, matcher, state)
                time.sleep(poll_interval)  # M2：交易时段也要限速，别靠网络耗时
            else:
                time.sleep(idle_interval)
    except Exception:
        # M2：崩溃兜底置 failed，避免账户状态停留 running 误导前端
        db.update_sim_account(account_id, status="failed")
        log.exception("[runner] 账户 %s 主循环异常退出", account_id)
        raise
    db.update_sim_account(account_id, status="paused")


# ---------------------------------------------------------------------------
# 策略驱动模式
# ---------------------------------------------------------------------------

def _emit_log(account_id: str, level: str, message: str,
              ts: str | None = None) -> None:
    """模拟盘日志落 sim_logs（供 /sim/accounts/{aid}/logs 读取），失败仅告警。

    level=="notify" 且账户开启钉钉推送且非补跑时，异步发送到钉钉。
    ts 默认用真实当前时间；补跑场景显式传引擎推进到的时间（见 _replay_*）。
    """
    try:
        db.insert_sim_log(account_id, ts or str(datetime.datetime.now()), level, message)
    except Exception:  # noqa: BLE001
        log.warning("[runner] 日志落库失败(%s): %s", level, message)
    if level == "notify" and account_id not in _replay_active_ids:
        try:
            acct = db.get_sim_account(account_id) or {}
            if acct.get("dingtalk_enabled"):
                _DINGTALK_EXECUTOR.submit(_send_dingtalk_async, account_id, message)
        except Exception:  # noqa: BLE001
            log.warning("[runner] 钉钉推送调度失败: %s", message)


_DINGTALK_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="dingtalk")
_replay_active_ids: set[str] = set()


def _send_dingtalk_async(account_id: str, msg: str) -> None:
    """异步发送钉钉消息（fire-and-forget）。"""
    try:
        webhook_url = db.get_quant_setting("dingtalk_webhook_url") or ""
        if not webhook_url:
            return
        secret = db.get_quant_setting("dingtalk_secret") or ""
        acct = db.get_sim_account(account_id) or {}
        name = acct.get("name", account_id)
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        title = f"模拟盘通知 [{name}]"
        text = f"### {title}\n\n{msg}\n\n> 时间: {now}  \n> 账户: {account_id}"
        from ..notify import send_dingtalk
        send_dingtalk(webhook_url, secret, title, text)
    except Exception as e:  # noqa: BLE001
        log.warning("[runner] 钉钉推送异常: %s", e)


def _load_engine():
    """惰性加载 jqengine 单机引擎（看护模式不依赖）。"""
    from ..jqengine.engine.jq import api as jq_api
    from ..jqengine.engine.jq import loader as jq_loader
    return jq_api, jq_loader


def _make_dm():
    """构造实时 DataManager（在线模式：允许联网回源；真实分钟优先）。"""
    from ..jqengine.datasource.manager import get_data_manager

    dm = get_data_manager()
    dm._use_real_minute = True
    dm._offline = False
    try:
        dm.preload_daily()
    except Exception as e:  # noqa: BLE001
        log.warning("[runner] 日线预加载失败（策略取数将按需回源）: %s", e)
    return dm


def _is_trading_day(dm, today) -> bool:
    """当日是否交易日：用日线数据校验历史日期，近期日期用 weekday 判定。

    盘中/刚收盘时近几日的日线数据可能尚未生成，用 idx[-1]==today 判定会误判，
    因此对 today >= now-2天 直接走 weekday 降级路径。
    """
    now_date = pd.Timestamp.now().date()
    td = pd.Timestamp(today).date()
    if td >= now_date - datetime.timedelta(days=2):
        # 近期日期：weekday 判定即可（日线可能尚未生成）
        return td.weekday() < 5
    try:
        start = str(pd.Timestamp(today) - pd.Timedelta(days=15))[:10]
        df = None
        dm_client = getattr(dm, "client", None)
        if dm_client is not None:
            try:
                out = dm_client.get_price("000300.XSHG", start_date=start,
                                          end_date=str(today), frequency="daily")
                df = out.get("000300.XSHG")
            except Exception:
                df = None
        if df is None:
            df = dm.fetch("get_daily", "000300.XSHG", start, str(today))
        if df is None or df.empty:
            raise RuntimeError("指数日线为空")
        idx = df.index if isinstance(df.index, pd.DatetimeIndex) else None
        if idx is None:
            dcol = next((c for c in ("date", "datetime", "dt", "trade_date", "trade_dt", "day")
                         if c in df.columns), None)
            if dcol is not None:
                idx = pd.DatetimeIndex(pd.to_datetime(df[dcol]))
            elif "timestamp" in df.columns:
                idx = pd.DatetimeIndex(pd.to_datetime(df["timestamp"], unit="s"))
            else:
                raise RuntimeError("指数日线无日期列")
        return bool(len(idx)) and pd.Timestamp(idx[-1]).date() == pd.Timestamp(today).date()
    except Exception as e:  # noqa: BLE001
        log.warning("[runner] 交易日判定失败，降级 weekday: %s", e)
        return pd.Timestamp(today).weekday() < 5


def _prev_close_dm(dm, code: str, today: str):
    """昨收（涨跌停判定用）。取不到返回 None → 不判定。"""
    try:
        start = str(pd.Timestamp(today) - pd.Timedelta(days=45))[:10]
        df = dm.fetch("get_daily", code, start, str(today))
    except Exception:  # noqa: BLE001
        return None
    if df is None or df.empty:
        return None
    col = "close" if "close" in df.columns else df.columns[-1]
    today_ts = pd.Timestamp(today).normalize()
    if isinstance(df.index, pd.DatetimeIndex):
        hist = df[df.index.normalize() < today_ts]
        return float(hist[col].iloc[-1]) if not hist.empty else None
    dcol = next((c for c in ("date", "datetime", "dt", "day") if c in df.columns), None)
    if dcol:
        hist = df[df[dcol].astype(str).str[:10] < str(today)]
        return float(hist[col].iloc[-1]) if not hist.empty else None
    return float(df[col].iloc[-2]) if len(df) >= 2 else None


def _seed_universe(ctx) -> None:
    """把策略 g.* 股票池注入 ctx.universe，供回放/实时行情馈送取价。

    聚宽策略常把股票池挂在自定义 ``g`` 变量上（如 g.global_etf_pool /
    g.fixed_etf_pool / g.merged_etf_pool），而不调用 set_universe()。runner 的
    ``_strategy_tick`` 仅按 ctx.universe 取价，若 universe 为空则每根 bar 都
    "实时行情为空" 跳过，run_daily 永远不触发 → 策略不执行 → 0 成交。这里在
    initialize 后把 g 上的池子收集进 universe，打破该死锁。
    """
    g = getattr(ctx, "g", None)
    if g is None:
        return
    # 保留策略已声明的 universe（init 里 context.universe = [...]），只做追加，
    # 避免把策略自身股票池覆盖掉导致行情馈送取不到价（order 拿 price 0 拒单）。
    pools = list(getattr(ctx, "universe", None) or [])
    for attr in ("fixed_etf_pool", "global_etf_pool", "merged_etf_pool",
                 "domestic_etf_pool", "overseas_etf_pool", "sector_etf_pool",
                 "etf_pool", "universe", "pool"):
        val = getattr(g, attr, None)
        if isinstance(val, (list, tuple, set)):
            pools.extend(val)
        elif isinstance(val, dict):
            pools.extend(val.keys())
    # 走弱期判定用到的指数（check_a_share_weak_period 固定列表）
    pools += ["000300.XSHG", "399101.XSHE", "399006.XSHE", "000510.XSHG"]
    codes = []
    for c in pools:
        c = str(c).strip()
        if c and c not in codes:
            codes.append(c)
    if codes:
        ctx.universe = codes


def _restore_portfolio(ctx, st: dict) -> None:
    """从 sim_state 恢复持仓与现金（崩溃续跑）。无持仓时保持初始组合。"""
    pf = ctx.portfolio
    for code, sp in (st.get("positions") or {}).items():
        pf.positions[code] = Position(
            amount=float(sp.get("amount", 0.0) or 0.0),
            avg_cost=float(sp.get("avg_cost", 0.0) or 0.0),
            price=float(sp.get("price", 0.0) or 0.0),
            today_amount=float(sp.get("today_amount", 0.0) or 0.0),
        )
    pf.cash = float(st.get("cash", pf.cash) or 0.0)


def _state_from_portfolio(ctx, state: dict) -> dict:
    """portfolio → 旧 state dict（供 Matcher 巡检与 protocol.save_state 落库）。"""
    state["positions"] = {
        code: {
            "amount": float(p.amount), "avg_cost": float(p.avg_cost),
            "price": float(p.price),
            "today_amount": float(getattr(p, "today_amount", 0.0) or 0.0),
        }
        for code, p in ctx.portfolio.positions.items()
    }
    state["cash"] = float(ctx.portfolio.cash)
    return state


def _apply_matcher_result(ctx, state: dict) -> None:
    """Matcher 止损卖出结果回写 portfolio（Matcher 已算好现金与剩余持仓）。"""
    pf = ctx.portfolio
    for code in list(pf.positions.keys()):
        if code not in state["positions"]:
            pf.positions.pop(code, None)
    for code, sp in state["positions"].items():
        pos = pf.positions.get(code)
        if pos is None:
            pos = Position()
            pf.positions[code] = pos
        pos.amount = sp["amount"]
        pos.avg_cost = sp.get("avg_cost", pos.avg_cost)
        pos.price = sp.get("price", pos.price)
        pos.today_amount = sp.get("today_amount", 0.0)
    pf.cash = float(state["cash"])


def _parse_hhmm(s):
    try:
        h, m = str(s).split(":")[:2]
        return datetime.time(int(h), int(m))
    except (ValueError, TypeError):
        return None


def _daily_due(task_time: str, bar_dt) -> bool:
    """run_daily 任务是否到点。open→首个 bar；close→14:59 起；HH:MM→对应时刻起。"""
    t = bar_dt.time()
    if task_time == "open":
        return t >= datetime.time(9, 30)
    if task_time == "close":
        return t >= datetime.time(14, 59)
    hhmm = _parse_hhmm(task_time)
    return t >= hhmm if hhmm else False


def _safe_call(account_id: str, func, ctx, tag: str) -> None:
    """策略回调保护性调用：异常落 sim_logs，不中断主循环。"""
    try:
        func(ctx)
    except Exception as e:  # noqa: BLE001
        name = getattr(func, "__name__", "?")
        _emit_log(account_id, "error", f"策略回调 {name}({tag}) 异常: {e}")


def _fire_session(account_id: str, bundle, ctx, bar_dt, fired: set, jq_api,
                  force_all: bool = False) -> None:
    """盘中触发：到期 run_daily（每日一次）+ run_minute + handle_data（每 bar）。

    force_all（日频账户）：忽略任务设定时刻，全部 run_daily 任务在本次唯一
    tick 各触发一次。
    """
    calls = []
    for func, t in bundle.daily:
        ts = str(t)
        if ts in ("before_open", "after_close"):
            continue  # 盘前/盘后钩子触发
        if ts == "every_bar" and not force_all:
            calls.append((func, ts))
            continue
        key = (id(func), ts)
        if key not in fired and (force_all or _daily_due(ts, bar_dt)):
            fired.add(key)
            calls.append((func, ts))
    for func, _m in bundle.minute:
        calls.append((func, "run_minute"))
    if bundle.handle_data is not None:
        calls.append((bundle.handle_data, "handle_data"))
    for func, tag in calls:
        _safe_call(account_id, func, ctx, tag)


def _pre_market(account_id: str, bundle, ctx, fired: set, jq_api, now, aux: dict | None = None) -> None:
    """盘前（每交易日一次）：调度器重置 + T+1 清零 + before_open/before_trading_start。"""
    fired.clear()
    if aux is not None:
        # 记录引擎当前推进到的时间（补跑时 = 当日 09:25；实时 = 真实当前时间），
        # 供 _replay_log_sink 给补跑日志打历史时间戳，而非真实 wall-clock。
        aux["replay_dt"] = pd.Timestamp(now)
        aux["prev_close_cache"] = {}
        # 每日清空无分钟数据缓存：新交易日可能有标的开始有数据（如新ETF上市）
        dm = aux.get("dm")
        if dm is not None and hasattr(dm, "_minute_empty"):
            dm._minute_empty.clear()
    jq_api.on_new_day()
    try:
        days = jq_api.get_trade_days(end_date=str(now.date()), count=5)
        prev = [d for d in days if pd.Timestamp(d).date() < now.date()]
        if prev:
            ctx.previous_date = pd.Timestamp(prev[-1]).date()
    except Exception:  # noqa: BLE001
        pass
    # 盘前刷新最新交易日的日线数据（策略 get_price('1d') 会用到）
    dm = aux.get("dm") if aux is not None else None
    if dm is not None:
        try:
            dm.preload_daily()
        except Exception:  # noqa: BLE001
            pass
    for func, t in bundle.daily:
        if str(t) == "before_open":
            fired.add((id(func), "before_open"))
            _safe_call(account_id, func, ctx, "before_open")
    if bundle.before_trading_start is not None:
        _safe_call(account_id, bundle.before_trading_start, ctx, "before_trading_start")
    # 盘前回调可能更新 g.* 池子，重新注入 universe（每日刷新）
    _seed_universe(ctx)


def _persist(account_id: str, ctx, state: dict, bar_dt, jq_api, aux: dict) -> None:
    """成交增量 / 状态 / 净值快照落库（每轮一次）。"""
    trades = getattr(jq_api, "_state", {}).get("trades") or []
    drained = aux.get("trades_drained", 0)
    is_replay = aux.get("replay_mode", False)
    batch_trades = aux.get("batch_trades") if is_replay else None
    for t in trades[drained:]:
        amount = abs(t["amount"])
        if t["amount"] < 0 and t.get("avg_cost"):
            pnl = (t["price"] - t["avg_cost"]) * amount
            pnl_pct = t["price"] / t["avg_cost"] - 1
        else:
            pnl, pnl_pct = 0.0, 0.0
        trade_row = (account_id, str(t["dt"]),
                     t["code"], "BUY" if t["amount"] > 0 else "SELL",
                     t["price"], amount, round(pnl, 4), round(pnl_pct, 4),
                     t.get("fee", 0.0))
        if batch_trades is not None:
            batch_trades.append(trade_row)
        else:
            db.insert_sim_trade(*trade_row)
    aux["trades_drained"] = len(trades)
    pf = ctx.portfolio
    positions_value = round(sum(p.amount * p.price for p in pf.positions.values()), 4)
    net = round(pf.cash + positions_value, 4)
    start_cash = aux.get("start_cash") or state.get("start_cash") or 0.0
    _state_from_portfolio(ctx, state)
    state["net_value"] = net
    state["pnl"] = round(net - start_cash, 4)
    state["dt"] = str(bar_dt)
    save_state(account_id, state)
    snapshot_row = (account_id, str(bar_dt), net, round(pf.cash, 4),
                    positions_value, round(net - start_cash, 4),
                    round(net / start_cash - 1, 6) if start_cash else 0.0)
    if is_replay:
        aux.setdefault("batch_snapshots", []).append(snapshot_row)
        if len(aux["batch_snapshots"]) % 60 == 0:
            _flush_replay_batch(account_id, aux)
    else:
        db.insert_sim_snapshot(*snapshot_row)


def _revalue_at_close(dm, ctx, state: dict, bar_dt) -> None:
    """收盘重估：把全部持仓按当日真实收盘价重打，并重算 state 净值。

    价格源：优先 ``dm.get_minute_price_at(code, 当日 15:00)``（真实 1m 收盘），
    无则回退日线当日 close（provider.get_daily 有进程内缓存）。只更新估值，
    不触发策略回调 / matcher，不落库（落库由调用方 ``_persist`` 完成）。
    """
    if dm is None:
        return
    today = pd.Timestamp(bar_dt)
    close_ts = today.replace(hour=15, minute=0, second=0, microsecond=0)
    pf = ctx.portfolio
    changed = False
    for code, pos in list(pf.positions.items()):
        price = dm.get_minute_price_at(code, close_ts)
        if price is None:
            try:
                df = dm.fetch("get_daily", code, str(today.date()), str(today.date()))
                if df is not None and not (hasattr(df, "empty") and df.empty):
                    price = float(df["close"].iloc[-1])
            except Exception as e:  # noqa: BLE001
                log.warning("[runner] %s 收盘价重估失败: %s", code, e)
        if price is None:
            log.warning("[runner] %s 收盘重估无价，保留现价 %.4f", code, pos.price)
            continue
        pos.price = float(price)
        changed = True
    if changed:
        _state_from_portfolio(ctx, state)
        start_cash = state.get("start_cash", 0.0) or 0.0
        positions_value = round(sum(p.amount * p.price for p in pf.positions.values()), 4)
        net = round(pf.cash + positions_value, 4)
        state["net_value"] = net
        state["pnl"] = round(net - start_cash, 4)
        state["dt"] = str(bar_dt)


def _eod(account_id: str, bundle, ctx, dm, state: dict, aux: dict, now) -> None:
    """收盘（每交易日一次）：after_close/after_trading_end + 真实分钟落盘 + 收盘重估 + 最终快照。"""
    if aux.get("replay_mode"):
        _flush_replay_batch(account_id, aux)
    for func, t in bundle.daily:
        if str(t) == "after_close":
            _safe_call(account_id, func, ctx, "after_close")
    if bundle.after_trading_end is not None:
        _safe_call(account_id, bundle.after_trading_end, ctx, "after_trading_end")
    live_feed.persist_real(dm, aux["fresh_frames"])
    _revalue_at_close(dm, ctx, state, pd.Timestamp(now))
    _persist(account_id, ctx, state, now, aux["jq_api"], aux)
    # now 在补跑时是引擎推进到的当日收市时刻（15:05），日志时间戳随引擎而非真实时钟
    _emit_log(account_id, "info", "收盘处理完成，当日真实分钟数据已落盘", ts=str(now))


# ---------------------------------------------------------------------------
# 历史补跑（start_date 早于今天：先按历史分钟追上今天，再进实时循环）
# ---------------------------------------------------------------------------

def _trade_days_between(dm, start, end) -> list:
    """[start, end] 内的交易日列表（date 对象），按沪深300 指数日线。

    经 dm.fetch 走网络客户端取数，异常时回退为空列表。
    """
    try:
        df = dm.fetch("get_daily", "000300.XSHG", str(start), str(end))
        # 只取交易日索引，不缓存窄窗口帧到 _daily_mem：否则后续 attribute_history
        # 请求全量（20000101~20300101）会命中这个补跑区间帧（_covers 只看 end 未来
        # 哨兵判覆盖），导致 000300 只有补跑区间的十几行 → 走弱期判断「数据不足」。
        _mem = getattr(dm, "_daily_mem", None)
        if _mem is not None:
            _mem.pop("get_daily_000300.XSHG", None)
            dm._daily_ver += 1
    except Exception:
        df = None
    if df is None or df.empty:
        return []
    idx = df.index if isinstance(df.index, pd.DatetimeIndex) else None
    if idx is None:
        dcol = next((c for c in ("date", "datetime", "dt", "trade_date", "trade_dt", "day")
                     if c in df.columns), None)
        if dcol is not None:
            idx = pd.DatetimeIndex(pd.to_datetime(df[dcol]))
        elif "timestamp" in df.columns:
            idx = pd.DatetimeIndex(pd.to_datetime(df["timestamp"], unit="s"))
        else:
            return []
    lo, hi = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
    return [d.date() for d in idx.normalize() if lo <= d <= hi]


def _session_minutes(day) -> list:
    """某交易日的 240 根 1m bar 时刻（09:31–11:30, 13:01–15:00，按收 bar 标记）。"""
    out = []
    for start_t, end_t in ((datetime.time(9, 31), datetime.time(11, 30)),
                           (datetime.time(13, 1), datetime.time(15, 0))):
        t = datetime.datetime.combine(day, start_t)
        end = datetime.datetime.combine(day, end_t)
        while t <= end:
            out.append(t)
            t += datetime.timedelta(minutes=1)
    return out


def _hist_feed(dm, codes, now, _acc):
    """补跑馈送：取各标的截至 now（历史时刻）的最后一分钟收盘价。

    走 ``dm.get_minute_price_at`` 滑窗加载（C1 近 3 月真实 1m / 更早 baostock 5m
    插值，均在内存，不落盘）；无数据标的缺席，全部无数据则 bar_dt=None（该 bar
    跳过，如停牌/数据空洞）。
    """
    prices = {}
    for code in dict.fromkeys(codes):
        p = dm.get_minute_price_at(code, now)
        if p is not None:
            prices[code] = float(p)
    return prices, (now if prices else None)


def _flush_replay_batch(account_id: str, aux: dict) -> None:
    """将补跑期间攒批的 snapshot/trade/log 一次性写入 DB。"""
    from .. import db as _db
    snapshots = aux.pop("batch_snapshots", [])
    trades = aux.pop("batch_trades", [])
    logs = aux.pop("batch_logs", [])
    if snapshots:
        _db.batch_insert_snapshots(snapshots)
    if trades:
        _db.batch_insert_trades(trades)
    if logs:
        _db.batch_insert_logs(logs)


def _replay_partial_day(account_id: str, bundle, ctx, dm, matcher: Matcher,
                        state: dict, aux: dict, from_ts: datetime.datetime) -> None:
    """补跑同一天内从 from_ts 到当前时间的分钟 bar（盘中重启场景）。"""
    _replay_active_ids.add(account_id)
    try:
        aux["replay_mode"] = True
        aux["batch_snapshots"] = []
        aux["batch_trades"] = []
        aux["batch_logs"] = []
        today = datetime.date.today()
        # 钉住补跑区间分钟窗口并批量预取池（当日场景，窗口即为当天）
        _pin_replay_minute_window(dm, ctx, from_ts, today)
        _pre_market(account_id, bundle, ctx, aux["fired"], aux["jq_api"],
                    datetime.datetime.combine(today, datetime.time(9, 25)), aux)
        now = datetime.datetime.now()
        _emit_log(account_id, "info",
                  f"补跑今日 {from_ts.strftime('%H:%M')} ~ {now.strftime('%H:%M')}",
                  ts=str(aux.get("replay_dt") or now))
        for bar in _session_minutes(today):
            if bar <= from_ts:
                continue
            if bar > now:
                break
            if aux.get("frequency") == "daily" and bar.time() != datetime.time(9, 31):
                continue
            _strategy_tick(account_id, bundle, ctx, dm, _hist_feed, matcher, state, aux, bar)
        # 收盘后启动/重置账户：补跑完今天后先按真实收盘价重估，再进实时
        if now.time() > SESSION_END_GRACE:
            _revalue_at_close(dm, ctx, state,
                              pd.Timestamp(datetime.datetime.combine(today, datetime.time(15, 5))))
            _persist(account_id, ctx, state,
                     datetime.datetime.combine(today, datetime.time(15, 5)), aux["jq_api"], aux)
        _emit_log(account_id, "info",
                  f"日内补跑完成，净值 {state.get('net_value', 0):.2f}",
                  ts=str(aux.get("replay_dt") or state.get("dt") or now))
    finally:
        _flush_replay_batch(account_id, aux)
        aux.pop("replay_mode", None)
        _replay_active_ids.discard(account_id)
        # 补跑结束进入实时：复位钉住的分钟窗口，避免 preload_minute_for_pool
        # 永远用补跑区间 full 窗口、as_of 前移也不滑动（逐日重载丢失批量预取）。
        # 已缓存的分钟帧不清空，覆盖检查与 live_feed 保持帧新鲜。
        _unset_replay_minute_window(dm)


def _pin_replay_minute_window(dm, ctx, start, end) -> None:
    """补跑前把整个补跑区间钉为分钟窗口并批量预取池，消除逐日滑动窗口的重复回源。

    回测由 rqalpha_bridge 在启动前 set_minute_window；模拟盘补跑此处补上同口径。
    用 hasattr 兜底：stub DM（既有测试）无此方法时静默跳过，不影响行为。
    """
    if dm is None:
        return
    set_win = getattr(dm, "set_minute_window", None)
    if set_win is not None:
        try:
            set_win(str(start)[:10], str(end)[:10])
        except Exception as e:
            log.warning("[replay] set_minute_window 失败（回退滑窗）: %s", e)
    codes = []
    if ctx is not None:
        pf = getattr(ctx, "portfolio", None)
        codes = list(dict.fromkeys(
            list(getattr(ctx, "universe", None) or [])
            + (list(pf.positions.keys()) if pf is not None else [])))
    preload = getattr(dm, "preload_minute_for_pool", None)
    if preload is not None and codes:
        try:
            preload(codes, pd.Timestamp(end))
        except Exception as e:
            log.warning("[replay] 分钟池批量预取失败（补跑将逐日回源）: %s", e)


def _unset_replay_minute_window(dm) -> None:
    """补跑结束后复位分钟窗口（unset_minute_window 的 stub-DM 兜底版本）。"""
    if dm is None:
        return
    unset_win = getattr(dm, "unset_minute_window", None)
    if unset_win is not None:
        try:
            unset_win()
        except Exception as e:
            log.warning("[replay] unset_minute_window 失败（实时模式沿用钉窗）: %s", e)


def _replay_history(account_id: str, bundle, ctx, dm, matcher: Matcher,
                    state: dict, aux: dict, start_date: str) -> None:
    """从 start_date 起按历史分钟补跑至今日（今天仅补跑到当前已走完的 bar），
    随后由主循环无缝接入实时，避免当天收盘后才启动/重置账户时日内行情丢失。"""
    _replay_active_ids.add(account_id)
    try:
        aux["replay_mode"] = True
        aux["batch_snapshots"] = []
        aux["batch_trades"] = []
        aux["batch_logs"] = []
        now = datetime.datetime.now()
        today = now.date()
        yesterday = today - datetime.timedelta(days=1)
        # 含今天：完整交易日回放到昨日；今天既已开市（盘前/盘中/收盘）则也回放到当前时刻
        days = _trade_days_between(dm, start_date, today)
        full_days = [d for d in days if d != today]
        # 钉住整个补跑区间分钟窗口并批量预取池，否则滑窗导致池内标的逐日网络回源
        _pin_replay_minute_window(dm, ctx, start_date, today)
        first_ts = (datetime.datetime.combine(days[0], datetime.time(9, 25))
                    if days else now)
        _emit_log(account_id, "info",
                  f"开始历史补跑: {start_date} ~ {yesterday}，共 {len(days)} 个交易日"
                  + (f"，今日 {now.strftime('%H:%M')} 前已回放" if today in days else ""),
                  ts=str(first_ts))
        for day in full_days:
            if is_paused(account_id):
                break
            _pre_market(account_id, bundle, ctx, aux["fired"], aux["jq_api"],
                        datetime.datetime.combine(day, datetime.time(9, 25)), aux)
            for bar in _session_minutes(day):
                if aux.get("frequency") == "daily" and bar.time() != datetime.time(9, 31):
                    continue
                _strategy_tick(account_id, bundle, ctx, dm, _hist_feed, matcher, state, aux, bar)
            _eod(account_id, bundle, ctx, dm, state, aux,
                 datetime.datetime.combine(day, datetime.time(15, 5)))
            _emit_log(account_id, "info", f"补跑 {day} 完成，净值 {state.get('net_value', 0):.2f}",
                      ts=str(datetime.datetime.combine(day, datetime.time(15, 5))))
        # 今天若已是交易日，把已走过（<= 当前时间）的 bar 也回补进来；
        # 今天不跑 _eod，剩余 bar / 收盘由主循环实时接管（last_bar 已推进避免重复触发）
        if today in days and not is_paused(account_id):
            _pre_market(account_id, bundle, ctx, aux["fired"], aux["jq_api"],
                        datetime.datetime.combine(today, datetime.time(9, 25)), aux)
            for bar in _session_minutes(today):
                if bar > now:
                    break
                if aux.get("frequency") == "daily" and bar.time() != datetime.time(9, 31):
                    continue
                _strategy_tick(account_id, bundle, ctx, dm, _hist_feed, matcher, state, aux, bar)
            # 收盘后启动/重置账户：补跑完今天后先按真实收盘价重估，再进实时
            if now.time() > SESSION_END_GRACE:
                _revalue_at_close(dm, ctx, state,
                                  pd.Timestamp(datetime.datetime.combine(today, datetime.time(15, 5))))
                _persist(account_id, ctx, state,
                         datetime.datetime.combine(today, datetime.time(15, 5)), aux["jq_api"], aux)
            _emit_log(account_id, "info",
                      f"今日 {now.strftime('%H:%M')} 前已回补，净值 {state.get('net_value', 0):.2f}",
                      ts=str(aux.get("replay_dt") or state.get("dt") or now))
        _emit_log(account_id, "info", "历史补跑完成，进入实时模式",
                  ts=str(datetime.datetime.combine(days[-1], datetime.time(15, 5)))
                  if days else str(now))
    finally:
        _flush_replay_batch(account_id, aux)
        aux.pop("replay_mode", None)
        _replay_active_ids.discard(account_id)
        # 补跑结束进入实时：复位钉住的分钟窗口，避免 preload_minute_for_pool
        # 永远用补跑区间 full 窗口、as_of 前移也不滑动（逐日重载丢失批量预取）。
        # 已缓存的分钟帧不清空，覆盖检查与 live_feed 保持帧新鲜。
        _unset_replay_minute_window(dm)


def _strategy_tick(account_id: str, bundle, ctx, dm, feed, matcher: Matcher,
                   state: dict, aux: dict, now=None):
    """单轮分钟驱动：喂数 -> 策略回调 -> 止损巡检 -> 落库。返回本轮 bar 时刻。

    同一 bar 不重复触发；非当日 bar / 无数据时跳过并返回 None。
    """
    jq_api = aux["jq_api"]
    now = now or datetime.datetime.now()
    watch = list(dict.fromkeys(
        list(getattr(ctx, "universe", None) or [])
        + list(ctx.portfolio.positions.keys())))
    prices, bar_dt = feed(dm, watch, now, aux["fresh_frames"])
    if bar_dt is None:
        # 收盘宽限期内无实时数据属正常，不报警
        t = pd.Timestamp(now).time()
        if t <= SESSION_END_GRACE:
            pass  # 静默跳过
        else:
            _emit_log(account_id, "warn", "实时行情为空，本轮跳过")
        return None
    bar_ts = pd.Timestamp(bar_dt)
    if bar_ts.date() != now.date():
        return None  # 盘前/数据源滞后只拿到昨日 bar，不触发
    last_bar = aux.get("last_bar")
    if last_bar is not None and bar_ts <= last_bar:
        return None  # 同一 bar 已处理
    aux["last_bar"] = bar_ts
    ctx.current_dt = bar_ts
    aux["replay_dt"] = bar_ts
    jq_api._state["minute_prices"] = prices
    jq_api._state["minute_mode"] = True
    # 涨跌停禁买卖（昨收缺失的标的不判定）
    today = str(bar_ts.date())
    no_sell, no_buy = set(), set()
    pc_cache = aux.setdefault("prev_close_cache", {})
    for code, px in prices.items():
        cache_key = (code, today)
        prev = pc_cache.get(cache_key)
        if prev is None:
            prev = _prev_close_dm(dm, code, today)
            if prev:
                pc_cache[cache_key] = prev
        if not prev:
            continue
        if px <= prev * (1 + LIMIT_DOWN_PCT):
            no_sell.add(code)
        elif px >= prev * (1 + LIMIT_UP_PCT):
            no_buy.add(code)
    jq_api._state["no_sell"] = no_sell
    jq_api._state["no_buy"] = no_buy
    _fire_session(account_id, bundle, ctx, bar_ts, aux["fired"], jq_api,
                  force_all=aux.get("frequency") == "daily")
    # 止损巡检（matcher 在 state 口径上工作，结果回写 portfolio）
    _state_from_portfolio(ctx, state)
    state["dt"] = str(bar_ts)
    matcher.step(state, prices, no_sell=no_sell)
    _apply_matcher_result(ctx, state)
    for code, pos in ctx.portfolio.positions.items():
        if code in prices:
            pos.price = prices[code]
    _persist(account_id, ctx, state, bar_ts, jq_api, aux)
    return bar_ts


def _run_strategy_loop(account_id: str, acct: dict, matcher: Matcher, dm=None,
                       feed=None, idle_interval: float = IDLE_INTERVAL) -> None:
    """策略驱动主循环：交易时段每分钟驱动聚宽式策略并本地撮合。"""
    sid = (acct.get("strategy_id") or "").strip()
    strat = get_strategy(sid)
    code = (strat or {}).get("code", "")
    if not code:
        db.update_sim_account(account_id, status="failed")
        _emit_log(account_id, "error", f"策略不存在或代码为空: {sid}")
        return
    jq_api, jq_loader = _load_engine()
    dm = dm if dm is not None else _make_dm()
    feed = feed or live_feed.refresh
    # 实时由 stock data 服务保证，feed 恒走网络客户端（无 mootdx 直连路径）。
    st = read_state(account_id)
    has_saved = bool(st.get("start_cash"))
    start_cash = float(st.get("start_cash") or acct.get("capital", 0.0) or 0.0)
    cash = float(st["cash"]) if has_saved else start_cash
    try:
        bundle = jq_loader.load_strategy(code, dm, CONFIG.fee_rate, CONFIG.slippage, cash)
    except Exception as e:  # noqa: BLE001
        db.update_sim_account(account_id, status="failed")
        _emit_log(account_id, "error", f"策略编译失败: {e}")
        return
    ctx = bundle.ctx
    if has_saved:
        _restore_portfolio(ctx, st)
    try:
        bundle.init_fn(ctx)
    except Exception as e:  # noqa: BLE001
        db.update_sim_account(account_id, status="failed")
        _emit_log(account_id, "error", f"策略 init 异常: {e}")
        return
    # initialize 后把策略 g.* 池子注入 universe（首次构建 + 打破回放取价死锁）
    _seed_universe(ctx)
    aux = {"jq_api": jq_api, "start_cash": start_cash, "fired": set(),
           "fresh_frames": {}, "trades_drained": 0, "last_bar": None,
           "frequency": (acct.get("frequency") or "minute"), "daily_done": None,
           "dm": dm}
    state: dict = {
        "cash": cash, "start_cash": start_cash, "net_value": cash, "pnl": 0.0,
        "positions": {}, "stop_loss_log": st.get("stop_loss_log") or [],
        "dt": st.get("dt"),
    }
    def _replay_log_sink(level, msg):
        if aux.get("replay_mode"):
            # 补跑日志打引擎当时推进到的时间（bar 内 = bar 时刻；盘前 = 当日 09:25），
            # 否则全部堆在真实当前时间，历史补跑逐日日志无法区分。
            ts = aux.get("replay_dt")
            if ts is None:
                ts = getattr(ctx, "current_dt", None)
            if ts is None:
                ts = datetime.datetime.now()
            aux.setdefault("batch_logs", []).append(
                (account_id, str(ts)[:19], level, msg))
        else:
            _emit_log(account_id, level, msg)
    jq_api._state["log_sink"] = _replay_log_sink
    start_date = (acct.get("start_date") or "").strip()
    _emit_log(account_id, "info",
              f"策略模拟盘启动: {sid} 资金 {start_cash}{'（恢复续跑）' if has_saved else ''}"
              f"{(' 起始日期 ' + start_date) if start_date else ''}")
    # 补跑逻辑：无存档从 start_date 补跑；有存档但 dt 早于当前时间也要补跑缺失数据
    now = datetime.datetime.now()
    today_str = str(now.date())
    replay_from = None
    replay_partial = False  # True = 需要补跑今天内缺失的分钟
    if has_saved:
        saved_dt = state.get("dt")
        if saved_dt:
            try:
                saved_ts = datetime.datetime.fromisoformat(str(saved_dt))
            except (ValueError, TypeError):
                saved_ts = None
            if saved_ts and saved_ts < now:
                if saved_ts.date() == now.date():
                    # 同一天但时间更早：需要补跑今天内缺失的分钟
                    replay_partial = True
                else:
                    # 不同天：补跑完整天
                    replay_from = str(saved_ts)[:10]
    elif start_date and start_date < today_str:
        replay_from = start_date
    if replay_from:
        _replay_history(account_id, bundle, ctx, dm, matcher, state, aux, replay_from)
    elif replay_partial:
        _replay_partial_day(account_id, bundle, ctx, dm, matcher, state, aux, saved_ts)
    hooks_done: dict[str, str | None] = {"pre": None, "eod": None}
    trading_day: tuple[str, bool] | None = None  # (today_str, bool) 每日缓存一次
    try:
        while not is_paused(account_id):
            now = datetime.datetime.now()
            today = str(now.date())
            if trading_day is None or trading_day[0] != today:
                trading_day = (today, _is_trading_day(dm, today))
            if not trading_day[1] or (start_date and today < start_date):
                # 非交易日，或 start_date 在未来（到日前空转等待）
                time.sleep(idle_interval)
                continue
            t = now.time()
            if hooks_done["pre"] != today and t >= datetime.time(9, 25):
                _pre_market(account_id, bundle, ctx, aux["fired"], jq_api, now, aux)
                hooks_done["pre"] = today
            if in_trading(now) or (datetime.time(15, 0) < t <= SESSION_END_GRACE):
                if aux["frequency"] == "daily" and aux["daily_done"] == today:
                    # 日频账户：当日唯一 tick 已完成
                    time.sleep(idle_interval)
                    continue
                bar = _strategy_tick(account_id, bundle, ctx, dm, feed, matcher,
                                     state, aux, now)
                if bar is not None and aux["frequency"] == "daily":
                    aux["daily_done"] = today
                # 对齐分钟边界 + 偏移，等刚收的 bar 可读
                time.sleep(max(1, 60 - now.second + TICK_OFFSET))
            elif t > SESSION_END_GRACE and hooks_done["eod"] != today:
                _eod(account_id, bundle, ctx, dm, state, aux, now)
                hooks_done["eod"] = today
            else:
                time.sleep(idle_interval)
    except Exception:
        # 崩溃兜底置 failed，避免账户状态停留 running 误导前端
        db.update_sim_account(account_id, status="failed")
        log.exception("[runner] 账户 %s 策略主循环异常退出", account_id)
        raise
    db.update_sim_account(account_id, status="paused")


# ---------------------------------------------------------------------------
# 入口分派
# ---------------------------------------------------------------------------

def run_loop(account_id: str, provider: QuantDataProvider | None = None,
             matcher: Matcher | None = None, dm=None, feed=None,
             poll_interval: float = POLL_INTERVAL, idle_interval: float = IDLE_INTERVAL):
    acct = db.get_sim_account(account_id)
    if not acct:
        return
    stop = acct.get("stop_loss") or 0.03
    matcher = matcher or Matcher(stop, account_id=account_id)
    if (acct.get("strategy_id") or "").strip():
        _run_strategy_loop(account_id, acct, matcher, dm=dm, feed=feed,
                           idle_interval=idle_interval)
        return
    _run_watcher_loop(account_id, acct, provider, matcher, poll_interval, idle_interval)
