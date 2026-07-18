"""模拟盘实时主循环（独立进程逻辑；由 scripts/run_quant_sim.py 调用）。"""
from __future__ import annotations

import datetime
import logging
import time

from .protocol import read_state, save_state, is_paused
from .matcher import Matcher
from .. import db
from ..datasource.manager import QuantDataProvider

log = logging.getLogger("app.quant.simulate.runner")

POLL_INTERVAL = 60  # 交易时段巡检间隔（秒）：分钟级止损，同时避免猛打数据源
IDLE_INTERVAL = 30  # 非交易时段空转间隔（秒）
LIMIT_DOWN_PCT = -0.098  # 跌停判定阈值（主板 10% 留容差；科创/创业 20% 简化不细分）


def in_trading(now=None):
    now = now or datetime.datetime.now()
    t = now.time()
    return (datetime.time(9, 30) <= t <= datetime.time(11, 30)
            or datetime.time(13, 0) <= t <= datetime.time(15, 0)) \
        and now.weekday() < 5  # M2：weekday 用传入的 now，而非真实当前时间


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
    dcol = next((c for c in ("date", "datetime", "time", "dt", "day") if c in df.columns), None)
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


def run_loop(account_id: str, provider: QuantDataProvider | None = None,
             matcher: Matcher | None = None,
             poll_interval: float = POLL_INTERVAL, idle_interval: float = IDLE_INTERVAL):
    provider = provider or QuantDataProvider()
    acct = db.get_sim_account(account_id)
    if not acct:
        return
    stop = acct.get("stop_loss") or 0.03
    matcher = matcher or Matcher(stop, account_id=account_id)
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

