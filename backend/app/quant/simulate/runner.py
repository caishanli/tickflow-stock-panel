"""模拟盘实时主循环（独立进程逻辑；由 scripts/run_quant_sim.py 调用）。"""
from __future__ import annotations

import datetime
import time

from .protocol import read_state, save_state, is_paused
from .matcher import Matcher
from .. import db
from ..datasource.manager import QuantDataProvider


def in_trading(now=None):
    t = (now or datetime.datetime.now()).time()
    return (datetime.time(9, 30) <= t <= datetime.time(11, 30)
            or datetime.time(13, 0) <= t <= datetime.time(15, 0)) \
        and datetime.datetime.now().weekday() < 5


def run_loop(account_id: str, provider: QuantDataProvider | None = None,
             matcher: Matcher | None = None):
    provider = provider or QuantDataProvider()
    acct = db.get_sim_account(account_id)
    if not acct:
        return
    stop = acct.get("stop_loss") or 0.03
    matcher = matcher or Matcher(stop)
    state = read_state(account_id)
    if not state.get("start_cash"):
        state["start_cash"] = float(acct.get("capital", 0.0))
        state["cash"] = float(acct.get("capital", 0.0))
        state["net_value"] = float(acct.get("capital", 0.0))
    while not is_paused(account_id):
        if in_trading():
            codes = list(state.get("positions", {}).keys())
            today = str(datetime.date.today())
            prices = {}
            for c in codes:
                try:
                    df = provider.get_minute(c, today)
                    if df is not None and not df.empty:
                        col = "close" if "close" in df.columns else df.columns[-1]
                        prices[c] = float(df[col].iloc[-1])
                    else:
                        raise RuntimeError(
                            f"[runner] 持仓 {c} 分钟数据获取失败: get_minute返回空"
                        )
                except RuntimeError:
                    raise
                except Exception as e:
                    raise RuntimeError(
                        f"[runner] 持仓 {c} 分钟数据获取异常: {e}"
                    ) from e
            state["dt"] = str(datetime.datetime.now())
            matcher.step(state, prices)
            save_state(account_id, state)
            db.insert_sim_snapshot(account_id, state["dt"], state["net_value"],
                                   state["cash"], 0.0, state["pnl"],
                                   (state["net_value"] / state["start_cash"] - 1) if state["start_cash"] else 0.0)
        else:
            time.sleep(30)
    db.update_sim_account(account_id, status="paused")
