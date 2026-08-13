"""本地撮合 + 每分钟持仓止损巡检（改编自 quant-daydayup simulate/matcher.py）。

费用/交易规则口径对齐主引擎 app/backtest/engine.py：
- 佣金双边收取（对应 engine 的 commission_pct，默认取 CONFIG.fee_rate）；
- 印花税仅卖出收取（对应 engine 的 stamp_tax_pct，A股 0.05%），ETF 免征；
- 滑点双边（对应 engine 的 slippage_bps，卖出按 price*(1-slippage) 成交）；
- T+1：当日买入数量不可卖（对应 engine 中 entry_idx==idx 当日不退出口径）；
- 跌停/停牌禁止卖出（对应 engine 的 sell_limit_down/sell_suspended，
  由调用方通过 no_sell 集合传入，停牌无行情时 prices 缺价本就不会成交）。
"""
from __future__ import annotations

import os

from ..config import CONFIG

# 印花税率（仅卖出，股票 0.05%）。config 暂无该字段，允许环境变量覆盖
DEFAULT_STAMP_TAX = float(os.environ.get("QUANT_SIM_STAMP_TAX", "0.0005"))


def _is_etf(code: str) -> bool:
    """简单代码前缀判定：沪市 5 开头、深市 15/16 开头为基金（免印花税）。"""
    num = (code or "").split(".")[0]
    return num.startswith(("5", "15", "16"))


def _resolve_name(code: str) -> str:
    """标的名称解析（延迟导入，失败回退代码本身）。"""
    try:
        from . import names
        return names.resolve_name(code)
    except Exception:  # noqa: BLE001
        return code


class Matcher:
    def __init__(self, stop_loss: float, account_id: str | None = None,
                 on_stop_loss=None):
        self.stop_loss = float(stop_loss)
        # M15：account_id 非空时止损事件同时落库 sim_stop_loss 表
        self.account_id = account_id
        # ding：止损触发回调（runner 注入 → log.notify → 钉钉）
        self.on_stop_loss = on_stop_loss

    def step(self, state: dict, prices: dict, fee: float | None = None,
             stamp_tax: float | None = None, slippage: float | None = None,
             no_sell: set | None = None, min_commission: float | None = None) -> dict:
        fee = CONFIG.fee_rate if fee is None else float(fee)
        min_commission = 0.0 if min_commission is None else float(min_commission)
        stamp_tax = DEFAULT_STAMP_TAX if stamp_tax is None else float(stamp_tax)
        slippage = CONFIG.slippage if slippage is None else float(slippage)
        no_sell = no_sell or set()
        today = (state.get("dt") or "")[:10]
        positions = state.setdefault("positions", {})
        log = state.setdefault("stop_loss_log", [])
        cash = float(state.get("cash", 0.0))
        for code, pos in list(positions.items()):
            price = prices.get(code)
            if price is None:
                continue
            pos["price"] = float(price)
            avg_cost = float(pos.get("avg_cost", 0.0) or 0.0)
            if avg_cost <= 0:
                continue
            pnl_pct = float(price) / avg_cost - 1
            if pnl_pct > -self.stop_loss:
                continue
            # 跌停/停牌禁止卖出（止损顺延，对应主引擎 sell_limit_down/sell_suspended）
            if code in no_sell:
                continue
            amount = float(pos.get("amount", 0.0) or 0.0)
            # T+1：当日买入数量不可卖（buy_dt 为当日 或 today_amount 部分冻结）
            sellable = 0.0 if pos.get("buy_dt") == today else amount
            sellable -= float(pos.get("today_amount", 0.0) or 0.0)
            if sellable <= 0:
                continue
            sell_amount = min(amount, sellable)
            fill = float(price) * (1 - slippage)  # 卖出滑点（主引擎滑点双边口径）
            tax = 0.0 if _is_etf(code) else stamp_tax  # ETF 免印花税
            commission = max(sell_amount * fill * fee, min_commission)  # 佣金含最低兜底
            proceeds = sell_amount * fill - commission - sell_amount * fill * tax
            cash += proceeds
            log.append({
                "dt": state.get("dt"),
                "code": code,
                "action": "STOP_LOSS",
                "price": round(fill, 4),
                "amount": sell_amount,
                "pnl_pct": round(pnl_pct, 4),
            })
            if self.on_stop_loss:
                self.on_stop_loss({
                    "dt": state.get("dt"),
                    "code": code,
                    "name": _resolve_name(code),
                    "action": "STOP_LOSS",
                    "price": round(fill, 4),
                    "amount": sell_amount,
                    "pnl": round((fill - avg_cost) * sell_amount, 4),
                    "pnl_pct": round(pnl_pct, 4),
                    "commission": round(commission, 4),
                })
            if self.account_id:
                # M15：止损落库——sim_stop_loss（止损日志）与 sim_trades（成交记录）
                # 双写，字段补全供表格展示
                from .. import db
                ts = str(state.get("dt"))
                name = _resolve_name(code)
                pnl = (fill - avg_cost) * sell_amount
                db.insert_sim_stoploss(self.account_id, ts, code, name,
                                       "STOP_LOSS", round(fill, 4), sell_amount,
                                       round(pnl, 4), round(pnl_pct, 4),
                                       round(commission, 4))
                db.insert_sim_trade(self.account_id, ts, code, "STOP_LOSS",
                                    round(fill, 4), sell_amount,
                                    round(pnl, 4), round(pnl_pct, 4),
                                    round(commission, 4), name)
            if sell_amount < amount:
                pos["amount"] = amount - sell_amount  # T+1 部分可卖：剩余数量留仓
            else:
                del positions[code]
        state["cash"] = round(cash, 4)
        pos_value = sum(
            float(p.get("amount", 0.0)) * float(p.get("price", 0.0))
            for p in positions.values()
        )
        state["positions"] = positions
        # M15：持仓市值挂到 state，runner 快照写库时取真实值
        state["positions_value"] = round(pos_value, 4)
        state["net_value"] = round(cash + pos_value, 4)
        start = float(state.get("start_cash", 0.0) or state.get("net_value", 0.0))
        state["pnl"] = round(state["net_value"] - start, 4)
        return state
