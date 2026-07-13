"""本地撮合 + 每分钟持仓止损巡检（改编自 quant-daydayup simulate/matcher.py）。"""
from __future__ import annotations


class Matcher:
    def __init__(self, stop_loss: float):
        self.stop_loss = float(stop_loss)

    def step(self, state: dict, prices: dict, fee: float = 0.0003) -> dict:
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
            if pnl_pct <= -self.stop_loss:
                amount = float(pos.get("amount", 0.0) or 0.0)
                proceeds = amount * float(price) * (1 - fee)
                cash += proceeds
                log.append({
                    "dt": state.get("dt"),
                    "code": code,
                    "action": "STOP_LOSS",
                    "price": float(price),
                    "pnl_pct": round(pnl_pct, 4),
                })
                del positions[code]
        state["cash"] = round(cash, 4)
        pos_value = sum(
            float(p.get("amount", 0.0)) * float(p.get("price", 0.0))
            for p in positions.values()
        )
        state["positions"] = positions
        state["net_value"] = round(cash + pos_value, 4)
        start = float(state.get("start_cash", 0.0) or state.get("net_value", 0.0))
        state["pnl"] = round(state["net_value"] - start, 4)
        return state
