"""费用与成交价计算——撮合数学的唯一实现。

本地三路（jq ``order`` / ptrade ``order`` / ``Matcher.step`` 止损）与回测
tick 补丁的成交价公式必须同源：
- 成交价：``deal × (1±slippage)``（买+卖-）后按 tick 取整；
- 佣金：``max(turnover × rate, min)``，保留 2 位小数，双边；
- 印花税：仅卖出，股票 ``turnover × rate``（2 位小数），基金全免。

rqalpha 桥内撮合数学由 rqalpha 原生 decider 执行（外部实现无法搬入），
约束方式：费率参数（``set_order_cost``）走策略显式值、tick 取整走补丁
（``core.tick.round_to_tick``），见契约 §2。
"""
from __future__ import annotations

from .instruments import STAMP_TAX_RATE, is_etf
from .tick import round_to_tick


def fill_price(price: float, side: str, slippage: float, code) -> float:
    """成交价：买 ``price*(1+slip)`` / 卖 ``price*(1-slip)``，按 tick 取整。"""
    slip = float(slippage or 0.0)
    raw = float(price) * (1 + slip) if side == "buy" else float(price) * (1 - slip)
    return round_to_tick(raw, code)


def resolve_commission(fee_config, side: str, default_fee: float):
    """佣金率/最低佣金解析：fee_config（open_/close_ 分买卖）优先，否则默认费率。"""
    if fee_config:
        if side == "buy":
            return fee_config["open_commission"], fee_config["min_commission"]
        return fee_config["close_commission"], fee_config["min_commission"]
    return float(default_fee), 0.0


def commission(turnover: float, rate: float, min_commission: float = 0.0,
               ndigits: int | None = 2) -> float:
    """佣金金额：``max(turnover × rate, min)``。

    常规下单路径保留 2 位小数（与历史行为一致）；``ndigits=None`` 时不舍入
    （止损路径：旧 Matcher.step 佣金不舍入直接进 proceeds，展示层另做 round）。
    """
    amount = max(float(turnover) * float(rate), float(min_commission))
    return round(amount, ndigits) if ndigits is not None else amount


def stamp_tax(turnover: float, code, rate: float = STAMP_TAX_RATE) -> float:
    """卖出印花税金额：基金免征，股票 ``turnover × rate``（2 位小数）。"""
    if is_etf(code):
        return 0.0
    return round(float(turnover) * float(rate), 2)


def stamp_tax_rate(code, rate: float = STAMP_TAX_RATE) -> float:
    """印花税率（止损路径用：基金 0，否则传入税率）。"""
    return 0.0 if is_etf(code) else float(rate)
