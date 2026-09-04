"""订单执行——本地撮合的唯一真身。

jq ``order`` 与 ptrade ``order`` 在此之前是两份逐行镜像的代码（整手、T+1、
滑点、tick、佣金、印花税、持仓更新、trades 落盘逐行相同），任何一边改公式
另一边必分叉。现在两边只许做三件事：取价（``core.pricing``）、调本函数、
返回布尔。

``portfolio`` 为鸭子类型：``.positions``（dict[str, Position]）、``.cash``
（float）；``Position`` 需 ``.amount/.avg_cost/.price/.today_amount``
属性与 ``.closeable_amount``（T+1 可卖量），``core`` 重导出 jqengine 的
唯一 ``Position``/``Portfolio`` 类，ptrade 子类继承它。

成交记录字段与现金语义冻结（契约 §1）：``price`` 为 tick 取整后成交价，
``pos.price`` 为快照价（非 fill），``cash -= cost``（买入 cost 为正、
卖出 cost 为负），清仓 pop。
"""
from __future__ import annotations

from .fees import commission, fill_price, resolve_commission, stamp_tax
from .lots import affordable_shares, round_buy_lot


def execute_order(*, portfolio, position_factory, code, amount, price,
                  current_dt, fee, slippage, fee_config=None,
                  no_buy=(), no_sell=(), trades=None) -> bool:
    """按股数下单（正买负卖）。返回是否成交。"""
    if price == 0 or amount == 0:
        return False
    amount = int(amount)
    if amount > 0:
        amount = round_buy_lot(amount)
        if amount <= 0 or code in (no_buy or ()):
            return False  # 不足一手 / 涨停禁买
    existing = portfolio.positions.get(code)
    prev_cost = float(existing.avg_cost or 0.0) if existing else 0.0
    if amount < 0:
        if code in (no_sell or ()):
            return False  # 跌停/停牌禁卖
        closeable = float(existing.closeable_amount) if existing else 0.0
        amount = -min(-amount, closeable)  # T+1：卖出不超过可卖量
        if amount == 0:
            return False
    side = "buy" if amount > 0 else "sell"
    fill = fill_price(price, side, slippage, code)
    turnover = abs(amount) * fill
    comm_rate, min_comm = resolve_commission(fee_config, side, fee)
    fee_amount = commission(turnover, comm_rate, min_comm)
    tax_amount = 0.0
    if amount > 0:
        cost = turnover + fee_amount
        if cost > portfolio.cash:
            return False
    else:
        tax_amount = stamp_tax(turnover, code)
        cost = -(turnover - fee_amount - tax_amount)
    pos = portfolio.positions.setdefault(code, position_factory())
    if amount > 0:
        if float(pos.amount or 0.0) <= 0:
            pos.entry_ts = current_dt  # 首次建仓记录买入时间
        total_cost = pos.amount * pos.avg_cost + amount * fill
        pos.amount += amount
        pos.avg_cost = total_cost / pos.amount if pos.amount else 0.0
        pos.today_amount = float(pos.today_amount or 0.0) + amount  # T+1 当日买入冻结
    else:
        pos.amount += amount
        if pos.amount <= 0:
            pos.amount = 0
            pos.avg_cost = 0.0
            portfolio.positions.pop(code, None)
    pos.price = price
    portfolio.cash -= cost
    if trades is not None:
        trades.append({
            "dt": current_dt, "code": code, "side": side,
            "amount": amount, "price": fill, "fee": fee_amount, "tax": tax_amount,
            "avg_cost": prev_cost,
        })
    return True


def order_value_amount(value: float, price: float, fee: float) -> int:
    """按金额下单的股数换算（买口径，调用方再调 execute_order）。"""
    return int(float(value) // (float(price) * (1 + float(fee))))


def target_percent_amount(*, portfolio_value: float, percent: float, price: float,
                          current_amount, cash: float, slippage: float,
                          fee: float) -> int:
    """目标仓位比例的股数差额（含买力钳制；卖出由 execute_order 按 T+1 截断）。"""
    target_value = float(portfolio_value) * float(percent)
    target_shares = int(target_value // float(price)) if percent > 0 else 0
    amount = target_shares - int(current_amount or 0)
    if amount > 0:
        # 受手续费/滑点影响，买入不能超过可用资金
        amount = min(amount, affordable_shares(cash, price, slippage, fee))
    return amount
