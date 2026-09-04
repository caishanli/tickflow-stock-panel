"""手数与买力计算——数量侧语义的唯一实现。"""
from __future__ import annotations


def round_buy_lot(amount: int) -> int:
    """买入向下取整到 100 股整手（A股/ETF 同规则）。"""
    return int(amount) // 100 * 100


def affordable_shares(cash: float, price: float, slippage: float = 0.0,
                      fee_rate: float = 0.0) -> int:
    """可用资金按 ``price×(1+slip)×(1+fee)`` 单价可买股数（向下取整）。

    ``slippage``/``fee_rate`` 为 None 时抛 TypeError（旧内联公式同行为；
    禁止静默按 0 处理，见契约 §7.3）。"""
    unit_cost = float(price) * (1 + float(slippage)) * (1 + float(fee_rate))
    if unit_cost <= 0:
        return 0
    return int(float(cash) // unit_cost)
