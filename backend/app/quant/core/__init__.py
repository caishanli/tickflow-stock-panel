"""量化执行核心（1 Core）：成交语义的唯一实现所在地。

jq / ptrade 只是 API 方言（码制、签名、state 形状），翻译层只许做翻译，
凡影响成交价/费用/税/持仓/取数窗口的逻辑必须调本包。新增语义先问这里
有没有——有就调，没有就加到这里 + 两边同调（见 docs/quant-core-contract.md §0）。
"""
from __future__ import annotations

# 持仓/组合唯一类（定义仍在 jqengine，core 重导出为正统 import 路径）
from ..jqengine.engine.jq.context import Position
from ..jqengine.engine.jq.portfolio import Portfolio
from .execution import execute_order, order_value_amount, target_percent_amount
from .fees import commission, fill_price, resolve_commission, stamp_tax, stamp_tax_rate
from .instruments import STAMP_TAX_RATE, classify_fund, is_etf
from .limits import limit_prices_from_prev_close, limit_rate, normalize_exchange
from .lots import affordable_shares, round_buy_lot
from .pricing import resolve_live_price
from .tick import ETF_TICK, STOCK_TICK, round_to_tick, tick_size

__all__ = [
    "ETF_TICK",
    "STAMP_TAX_RATE",
    "STOCK_TICK",
    "Portfolio",
    "Position",
    "affordable_shares",
    "classify_fund",
    "commission",
    "execute_order",
    "fill_price",
    "is_etf",
    "limit_prices_from_prev_close",
    "limit_rate",
    "normalize_exchange",
    "order_value_amount",
    "resolve_commission",
    "resolve_live_price",
    "round_buy_lot",
    "round_to_tick",
    "stamp_tax",
    "stamp_tax_rate",
    "target_percent_amount",
    "tick_size",
]
