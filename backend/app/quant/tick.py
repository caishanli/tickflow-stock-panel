"""最小报价单位（tick）——已搬入 1 Core。

正统位置：``app.quant.core.tick``。本模块仅为兼容垫片（存量 import 与测试
仍从此处引入），不许在此新增任何逻辑。
"""
from __future__ import annotations

from .core.tick import ETF_TICK, STOCK_TICK, round_to_tick, tick_size

__all__ = ["ETF_TICK", "STOCK_TICK", "round_to_tick", "tick_size"]
