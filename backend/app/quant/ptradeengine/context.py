"""PTrade 兼容层 - context / g / Position / Portfolio（镜像 jqengine/engine/jq/context.py）。"""
from __future__ import annotations

import types

from app.quant.jqengine.engine.jq.context import Position
from app.quant.jqengine.engine.jq.portfolio import Portfolio


class PtradePosition(Position):
    """PTrade 字段别名（enable_amount/cost_basis/last_sale_price）。"""

    @property
    def enable_amount(self):
        return self.closeable_amount

    @property
    def cost_basis(self):
        return self.avg_cost

    @property
    def last_sale_price(self):
        return self.price


class PtradePortfolio(Portfolio):
    @property
    def portfolio_value(self):
        return self.total_value


class PtradeContext:
    """PTrade context 子集：blotter.current_dt / portfolio / g / _code_conv。"""

    def __init__(self):
        self.current_dt = None
        self.previous_date = None
        self.universe = []
        self.g = types.SimpleNamespace()
        self.portfolio = None
        self.run_params = types.SimpleNamespace(type="backtest")
        self._code_conv = ptrade_code_conv()

    @property
    def blotter(self):
        return types.SimpleNamespace(current_dt=self.current_dt)


def ptrade_code_conv():
    """返回 (to_engine, to_pt)。to_engine 对已是引擎码(JQ)的输入幂等。"""
    def to_engine(code):
        s = str(code)
        return s.replace(".SS", ".XSHG").replace(".SZ", ".XSHE")

    def to_pt(code):
        s = str(code)
        return s.replace(".XSHG", ".SS").replace(".XSHE", ".SZ")

    return to_engine, to_pt
