"""聚宽兼容层 - context / g / Position 对象。"""

from types import SimpleNamespace


class G(SimpleNamespace):
    """聚宽 ``g`` 全局可变对象，策略可在其上挂任意属性。"""


class Position:
    """持仓：数量 / 成本 / 现价。"""

    def __init__(self, amount=0, avg_cost=0.0, price=0.0):
        self.amount = amount
        self.avg_cost = avg_cost
        self.price = price

    @property
    def total_amount(self):
        return self.amount

    @property
    def closeable_amount(self):
        return self.amount

    @property
    def value(self):
        return self.amount * self.price


class Context:
    """聚宽 ``context`` 对象子集。"""

    def __init__(self):
        self.current_dt = None
        self.previous_date = None
        self.universe = []
        self.g = G()
        self.portfolio = None
        self.run_params = SimpleNamespace(type="backtest")
