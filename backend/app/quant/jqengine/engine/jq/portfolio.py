"""聚宽兼容层 - portfolio / 持仓管理。"""

from .context import Position


class Portfolio:
    """账户组合：现金 + 持仓。"""

    def __init__(self, cash):
        self.cash = cash
        self.start_cash = cash
        self.positions = {}

    @property
    def value(self):
        return self.cash + sum(p.value for p in self.positions.values())

    @property
    def positions_value(self):
        return sum(p.value for p in self.positions.values())

    @property
    def total_value(self):
        return self.value

    @property
    def available_cash(self):
        return self.cash

    def get_position(self, code):
        return self.positions.get(code, Position())

    def update_price(self, code, price):
        """更新已持有标的的最新价；不创建新条目（避免虚持仓）。"""
        pos = self.positions.get(code)
        if pos is not None:
            pos.price = price
