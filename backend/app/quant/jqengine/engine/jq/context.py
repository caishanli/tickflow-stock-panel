"""聚宽兼容层 - context / g / Position 对象。"""

from types import SimpleNamespace


class G(SimpleNamespace):
    """聚宽 ``g`` 全局可变对象，策略可在其上挂任意属性。"""


class Position:
    """持仓：数量 / 成本 / 现价。``today_amount`` 为当日买入量（T+1 当日不可卖）。"""

    def __init__(self, amount=0, avg_cost=0.0, price=0.0, today_amount=0.0,
                 entry_ts=None, price_ts=None):
        self.amount = amount
        self.avg_cost = avg_cost
        self.price = price
        self.today_amount = today_amount
        self.entry_ts = entry_ts  # 首次建仓时间（模拟盘展示用）
        self.price_ts = price_ts  # 现价对应行情 bar 时间（模拟盘展示用）

    @property
    def total_amount(self):
        return self.amount

    @property
    def closeable_amount(self):
        return max(0.0, self.amount - float(self.today_amount or 0.0))

    # ---- PTrade 字段别名 ----
    # 模拟盘重启续跑时 _restore_portfolio 构造的是本基础类；ptrade 移植版策略
    # （如 a56cb087）按 PTrade 口径访问 enable_amount/cost_basis/last_sale_price，
    # 缺失即每 bar AttributeError（960366ab 08-21 117 条错误日志根因）。

    @property
    def enable_amount(self):
        """PTrade 口径可用数量（T+1：当日买入不可卖）。"""
        return self.closeable_amount

    @property
    def cost_basis(self):
        """PTrade 口径持仓成本。"""
        return self.avg_cost

    @property
    def last_sale_price(self):
        """PTrade/rqalpha 口径最新价。"""
        return self.price

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
