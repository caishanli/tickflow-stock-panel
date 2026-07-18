"""engine/jq 撮合规则单测：整手 / 印花税 / T+1 / 涨跌停禁买卖 / trades 增量字段。

全部离线：manager 用 stub，价格经 ``_state["minute_prices"]`` 快照注入。
"""
from __future__ import annotations

import pytest

from app.quant.jqengine.engine.jq import api
from app.quant.jqengine.engine.jq.context import Position
from app.quant.jqengine.engine.jq.loader import load_strategy

STOCK = "600000.XSHG"
ETF = "510300.XSHG"


class _Mgr:
    """最小 manager stub：取价/下单路径不触碰它。"""
    sources = {}
    _daily_mem = {}
    _minute_mem = {}

    def fetch(self, *a, **k):
        raise RuntimeError("stub")

    def get_minute_price_at(self, code, dt):
        return None


def _setup(cash=100000.0, prices=None, fee=0.0003, slippage=0.001):
    ctx = api._reset(_Mgr(), fee, slippage, cash)
    api._state["minute_prices"] = prices or {}
    api._state["minute_mode"] = True
    return ctx


def _seed(ctx, code, amount, avg_cost, price, today_amount=0.0):
    ctx.portfolio.positions[code] = Position(
        amount=amount, avg_cost=avg_cost, price=price, today_amount=today_amount)


# ---- 整手 ----
def test_buy_rounds_down_to_lot():
    ctx = _setup(prices={ETF: 1.0})
    assert api.order(ETF, 150) is True
    assert ctx.portfolio.positions[ETF].amount == 100


def test_buy_less_than_one_lot_rejected():
    ctx = _setup(prices={ETF: 1.0})
    assert api.order(ETF, 50) is False
    assert ETF not in ctx.portfolio.positions
    assert api._state["trades"] == []


# ---- 费用口径 ----
def test_buy_fee_and_slippage():
    ctx = _setup(prices={ETF: 10.0}, fee=0.0003, slippage=0.001)
    assert api.order(ETF, 1000) is True
    # 成交价 10*1.001=10.01，佣金 round(10010*0.0003, 2)=3.0
    assert ctx.portfolio.cash == pytest.approx(100000.0 - 10010.0 - 3.0, abs=1e-6)
    t = api._state["trades"][-1]
    assert t["side"] == "buy" and t["price"] == 10.01 and t["tax"] == 0.0


def test_sell_stock_charges_stamp_tax():
    ctx = _setup(cash=0.0, prices={STOCK: 10.0}, fee=0.0003, slippage=0.001)
    _seed(ctx, STOCK, 1000, 9.0, 10.0)
    assert api.order(STOCK, -1000) is True
    # fill=9.99，turnover=9990，佣金 3.0，印花税 round(9990*0.0005, 2)=5.0
    assert ctx.portfolio.cash == pytest.approx(9990.0 - 3.0 - 5.0, abs=1e-6)
    t = api._state["trades"][-1]
    assert t["side"] == "sell" and t["tax"] == 5.0 and t["avg_cost"] == 9.0


def test_sell_etf_exempt_stamp_tax():
    ctx = _setup(cash=0.0, prices={ETF: 10.0}, fee=0.0003, slippage=0.001)
    _seed(ctx, ETF, 1000, 9.0, 10.0)
    assert api.order(ETF, -1000) is True
    assert ctx.portfolio.cash == pytest.approx(9990.0 - 3.0, abs=1e-6)
    assert api._state["trades"][-1]["tax"] == 0.0


# ---- T+1 ----
def test_t1_same_day_buy_not_closeable():
    ctx = _setup(prices={STOCK: 10.0})
    assert api.order(STOCK, 1000) is True
    assert ctx.portfolio.positions[STOCK].today_amount == 1000
    assert ctx.portfolio.positions[STOCK].closeable_amount == 0.0
    assert api.order(STOCK, -1000) is False  # 当日买入不可卖
    assert len(api._state["trades"]) == 1    # 卖出未成交


def test_t1_partial_closeable():
    ctx = _setup(cash=0.0, prices={STOCK: 10.0}, fee=0.0, slippage=0.0)
    _seed(ctx, STOCK, 1000, 9.0, 10.0, today_amount=400.0)
    assert api.order(STOCK, -1000) is True   # 截断到可卖 600
    pos = ctx.portfolio.positions[STOCK]
    assert pos.amount == 400
    assert api._state["trades"][-1]["amount"] == -600


def test_on_new_day_releases_today_amount():
    ctx = _setup(prices={STOCK: 10.0})
    api.order(STOCK, 1000)
    api.on_new_day()
    assert ctx.portfolio.positions[STOCK].today_amount == 0.0
    assert api.order(STOCK, -1000) is True
    assert STOCK not in ctx.portfolio.positions  # 全出后持仓清除


# ---- 涨跌停禁买卖 ----
def test_no_buy_blocks_limit_up():
    ctx = _setup(prices={STOCK: 10.0})
    api._state["no_buy"] = {STOCK}
    assert api.order(STOCK, 1000) is False
    assert api._state["trades"] == []


def test_no_sell_blocks_limit_down():
    ctx = _setup(prices={STOCK: 10.0})
    _seed(ctx, STOCK, 1000, 9.0, 10.0)
    api._state["no_sell"] = {STOCK}
    assert api.order(STOCK, -1000) is False
    assert STOCK in ctx.portfolio.positions


# ---- order_target_percent / 资金校验 ----
def test_order_target_percent_buy_and_close():
    ctx = _setup(prices={ETF: 10.0})
    assert api.order_target_percent(ETF, 0.5) is True
    assert ctx.portfolio.positions[ETF].amount == 5000
    api.on_new_day()
    assert api.order_target_percent(ETF, 0.0) is True
    assert ETF not in ctx.portfolio.positions


def test_buy_rejected_when_cash_insufficient():
    ctx = _setup(cash=1000.0, prices={ETF: 10.0})
    assert api.order(ETF, 1000) is False
    assert api._state["trades"] == []


# ---- 日志 sink ----
def test_log_sink_receives_strategy_logs():
    _setup()
    got = []
    api._state["log_sink"] = lambda level, msg: got.append((level, msg))
    api.log.info("hello")
    assert got == [("info", "hello")]
    api._state["log_sink"] = None


# ---- loader 钩子暴露 ----
def test_loader_exposes_intraday_hooks():
    code = (
        "def init(context):\n    pass\n\n"
        "def handle_data(context):\n    pass\n\n"
        "def before_trading_start(context):\n    pass\n"
    )
    b = load_strategy(code, _Mgr(), 0.0003, 0.001, 10000.0)
    assert b.handle_data is not None
    assert b.before_trading_start is not None
    assert b.after_trading_end is None
