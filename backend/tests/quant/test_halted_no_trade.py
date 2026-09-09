"""停牌禁交易回归（2026-09-09 龙版传媒 605577 全天停牌，两模拟盘以陈旧价卖出）。

三层防御：
1. ``manager.is_halted_by_volume``：分钟窗正证据判定（全天/盘中），无数据放行；
2. jq ``order()``：停牌拒绝买卖（双向），无数据放行；
3. ``current_data.paused``：日线当日 bar 存在且无量 → True，未知保持 False。

全部离线：manager 用 stub，分钟帧内存注入；日期取当天，asof 用墙钟。
"""
from __future__ import annotations

import pandas as pd

from app.quant.jqengine.datasource.manager import is_halted_by_volume
from app.quant.jqengine.engine.jq import api
from app.quant.jqengine.engine.jq.context import Position

SENTINEL = 2.0 ** -127  # 停牌占位量（mootdx 无量 bar 原样透传）
STOCK = "605577.XSHG"


def _today_bars(n=60, start="09:31"):
    day = pd.Timestamp.now().date().isoformat()
    return pd.DatetimeIndex(pd.date_range(f"{day} {start}", periods=n, freq="min"))


def _df(idx, close=18.67, volume=SENTINEL):
    n = len(idx)
    return pd.DataFrame({
        "open": [close] * n, "high": [close] * n,
        "low": [close] * n, "close": [close] * n,
        "volume": [volume] * n, "amount": [volume * close] * n,
    }, index=idx)


class _Mgr:
    def __init__(self):
        self._minute_mem = {}

    def fetch(self, *a, **k):
        raise RuntimeError("stub")


# ---- helper：分钟窗判定 ----

def test_halted_full_day_sentinel_flat():
    mgr = _Mgr()
    mgr._minute_mem[STOCK] = _df(_today_bars(240))
    assert is_halted_by_volume(mgr, STOCK) is True


def test_normal_trading_not_halted():
    mgr = _Mgr()
    idx = _today_bars(60)
    df = _df(idx, volume=10000.0)
    df["close"] = [18.67 + (i % 5) * 0.01 for i in range(len(idx))]
    mgr._minute_mem[STOCK] = df
    assert is_halted_by_volume(mgr, STOCK) is False


def test_limit_up_with_volume_not_halted():
    """一字板但有成交（一字板被砸开又封住）：有量 → 不是停牌，涨跌停逻辑处理。"""
    mgr = _Mgr()
    mgr._minute_mem[STOCK] = _df(_today_bars(60), volume=50000.0)
    assert is_halted_by_volume(mgr, STOCK) is False


def test_no_bars_fail_open():
    """无数据（瞬态缺数）→ 未知，放行。08-25 误判教训：缺数≠停牌。"""
    assert is_halted_by_volume(_Mgr(), STOCK) is False
    mgr = _Mgr()
    mgr._minute_mem[STOCK] = pd.DataFrame()
    assert is_halted_by_volume(mgr, STOCK) is False


def test_no_volume_col_fail_open():
    mgr = _Mgr()
    mgr._minute_mem[STOCK] = pd.DataFrame({"close": [1.0, 1.0]}, index=_today_bars(2))
    assert is_halted_by_volume(mgr, STOCK) is False


def test_intraday_halt_trailing_window():
    """盘中停牌：上午真实成交 + 午后零量平线，14:30 判定为停牌；10:30 正常。"""
    mgr = _Mgr()
    day = pd.Timestamp.now().date().isoformat()
    am = pd.DatetimeIndex(pd.date_range(f"{day} 09:31", periods=60, freq="min"))
    pm = pd.DatetimeIndex(pd.date_range(f"{day} 13:01", periods=90, freq="min"))
    df_am = _df(am, volume=20000.0)
    df_am["close"] = [18.0 + i * 0.01 for i in range(len(am))]
    df_pm = _df(pm)  # 午后停牌：占位平线
    mgr._minute_mem[STOCK] = pd.concat([df_am, df_pm])
    assert is_halted_by_volume(mgr, STOCK, asof=pd.Timestamp(f"{day} 10:30")) is False
    assert is_halted_by_volume(mgr, STOCK, asof=pd.Timestamp(f"{day} 14:30")) is True


# ---- order() 双向拦截 ----

def _setup_order(prices, mem):
    ctx = api._reset(_Mgr(), 0.0003, 0.001, 100000.0)
    api._state["minute_prices"] = prices
    api._state["minute_mode"] = True
    api._state["manager"]._minute_mem.update(mem)
    return ctx


def test_order_sell_halted_refused():
    ctx = _setup_order({STOCK: 18.67}, {STOCK: _df(_today_bars(120))})
    ctx.portfolio.positions[STOCK] = Position(
        amount=3500, avg_cost=17.79, price=18.67, today_amount=0.0)
    assert api.order(STOCK, -3500) is False
    assert api._state["trades"] == []
    assert ctx.portfolio.positions[STOCK].amount == 3500


def test_order_buy_halted_refused():
    ctx = _setup_order({STOCK: 18.67}, {STOCK: _df(_today_bars(120))})
    assert api.order(STOCK, 100) is False
    assert STOCK not in ctx.portfolio.positions


def test_order_normal_sell_allowed():
    """对照组：有量有波动 → 放行（今日龙版场景外一切如常）。"""
    idx = _today_bars(120)
    df = _df(idx, volume=15000.0)
    df["close"] = [18.60 + (i % 7) * 0.01 for i in range(len(idx))]
    ctx = _setup_order({STOCK: 18.65}, {STOCK: df})
    ctx.portfolio.positions[STOCK] = Position(
        amount=3500, avg_cost=17.79, price=18.65, today_amount=0.0)
    assert api.order(STOCK, -3500) is True
    assert len(api._state["trades"]) == 1


# ---- current_data.paused ----

class _DailyMgr(_Mgr):
    def __init__(self, daily):
        super().__init__()
        self._daily = daily

    def fetch(self, method, code, start=None, end=None, **kwargs):
        return self._daily


def _daily_df(today_vol):
    day = pd.Timestamp.now().date()
    idx = pd.DatetimeIndex([day - pd.Timedelta(days=1), day])
    return pd.DataFrame({
        "open": [17.79, 18.67], "high": [17.79, 18.67],
        "low": [17.79, 18.67], "close": [17.79, 18.67],
        "volume": [1e7, today_vol], "amount": [1e8, today_vol * 18.67],
    }, index=idx)


def test_paused_true_on_sentinel_day_bar():
    ctx = api._reset(_DailyMgr(_daily_df(SENTINEL)), 0.0003, 0.001, 100000.0)
    ctx.current_dt = pd.Timestamp.now()
    assert api.get_current_data()[STOCK].paused is True


def test_paused_false_normal_day():
    ctx = api._reset(_DailyMgr(_daily_df(5e6)), 0.0003, 0.001, 100000.0)
    ctx.current_dt = pd.Timestamp.now()
    assert api.get_current_data()[STOCK].paused is False


def test_paused_false_no_today_bar():
    """无当日 bar（瞬态缺数）→ 未知，保持 False。08-25 误判教训。"""
    ctx = api._reset(
        _DailyMgr(_daily_df(5e6).iloc[:1]), 0.0003, 0.001, 100000.0)
    ctx.current_dt = pd.Timestamp.now()
    assert api.get_current_data()[STOCK].paused is False
