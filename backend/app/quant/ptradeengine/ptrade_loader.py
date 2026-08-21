"""PTrade 策略加载器：编译并执行 ptrade 策略代码，收集 init 与定时任务。

镜像 jqengine/engine/jq/loader.py。bundle 形状与 jq bundle 一致
（init_fn / hooks / daily / minute / conv），runner 驱动无需区分语言层。
"""
from __future__ import annotations

import inspect
import json
import sys

from . import ptrade_api


class StrategyBundle:
    """一次策略编译的产物。"""

    def __init__(self, init_fn, ctx, ns=None):
        self.init_fn = init_fn      # 用户 initialize(context)
        self.ctx = ctx
        ns = ns or {}
        # PTrade 钩子：handle_data/before_trading_start 为 (context, data) 签名；
        # after_trading_end 官方仅 (context)（部分移植版仍写 (context, data)）。
        # runner 只传 (context)，这里按函数签名自适应包装并注入 build_data_snapshot 快照
        # （after_trading_end 的 data 为保留字段，注入空快照即可）。
        self.handle_data = _wrap_with_data(ns.get("handle_data"))
        self.before_trading_start = _wrap_with_data(ns.get("before_trading_start"))
        self.after_trading_end = _wrap_with_data(ns.get("after_trading_end"))
        self._code_conv = getattr(ctx, "_code_conv", None) \
            or (ptrade_api.to_engine, ptrade_api.to_pt)
        # run_daily 触发前刷新策略 _BARS 快照到当前 bar：
        # runner 先触发 run_daily、后触发 handle_data（handle_data 才调 _capture_bars），
        # 若不提前刷新，13:10 回调读到的是上一根 bar 的价（与 jq 引擎读 minute_prices
        # 当前价差 1 tick → 股数错位）。jq bundle 无此方法，runner 跳过。
        capture = ns.get("_capture_bars") or ns.get("_set_last_data")
        if capture is not None and callable(capture):
            self.refresh_snapshot = _make_refresh(capture)
        else:
            self.refresh_snapshot = None

    @property
    def conv(self):
        return self._code_conv

    @property
    def daily(self):
        return list(ptrade_api._state["daily"])

    @property
    def minute(self):
        return list(ptrade_api._state["minute"])


def _wrap_with_data(fn):
    """把 (context, data) 钩子包装为 runner 的 (context) 签名，注入行情快照。

    官方 PTrade 中 after_trading_end(context) 仅 1 参（无 data）；按函数签名自适应：
    接受 2 参才注入 data 快照，1 参只传 context（对齐 wufu-v5.4.ptrade.py 等移植版）。
    """
    if fn is None:
        return None
    try:
        nargs = len(inspect.signature(fn).parameters)
    except (TypeError, ValueError):
        nargs = 2  # 签名不可得时保守按 (context, data) 传
    if nargs < 2:
        return lambda ctx: fn(ctx)

    def wrapped(ctx):
        data = ptrade_api.build_data_snapshot(ctx)
        return fn(ctx, data)

    return wrapped


def _make_refresh(capture):
    """构造 run_daily 前刷新策略 _BARS 快照的函数（capture 为策略 _capture_bars/_set_last_data）。"""

    def refresh(_ctx):
        from contextlib import suppress
        with suppress(Exception):
            capture(ptrade_api.build_data_snapshot(_ctx))

    return refresh


def load_strategy(code, manager, fee, slippage, cash):
    """编译执行 ptrade 策略文本，返回 :class:`StrategyBundle`。"""
    ctx = ptrade_api._reset(manager, fee, slippage, cash)
    ns = {
        "g": ctx.g,
        "context": ctx,
        "run_daily": ptrade_api.run_daily,
        "run_minute": ptrade_api.run_minute,
        "get_history": ptrade_api.get_history,
        "order": ptrade_api.order,
        "order_value": ptrade_api.order_value,
        "order_target_percent": ptrade_api.order_target_percent,
        "get_position": ptrade_api.get_position,
        "get_positions": ptrade_api.get_positions,
        "set_universe": ptrade_api.set_universe,
        "get_stock_status": ptrade_api.get_stock_status,
        "get_stock_name": ptrade_api.get_stock_name,
        "get_market_list": ptrade_api.get_market_list,
        "get_etf_list": ptrade_api.get_etf_list,
        "get_market_detail": ptrade_api.get_market_detail,
        "get_trading_day": ptrade_api.get_trading_day,
        "get_trade_days": ptrade_api.get_trade_days,
        "set_benchmark": ptrade_api.set_benchmark,
        "set_commission": ptrade_api.set_commission,
        "set_slippage": ptrade_api.set_slippage,
        "log": ptrade_api.log,
        "record": ptrade_api.record,
        "get_price": ptrade_api.get_price,
        "check_limit": ptrade_api.check_limit,
        "get_stock_info": ptrade_api.get_stock_info,
        "get_snapshot": ptrade_api.get_snapshot,
        "order_target": ptrade_api.order_target,
        "order_target_value": ptrade_api.order_target_value,
        "get_all_trades_days": ptrade_api.get_all_trades_days,
        "get_trading_day_by_date": ptrade_api.get_trading_day_by_date,
        "get_etf_info": ptrade_api.get_etf_info,
    }
    ns["json"] = json
    import math
    import os
    import warnings
    ns["math"] = math
    ns["os"] = os
    ns["warnings"] = warnings
    ns["datetime"] = sys.modules.get("datetime")
    exec(compile(code, "<strategy>", "exec"), ns)
    ctx.g = ns.get("g", ctx.g)
    init_fn = ns.get("initialize")
    if init_fn is None:
        raise ValueError("ptrade 策略缺少 def initialize(context)")
    return StrategyBundle(init_fn, ctx, ns)
