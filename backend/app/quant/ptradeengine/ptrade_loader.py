"""PTrade 策略加载器：编译并执行 ptrade 策略代码，收集 init 与定时任务。

镜像 jqengine/engine/jq/loader.py。bundle 形状与 jq bundle 一致
（init_fn / hooks / daily / minute / conv），runner 驱动无需区分语言层。
"""
from __future__ import annotations

import json
import sys

from . import ptrade_api


class StrategyBundle:
    """一次策略编译的产物。"""

    def __init__(self, init_fn, ctx, ns=None):
        self.init_fn = init_fn      # 用户 initialize(context)
        self.ctx = ctx
        ns = ns or {}
        # PTrade 钩子：handle_data/before_trading_start 需 (context, data) 快照，
        # runner 只传 (context)，这里包装为单参并注入 build_data_snapshot。
        self.handle_data = _wrap_with_data(ns.get("handle_data"))
        self.before_trading_start = _wrap_with_data(ns.get("before_trading_start"))
        self.after_trading_end = ns.get("after_trading_end")
        self._code_conv = getattr(ctx, "_code_conv", None) \
            or (ptrade_api.to_engine, ptrade_api.to_pt)

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
    """把 (context, data) 钩子包装为 runner 的 (context) 签名，注入行情快照。"""
    if fn is None:
        return None

    def wrapped(ctx):
        data = ptrade_api.build_data_snapshot(ctx)
        return fn(ctx, data)

    return wrapped


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
        "get_market_detail": ptrade_api.get_market_detail,
        "set_benchmark": ptrade_api.set_benchmark,
        "set_commission": ptrade_api.set_commission,
        "set_slippage": ptrade_api.set_slippage,
        "log": ptrade_api.log,
        "record": ptrade_api.record,
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
