"""聚宽策略加载器：编译并执行用户策略代码，收集 init 与定时任务。"""

import json
import sys
import types

from . import api
from .context import Context, G
from .portfolio import Portfolio


# Fake jqdata module so "from jqdata import *" in strategies doesn't crash
_jqdata = types.ModuleType("jqdata")
_jqdata.__all__ = []
sys.modules.setdefault("jqdata", _jqdata)


class StrategyBundle:
    """一次策略编译的产物。"""

    def __init__(self, init_fn, ctx, ns=None):
        self.init_fn = init_fn      # 用户 init(context) 函数
        self.ctx = ctx
        ns = ns or {}
        # 聚宽式盘中/盘前/盘后钩子（策略未定义则为 None，由驱动方判空调用）
        self.handle_data = ns.get("handle_data")
        self.before_trading_start = ns.get("before_trading_start")
        self.after_trading_end = ns.get("after_trading_end")

    @property
    def daily(self):
        return list(api._state["daily"])

    @property
    def minute(self):
        return list(api._state["minute"])


def load_strategy(code, manager, fee, slippage, cash):
    """编译执行策略文本，返回 :class:`StrategyBundle`。

    用户代码在注入了聚宽兼容 API 的命名空间中执行；执行期间对
    ``run_daily``/``run_minute`` 的注册会被收集。
    """
    ctx = api._reset(manager, fee, slippage, cash)
    ns = {
        "g": ctx.g,
        "context": ctx,
        "init": api.init,
        "run_daily": api.run_daily,
        "run_minute": api.run_minute,
        "get_price": api.get_price,
        "order": api.order,
        "order_value": api.order_value,
        "order_target_percent": api.order_target_percent,
        "log": api.log,
        "record": api.record,
        "set_option": api.set_option,
        "set_slippage": api.set_slippage,
        "set_order_cost": api.set_order_cost,
        "set_benchmark": api.set_benchmark,
        "get_current_data": api.get_current_data,
        "get_security_name": api.get_security_name,
        "get_all_securities": api.get_all_securities,
        "get_trade_days": api.get_trade_days,
        "get_extras": api.get_extras,
        "attribute_history": api.attribute_history,
        "is_temporarily_suspended": api.is_temporarily_suspended,
        "PriceRelatedSlippage": api.PriceRelatedSlippage,
        "OrderCost": api.OrderCost,
        "get_security_info": api.get_security_info,
    }
    # 策略脚本中用到的标准库
    import requests as _requests_mod
    ns["requests"] = _requests_mod
    ns["json"] = json
    exec(compile(code, "<strategy>", "exec"), ns)
    # 用户可能在模块级重新赋值 g，保留其版本
    ctx.g = ns.get("g", ctx.g)
    init_fn = ns.get("init", api.init)
    if init_fn is api.init and "initialize" in ns:
        init_fn = ns["initialize"]
    return StrategyBundle(
        init_fn,
        ctx,
        ns,
    )
