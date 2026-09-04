"""最小报价单位（tick）共享口径。

回测撮合（``jqcompat._patch_matcher_tick_rounding``）、模拟盘引擎
（``jqengine order()``）、止损撮合（``simulate.matcher``）三处成交价取整
必须走同一实现，否则同策略回测 vs 补跑逐笔价差半个 tick、净值系统性漂移。

规则（纯代码前缀判定，ETF/基金 0.001、其余 0.01）：
- 沪市 5 开头（ETF/LOF/基金）→ 0.001；
- 深市 15/16/18 开头（ETF/LOF/分级等场内基金）→ 0.001；
- 其余（股票、北交所等）→ 0.01。

无交易所后缀的纯数字代码同样按前缀判定。

注意：本判定只用于成交价取整。各处的 ``_is_etf`` 是印花税免征判定，
口径相近但用途不同，不要混用（税改要改那边，tick 改这边）。
"""
from __future__ import annotations

import math

ETF_TICK = 0.001
STOCK_TICK = 0.01


def tick_size(code: str | None) -> float:
    """给定聚宽代码（``510300.XSHG``，纯数字亦可）返回最小报价单位。"""
    num = (code or "").split(".")[0].strip()
    if num.startswith("5") or num.startswith(("15", "16", "18")):
        return ETF_TICK
    return STOCK_TICK


def round_to_tick(price: float, code: str | None) -> float:
    """成交价按标的 tick 取整（step quantize，三处调用方的唯一实现）。

    不要在调用方各写一种：``round(x, 2)`` 与
    ``round(round(x / step) * step, …)`` 在 half-way 边界上结果不同
    （banker's rounding 作用量级不同），各写一种等于没统一。
    非有限输入（NaN/inf，无行情脏数据）原样返回，由调用方既有逻辑处理。
    """
    px = float(price)
    if not math.isfinite(px):
        return px
    if tick_size(code) >= STOCK_TICK:
        return round(round(px / STOCK_TICK) * STOCK_TICK, 2)
    return round(round(px / ETF_TICK) * ETF_TICK, 3)
