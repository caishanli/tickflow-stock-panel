"""品种分类与印花税——税种判定的唯一实现。

- ``is_etf``：印花税免征判定（沪 5 / 深 15/16/18 开头为基金），唯一定义点。
  调用方（jq api / matcher / ptrade api / rqalpha 桥的印花税决策）只许 import，
  不许各写一份前缀表。
- ``classify_fund``：回测 instrument 类型（ETF/LOF/CS，决定 rqalpha 是否收
  印花税），纯 JQ 码前缀逻辑，``jqcompat._fund_instrument_type`` 为其薄别名。
- ``STAMP_TAX_RATE``：卖出印花税率，唯一来源（env ``QUANT_SIM_STAMP_TAX``，
  默认 0.0005）。各引擎的 ``DEFAULT_STAMP_TAX`` 只许是本值的 import 别名。
"""
from __future__ import annotations

import os

#: 卖出印花税率（股票 0.05%，ETF/LOF 免征由 is_etf 决定）
STAMP_TAX_RATE = float(os.environ.get("QUANT_SIM_STAMP_TAX", "0.0005"))


def is_etf(code: str | None) -> bool:
    """印花税免征判定：沪市 5 开头、深市 15/16/18 开头为基金。

    只用于税费判定，成交价取整走 ``core.tick.round_to_tick``（勿混用）。
    无后缀纯数字码同样按前缀判定；沪市无 5 开头股票。
    """
    num = (code or "").split(".")[0]
    return num.startswith("5") or num.startswith(("15", "16", "18"))


def classify_fund(code: str) -> str:
    """按代码段判定基金/证券类型（"ETF" / "LOF" / "CS"，JQ 码）。

    上交所(XSHG)：50 → LOF，其余 5 开头（51/52/53/55/56/58，宇宙实证）
    → ETF；深交所(XSHE)：15 → ETF，16 → LOF，18 → ETF；其余 → CS。
    注意深市 ``000xxx`` 是股票不是指数（后缀+前缀双判）。
    血泪：52 系曾被误判 CS 致单笔多扣 49.76 元印花税并滚雪球（2026-09-03 长窗
    对齐实锤；rqalpha_bridge 曾私藏过期副本，1 Core 已收敛）。
    """
    pure, _, exch = (code or "").partition(".")
    if exch == "XSHG":
        if pure.startswith("50"):
            return "LOF"
        if pure.startswith("5"):
            return "ETF"
    elif exch == "XSHE":
        if pure.startswith("15"):
            return "ETF"
        if pure.startswith("16"):
            return "LOF"
        if pure.startswith("18"):
            return "ETF"
    return "CS"
