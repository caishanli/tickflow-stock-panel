"""核验结论的统一载体。

L0/L1/L2/因子各层只负责产出 :class:`Finding`，由 runner 汇总、report 落盘、
policy 决定修还是隔离。``Finding`` 必须可 JSON 序列化（``to_dict`` 把 ``date``
转 ISO 字符串），且不可变（``frozen=True``）——同一条结论被多处引用时不该被
就地改写。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

#: 严重程度：``hard`` = 数据确定有错（可据此修复）；``soft`` = 可疑/口径差异
#: （只报告，不驱动修复）。
SEVERITY_HARD = "hard"
SEVERITY_SOFT = "soft"


@dataclass(frozen=True)
class Finding:
    """一处核验发现。

    ``table`` 是目标分区表名（``kline_minute`` / ``kline_daily`` / ...），
    ``day`` 是分区交易日，``symbol`` 为该条对应的标的（分区级结论可用 ``None``
    表示整表），``code`` 是错误码常量，``detail`` 放定位/量化信息。
    """

    table: str
    day: date
    symbol: str | None
    code: str
    severity: str
    detail: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """JSON 友好形态：``day`` → ``"YYYY-MM-DD"``。"""
        return {
            "table": self.table,
            "day": self.day.isoformat(),
            "symbol": self.symbol,
            "code": self.code,
            "severity": self.severity,
            "detail": dict(self.detail),
        }
