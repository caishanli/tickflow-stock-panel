"""修复后的 enriched 失效与重算（design §6.5）。

日线族（``kline_daily`` / ``kline_etf_daily`` / ``kline_index_daily``）被修好后，
下游 ``*_enriched`` **不会自动失效**：增量计算只补 enriched 里不存在的日期，
已存在的（错误的）分区永远不会重算。所以每次日线写入修复之后必须：

1. 按表逐个 :func:`app.services.data_integrity.prune_enriched_partitions` 删除
   ``date >= start`` 的 enriched 分区（``start`` = 本次修复最早的一天）；
2. 调一次盘后管道的重算入口（``runner``），让这些日期按"新日期"重新生成。

``prune`` / ``runner`` 由调用方注入：本模块只做编排，不 import 具体管道实现
（真实接线在每日增量调度里，见 design §7.2）。这一阶段失败**不回滚已修数据**，
只把异常收进返回值 —— 修复本身是对的，重算是下一轮可以补的。

设计约束：分钟族没有派生表，不参与重算；``days`` 为空时什么都不做（不 prune、
不跑 pipeline），避免空范围把整表重算一遍。
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: ``prune(data_dir, start, table)`` —— 真实实现见
#: :func:`app.services.data_integrity.prune_enriched_partitions`。
PruneFn = Callable[[Path, date, str], Any]
#: ``runner(repo, capset, days)`` —— 由调用方注入盘后管道重算实现。
RunnerFn = Callable[[Any, Any, list[date]], Any]


def _as_date(value: Any) -> date:
    """``date`` / ``datetime`` / ISO 字符串统一成 ``date``。"""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _resolve_data_dir(data_dir: Path | str | None) -> Path:
    """未显式给 ``data_dir`` 时用全局 ``settings.data_dir``（与落盘路径同源）。"""
    if data_dir is not None:
        return Path(data_dir)
    from app.config import settings

    return Path(settings.data_dir)


def _default_prune() -> PruneFn:
    """默认 prune 适配器；延迟 import 避免 quant 包在启动期拖入 services。"""
    from app.services.data_integrity import prune_enriched_partitions

    return prune_enriched_partitions


def recompute_enriched(
    days: Iterable[date],
    *,
    tables: list[str],
    prune: PruneFn | None = None,
    runner: RunnerFn,
    data_dir: Path | str | None = None,
    repo: Any = None,
    capset: Any = None,
) -> dict:
    """失效并重算 ``days`` 覆盖的 enriched 日期。

    - ``tables``：要 prune 的 enriched 表（按族传入，见 design §6.5）；
    - ``prune``：``prune(data_dir, start, table)``，默认用
      :func:`app.services.data_integrity.prune_enriched_partitions`；
    - ``runner``：``runner(repo, capset, sorted(days))``，修复后的重算入口；
    - ``data_dir`` / ``repo`` / ``capset``：透传给上面两个可调用对象（测试用
      fake 时可省略）。

    返回 ``{"ok": bool, "days": int, "error": str | None}``；任何异常（含 prune
    中途失败、pipeline 抛错）都收敛成 ``ok=False``，**不向调用方抛**。
    """
    try:
        normalized = sorted({_as_date(day) for day in days})
    except (TypeError, ValueError) as e:
        logger.warning("enriched recompute: invalid days: %s", e)
        return {"ok": False, "days": 0, "error": str(e)}

    if not normalized:
        return {"ok": True, "days": 0, "error": None}

    try:
        prune_fn = prune or _default_prune()
        target_dir = _resolve_data_dir(data_dir)
        start = normalized[0]
        removed = {table: prune_fn(target_dir, start, table) for table in tables}
        runner(repo, capset, list(normalized))
    except Exception as e:
        logger.warning("enriched recompute failed (days=%s, tables=%s): %s",
                       normalized[0], tables, e)
        return {"ok": False, "days": len(normalized), "error": str(e)}

    logger.info("enriched recompute done: days=%s..%s tables=%s pruned=%s",
                normalized[0], normalized[-1], tables, removed)
    return {"ok": True, "days": len(normalized), "error": None}


__all__ = ["recompute_enriched"]
