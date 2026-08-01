"""启动时 ETF 数据完整性检查 + 自动回源。"""
from __future__ import annotations

import logging
from datetime import date

import polars as pl

logger = logging.getLogger(__name__)


def backfill_etf_data(repo) -> dict:
    """检查 ETF 日线数据完整性（DuckDB 唯一数据源，无网络回源渠道）。

    返回统计 dict: {"daily_backfilled": int, "errors": int}
    """
    stats = {"daily_backfilled": 0, "errors": 0}

    etf_inst = repo.get_etf_instruments()
    if etf_inst.is_empty() or "symbol" not in etf_inst.columns:
        logger.info("ETF 回源: instruments_etf 为空，跳过")
        return stats

    symbols = etf_inst["symbol"].cast(pl.Utf8).to_list()
    today = date.today()

    # 查每个 ETF 的最新日线日期
    try:
        sym_placeholders = ", ".join(["?"] * len(symbols))
        latest_rows = repo.db.execute(
            f"SELECT symbol, MAX(date) as latest FROM kline_etf_daily "
            f"WHERE symbol IN ({sym_placeholders}) GROUP BY symbol",
            symbols,
        ).fetchall()
        latest_map = {r[0]: r[1] for r in latest_rows}
    except Exception:
        latest_map = {}

    # 找出需要回源的 ETF（缺失或过期 > 1 天）
    need_backfill = []
    for sym in symbols:
        latest = latest_map.get(sym)
        if latest is None or (today - latest).days > 1:
            need_backfill.append(sym)

    if not need_backfill:
        logger.info("ETF 回源: 全部 %d 只 ETF 数据完整", len(symbols))
        return stats

    logger.info("ETF 回源: %d/%d 只 ETF 缺失/过期，无网络回源渠道，跳过", len(need_backfill), len(symbols))

    logger.info(
        "ETF 回源完成: 日线 %d 只, 错误 %d",
        stats["daily_backfilled"],
        stats["errors"],
    )
    return stats
