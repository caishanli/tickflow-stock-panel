"""启动时 ETF 数据完整性检查 + 自动回源。"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

logger = logging.getLogger(__name__)


def backfill_etf_data(repo) -> dict:
    """检查 ETF 日线数据完整性，缺失的从 mootdx 回源补齐。

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

    logger.info("ETF 回源: %d/%d 只 ETF 需要补齐", len(need_backfill), len(symbols))

    # 从 mootdx 回源日线
    try:
        from app.quant.jqengine.datasource.mootdx_src import MootdxSource

        msrc = MootdxSource()
        for sym in need_backfill:
            try:
                latest = latest_map.get(sym)
                start = latest + timedelta(days=1) if latest else today - timedelta(days=365)
                # mootdx 需要纯数字代码
                pure = sym.split(".")[0]
                df = msrc.get_daily(pure, start, today)
                if df is not None and not df.empty:
                    pdf = pl.from_pandas(df)
                    pdf = pdf.with_columns(pl.lit(sym).alias("symbol"))
                    # 列映射: mootdx 返回 money/volume, 需要 amount/volume
                    if "money" in pdf.columns and "amount" not in pdf.columns:
                        pdf = pdf.rename({"money": "amount"})
                    cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
                    available = [c for c in cols if c in pdf.columns]
                    pdf = pdf.select(available)
                    repo.append_etf_daily(pdf)
                    stats["daily_backfilled"] += 1
            except Exception as e:
                stats["errors"] += 1
                logger.debug("ETF 日线回源失败 %s: %s", sym, e)
    except Exception as e:
        logger.warning("ETF 日线回源 mootdx 初始化失败: %s", e)

    logger.info(
        "ETF 回源完成: 日线 %d 只, 错误 %d",
        stats["daily_backfilled"],
        stats["errors"],
    )
    return stats
