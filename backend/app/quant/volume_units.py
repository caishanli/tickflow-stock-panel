"""ETF 日线成交量单位归一。

``kline_etf_daily`` 的落盘契约是 **volume = 股**，且读侧不做换算
（``DataManager._load_daily_from_partitions`` 明确「ETF 日线存盘即为股，不换算」）。
但往这张表写数据的入口不止一个，历史上出现过把**股票口径（手）**套到 ETF 上：

- ``archive_import.convert_daily`` 的 ÷100 是股票口径（本机 kline_daily 存手），
  ``archive_fill`` 原先只按 ``--kind`` 选转换器、不看目标表，用
  ``--target-root data/kline_etf_daily`` 跑一次 ETF 的 volume 就被 ÷100；
- TickFlow 跨源补齐（``index_sync.sync_and_persist_etf_daily``）原先原样落盘。

写错不会报错：策略的 ``volume_ratio`` 只会差 100 倍，然后被成交量过滤器
静默踢掉，选股悄悄改变（2026-10 五福模拟盘 10-08 那天动量第一名 159509
「成交量比值 108.85 ❌」被否，就是这起事故）。

本模块用 ``amount / (volume × close)`` 的中位数判单位：≈1 是「股」，
≈100 是「手」，据此归一，并在无法判定时告警。
"""

from __future__ import annotations

import logging
from typing import cast

import polars as pl

logger = logging.getLogger(__name__)

# amount/(volume×close) 落在这些区间才认定为「可判定」，避免把别的量纲硬改
SHARES_RANGE = (0.5, 2.0)
LOTS_RANGE = (50.0, 200.0)


def infer_etf_volume_unit(df: pl.DataFrame) -> str:
    """推断 ETF 日线 volume 的单位。

    返回 ``"shares"``（股）、``"lots"``（手）或 ``"unknown"``（列缺失/全空/量纲异常）。
    """
    if df.is_empty() or not {"close", "volume", "amount"} <= set(df.columns):
        return "unknown"
    ratio = df.select(
        pl.when((pl.col("volume") > 0) & (pl.col("close") > 0) & (pl.col("amount") > 0))
        .then(pl.col("amount") / (pl.col("volume") * pl.col("close")))
        .otherwise(None)
        .alias("r")
    )["r"].drop_nulls()
    if ratio.is_empty():
        return "unknown"
    med_raw = ratio.median()
    if med_raw is None:
        return "unknown"
    # polars 的 median() 返回类型很宽（int|float|Decimal|…），这里只可能是 float
    med = float(cast(float, med_raw))
    if SHARES_RANGE[0] <= med <= SHARES_RANGE[1]:
        return "shares"
    if LOTS_RANGE[0] <= med <= LOTS_RANGE[1]:
        return "lots"
    return "unknown"


def normalize_etf_daily_volume(
    df: pl.DataFrame, *, source: str = ""
) -> tuple[pl.DataFrame, str]:
    """把 ETF 日线 volume 归一到「股」。

    返回 ``(归一后的帧, 判定出的原单位)``。``"unknown"`` 时原样返回（只告警），
    绝不猜测性改写。
    """
    unit = infer_etf_volume_unit(df)
    if unit == "lots":
        logger.warning(
            "ETF 日线 volume 判定为「手」（来源 %s），已 ×100 归一到「股」",
            source or "unknown",
        )
        return df.with_columns((pl.col("volume") * 100).alias("volume")), unit
    if unit == "unknown":
        logger.warning(
            "ETF 日线 volume 单位无法判定（来源 %s），保持原值；"
            "若该帧确实缺 amount/close，请人工核对该分区的量纲",
            source or "unknown",
        )
    return df, unit
