"""L1 内部一致性：分钟分区聚合 → 与同日日线对拍（无需任何网络源）。

这是覆盖全历史的一层：外部 1min 源最多只有 ~94 个交易日，但日线有 10 年。
把当日分钟按 ``symbol`` 聚合成「日线形态」（open=first / high=max / low=min /
close=last / volume=sum / amount=sum）后与 ``kline_daily`` / ``kline_etf_daily``
比对，就能在没有任何外部源的情况下抓出量级错误。

两条硬性口径（务必先做单位归一，否则量纲问题会被误判成数值问题，见
docs/superpowers/specs/2026-11-07-multisource-data-verification-design.md §5.1）：

- 分钟量统一存**股**；``kline_daily`` 存**手**、``kline_etf_daily`` 存**股**。
  所以比对前一律走 :func:`~app.quant.data_verify.units.minute_volume_to_daily`。
- 收盘价取**最后一根 ``volume > 0`` 的 bar**：提前收盘/停牌日尾段是 0 量平推，
  直接 ``last(close)`` 会在 2016-01-04 这类日子误报（§2.2）。

判定：

- 归一后的量比 ≈100（95~105）→ ``MINUTE_VOLUME_X100``（``hard``）：这是科创板
  旧解析器把腾讯「已经是股」的原值又 ×100 的指纹，2026-09-10 起约 60 个交易日
  ×616 只被污染；
- 量比超出容差 → ``MINUTE_VOLUME_MISMATCH``（``hard``）；
- 收盘价差超出 ``max(abs, rel)`` → ``MINUTE_CLOSE_MISMATCH``（``soft``，提前收盘
  日属正常，需外部源佐证才修）；
- 成交额差超容差 → ``MINUTE_AMOUNT_MISMATCH``（``soft``，腾讯分钟 amount 是按
  ``vol × typical`` 估出来的，永不作为修复依据）。

日线 ``volume == 0``（停牌）的标的整体跳过——停牌日分钟空、日线 0 量是正常态，
不该刷告警。只在一侧出现的 symbol 只在 ``detail`` 里计数，不逐条报。
"""

from __future__ import annotations

from typing import Any, Final

import polars as pl

from app.quant.data_verify.findings import (
    SEVERITY_HARD,
    SEVERITY_SOFT,
    Finding,
)
from app.quant.data_verify.units import minute_volume_to_daily

#: 错误码常量（report / policy / repair 按码分派）。
MINUTE_VOLUME_X100: Final = "MINUTE_VOLUME_X100"
MINUTE_VOLUME_MISMATCH: Final = "MINUTE_VOLUME_MISMATCH"
MINUTE_CLOSE_MISMATCH: Final = "MINUTE_CLOSE_MISMATCH"
MINUTE_AMOUNT_MISMATCH: Final = "MINUTE_AMOUNT_MISMATCH"
NON_FINITE: Final = "NON_FINITE"

#: 「量被放大 100 倍」的判定区间（单位归一后的比值）。
X100_RATIO_LOW: Final = 95.0
X100_RATIO_HIGH: Final = 105.0

#: :func:`aggregate_minute` 的输出列与类型。
AGG_SCHEMA: Final[dict[str, pl.DataType]] = {
    "symbol": pl.String(),
    "m_open": pl.Float64(),
    "m_high": pl.Float64(),
    "m_low": pl.Float64(),
    "m_close": pl.Float64(),
    "m_volume": pl.Float64(),
    "m_amount": pl.Float64(),
    "bars": pl.UInt32(),
}

#: 分钟聚合的必需列。
_MINUTE_REQUIRED: Final = ("symbol", "datetime", "open", "high", "low", "close", "volume")

#: 日线比对的必需列。
_DAILY_REQUIRED: Final = ("symbol", "volume")


def aggregate_minute(minute: pl.DataFrame) -> pl.DataFrame:
    """把一个分区（或多日）的分钟行按 ``symbol`` 聚合成日线形态。

    返回列：``symbol, m_open, m_high, m_low, m_close, m_volume, m_amount, bars``。

    ``m_close`` 取**最后一根 ``volume > 0`` 的 bar** 的收盘价；若该 symbol 全
    0 量（停牌日）则退回最后一根 bar 的收盘价——此时收盘价没有意义，但保留
    原值让上层能记录「全 0 量」而不是被 null 吞掉。``m_amount`` 在分钟分区
    没有 ``amount`` 列时记 0.0（指数分钟没有成交额）。
    """
    absent = [c for c in _MINUTE_REQUIRED if c not in minute.columns]
    if absent:
        raise ValueError(f"aggregate_minute 缺少必需列: {absent}")
    if minute.is_empty():
        return pl.DataFrame(schema=AGG_SCHEMA)

    amount_expr = (
        pl.col("amount").cast(pl.Float64).sum()
        if "amount" in minute.columns
        else pl.lit(0.0, dtype=pl.Float64)
    )
    volume_positive = pl.col("volume").fill_null(0.0) > 0

    return (
        minute.sort(["symbol", "datetime"])
        .group_by("symbol", maintain_order=True)
        .agg(
            pl.col("open").cast(pl.Float64).first().alias("m_open"),
            pl.col("high").cast(pl.Float64).max().alias("m_high"),
            pl.col("low").cast(pl.Float64).min().alias("m_low"),
            pl.coalesce(
                pl.col("close").cast(pl.Float64).filter(volume_positive).last(),
                pl.col("close").cast(pl.Float64).last(),
            ).alias("m_close"),
            pl.col("volume").cast(pl.Float64).sum().alias("m_volume"),
            amount_expr.alias("m_amount"),
            pl.len().cast(pl.UInt32).alias("bars"),
        )
        .sort("symbol")
        .select(list(AGG_SCHEMA))
    )


def _nonfinite_inputs(
    minute: pl.DataFrame, daily: pl.DataFrame, daily_day: pl.Expr, *, table: str
) -> tuple[list[Finding], pl.DataFrame | None]:
    """先找输入侧的 NaN/Inf；这些 symbol 不再参与后续数值判定。"""
    bad: dict[tuple[str, Any], dict[str, set[str]]] = {}
    datasets = (
        ("minute", minute, pl.col("datetime").dt.date()),
        ("daily", daily, daily_day),
    )
    for side, frame, day_expr in datasets:
        fields = [name for name in ("volume", "close", "amount") if name in frame.columns]
        if not fields:
            continue
        selected = frame.select(
            pl.col("symbol"),
            day_expr.alias("_day"),
            *[
                (~pl.col(name).cast(pl.Float64).is_finite()).fill_null(False).alias(name)
                for name in fields
            ],
        )
        invalid = selected.filter(pl.any_horizontal([pl.col(name) for name in fields]))
        for row in invalid.iter_rows(named=True):
            key = (str(row["symbol"]), row["_day"])
            entry = bad.setdefault(key, {side: set() for side in ("minute", "daily")})
            entry[side].update(name for name in fields if row[name])

    findings: list[Finding] = []
    for (symbol, day), sides in sorted(bad.items(), key=lambda item: item[0][0]):
        findings.append(Finding(
            table=table,
            day=day,
            symbol=symbol,
            code=NON_FINITE,
            severity=SEVERITY_HARD,
            detail={"fields": {
                side: sorted(names) for side, names in sides.items() if names
            }},
        ))
    if not bad:
        return [], None
    keys = pl.DataFrame(
        [{"symbol": symbol, "_day": day} for symbol, day in bad],
        schema={"symbol": pl.String, "_day": pl.Date},
        orient="row",
    )
    return findings, keys


def compare_minute_to_daily(
    minute: pl.DataFrame,
    daily: pl.DataFrame,
    *,
    table: str,
    price_abs_tol: float = 0.011,
    price_rel_tol: float = 0.001,
    volume_rel_tol: float = 0.005,
    amount_rel_tol: float = 0.02,
) -> list[Finding]:
    """分钟聚合 vs 同日日线，返回逐 symbol 的 :class:`Finding` 列表。

    ``table`` 是分钟分区表名（``kline_minute`` / ``kline_etf_minute``），用于
    单位折算（见 :func:`~app.quant.data_verify.units.minute_volume_to_daily`）
    并原样写进 ``Finding.table``——修复动作发生在分钟分区上。

    ``daily`` 需要 ``symbol`` / ``date`` / ``volume`` 列（``close`` / ``amount``
    可选）；没有 ``date`` 列时退回分钟侧的唯一交易日，跨多日则报错。日线
    ``volume == 0`` 的行（停牌）整体跳过。

    只在一侧出现的 symbol 不产出 ``Finding``，而是在每条 ``detail`` 的
    ``unmatched`` 里计数（覆盖度结论由 runner 汇总；这里逐条报会刷屏）。
    """
    if minute.is_empty() or daily.is_empty():
        return []

    absent = [c for c in _DAILY_REQUIRED if c not in daily.columns]
    if absent:
        raise ValueError(f"compare_minute_to_daily 缺少日线必需列: {absent}")

    agg = aggregate_minute(minute)

    if "date" in daily.columns:
        day_col = pl.col("date").cast(pl.Date)
    else:
        days = minute["datetime"].dt.date().unique()
        if days.len() != 1:
            raise ValueError("daily 缺 date 列且分钟跨多个交易日，无法确定 day")
        day_col = pl.lit(days[0], dtype=pl.Date)

    daily_exprs: list[pl.Expr] = [
        pl.col("symbol"),
        day_col.alias("_day"),
        pl.col("volume").cast(pl.Float64).alias("d_volume"),
    ]
    if "close" in daily.columns:
        daily_exprs.append(pl.col("close").cast(pl.Float64).alias("d_close"))
    if "amount" in daily.columns:
        daily_exprs.append(pl.col("amount").cast(pl.Float64).alias("d_amount"))

    unmatched_minute = len(set(agg["symbol"].to_list()) - set(daily["symbol"].to_list()))
    unmatched_daily = len(set(daily["symbol"].to_list()) - set(agg["symbol"].to_list()))
    unmatched: dict[str, int] = {}
    if unmatched_minute:
        unmatched["minute_only"] = unmatched_minute
    if unmatched_daily:
        unmatched["daily_only"] = unmatched_daily

    findings, nonfinite_keys = _nonfinite_inputs(minute, daily, day_col, table=table)
    joined = agg.join(daily.select(daily_exprs), on="symbol", how="inner")
    if nonfinite_keys is not None:
        joined = joined.join(nonfinite_keys, on=["symbol", "_day"], how="anti")
    joined = joined.filter(pl.col("d_volume").is_not_null() & (pl.col("d_volume") != 0))

    for row in joined.sort("symbol").iter_rows(named=True):
        symbol = str(row["symbol"])
        day = row["_day"]
        m_volume = float(row["m_volume"] or 0.0)
        d_volume = float(row["d_volume"])
        converted = minute_volume_to_daily(m_volume, table)
        raw_ratio = m_volume / d_volume
        norm_ratio = converted / d_volume

        detail: dict[str, Any] = {
            "minute_volume": m_volume,
            "daily_volume": d_volume,
            "converted_volume": converted,
            "ratio": norm_ratio,
            "raw_ratio": raw_ratio,
        }
        if unmatched:
            detail["unmatched"] = dict(unmatched)

        if X100_RATIO_LOW <= norm_ratio <= X100_RATIO_HIGH:
            # 单位归一后仍大两个数量级：分钟量被 ×100（腾讯科创板原值已是股）。
            findings.append(
                Finding(
                    table=table,
                    day=day,
                    symbol=symbol,
                    code=MINUTE_VOLUME_X100,
                    severity=SEVERITY_HARD,
                    detail=detail,
                )
            )
        elif abs(converted - d_volume) > volume_rel_tol * abs(d_volume):
            findings.append(
                Finding(
                    table=table,
                    day=day,
                    symbol=symbol,
                    code=MINUTE_VOLUME_MISMATCH,
                    severity=SEVERITY_HARD,
                    detail={**detail, "rel_deviation": abs(converted - d_volume) / d_volume},
                )
            )

        m_close = row.get("m_close")
        d_close = row.get("d_close")
        if m_close is not None and d_close is not None:
            tolerance = max(price_abs_tol, price_rel_tol * abs(float(d_close)))
            diff = abs(float(m_close) - float(d_close))
            if diff > tolerance:
                # 提前收盘/停牌尾段差异属正常，只有外部源能定性 → soft。
                findings.append(
                    Finding(
                        table=table,
                        day=day,
                        symbol=symbol,
                        code=MINUTE_CLOSE_MISMATCH,
                        severity=SEVERITY_SOFT,
                        detail={
                            "minute_close": float(m_close),
                            "daily_close": float(d_close),
                            "diff": diff,
                            "tolerance": tolerance,
                        },
                    )
                )

        d_amount = row.get("d_amount")
        if d_amount is not None:
            m_amount = float(row.get("m_amount") or 0.0)
            d_amount = float(d_amount)
            if d_amount > 0:
                amount_deviation = abs(m_amount - d_amount) / d_amount
                if amount_deviation > amount_rel_tol:
                    findings.append(
                        Finding(
                            table=table,
                            day=day,
                            symbol=symbol,
                            code=MINUTE_AMOUNT_MISMATCH,
                            severity=SEVERITY_SOFT,
                            detail={
                                "minute_amount": m_amount,
                                "daily_amount": d_amount,
                                "rel_deviation": amount_deviation,
                            },
                        )
                    )
            elif m_amount > 0:
                findings.append(
                    Finding(
                        table=table,
                        day=day,
                        symbol=symbol,
                        code=MINUTE_AMOUNT_MISMATCH,
                        severity=SEVERITY_SOFT,
                        detail={
                            "minute_amount": m_amount,
                            "daily_amount": d_amount,
                            "rel_deviation": None,
                        },
                    )
                )

    return findings
