"""L0 结构检查：只看分区自身是否自洽，不需要任何网络。

对每个 symbol 检查四类问题：

- **bar 数**：不足 ``expected_bars``（默认 241）记 ``MISSING_BARS``。停牌、新股
  首日、提前收盘都会让 bar 数不足，所以这条只是 ``soft``，且停牌名单内的
  symbol 直接跳过——不能把正常场景刷成告警。
- **时间戳**：``09:30``–``11:30`` / ``13:01``–``15:00`` 之外 → ``OUT_OF_SESSION``；
  腾讯/新浪的午休边界偶尔会写出 ``13:00`` bar，单独记 ``FORBIDDEN_1300``；
  同一 symbol 同一分钟出现多行 → ``DUPLICATE_BAR``。
- **数值**：NaN/Inf/空值 → ``NON_FINITE``；``low > min(open, close)``、
  ``high < max(open, close)``、``high < low``、量/额为负 → ``OHLC_INVALID``。
- **非空但全 0 量**：停牌名单外记 ``ALL_ZERO``（可疑，``soft``）。

所有判定用整表向量化掩码 + 一次 ``group_by("symbol")`` 聚合完成，分区再大
（百万行级）也只扫一遍。
"""

from __future__ import annotations

from datetime import date
from typing import Any, Final

import polars as pl

from app.quant.data_verify.findings import (
    SEVERITY_HARD,
    SEVERITY_SOFT,
    Finding,
)

#: 错误码常量（report / policy 按码分派）。
MISSING_BARS: Final = "MISSING_BARS"
DUPLICATE_BAR: Final = "DUPLICATE_BAR"
OUT_OF_SESSION: Final = "OUT_OF_SESSION"
FORBIDDEN_1300: Final = "FORBIDDEN_1300"
OHLC_INVALID: Final = "OHLC_INVALID"
NON_FINITE: Final = "NON_FINITE"
ALL_ZERO: Final = "ALL_ZERO"

#: 参与数值检查的列（缺失的列自动跳过，例如指数分钟没有 amount）。
NUMERIC_COLUMNS: Final = ("open", "high", "low", "close", "volume", "amount")

#: 交易分钟的「当日分钟数」（hour * 60 + minute）边界。
_SESSION_START: Final = 9 * 60 + 30  # 09:30
_SESSION_AM_END: Final = 11 * 60 + 30  # 11:30（含）
_FORBIDDEN_LUNCH_BAR: Final = 13 * 60  # 13:00 单独记 FORBIDDEN_1300
_SESSION_END: Final = 15 * 60  # 15:00（含）


def check_structure(
    df: pl.DataFrame,
    *,
    table: str,
    day: date,
    expected_bars: int = 241,
    suspended_symbols: set[str] | None = None,
) -> list[Finding]:
    """检查一个分区（``df`` 为当日全部 symbol 的分钟/日线行）的结构问题。

    ``df`` 至少要有 ``symbol`` / ``datetime`` 列；数值列按
    :data:`NUMERIC_COLUMNS` 中实际存在的列检查。停牌名单内的 symbol 跳过
    ``MISSING_BARS`` 与 ``ALL_ZERO``（停牌日 bar 数天然不足、量天然为 0）。
    """
    if df.is_empty():
        return []

    absent = {"symbol", "datetime"} - set(df.columns)
    if absent:
        raise ValueError(f"check_structure 缺少必需列: {sorted(absent)}")

    suspended = suspended_symbols or set()
    num_cols = [c for c in NUMERIC_COLUMNS if c in df.columns]
    price_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]

    # dt.hour()/dt.minute() 是 Int8：先转 Int32，否则 hour * 60 会溢出回绕
    # （9 * 60 = 540 > 127），整表会被误判成越界。
    minutes = (
        pl.col("datetime").dt.hour().cast(pl.Int32) * 60
        + pl.col("datetime").dt.minute().cast(pl.Int32)
    )

    forbidden = minutes == _FORBIDDEN_LUNCH_BAR
    out_of_session = (
        (minutes < _SESSION_START)
        | (minutes > _SESSION_END)
        | ((minutes > _SESSION_AM_END) & (minutes < _FORBIDDEN_LUNCH_BAR))
    )

    # 空值也算不可用数值：NaN/Inf 一定抓，null 同样不能参与后续比对。
    nonfinite = pl.lit(False)
    for col in num_cols:
        nonfinite = nonfinite | (~pl.col(col).is_finite()).fill_null(True)

    price_nonfinite = pl.lit(False)
    for col in price_cols:
        price_nonfinite = price_nonfinite | (~pl.col(col).is_finite()).fill_null(True)

    if len(price_cols) == 4:
        price_order_ok = (
            (pl.col("high") >= pl.max_horizontal(pl.col("open"), pl.col("close")))
            & (pl.col("low") <= pl.min_horizontal(pl.col("open"), pl.col("close")))
            & (pl.col("high") >= pl.col("low"))
        )
        price_order_bad = (~price_order_ok).fill_null(False) & ~price_nonfinite
    else:
        price_order_bad = pl.lit(False)

    has_volume = "volume" in df.columns
    has_amount = "amount" in df.columns
    neg_volume = (pl.col("volume") < 0).fill_null(False) if has_volume else pl.lit(False)
    neg_amount = (pl.col("amount") < 0).fill_null(False) if has_amount else pl.lit(False)
    ohlc_bad = price_order_bad | neg_volume | neg_amount
    zero_volume = (pl.col("volume").fill_null(0) == 0) if has_volume else pl.lit(False)

    annotated = df.with_columns(
        forbidden.alias("_forbidden"),
        out_of_session.alias("_oos"),
        nonfinite.alias("_nonfinite"),
        ohlc_bad.alias("_ohlc_bad"),
        price_order_bad.alias("_price_order_bad"),
        neg_volume.alias("_neg_volume"),
        neg_amount.alias("_neg_amount"),
        zero_volume.alias("_zero_volume"),
    )
    for col in num_cols:
        annotated = annotated.with_columns(
            (~pl.col(col).is_finite()).fill_null(True).alias(f"_nf_{col}")
        )

    agg_exprs: list[pl.Expr] = [
        pl.len().alias("rows"),
        pl.col("datetime").n_unique().alias("bars"),
        pl.col("_forbidden").sum().alias("forbidden_bars"),
        pl.col("_oos").sum().alias("oos_bars"),
        pl.col("_nonfinite").sum().alias("nonfinite_rows"),
        pl.col("_ohlc_bad").sum().alias("ohlc_bad_rows"),
        pl.col("_price_order_bad").sum().alias("price_order_rows"),
        pl.col("_neg_volume").sum().alias("neg_volume_rows"),
        pl.col("_neg_amount").sum().alias("neg_amount_rows"),
        pl.col("_zero_volume").all().alias("all_zero_volume"),
    ]
    agg_exprs.extend(pl.col(f"_nf_{col}").sum().alias(f"nf_{col}") for col in num_cols)
    per_symbol = annotated.group_by("symbol", maintain_order=True).agg(agg_exprs)

    duplicates = (
        df.group_by(["symbol", "datetime"])
        .len()
        .filter(pl.col("len") > 1)
        .group_by("symbol")
        .agg(
            pl.len().alias("dup_timestamps"),
            (pl.col("len") - 1).sum().alias("dup_extra_rows"),
            pl.col("datetime").min().alias("first_dup"),
        )
    )
    per_symbol = per_symbol.join(duplicates, on="symbol", how="left")

    findings: list[Finding] = []
    for row in per_symbol.sort("symbol").iter_rows(named=True):
        symbol = row["symbol"]
        is_suspended = symbol in suspended
        bars = int(row["bars"])

        def add(
            code: str,
            severity: str,
            detail: dict[str, Any],
            symbol: str | None = symbol,
        ) -> None:
            findings.append(
                Finding(
                    table=table,
                    day=day,
                    symbol=symbol,
                    code=code,
                    severity=severity,
                    detail=detail,
                )
            )

        if row["nonfinite_rows"]:
            bad_columns = {col: int(row[f"nf_{col}"]) for col in num_cols if row[f"nf_{col}"]}
            add(NON_FINITE, SEVERITY_HARD,
                {"rows": int(row["nonfinite_rows"]), "columns": bad_columns})

        ohlc_detail: dict[str, Any] = {}
        if row["price_order_rows"]:
            ohlc_detail["price_order_rows"] = int(row["price_order_rows"])
        if row["neg_volume_rows"]:
            ohlc_detail["negative_volume_rows"] = int(row["neg_volume_rows"])
        if row["neg_amount_rows"]:
            ohlc_detail["negative_amount_rows"] = int(row["neg_amount_rows"])
        if ohlc_detail:
            ohlc_detail["rows"] = int(row["ohlc_bad_rows"])
            add(OHLC_INVALID, SEVERITY_HARD, ohlc_detail)

        if row["dup_timestamps"]:
            add(DUPLICATE_BAR, SEVERITY_HARD, {
                "timestamps": int(row["dup_timestamps"]),
                "extra_rows": int(row["dup_extra_rows"]),
                "first": row["first_dup"].isoformat() if row["first_dup"] else None,
            })

        if row["oos_bars"]:
            add(OUT_OF_SESSION, SEVERITY_HARD, {"bars": int(row["oos_bars"])})

        if row["forbidden_bars"]:
            add(FORBIDDEN_1300, SEVERITY_HARD, {"bars": int(row["forbidden_bars"])})

        if bars < expected_bars and not is_suspended:
            add(MISSING_BARS, SEVERITY_SOFT, {"expected": expected_bars, "actual": bars})

        if row["all_zero_volume"] and not is_suspended:
            add(ALL_ZERO, SEVERITY_SOFT, {"bars": bars})

    return findings
