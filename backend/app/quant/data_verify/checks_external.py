"""L2 外部一致性：本地日线 vs 外部源日线逐字段比对。

本模块只做「比」，不做「修」，也不对来源做隐式单位换算——比对结果必须能用
明确的单位口径复现，否则 100 倍这类真实污染会被「自动归一」掩盖。

三个错误码（design §5.2）：

- ``DAILY_CLOSE_MISMATCH``（``hard``）：收盘价差 > ``max(绝对容差, 相对容差 × 本地价)``；
- ``DAILY_VOLUME_MISMATCH``（``hard``）：成交量相对差 > ``volume_rel_tol``；
- ``DAILY_AMOUNT_MISMATCH``（``soft``）：成交额差 > ``amount_rel_tol``，只报告不驱动修复。

成交额的比对**先按双方 ``close × volume`` 归一**：成交额本应是
``VWAP × 量``，价格或量本身的差异会连带 amount 差异；不归一的话，每条价格
结论都会附带一条无信息的 amount 噪音（design §5.2 的容差是按「同一口径」
定义的）。归一后仍偏离才说明成交额字段本身有问题（单位错、估算口径错）。

单位契约（design §5.1，务必显式处理）：``kline_daily`` 的 ``volume`` 存**手**，
通达信盘后包 / baostock 的 ``volume`` 是**股**。调用方必须传
``volume_factor=SOURCE_VOLUME_FACTOR["tdx"]``（0.01）把外部量折成「手」；
腾讯日线给「手」，用默认 1.0。**本模块不会按 ``source`` 自动套系数**——隐式
换算会让「谁在什么单位下比的」不可见，正是这类项目要避免的。

只比较 ``symbol`` 交集：外部包含指数 / ETF / 北交所等本地没有的行属于覆盖度
问题（Task 11 汇总），不在这里逐条报。``date`` 列可选；两侧都有时必须同一天。
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Final

import polars as pl

from app.quant.data_verify.findings import (
    SEVERITY_HARD,
    SEVERITY_SOFT,
    Finding,
)

#: 错误码常量（report / policy / repair 按码分派）。
DAILY_CLOSE_MISMATCH: Final = "DAILY_CLOSE_MISMATCH"
DAILY_VOLUME_MISMATCH: Final = "DAILY_VOLUME_MISMATCH"
DAILY_AMOUNT_MISMATCH: Final = "DAILY_AMOUNT_MISMATCH"
NON_FINITE: Final = "NON_FINITE"

#: 比对目标表（修复动作发生在这里）。本任务只覆盖股票日线。
DAILY_TABLE: Final = "kline_daily"

#: 外部源 volume → ``kline_daily``（手）的折算系数，供调用方显式传给
#: ``volume_factor``。只登记已实测/规格明确的源；通达信盘后包与 baostock 的
#: volume 是「股」（Task 6 实测 4,284,772 股 == 本地 42,847.72 手，baostock 见
#: design §5.1），腾讯日线给「手」。未登记的源由调用方自行确认，不猜。
SOURCE_VOLUME_FACTOR: Final[dict[str, float]] = {
    "tdx": 0.01,
    "baostock": 0.01,
    "tencent": 1.0,
}

#: 比对前必需的列（两侧都要有）。
_REQUIRED_COLUMNS: Final[tuple[str, ...]] = ("symbol", "close", "volume")


def _finite(value: Any) -> float | None:
    """``None`` / NaN / ±Inf 一律当「没有可比的值」（L0 负责管这些行）。"""
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _nonfinite_fields(row: dict[str, Any], prefix: str, *, with_amount: bool) -> list[str]:
    fields = ["close", "volume"] + (["amount"] if with_amount else [])
    bad = []
    for field in fields:
        value = row.get(f"{prefix}_{field}")
        if value is not None and not math.isfinite(float(value)):
            bad.append(field)
    return bad


def _single_day(df: pl.DataFrame, label: str) -> date | None:
    """帧内唯一交易日；没有 ``date`` 列返回 ``None``，跨多日直接报错。"""
    if "date" not in df.columns:
        return None
    days = [day for day in df["date"].cast(pl.Date).unique().to_list() if day is not None]
    if len(days) > 1:
        raise ValueError(
            f"{label} 跨多个交易日 {sorted(days)[:5]}，请按日切片后再比对"
        )
    return days[0] if days else None


def _check_columns(df: pl.DataFrame, label: str) -> None:
    absent = [column for column in _REQUIRED_COLUMNS if column not in df.columns]
    if absent:
        raise ValueError(f"{label} 缺少必需列: {absent}")


def _validate_params(
    *,
    source: str,
    price_abs_tol: float,
    price_rel_tol: float,
    volume_rel_tol: float,
    amount_rel_tol: float,
    volume_factor: float,
) -> None:
    if not source:
        raise ValueError("source 不能为空")
    for name, value in (
        ("price_abs_tol", price_abs_tol),
        ("price_rel_tol", price_rel_tol),
        ("volume_rel_tol", volume_rel_tol),
        ("amount_rel_tol", amount_rel_tol),
    ):
        if value < 0:
            raise ValueError(f"{name} 必须为非负数，收到 {value!r}")
    if not volume_factor > 0:
        raise ValueError(f"volume_factor 必须为正数，收到 {volume_factor!r}")


def _select(df: pl.DataFrame, prefix: str, *, with_amount: bool) -> pl.DataFrame:
    """挑出比对列并加前缀，避免与另一侧同名冲突。"""
    exprs = [
        pl.col("symbol"),
        pl.col("close").cast(pl.Float64).alias(f"{prefix}_close"),
        pl.col("volume").cast(pl.Float64).alias(f"{prefix}_volume"),
    ]
    if with_amount:
        exprs.append(pl.col("amount").cast(pl.Float64).alias(f"{prefix}_amount"))
    return df.select(exprs)


def compare_daily_external(
    local: pl.DataFrame,
    external: pl.DataFrame,
    *,
    source: str,
    price_abs_tol: float = 0.011,
    price_rel_tol: float = 0.001,
    volume_rel_tol: float = 0.005,
    amount_rel_tol: float = 0.005,
    volume_factor: float = 1.0,
) -> list[Finding]:
    """比对本地日线与某一外部源的日线，返回逐 symbol 的 :class:`Finding`。

    ``local`` / ``external`` 都要有 ``symbol, close, volume``（``amount`` /
    ``date`` 可选）。按 ``symbol`` **内连接**（只比交集），``date`` 可选但两侧
    都有时必须是同一天。``source`` 写进 ``detail["source"]``，供
    :func:`~app.quant.data_verify.policy.decide` 归票。

    ``volume_factor`` 把外部量折成本地表的口径（``kline_daily`` = 手），
    见 :data:`SOURCE_VOLUME_FACTOR`；**不按 source 自动推断**。

    每个 symbol 的结论顺序固定为 close → volume → amount，整体按 ``symbol``
    排序，便于报告与逐日 diff 稳定。
    """
    _validate_params(
        source=source,
        price_abs_tol=price_abs_tol,
        price_rel_tol=price_rel_tol,
        volume_rel_tol=volume_rel_tol,
        amount_rel_tol=amount_rel_tol,
        volume_factor=volume_factor,
    )
    _check_columns(local, "local")
    _check_columns(external, "external")
    if local.is_empty() or external.is_empty():
        return []

    local_day = _single_day(local, "local")
    external_day = _single_day(external, "external")
    if local_day is not None and external_day is not None and local_day != external_day:
        raise ValueError(
            f"local 是 {local_day}、external 是 {external_day}，不能跨日比对"
        )
    day = local_day if local_day is not None else external_day
    if day is None:
        raise ValueError("local / external 都没有 date 列，无法确定 Finding.day")

    with_amount = "amount" in local.columns and "amount" in external.columns
    joined = _select(local, "l", with_amount=with_amount).join(
        _select(external, "e", with_amount=with_amount), on="symbol", how="inner"
    )
    if joined.is_empty():
        return []

    findings: list[Finding] = []
    for row in joined.sort("symbol").iter_rows(named=True):
        symbol = str(row["symbol"])
        local_bad = _nonfinite_fields(row, "l", with_amount=with_amount)
        external_bad = _nonfinite_fields(row, "e", with_amount=with_amount)
        if local_bad or external_bad:
            findings.append(Finding(
                table=DAILY_TABLE,
                day=day,
                symbol=symbol,
                code=NON_FINITE,
                severity=SEVERITY_HARD,
                detail={
                    "source": source,
                    "side": "both" if local_bad and external_bad
                    else "local" if local_bad else "external",
                    "fields": {"local": local_bad, "external": external_bad},
                },
            ))
            continue
        local_close = _finite(row.get("l_close"))
        external_close = _finite(row.get("e_close"))
        local_volume = _finite(row.get("l_volume"))
        external_volume = _finite(row.get("e_volume"))
        if external_volume is not None:
            external_volume *= volume_factor

        if local_close is not None and external_close is not None:
            tolerance = max(price_abs_tol, price_rel_tol * abs(local_close))
            diff = abs(external_close - local_close)
            if diff > tolerance:
                findings.append(
                    Finding(
                        table=DAILY_TABLE,
                        day=day,
                        symbol=symbol,
                        code=DAILY_CLOSE_MISMATCH,
                        severity=SEVERITY_HARD,
                        detail={
                            "source": source,
                            "local_close": local_close,
                            "external_close": external_close,
                            "diff": diff,
                            "tolerance": tolerance,
                        },
                    )
                )

        if local_volume is not None and external_volume is not None:
            if local_volume == 0:
                # 本地 0 量（停牌/缺口），外部有任何量都是「本地缺交易」。
                bad = external_volume != 0
                deviation = None if bad else 0.0
            else:
                deviation = abs(external_volume - local_volume) / abs(local_volume)
                bad = deviation > volume_rel_tol
            if bad:
                findings.append(
                    Finding(
                        table=DAILY_TABLE,
                        day=day,
                        symbol=symbol,
                        code=DAILY_VOLUME_MISMATCH,
                        severity=SEVERITY_HARD,
                        detail={
                            "source": source,
                            "local_volume": local_volume,
                            "external_volume": external_volume / volume_factor,
                            "converted_external_volume": external_volume,
                            "volume_factor": volume_factor,
                            "ratio": (
                                external_volume / local_volume if local_volume else None
                            ),
                            "rel_deviation": deviation,
                        },
                    )
                )

        if not with_amount:
            continue
        local_amount = _finite(row.get("l_amount"))
        external_amount = _finite(row.get("e_amount"))
        if (
            local_amount is None
            or external_amount is None
            or local_amount <= 0
            or external_volume is None
            or local_volume is None
            or local_close is None
            or external_close is None
        ):
            continue
        # amount ≈ close × volume：先按双方价格×量归一，再比成交额字段本身。
        denominator = local_close * local_volume
        numerator = external_close * external_volume
        if denominator <= 0 or numerator <= 0:
            continue
        expected = local_amount * numerator / denominator
        deviation = abs(external_amount - expected) / expected
        if deviation > amount_rel_tol:
            findings.append(
                Finding(
                    table=DAILY_TABLE,
                    day=day,
                    symbol=symbol,
                    code=DAILY_AMOUNT_MISMATCH,
                    severity=SEVERITY_SOFT,
                    detail={
                        "source": source,
                        "local_amount": local_amount,
                        "external_amount": external_amount,
                        "expected_external_amount": expected,
                        "rel_deviation": deviation,
                    },
                )
            )

    return findings


__all__ = [
    "DAILY_AMOUNT_MISMATCH",
    "DAILY_CLOSE_MISMATCH",
    "DAILY_TABLE",
    "DAILY_VOLUME_MISMATCH",
    "NON_FINITE",
    "SOURCE_VOLUME_FACTOR",
    "compare_daily_external",
]
