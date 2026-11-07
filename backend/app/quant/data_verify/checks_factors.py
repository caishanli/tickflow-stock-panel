"""L3 复权口径核验：本地 qfq（raw × ex_factor）vs 外部 qfq 的比例序列。

2026-10-22 事故的指纹：回补的日线被**写成了已前复权价**，读侧
``DataSources.apply_qfq_daily`` 再乘一次 ``ex_factor`` → 价格被重复复权。
这类错误在「本地 qfq vs 外部 qfq」的比例序列上有两个明显形态：

- **台阶**（``QFQ_STEP``）：某段区间比例恒定偏低/偏高，段与段之间跳一下——
  典型是部分分区重复复权、部分没有，或因子表某天起换了一套口径；
- **整体偏移**（``QFQ_OFFSET``）：整段比例是同一个常数（≠1）——因子方向用反
  （× 变成 ÷）或锚点不同，但序列内部一致。

口径（与生产读侧一致，勿改方向）：``qfq_close = close × ex_factor``。
见 ``app/services/stockdata/adj_factor_quality.py::apply_qfq``——服务端
``DataSources.apply_qfq_daily`` 直接复用它，是「最新锚定前复权」的唯一实现：

- 因子是**累计**量，最新日 = 1.0，越往前越小，所以**乘**（不是除）；
- 逐日表与事件行表（step function 稀疏表示）都支持——因此用
  **as-of backward join** 而不是精确 join；
- 因子表起始日之前的行回填该标的**首个**因子值（不是 1.0），否则首日会出现
  假跳变（实测 ETF 159971.SZ 2025-06-30 单日 −49.2%）。

本模块只做「比」，不做「修」：发现台阶/偏移后由调用方回查分区、重跑或隔离。
"""

from __future__ import annotations

import statistics
from typing import Any, Final

import polars as pl

from app.quant.data_verify.findings import (
    SEVERITY_HARD,
    Finding,
)

#: 错误码常量（report / policy / repair 按码分派）。
QFQ_STEP: Final = "QFQ_STEP"
QFQ_OFFSET: Final = "QFQ_OFFSET"

#: 结论所属表：qfq 口径问题最终反映在日线上。
QFQ_TABLE: Final = "kline_daily"

#: 比例序列「不再是一条水平线」的判据：``pstdev(ratio) / median(ratio)``。
#: 正常口径差异只会让比例在千分位内抖动（因子表精度、外源舍入），出现复权
#: 台阶至少是百分之几；1% 既能抓住台阶又不会被舍入噪音刷屏。
QFQ_STEP_REL_STD: Final = 0.01

#: :func:`apply_local_qfq` 的输出列顺序（原始列 + ``qfq_close``）。
_QFQ_COLUMN: Final = "qfq_close"


def _check_columns(df: pl.DataFrame, label: str, required: tuple[str, ...]) -> None:
    absent = [name for name in required if name not in df.columns]
    if absent:
        raise ValueError(f"{label} 缺少必需列: {absent}")


def _identity_qfq(raw: pl.DataFrame) -> pl.DataFrame:
    """没有可用因子时的退化结果：``qfq_close = close``（等价于 ex_factor=1.0）。"""
    return raw.with_columns(
        pl.col("close").cast(pl.Float64).alias(_QFQ_COLUMN)
    )


def apply_local_qfq(raw: pl.DataFrame, factors: pl.DataFrame) -> pl.DataFrame:
    """用本地因子表给不复权日线算 qfq，新增 ``qfq_close``（不改动原始列）。

    ``raw`` 需有 ``symbol, date, close``；``factors`` 需有
    ``symbol, date, ex_factor``。语义与生产读侧
    ``DataSources.apply_qfq_daily`` / ``adj_factor_quality.apply_qfq`` 一致：

    - ``qfq_close = close × ex_factor(date)``（**乘**，不是除）；
    - **as-of backward join**：因子表可以是逐日表，也可以是只含基准行 + 变点的
      事件行表，两次事件之间的日期沿用前一个因子；
    - 因子表起始日之前的行回填该标的**首个**因子；因子表里根本没有该标的的行
      按 1.0 处理（与 ``apply_qfq`` 的 ``fill_null(1.0)`` 一致）。

    输出保持输入行序与原始列，``qfq_close`` 追加在末尾。
    """
    _check_columns(raw, "raw", ("symbol", "date", "close"))
    if raw.is_empty():
        return raw.with_columns(pl.lit(None, dtype=pl.Float64).alias(_QFQ_COLUMN))
    _check_columns(factors, "factors", ("symbol", "date", "ex_factor"))
    if factors.is_empty():
        return _identity_qfq(raw)

    fac = (
        factors.select(
            pl.col("symbol"),
            pl.col("date").cast(pl.Date).alias("date"),
            pl.col("ex_factor").cast(pl.Float64).alias("ex_factor"),
        )
        .unique(subset=["symbol", "date"], keep="first")
        .sort(["symbol", "date"])
    )
    out = raw.with_columns(pl.col("date").cast(pl.Date).alias("date"))
    # join_asof 要求 asof 键在**每个 by 分组内**有序；两侧都按 (symbol, date) 排，
    # 因此显式关掉 sortedness 检查（带 by 时它本来也检查不了，只会刷 warning）。
    # 行号占位保证 join 后能恢复原序（同 apply_qfq）。
    out = out.with_row_index("__ord").sort(["symbol", "date"])
    out = out.join_asof(
        fac.sort(["symbol", "date"]),
        on="date",
        by="symbol",
        strategy="backward",
        check_sortedness=False,
    )
    firsts = fac.group_by("symbol").agg(pl.col("ex_factor").first().alias("__first"))
    out = out.join(firsts, on="symbol", how="left").with_columns(
        pl.coalesce([pl.col("ex_factor"), pl.col("__first")])
        .fill_null(1.0)
        .alias("__factor")
    )
    return (
        out.with_columns(
            (pl.col("close").cast(pl.Float64) * pl.col("__factor")).alias(_QFQ_COLUMN)
        )
        .sort("__ord")
        .drop("__ord", "ex_factor", "__first", "__factor")
    )


def _ratio_frame(local: pl.DataFrame, external: pl.DataFrame, symbol: str) -> pl.DataFrame:
    """按 ``date`` 内连接，产出 ``date, ratio = local / external``（只留有限正值）。

    只比同一个 ``symbol``（因子是逐标的的，跨标的没有可比性）；``symbol``
    按调用方传入的原始字符串精确匹配——不同源的名字归一（``600000.XSHG`` →
    ``600000.SH``）是调用方的事，本模块不做隐式换算。
    """
    left = (
        local.filter(pl.col("symbol").cast(pl.String) == symbol)
        .select(
            pl.col("date").cast(pl.Date).alias("date"),
            pl.col("qfq_close").cast(pl.Float64).alias("local_qfq"),
        )
        .unique(subset=["date"], keep="last")
    )
    right = (
        external.filter(pl.col("symbol").cast(pl.String) == symbol)
        .select(
            pl.col("date").cast(pl.Date).alias("date"),
            pl.col("qfq_close").cast(pl.Float64).alias("external_qfq"),
        )
        .unique(subset=["date"], keep="last")
    )
    if left.is_empty() or right.is_empty():
        return pl.DataFrame()
    joined = left.join(right, on="date", how="inner").sort("date")
    return joined.with_columns(
        (pl.col("local_qfq") / pl.col("external_qfq")).alias("ratio")
    ).filter(pl.col("ratio").is_finite() & (pl.col("ratio") > 0))


def compare_qfq(
    local: pl.DataFrame,
    external: pl.DataFrame,
    *,
    symbol: str,
    rel_tol: float = 0.005,
) -> list[Finding]:
    """比对同一标的的本地 qfq 与外部 qfq，返回口径类 :class:`Finding`。

    ``local`` / ``external`` 需有 ``symbol, date, qfq_close``（``local`` 通常来自
    :func:`apply_local_qfq`）。两侧按 ``date`` 内连接，比例 ``ratio =
    local_qfq / external_qfq``：

    - ``QFQ_STEP``（``hard``）：``pstdev(ratio) / median(ratio) > QFQ_STEP_REL_STD``
      ——比例不是水平线，说明某段区间被重复/漏复权。``day`` 取比例跳变最大的
      那一天（台阶位置），``detail`` 带前后两段的比例；
    - ``QFQ_OFFSET``（``hard``）：``abs(median(ratio) - 1) > rel_tol``——整体
      偏移（因子方向用反、锚点不同、整段重复复权）。

    两者可以同时出现（台阶 + 台阶之上的整体偏移）。只在一侧出现的日期不参与
    比对（覆盖度问题归 Task 11 汇总）；完全没有可比日期时返回空列表。
    """
    if not symbol:
        raise ValueError("symbol 不能为空")
    if rel_tol < 0:
        raise ValueError(f"rel_tol 必须为非负数，收到 {rel_tol!r}")
    _check_columns(local, "local", ("symbol", "date", "qfq_close"))
    _check_columns(external, "external", ("symbol", "date", "qfq_close"))
    if local.is_empty() or external.is_empty():
        return []

    frame = _ratio_frame(local, external, symbol)
    if frame.is_empty():
        return []

    ratios: list[float] = [float(value) for value in frame["ratio"].to_list()]
    days: list[Any] = frame["date"].to_list()
    median_ratio = float(statistics.median(ratios))
    window = {
        "n_days": len(ratios),
        "first_day": days[0].isoformat(),
        "last_day": days[-1].isoformat(),
        "median_ratio": median_ratio,
    }

    findings: list[Finding] = []
    if len(ratios) >= 2 and median_ratio > 0:
        rel_std = float(statistics.pstdev(ratios)) / median_ratio
        if rel_std > QFQ_STEP_REL_STD:
            # 台阶位置 = 相邻比例差最大的那天（并列取最早）。
            index = max(
                range(1, len(ratios)),
                key=lambda i: abs(ratios[i] - ratios[i - 1]),
            )
            findings.append(
                Finding(
                    table=QFQ_TABLE,
                    day=days[index],
                    symbol=symbol,
                    code=QFQ_STEP,
                    severity=SEVERITY_HARD,
                    detail={
                        **window,
                        "rel_std": rel_std,
                        "step_rel_std": QFQ_STEP_REL_STD,
                        "ratio_before": ratios[index - 1],
                        "ratio_after": ratios[index],
                    },
                )
            )

    deviation = abs(median_ratio - 1.0)
    if deviation > rel_tol:
        findings.append(
            Finding(
                table=QFQ_TABLE,
                day=days[0],
                symbol=symbol,
                code=QFQ_OFFSET,
                severity=SEVERITY_HARD,
                detail={
                    **window,
                    "rel_tol": rel_tol,
                    "rel_deviation": deviation,
                    "direction": "high" if median_ratio > 1.0 else "low",
                },
            )
        )
    return findings


__all__ = [
    "QFQ_OFFSET",
    "QFQ_STEP",
    "QFQ_STEP_REL_STD",
    "QFQ_TABLE",
    "apply_local_qfq",
    "compare_qfq",
]
