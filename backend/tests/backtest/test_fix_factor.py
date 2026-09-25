"""因子回测已确认 bug 的回归测试。

覆盖:
- S3  FactorConfig.fees_pct/slippage_bps/weight 从不参与计算 (极端参数与默认参数
      输出逐字节相同) → 每个调仓点扣双边成本; factor_weight 组内按强度加权。
- weekly 调仓口径: 原为"每周一", 周一休市的周没有调仓日, 两周收益并成一个周期
      污染 Sharpe → 改为"每周首个交易日" (对齐 monthly 的每月首个交易日)。
"""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from app.backtest.factor import FactorBacktestService, FactorConfig


def _cfg(**kw) -> FactorConfig:
    base = dict(
        factor_name="f", symbols=None,
        start=date(2024, 1, 1), end=date(2024, 1, 31),
        rebalance="monthly", weight="equal",
        fees_pct=0.0, slippage_bps=0.0,
    )
    base.update(kw)
    return FactorConfig(**base)


def _nav_panel(rows: list[tuple], strengths: list[float] | None = None) -> pl.DataFrame:
    """rows: (symbol, date, group, next_return, factor_value)。

    strengths 与 rows 等长时加 _factor_strength 列：强度是分组流水线的
    rank 口径 abs(rank-(n+1)/2)+0.5（factor.py），_calc_group_nav 只认该列
    （strength>0 取强度，否则权重按 1.0 兜底）。
    """
    df = pl.DataFrame({
        "symbol": [r[0] for r in rows],
        "date": [r[1] for r in rows],
        "_group": [r[2] for r in rows],
        "_next_return": [r[3] for r in rows],
        "f": [r[4] for r in rows],
    })
    if strengths is not None:
        df = df.with_columns(pl.Series("_factor_strength", strengths))
    return df


# ---------------------------------------------------------------- S3 费用生效

def test_fees_reduce_group_nav():
    """fees_pct>0 时每个调仓点扣双边成本 → 净值严格低于 fees=0。"""
    d0, d1 = date(2024, 1, 1), date(2024, 1, 8)
    panel = _nav_panel([
        ("A", d0, "Q1", 0.10, 1.0), ("B", d0, "Q2", -0.05, 2.0),
        ("A", d1, "Q1", 0.10, 1.0), ("B", d1, "Q2", -0.05, 2.0),
    ])

    nav_free = FactorBacktestService._calc_group_nav(panel, _cfg(fees_pct=0.0, slippage_bps=0.0))
    nav_cost = FactorBacktestService._calc_group_nav(panel, _cfg(fees_pct=0.001, slippage_bps=0.0))

    # fees=0: Q1 = 1.1^2 = 1.21; 费用 0.001×2=0.002/期: Q1 = (1.1-0.002)^2 ≈ 1.2056
    # 1.1**2 二进制浮点为 1.2100000000000002，不直接判等。
    assert abs(nav_free[-1]["Q1"] - 1.21) < 1e-9
    assert nav_cost[-1]["Q1"] < nav_free[-1]["Q1"]
    assert abs(nav_cost[-1]["Q1"] - round(1.098 ** 2, 4)) < 1e-4
    # 滑点同样计入
    nav_slip = FactorBacktestService._calc_group_nav(panel, _cfg(fees_pct=0.0, slippage_bps=10.0))
    assert nav_slip[-1]["Q1"] < nav_free[-1]["Q1"]


def test_fees_reduce_long_short_nav():
    """多空组合两腿都计费 (经分组净值传导): fees>0 时 LS 净值更低。"""
    d0, d1 = date(2024, 1, 1), date(2024, 1, 8)
    panel = _nav_panel([
        ("A", d0, "Q1", -0.05, 1.0), ("B", d0, "Q2", 0.10, 2.0),
        ("A", d1, "Q1", -0.05, 1.0), ("B", d1, "Q2", 0.10, 2.0),
    ])

    nav_free = FactorBacktestService._calc_group_nav(panel, _cfg(fees_pct=0.0))
    nav_cost = FactorBacktestService._calc_group_nav(panel, _cfg(fees_pct=0.001))
    _, stats_free = FactorBacktestService._calc_long_short(nav_free, _cfg(fees_pct=0.0))
    _, stats_cost = FactorBacktestService._calc_long_short(nav_cost, _cfg(fees_pct=0.001))

    # 无费: 每期 (0.10 + 0.05)/2 = 0.075;
    # 有费: 腿净值 (0.098 + 0.052)/2 再扣两腿成本 0.002 → 0.073
    assert stats_cost["total_return"] < stats_free["total_return"]
    assert abs(stats_free["total_return"] - (1.075 ** 2 - 1)) < 1e-3
    assert abs(stats_cost["total_return"] - (1.073 ** 2 - 1)) < 1e-3


# ---------------------------------------------------------------- S3 factor_weight

def test_factor_weight_differs_from_equal():
    """factor_weight: 组内按强度加权，结果不同于等权。

    强度 rank 口径：3 只按因子值排序，rank 1/2/3 → 强度 1.5/0.5/1.5。
    等权 = 0.30/3 = 0.10；加权 = (0.30×1.5)/3.5 ≈ 0.1286。
    """
    d0 = date(2024, 1, 1)
    panel = _nav_panel([
        ("A", d0, "Q1", 0.30, 30.0),
        ("B", d0, "Q1", 0.00, 20.0),
        ("C", d0, "Q1", 0.00, 10.0),
    ], strengths=[1.5, 0.5, 1.5])

    nav_eq = FactorBacktestService._calc_group_nav(panel, _cfg(weight="equal"))
    nav_fw = FactorBacktestService._calc_group_nav(panel, _cfg(weight="factor_weight"))

    assert abs(nav_eq[-1]["Q1"] - 1.1) < 1e-9
    assert abs(nav_fw[-1]["Q1"] - (1 + 0.45 / 3.5)) < 1e-9
    assert nav_fw[-1]["Q1"] != nav_eq[-1]["Q1"]


def test_factor_weight_nonpositive_strength_falls_back_to_one():
    """强度缺失/异常（≤0，上游 rank 算不出时）→ 该股权重按 1.0 兜底。

    两只强度 0.0/-2.0 → 权重 (1.0, 1.0) → 与等权一致。
    （旧语义"减最小值归一、最小权重为 0"已随 rank 强度口径废止。）
    """
    d0 = date(2024, 1, 1)
    panel = _nav_panel([
        ("A", d0, "Q1", 0.20, -5.0),
        ("C", d0, "Q1", 0.00, -1.0),
    ], strengths=[0.0, -2.0])

    nav_fw = FactorBacktestService._calc_group_nav(panel, _cfg(weight="factor_weight"))
    nav_eq = FactorBacktestService._calc_group_nav(panel, _cfg(weight="equal"))
    assert abs(nav_fw[-1]["Q1"] - 1.1) < 1e-9
    assert nav_fw[-1]["Q1"] == nav_eq[-1]["Q1"]


def test_factor_weight_all_equal_falls_back_to_equal():
    """组内因子值全相等 → rank 并列 → 强度全等（n=2 时 rank 1.5/1.5 → 0.5/0.5）
    → 退化为等权。"""
    d0 = date(2024, 1, 1)
    panel = _nav_panel([
        ("A", d0, "Q1", 0.10, 3.0),
        ("C", d0, "Q1", 0.30, 3.0),
    ], strengths=[0.5, 0.5])

    nav_fw = FactorBacktestService._calc_group_nav(panel, _cfg(weight="factor_weight"))
    assert abs(nav_fw[-1]["Q1"] - 1.2) < 1e-9  # (0.10+0.30)/2


# ---------------------------------------------------------------- weekly 调仓口径

def test_weekly_rebalance_uses_first_trading_day_of_week():
    """周一休市的周也要有调仓日: 调仓日 = 每周首个交易日。

    构造: W1 周一 01-01 在; W2 周一 01-08 休市 (首个交易日 01-09 周二); W3 周一 01-15 在。
    修复前调仓日 = {01-01, 01-15} → 两周收益并成一个周期;
    修复后调仓日 = {01-01, 01-09, 01-15}。
    """
    days = [date(2024, 1, 1) + timedelta(days=i) for i in
            [0, 1, 2, 8, 9, 14, 15]]  # 01-01,02,03 | 01-09,10 | 01-15,16
    panel = pl.DataFrame({
        "symbol": ["A"] * len(days),
        "date": days,
        "close": [10.0] * len(days),
    })

    out = FactorBacktestService._calc_period_return(panel, "weekly")
    marked = {
        str(r["date"])[:10]
        for r in out.filter(pl.col("_next_return").is_not_null()).iter_rows(named=True)
    }

    # 01-01 → 下周首个交易日 01-09; 01-09 → 01-15; 01-15 无下一调仓日 (null)
    assert marked == {"2024-01-01", "2024-01-09"}
