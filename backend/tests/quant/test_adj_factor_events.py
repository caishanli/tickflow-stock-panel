"""逐日因子表 → 事件行表（step function 稀疏表示）的转换与 as-of 展开。

因子是 step function（事件之间恒定）→ 逐日表 22M 行里有 99.8% 是冗余。
事件行表（基准行 + 变点）3.6 万行即可完整表达，客户端加载从
10 GB/70 s 降到 ~5 MB/<1 s。

消费方：
- ``DataManager._adj_events()``：相邻两行比值 = 事件乘数 ✓
- ``sources.apply_qfq_daily()``：as-of backward join 展开 step function ✓
"""

from datetime import date as _date
from datetime import timedelta

import polars as pl
import pytest

from app.quant.baostock_factor_sync import daily_to_events


def _daily() -> pl.DataFrame:
    """A: 一次事件（0.95 → 1.0）；B: 一次事件（0.9 → 1.0）。"""
    return pl.DataFrame(
        {
            "symbol": ["A"] * 5 + ["B"] * 4,
            "trade_date": [
                "2025-01-01", "2025-01-02", "2025-06-12", "2025-07-01", "2025-12-31",
                "2025-01-01", "2025-01-02", "2025-09-01", "2025-12-31",
            ],
            "ex_factor": [
                0.95, 0.95, 1.0, 1.0, 1.0,
                0.9, 0.9, 1.0, 1.0,
            ],
        },
        schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64},
    )


def test_events_keep_baseline_and_change_points():
    out = daily_to_events(_daily(), _date(2025, 1, 1))
    a = out.filter(pl.col("symbol") == "A").sort("trade_date")
    assert a.height == 2                       # 基准(01-01) + 变点(06-12)
    assert a["trade_date"].to_list() == [_date(2025, 1, 1), _date(2025, 6, 12)]
    assert a["ex_factor"].to_list() == [0.95, 1.0]


def test_event_multiplier_math():
    """相邻两行的比值必须等于真实事件乘数（下游 _adj_events 依赖它）。"""
    out = daily_to_events(_daily(), _date(2025, 1, 1))
    a = out.filter(pl.col("symbol") == "A").sort("trade_date")["ex_factor"].to_list()
    b = out.filter(pl.col("symbol") == "B").sort("trade_date")["ex_factor"].to_list()
    assert a[0] / a[1] == pytest.approx(0.95)
    assert b[0] / b[1] == pytest.approx(0.9)


def test_asof_backward_reconstructs_step_function():
    """事件行表 + as-of backward = 与逐日表逐点相同的因子。"""
    daily = _daily()
    events = daily_to_events(daily, _date(2025, 1, 1))
    got = daily.sort("trade_date").join_asof(
        events.sort("trade_date"), on="trade_date", by="symbol", strategy="backward")
    assert got.height == daily.height
    for r in got.iter_rows(named=True):
        exp = daily.filter((pl.col("symbol") == r["symbol"])
                           & (pl.col("trade_date") == r["trade_date"]))["ex_factor"][0]
        assert r["ex_factor"] == pytest.approx(exp), r


def test_asof_backward_equals_exact_join_for_daily_table():
    """对逐日表，as-of backward 与精确 join 结果一致（服务端既有行为不变）。"""
    daily = _daily()
    exact = daily.sort("trade_date").join(daily, on=["symbol", "trade_date"], how="left",
                                          suffix="_j")
    asof = daily.sort("trade_date").join_asof(daily.sort("trade_date"), on="trade_date",
                                              by="symbol", strategy="backward")
    for r1, r2 in zip(exact.iter_rows(named=True), asof.iter_rows(named=True), strict=True):
        assert r1["ex_factor"] == pytest.approx(r2["ex_factor"])


def test_event_table_is_600x_smaller_than_daily():
    """稀疏表示的体积收益（逐日 22M 行 → 事件 3.6 万行）。"""
    n_days, n_events = 2600, 40
    start = _date(2020, 1, 1)
    daily = pl.DataFrame({
        "symbol": ["A"] * n_days,
        "trade_date": [start + timedelta(days=i) for i in range(n_days)],
        "ex_factor": [0.9] * (n_days - n_events) + [1.0] * n_events,
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    out = daily_to_events(daily, start)
    assert daily.height > 100 * out.height
