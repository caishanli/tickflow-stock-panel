"""ETF 因子表构建（xdxr + 原始价 → 事件行）的纯函数测试（不连网）。"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from app.quant.etf_factor_build import (
    compress_steps,
    factor_events_for_symbol,
    rebuild_factors,
    steps_from_daily,
    validate_events,
)


def _d(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def _series(prices: list[float], start: str = "2024-01-02") -> pd.Series:
    d0 = dt.date.fromisoformat(start)
    days, cur = [], d0
    while len(days) < len(prices):
        if cur.weekday() < 5:
            days.append(cur)
        cur += dt.timedelta(days=1)
    return pd.Series(prices, index=pd.DatetimeIndex([pd.Timestamp(d) for d in days]))


def _row(cat: int, day: str, **kw) -> dict:
    d = _d(day)
    base = {"category": cat, "year": d.year, "month": d.month, "day": d.day,
            "suogu": 0.0, "fenhong": 0.0, "songzhuangu": 0.0, "peigu": 0.0, "peigujia": 0.0}
    base.update(kw)
    return base


# ---------------- 压缩 / 重算 ----------------


def test_compress_steps_keeps_only_changes():
    daily = [(_d("2024-01-02"), 0.9), (_d("2024-01-03"), 0.9),
             (_d("2024-01-04"), 0.95), (_d("2024-01-05"), 1.0)]
    assert compress_steps(daily) == [
        (_d("2024-01-02"), 0.9), (_d("2024-01-04"), 0.95), (_d("2024-01-05"), 1.0)]


def test_steps_from_daily_returns_ratios():
    daily = [(_d("2024-01-02"), 0.9), (_d("2024-01-03"), 0.9), (_d("2024-01-04"), 0.945)]
    steps = steps_from_daily(daily)
    assert len(steps) == 1
    assert steps[0][0] == _d("2024-01-04")
    assert steps[0][1] == pytest.approx(0.945 / 0.9)


def test_rebuild_factors_is_anchored_and_inverse():
    """因子(t) = 1/Π(事件日>t) —— 越往前越小，末值恒为 1.0。"""
    dates = [_d("2024-01-02"), _d("2024-01-03"), _d("2024-01-04")]
    steps = [(_d("2024-01-04"), 1.0 / 0.9)]        # 1/0.9 表示"因子 0.9 → 1.0"
    out = rebuild_factors(dates, steps)
    assert [round(f, 6) for _, f in out] == [0.9, 0.9, 1.0]
    assert out[-1][1] == pytest.approx(1.0)


def test_rebuild_factors_dropping_step_removes_its_effect():
    """丢弃某事件后，其余事件仍然生效，且锚点不变。"""
    dates = [_d("2024-01-02"), _d("2024-01-03"), _d("2024-01-04")]
    # 两次分红：因子 0.95 → 0.96 → 1.0
    both = rebuild_factors(dates, [(_d("2024-01-03"), 1.0 / 0.95),
                                   (_d("2024-01-04"), 1.0 / 0.96)])
    assert [round(f, 6) for _, f in both] == [pytest.approx(0.912), pytest.approx(0.96), 1.0]
    # 丢弃第二次分红 → 首次分红仍然生效：0.95 → 1.0 → 1.0（锚点恒为 1.0）
    only = rebuild_factors(dates, [(_d("2024-01-03"), 1.0 / 0.95)])
    assert [round(f, 6) for _, f in only] == [0.95, 1.0, 1.0]


# ---------------- 校验（价格是否有对应跳空） ----------------


def test_validate_keeps_dividend_with_matching_drop():
    closes = {_d("2024-01-02"): 10.0, _d("2024-01-03"): 10.0,
              _d("2024-01-04"): 9.5, _d("2024-01-05"): 9.5}
    steps = [(_d("2024-01-04"), 1.0 / 0.95)]
    kept, problems = validate_events(steps, closes)
    assert kept == steps and not problems


def test_validate_drops_step_without_price_move():
    closes = {_d("2024-01-02"): 10.0, _d("2024-01-03"): 10.0,
              _d("2024-01-04"): 10.01, _d("2024-01-05"): 10.0}
    steps = [(_d("2024-01-04"), 1.0 / 0.95)]      # 声称 -5%，价格却没动
    kept, problems = validate_events(steps, closes)
    assert kept == [] and problems and problems[0].startswith("step_without_price_move")


def test_validate_accepts_large_split():
    """1:12 拆分：因子 step +1100%，价格只跌 91.7% —— 门槛必须按 |s|/(1+|s|)。"""
    closes = {_d("2024-01-02"): 12.0, _d("2024-01-03"): 12.0,
              _d("2024-01-04"): 1.0, _d("2024-01-05"): 1.0}
    steps = [(_d("2024-01-04"), 12.0)]
    kept, problems = validate_events(steps, closes)
    assert kept == steps and not problems


def test_validate_uses_market_excess():
    """大盘同日大涨时，分红日的"价格没跌"要按超额收益判，不能误杀。"""
    closes = {_d("2024-01-02"): 10.0, _d("2024-01-03"): 10.0,
              _d("2024-01-04"): 10.2, _d("2024-01-05"): 10.2}
    market = {_d("2024-01-04"): 0.08}             # 大盘 +8%
    steps = [(_d("2024-01-04"), 1.0 / 0.97)]      # 承诺 -3%
    kept, _ = validate_events(steps, closes, market)
    assert kept == steps                          # 超额 +2% vs -8% → +2-8 = ...
    # 注：超额 = +2% - 8% = -6%，与 step 反向且超过门槛 → 保留


# ---------------- 端到端（xdxr 行 → 事件行） ----------------


def test_dividend_end_to_end():
    closes = _series([10.0, 10.0, 10.0, 9.5, 9.5])
    rows = [_row(1, "2024-01-05", fenhong=5.0)]   # 每 10 股派 5 元 → 每股 0.5
    events, problems = factor_events_for_symbol("510880.SH", closes, rows)
    assert not problems
    assert events[0] == (closes.index[0].date(), pytest.approx(0.95))
    assert events[-1] == (_d("2024-01-05"), pytest.approx(1.0))


def test_split_end_to_end():
    closes = _series([2.0, 2.0, 2.0, 1.0, 1.0])
    rows = [_row(11, "2024-01-05", suogu=2.0)]
    events, problems = factor_events_for_symbol("159876.SZ", closes, rows)
    assert not problems
    assert events[0][1] == pytest.approx(0.5)
    assert events[-1] == (_d("2024-01-05"), pytest.approx(1.0))


def test_no_events_returns_empty():
    closes = _series([1.0, 1.1, 1.2])
    events, problems = factor_events_for_symbol("510300.SH", closes, [])
    assert events == [] and problems == []


def test_ignores_pre_since_year_events():
    """2019 年的事件（早于 SINCE_YEAR=2005 之外）不参与 —— 这里用 1999 验证过滤。"""
    closes = _series([1.0, 1.0, 1.0])
    rows = [_row(1, "1999-01-05", fenhong=5.0)]
    events, _ = factor_events_for_symbol("510300.SH", closes, rows)
    assert events == []
