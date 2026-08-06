"""jqengine api._filter_up_to 日线未来数据泄漏回归测试。

回归场景：补跑/回测在盘中（<15:00）取日线时，当日 bar（索引 00:00）不应被
纳入（聚宽 attribute_history 恒不含当前 bar 语义）。修复前 `idx <= current_dt`
把 08-05 00:00 当 < 08-05 09:40，造成未来数据泄漏 → 走弱期判断误用当日收盘。
"""
from __future__ import annotations

import datetime as _dt

import pandas as pd

from app.quant.jqengine.engine.jq import api


def _daily_frame(days: list[str], closes: list[float]):
    idx = pd.DatetimeIndex([pd.Timestamp(d).normalize() for d in days])
    return pd.DataFrame({"close": closes}, index=idx)


def test_filter_up_to_daily_excludes_today_intraday():
    """盘中（09:40）取日线：当日 00:00 bar 不含（生效于 15:00）。"""
    df = _daily_frame(
        ["2026-08-03", "2026-08-04", "2026-08-05"],
        [4543.18, 4600.93, 4658.15],
    )
    out = api._filter_up_to(df, _dt.datetime(2026, 8, 5, 9, 40))
    assert out["close"].tolist() == [4543.18, 4600.93]  # 不含 08-05 4658.15
    assert out.index[-1].normalize() == pd.Timestamp("2026-08-04")


def test_filter_up_to_daily_includes_today_after_close():
    """收盘后（15:05）取日线：当日 bar 已生效，纳入。"""
    df = _daily_frame(
        ["2026-08-03", "2026-08-04", "2026-08-05"],
        [4543.18, 4600.93, 4658.15],
    )
    out = api._filter_up_to(df, _dt.datetime(2026, 8, 5, 15, 5))
    assert out["close"].tolist() == [4543.18, 4600.93, 4658.15]


def test_filter_up_to_minute_unchanged():
    """分钟线索引含具体时刻，仍按原语义截到 current_dt。"""
    idx = pd.DatetimeIndex([
        _dt.datetime(2026, 8, 5, 13, 0),
        _dt.datetime(2026, 8, 5, 13, 30),
        _dt.datetime(2026, 8, 5, 14, 0),
    ])
    df = pd.DataFrame({"close": [1.0, 1.1, 1.2]}, index=idx)
    out = api._filter_up_to(df, _dt.datetime(2026, 8, 5, 13, 31))
    assert len(out) == 2  # 13:00, 13:30（<=13:31）


def test_filter_up_to_empty_safe():
    assert api._filter_up_to(pd.DataFrame(), _dt.datetime(2026, 8, 5, 9, 40)).empty
    assert api._filter_up_to(None, _dt.datetime(2026, 8, 5, 9, 40)) is None
