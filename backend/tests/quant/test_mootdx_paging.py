"""get_minute 按需分页（since）测试。"""
from __future__ import annotations

import datetime as _dt

import pandas as pd

from app.quant.jqengine.datasource.mootdx_src import MootdxSource, _weekday_days


def _session_ts(day: _dt.date) -> list[pd.Timestamp]:
    """某交易日 240 根 A 股分钟时间戳（09:31-11:30 + 13:01-15:00）。"""
    out = []
    t = _dt.datetime.combine(day, _dt.time(9, 31))
    for _ in range(120):
        out.append(pd.Timestamp(t))
        t += _dt.timedelta(minutes=1)
    t = _dt.datetime.combine(day, _dt.time(13, 1))
    for _ in range(120):
        out.append(pd.Timestamp(t))
        t += _dt.timedelta(minutes=1)
    return out


class _SessionClient:
    """按真实交易日会话生成 bar 的假客户端（240 根/日，升序存储）。

    bars(start=N, offset=M) 模拟 pytdx 分页：返回从最新往回第 N+1..N+M 根
    （时间升序），即 all[start..start+offset) 的倒序窗口。
    """

    def __init__(self, total_days: int):
        days = []
        d = _dt.date(2026, 8, 21)
        while len(days) < total_days:
            if d.weekday() < 5:
                days.append(d)
            d -= _dt.timedelta(days=1)
        self.all_bars: list[pd.Timestamp] = []
        for dd in sorted(days):
            self.all_bars.extend(_session_ts(dd))
        self.calls: list[int] = []

    def bars(self, symbol, frequency, start=0, offset=800):
        self.calls.append(start)
        total = len(self.all_bars)
        lo = max(0, total - start - offset)
        hi = total - start
        chunk = self.all_bars[lo:hi]
        if not chunk:
            return None
        idx = pd.DatetimeIndex(chunk)
        return pd.DataFrame({"close": [1.0] * len(idx)}, index=idx)


def test_weekday_days_counts_both_ends():
    mon = _dt.date(2026, 8, 17)
    fri = _dt.date(2026, 8, 21)
    assert _weekday_days(mon, fri) == 5


def test_get_minute_since_limits_pages():
    """since=2 个交易日前（08-19）→ 首页即越过 since，不再拉满全历史。

    覆盖性关键断言：结果必须包含 since 当日全部 bar（最老 bar 严格早于
    since 当天），且页数远小于全量（30 日 ≈ 9 页）。
    """
    src = MootdxSource()
    client = _SessionClient(total_days=30)
    src._client = client
    df = src.get_minute("600519", since=_dt.date(2026, 8, 19))
    assert not df.empty
    assert len(client.calls) <= 3
    # since 当日（08-19）的最早 bar 必须在结果里 → 最老 bar 早于该日
    assert df.index.min() < pd.Timestamp("2026-08-19 09:31:00")
    assert (df.index > pd.Timestamp("2026-08-21 15:00:00")).sum() == 0
    # 全量应为 30 日 ×240；since 模式只取了远少于全量的页
    full = MootdxSource()
    full_client = _SessionClient(total_days=30)
    full._client = full_client
    full_df = full.get_minute("600519")
    assert len(df) < len(full_df)


def test_get_minute_no_since_pulls_all():
    src = MootdxSource()
    client = _SessionClient(total_days=5)  # 1200 根 = 2 页
    src._client = client
    src.get_minute("600519")
    assert len(client.calls) == 2  # 第二页 <800 根触发停止
