# -*- coding: utf-8 -*-
"""get_day_open 今日缺片经 snapshot 回补的回归测试。

根因（lb_v2opt_sim 2026-09-22~29 连续 gap=0）：盘中今日分钟分区未落盘、
minute_store 又没有该标的今日记录时，get_price-1m 只返回历史帧；
_load_real_minute 见非空直接返回、跳过 current_snapshot 兜底，
get_day_open 按今日过滤得空 → 0.0 → 开盘涨幅/生态门控全灭、静默零交易。
"""

import pandas as pd

from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.manager import DataManager

CODE = "000993.XSHE"


def _bars(dates, price, vol=1000.0):
    idx = pd.DatetimeIndex(
        [pd.Timestamp(d) + pd.Timedelta(hours=9, minutes=31) for d in dates])
    n = len(idx)
    return pd.DataFrame(
        {"open": [price] * n, "high": [price] * n, "low": [price] * n,
         "close": [price] * n, "volume": [vol] * n, "amount": [price * vol] * n},
        index=idx)


class _TodayMissingClient:
    """模拟服务端现状：1m 窗口只有历史（今日未落盘＋内存库无该标的）。"""

    def __init__(self, history):
        self.history = history
        self.snapshot_calls = 0

    def get_price(self, security, start_date=None, end_date=None,
                  frequency="daily", fields=None):
        codes = [security] if isinstance(security, str) else list(security)
        lo = pd.Timestamp(start_date) if start_date else None
        hi = pd.Timestamp(end_date) if end_date else None
        out = {}
        for c in codes:
            df = self.history.get(c)
            if df is None:
                continue
            if lo is not None:
                df = df[df.index >= lo]
            if hi is not None:
                df = df[df.index <= hi]
            out[c] = df
        return out

    def get_minute_pool(self, codes, lo_ts, hi_ts):
        return self.get_price(
            codes,
            start_date=str(lo_ts) if lo_ts is not None else None,
            end_date=str(hi_ts) if hi_ts is not None else None,
            frequency="1m")

    def current_snapshot(self, codes, as_of=None):
        self.snapshot_calls += 1
        today = pd.Timestamp.today().normalize()
        out = {}
        for c in codes:
            out[c] = _bars([today], 10.0)
        return out


def _make_dm(tmp_path, history):
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)))
    dm.client = _TodayMissingClient(history)
    return dm


def test_get_day_open_backfills_today_via_snapshot(tmp_path):
    """有历史但缺今天 → snapshot 补今天，返回今日首 bar open（而非 0.0）。"""
    today = pd.Timestamp.today().normalize()
    history_days = [(today - pd.Timedelta(days=d)).date() for d in range(1, 11)]
    dm = _make_dm(tmp_path, {CODE: _bars(history_days, 9.5)})
    assert dm.get_day_open(CODE, today + pd.Timedelta(hours=9, minutes=31)) == 10.0


def test_get_day_open_history_date_needs_no_snapshot(tmp_path):
    """历史日期（已落盘）→ 直接返回当日 open，不触 snapshot。"""
    today = pd.Timestamp.today().normalize()
    history_days = [(today - pd.Timedelta(days=d)).date() for d in range(1, 11)]
    dm = _make_dm(tmp_path, {CODE: _bars(history_days, 9.5)})
    yesterday = today - pd.Timedelta(days=1)
    assert dm.get_day_open(CODE, yesterday + pd.Timedelta(hours=9, minutes=31)) == 9.5
    assert dm.client.snapshot_calls == 0
