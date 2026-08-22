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
    # 第二页 <800 根（真·历史尽头）触发停止，但需补发一页验证非限速截断：
    # calls = [0(首页), 800(次页), 1200(短页探测→空→停)]
    assert len(client.calls) == 3


class _TruncatingClient:
    """模拟服务器限速截断的假客户端。

    bars(start=N, offset=M) 返回从最新往回第 N+1..N+M 根（时间升序），
    与 _SessionClient 同口径；但**第 truncate_call_idx+1 次请求**只回传
    窗口内最新的一半（真实 pytdx 协议最新在前传输，限速截断丢的是窗口
    最老一段），其余请求均满页——限速是瞬时的，紧随其后的重发即恢复。

    注意截断必须落在分页循环内的请求上（首页在循环外拉取，若首页被截断
    丢掉的最老段永远无人补拉）；也不能按"每个 start 首次请求都截断"建模
    ——那样补发探测落在全新 start 上同样被截断，数据几何衰减永拉不满。
    """

    def __init__(self, total: int, truncate_call_idx: int = 1):
        days = []
        d = _dt.date(2026, 8, 21)
        while len(days) * 240 < total:
            if d.weekday() < 5:
                days.append(d)
            d -= _dt.timedelta(days=1)
        self.all_bars: list[pd.Timestamp] = []
        for dd in sorted(days):
            self.all_bars.extend(_session_ts(dd))
        self.all_bars = self.all_bars[:total]
        self.truncate_call_idx = truncate_call_idx
        self.calls: list[int] = []

    def bars(self, symbol, frequency, start=0, offset=800):
        is_truncated_call = len(self.calls) == self.truncate_call_idx
        self.calls.append(start)
        total = len(self.all_bars)
        lo = max(0, total - start - offset)
        hi = total - start
        chunk = self.all_bars[lo:hi]
        if not chunk:
            return None
        if is_truncated_call:
            chunk = chunk[len(chunk) // 2:]  # 截断：丢窗口最老一半
        idx = pd.DatetimeIndex(chunk)
        return pd.DataFrame({"close": [1.0] * len(idx)}, index=idx)


def test_short_page_heals_and_continues():
    """限速截断的短页不应被当作历史尽头——补发验证后继续拉满全历史。"""
    src = MootdxSource()
    client = _TruncatingClient(total=1600)  # 正常应为 2 满页
    src._client = client
    df = src.get_minute("600519")
    # 旧逻辑遇短页即 break 只会拿到 ~400+800；自愈后必须拿满全历史
    assert len(df) >= 1600
    # 最老一根 bar 也恢复了（证明不是靠重复行凑数）
    assert df.index.min() == client.all_bars[0]
    # 真·历史尽头仍会停：最后一次请求越过末页（探测返回空后终止，
    # 而非耗尽页数预算）calls = [0, 800(截断), 1200(探测), 1600(空→停)]
    assert client.calls[-1] == 1600
