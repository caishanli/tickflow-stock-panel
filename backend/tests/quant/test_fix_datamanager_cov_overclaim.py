# -*- coding: utf-8 -*-
"""DataManager 分钟缓存覆盖区间虚报的修复回归测试（合成数据，离线模式）。

根因：``preload_minute_for_pool`` / ``_ensure_minute_windowed`` 在缓存帧时把
``_minute_cov`` 记为**请求窗口上界**（``hi_eff``，如 08-17 15:00），而非帧内
**实际数据上界**（``df.index.max()``）。补跑/回放撞上目标日分区未落盘时（如
08-17 15:15 重启补跑、08-17 分区 15:40 才落盘），帧只到 08-14（收盘 1.940），
但 ``_minute_cov`` 虚报覆盖到 08-17 15:00 → ``get_minute_price_at`` 命中该假覆盖、
把前一交易日收盘价当成当日价，造成 513030 以 1.940 买入（真实 08-17 13:10 为
1.985），净值虚低、后续换仓路径全部错位。

修复：缓存帧时一律记录实际覆盖区间 ``(df.index.min(), df.index.max())``；
``get_minute_price_at`` 对帧内无目标日数据的请求返回 None（触发调用方实时兜底）。
"""

import pandas as pd

from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.manager import DataManager

CODE = "513030.XSHG"


def _make_dm(tmp_path, set_window=True, **kw):
    dm = DataManager(token="", cache=DataCache(root=str(tmp_path)), **kw)
    dm._offline = True
    if set_window:
        dm.set_minute_window("2026-08-14", "2026-08-17")
    return dm


class _FakeClient:
    """网络客户端替身：get_price/get_minute_pool 从内存帧返回合成 1m 数据。"""

    def __init__(self, frames):
        self.frames = frames

    def get_price(self, security, start_date=None, end_date=None,
                  frequency="daily", fields=None):
        codes = [security] if isinstance(security, str) else list(security)
        lo = pd.Timestamp(start_date) if start_date else None
        hi = pd.Timestamp(end_date) if end_date else None
        if hi is not None and hi == hi.normalize():
            hi = hi + pd.Timedelta(hours=15)
        out = {}
        for c in codes:
            df = self.frames.get(c)
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


class _StrictMinuteClient(_FakeClient):
    """忠实模拟 stockdata 服务端：纯日期上界不自动补 15:00，精确按 ``<= hi_ts`` 过滤。

    用于复现补跑时目标日分区尚未落盘、帧只到前一交易日的场景。
    """

    def get_minute_pool(self, codes, lo_ts, hi_ts):
        lo = pd.Timestamp(lo_ts) if lo_ts is not None else None
        hi = pd.Timestamp(hi_ts) if hi_ts is not None else None
        out = {}
        for c in codes:
            df = self.frames.get(c)
            if df is None:
                continue
            if lo is not None:
                df = df[df.index >= lo]
            if hi is not None:
                df = df[df.index <= hi]
            out[c] = df
        return out


def _frame(close, days=("2026-08-13", "2026-08-14")):
    idx = pd.date_range(f"{days[0]} 09:31", f"{days[1]} 15:00", freq="1min")
    return pd.DataFrame(
        {"open": close, "high": close, "low": close,
         "close": close, "volume": 1.0, "money": 1.0},
        index=idx)


def test_preload_pool_cov_records_actual_bounds_not_requested(tmp_path):
    """补跑钉窗结束于"今天"、今日分区尚未落盘时，_minute_cov 必须记录实际帧上界
    （08-14 15:00），而非请求窗口上界（08-17 15:00）。

    回归：d092ad90 08-17 15:15 重启补跑，08-17 ETF 分钟分区 15:40 才落盘 → 预取
    帧只到 08-14（收盘 1.940），但 _minute_cov 记为覆盖到 08-17 15:00 → 13:10
    调仓 get_minute_price_at 命中假覆盖、取到 08-14 收盘 1.940（真实 08-17 13:10
    为 1.985），净值虚低、后续换仓路径全部错位。
    """
    dm = _make_dm(tmp_path)
    dm.client = _StrictMinuteClient({CODE: _frame(1.940)})
    dm.preload_minute_for_pool([CODE], as_of="2026-08-17")
    cached = dm._minute_mem.get(CODE)
    assert cached is not None and not cached.empty
    assert cached.index.max() == pd.Timestamp("2026-08-14 15:00")
    # 覆盖区间必须与实际帧上界一致（08-14 15:00），而非请求上界（08-17 15:00）
    assert dm._minute_cov[CODE][1] == pd.Timestamp("2026-08-14 15:00")
    # 08-17 无数据 → 返回 None（触发 _hist_feed 实时兜底），绝不返回 08-14 收盘 1.940
    assert dm.get_minute_price_at(CODE, "2026-08-17 13:10") is None


def test_ensure_windowed_cov_records_actual_bounds_not_requested(tmp_path):
    """滑窗加载路径（_ensure_minute_windowed）同样必须记录实际帧上界，而非请求上界。

    补跑逐日滑窗加载到"今天"时，目标日分区未落盘 → 帧只到前一日，但 _minute_cov
    记为覆盖到当日 15:00 → 取价命中假覆盖、取到前一日收盘价。
    """
    dm = _make_dm(tmp_path)
    dm.client = _StrictMinuteClient({CODE: _frame(1.940)})
    df = dm._ensure_minute_windowed(CODE, "2026-08-17")
    assert df is not None and not df.empty
    # 覆盖区间必须与实际帧上界一致，而非请求上界（08-17 15:00）
    assert dm._minute_cov[CODE][1] == pd.Timestamp("2026-08-14 15:00")
    # 08-17 无数据 → 返回 None，绝不返回 08-14 收盘 1.940
    assert dm.get_minute_price_at(CODE, "2026-08-17 13:10") is None


def test_get_minute_price_at_returns_none_when_frame_lacks_target_date(tmp_path):
    """get_minute_price_at 对帧内没有目标日数据的请求必须返回 None（数据缺口），
    而不是把前一交易日收盘价当成当日价返回。

    回归：d092ad90 08-17 收盘后重启补跑，08-17 分区尚未落盘时 13:10 调仓取 513030
    价格，旧实现返回 08-14 收盘 1.940 造成错价买入（真实 08-17 13:10 为 1.985）。
    """
    dm = _make_dm(tmp_path)
    dm.client = _StrictMinuteClient({CODE: _frame(1.940)})
    # 模拟 08-17 15:15 重启补跑：预取池时 08-17 分区尚未落盘，帧只到 08-14
    dm.preload_minute_for_pool([CODE], as_of="2026-08-17")
    # 覆盖区间已记录实际帧上界（修复后）→ dt 越界 → 重新加载，仍无 08-17 数据
    # → get_minute_price_at 返回 None（调用方 _hist_feed 会实时兜底）
    assert dm.get_minute_price_at(CODE, "2026-08-17 13:10") is None
    assert dm.get_minute_price_at(CODE, "2026-08-17 09:31") is None
    # 08-14 当日数据存在 → 正常返回当日 bar 收盘价（不回归）
    assert dm.get_minute_price_at(CODE, "2026-08-14 15:00") == 1.940


def test_preload_pool_cov_full_window_when_partition_written(tmp_path):
    """目标日分区已落盘时，_minute_cov 覆盖到窗口上界，取价返回当日真实价（不回归）。"""
    dm = _make_dm(tmp_path)
    # 帧覆盖整个补跑窗口（08-13 ~ 08-17），模拟分区已落盘的正常补跑
    df = _frame(1.940)
    idx2 = pd.date_range("2026-08-17 09:31", "2026-08-17 15:00", freq="1min")
    df = pd.concat([df, pd.DataFrame(
        {"open": 1.985, "high": 1.985, "low": 1.985,
         "close": 1.985, "volume": 1.0, "money": 1.0},
        index=idx2)])
    dm.client = _StrictMinuteClient({CODE: df})
    dm.preload_minute_for_pool([CODE], as_of="2026-08-17")
    cached = dm._minute_mem.get(CODE)
    assert cached.index.max() == pd.Timestamp("2026-08-17 15:00")
    assert dm._minute_cov[CODE][1] == pd.Timestamp("2026-08-17 15:00")
    # 当日 13:10 取价 = 当日 1.985，而非 08-14 的 1.940
    assert dm.get_minute_price_at(CODE, "2026-08-17 13:10") == 1.985