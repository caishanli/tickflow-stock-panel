"""live_feed：实时帧合并 / 价格快照 / 单源失败降级 / persist_real 空操作（C1/C2 口径）。

loader 恒走网络客户端（``dm.client.current_snapshot``），无 mootdx/分区直读路径。
"""
from __future__ import annotations

import pandas as pd

from app.quant.simulate import live_feed


def _frame(times, closes):
    return pd.DataFrame({"close": [float(c) for c in closes]},
                        index=pd.DatetimeIndex(times))


class _FakeClient:
    """fake dm.client：按 frames 提供当日快照；``current_snapshot`` 为批次接口。

    - frames[code] 为 None → 该 code 无数据（不出现在结果中）；
    - frames[code] 为 Exception → 整批抛错（模拟网络失败，refresh 整批降级旧帧）；
    - 未配置的 code → 默认单根 bar 快照（close=1.0，位于 as_of）。
    """

    def __init__(self, frames=None):
        self.frames = frames or {}

    def current_snapshot(self, codes, as_of=None):
        for c in codes:
            f = self.frames.get(c)
            if isinstance(f, Exception):
                raise f
        idx = pd.DatetimeIndex([pd.Timestamp(as_of)])
        out = {}
        for c in codes:
            if c not in self.frames:
                out[c] = pd.DataFrame({"open": [1.0], "high": [1.0], "low": [1.0],
                                       "close": [1.0], "volume": [100], "amount": [100.0]},
                                      index=idx)
            elif self.frames[c] is None:
                continue  # 显式 None = 无数据
            else:
                out[c] = self.frames[c]
        return out


class _Cache:
    def __init__(self):
        self.store = {}

    def peek(self, kind, key):
        return self.store.get(key)

    def put(self, kind, key, df):
        self.store[key] = df


class _DM:
    def __init__(self, client=None):
        self.client = client
        self._minute_mem = {}
        self._minute_cov = {}
        self.cache = _Cache()


def test_refresh_default_loader_uses_client_snapshot():
    """默认 loader 走网络客户端 current_snapshot（不再有 mootdx/分区直读路径）。"""
    dm = _DM(_FakeClient())
    now = pd.Timestamp("2026-07-17 09:31:30")
    prices, bar_dt, price_ts = live_feed.refresh(dm, ["510300.XSHG"], now)
    assert prices == {"510300.XSHG": 1.0}
    assert bar_dt == now
    assert price_ts == {"510300.XSHG": "2026-07-17 09:31:30"}
    assert dm._minute_mem["510300.XSHG"]["close"].iloc[-1] == 1.0


def test_load_recent_via_client_missing_client_returns_empty():
    dm = _DM(client=None)
    assert live_feed._load_recent_via_client(dm, ["510300.XSHG"], None) == {}


def test_refresh_custom_loader_overrides_default():
    called = []

    def _loader(dm, codes, now):
        called.append((codes, now))
        return {}

    dm = _DM(_FakeClient())
    now = pd.Timestamp("2026-07-17 10:00")
    prices, bar_dt, price_ts = live_feed.refresh(dm, ["510300.XSHG"], now, loader=_loader)
    assert prices == {} and bar_dt is None and price_ts == {}
    assert called == [(["510300.XSHG"], now)]


def test_refresh_merges_into_minute_mem_and_snapshots():
    old = _frame(["2026-07-16 14:59", "2026-07-16 15:00"], [9.9, 10.0])
    fresh = _frame(["2026-07-17 09:30", "2026-07-17 09:31"], [10.1, 10.2])
    dm = _DM(_FakeClient({"510300.XSHG": fresh}))
    dm._minute_mem["510300.XSHG"] = old
    dm._minute_cov["510300.XSHG"] = (old.index.min(), old.index.max())
    acc = {}
    now = pd.Timestamp("2026-07-17 09:31:30")
    prices, bar_dt, price_ts = live_feed.refresh(dm, ["510300.XSHG"], now, acc)
    assert prices == {"510300.XSHG": 10.2}
    assert bar_dt == pd.Timestamp("2026-07-17 09:31")
    assert price_ts == {"510300.XSHG": "2026-07-17 09:31"}
    merged = dm._minute_mem["510300.XSHG"]
    assert len(merged) == 4                     # 历史段与当日段合并
    assert dm._minute_cov["510300.XSHG"] == (merged.index.min(), merged.index.max())
    assert acc["510300.XSHG"] is fresh          # 原始帧累积供调用方使用


def test_refresh_dedupes_overlapping_bars_keep_last():
    old = _frame(["2026-07-17 09:30", "2026-07-17 09:31"], [10.0, 10.1])
    fresh = _frame(["2026-07-17 09:31", "2026-07-17 09:32"], [99.0, 10.2])
    dm = _DM(_FakeClient({"510300.XSHG": fresh}))
    dm._minute_mem["510300.XSHG"] = old
    now = pd.Timestamp("2026-07-17 09:32:10")
    prices, _, _ = live_feed.refresh(dm, ["510300.XSHG"], now)
    merged = dm._minute_mem["510300.XSHG"]
    assert len(merged) == 3
    # 重复 bar 以最新一帧为准
    assert merged.loc[pd.Timestamp("2026-07-17 09:31"), "close"] == 99.0
    assert prices["510300.XSHG"] == 10.2


def test_refresh_failure_falls_back_to_old_frame():
    old = _frame(["2026-07-17 09:30"], [10.0])
    dm = _DM(_FakeClient({"510300.XSHG": RuntimeError("网络抖动")}))
    dm._minute_mem["510300.XSHG"] = old
    now = pd.Timestamp("2026-07-17 09:31:10")
    prices, bar_dt, price_ts = live_feed.refresh(dm, ["510300.XSHG"], now)
    assert prices == {"510300.XSHG": 10.0}      # 失败沿用旧帧最后价
    assert bar_dt == pd.Timestamp("2026-07-17 09:30")
    assert price_ts == {"510300.XSHG": "2026-07-17 09:30"}   # 旧帧的 bar 时间


def test_refresh_no_data_returns_none_bar_dt():
    dm = _DM(_FakeClient({"510300.XSHG": None}))
    prices, bar_dt, price_ts = live_feed.refresh(
        dm, ["510300.XSHG"], pd.Timestamp("2026-07-17 10:00"))
    assert prices == {} and bar_dt is None and price_ts == {}


def test_persist_real_is_noop():
    """落盘归 stock data 服务：persist_real 不再写本地缓存。"""
    dm = _DM(_FakeClient())
    dm.cache.store["real_510300.XSHG"] = _frame(["2026-07-17 09:31"], [10.0])
    fresh = _frame(["2026-07-17 09:31", "2026-07-17 15:00"], [10.1, 10.5])
    live_feed.persist_real(dm, {"510300.XSHG": fresh})
    assert list(dm.cache.store["real_510300.XSHG"].index) == [pd.Timestamp("2026-07-17 09:31")]
    live_feed.persist_real(dm, {"510300.XSHG": pd.DataFrame(), "159915.XSHE": None})
    assert list(dm.cache.store) == ["real_510300.XSHG"]
