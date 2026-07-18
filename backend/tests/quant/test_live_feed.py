"""live_feed：实时帧合并 / 价格快照 / 单源失败降级 / 收盘真实分钟落盘（C1/C2 口径）。"""
from __future__ import annotations

import pandas as pd
import pytest

from app.quant.simulate import live_feed


def _frame(times, closes):
    return pd.DataFrame({"close": [float(c) for c in closes]},
                        index=pd.DatetimeIndex(times))


class _Mootdx:
    def __init__(self, frames):
        self.frames = frames

    def get_minute_recent(self, code):
        f = self.frames.get(code)
        if isinstance(f, Exception):
            raise f
        return f


class _Cache:
    def __init__(self):
        self.store = {}

    def peek(self, kind, key):
        return self.store.get(key)

    def put(self, kind, key, df):
        self.store[key] = df


class _DM:
    def __init__(self, mootdx):
        self.sources = {"mootdx": mootdx}
        self._minute_mem = {}
        self._minute_cov = {}
        self.cache = _Cache()


def test_refresh_merges_into_minute_mem_and_snapshots():
    old = _frame(["2026-07-16 14:59", "2026-07-16 15:00"], [9.9, 10.0])
    fresh = _frame(["2026-07-17 09:30", "2026-07-17 09:31"], [10.1, 10.2])
    dm = _DM(_Mootdx({"510300.XSHG": fresh}))
    dm._minute_mem["510300.XSHG"] = old
    dm._minute_cov["510300.XSHG"] = (old.index.min(), old.index.max())
    acc = {}
    now = pd.Timestamp("2026-07-17 09:31:30")
    prices, bar_dt = live_feed.refresh(dm, ["510300.XSHG"], now, acc)
    assert prices == {"510300.XSHG": 10.2}
    assert bar_dt == pd.Timestamp("2026-07-17 09:31")
    merged = dm._minute_mem["510300.XSHG"]
    assert len(merged) == 4                     # 历史段与当日段合并
    assert dm._minute_cov["510300.XSHG"] == (merged.index.min(), merged.index.max())
    assert acc["510300.XSHG"] is fresh          # 原始帧累积供收盘落盘


def test_refresh_dedupes_overlapping_bars_keep_last():
    old = _frame(["2026-07-17 09:30", "2026-07-17 09:31"], [10.0, 10.1])
    fresh = _frame(["2026-07-17 09:31", "2026-07-17 09:32"], [99.0, 10.2])
    dm = _DM(_Mootdx({"510300.XSHG": fresh}))
    dm._minute_mem["510300.XSHG"] = old
    now = pd.Timestamp("2026-07-17 09:32:10")
    prices, _ = live_feed.refresh(dm, ["510300.XSHG"], now)
    merged = dm._minute_mem["510300.XSHG"]
    assert len(merged) == 3
    # 重复 bar 以最新一帧为准
    assert merged.loc[pd.Timestamp("2026-07-17 09:31"), "close"] == 99.0
    assert prices["510300.XSHG"] == 10.2


def test_refresh_failure_falls_back_to_old_frame():
    old = _frame(["2026-07-17 09:30"], [10.0])
    dm = _DM(_Mootdx({"510300.XSHG": RuntimeError("网络抖动")}))
    dm._minute_mem["510300.XSHG"] = old
    now = pd.Timestamp("2026-07-17 09:31:10")
    prices, bar_dt = live_feed.refresh(dm, ["510300.XSHG"], now)
    assert prices == {"510300.XSHG": 10.0}      # 失败沿用旧帧最后价
    assert bar_dt == pd.Timestamp("2026-07-17 09:30")


def test_refresh_no_data_returns_none_bar_dt():
    dm = _DM(_Mootdx({"510300.XSHG": None}))
    prices, bar_dt = live_feed.refresh(dm, ["510300.XSHG"], pd.Timestamp("2026-07-17 10:00"))
    assert prices == {} and bar_dt is None


def test_persist_real_merges_with_local_real_cache():
    local = _frame(["2026-07-16 14:59", "2026-07-17 09:31"], [9.9, 10.0])
    fresh = _frame(["2026-07-17 09:31", "2026-07-17 15:00"], [10.1, 10.5])
    dm = _DM(_Mootdx({}))
    dm.cache.store["real_510300.XSHG"] = local
    live_feed.persist_real(dm, {"510300.XSHG": fresh})
    out = dm.cache.store["real_510300.XSHG"]
    assert len(out) == 3                        # 去重后 3 根
    # 重叠 bar 以盘中最新获取为准（keep=last）
    assert out.loc[pd.Timestamp("2026-07-17 09:31"), "close"] == 10.1


def test_persist_real_skips_empty_frames():
    dm = _DM(_Mootdx({}))
    live_feed.persist_real(dm, {"510300.XSHG": pd.DataFrame(), "159915.XSHE": None})
    assert dm.cache.store == {}
