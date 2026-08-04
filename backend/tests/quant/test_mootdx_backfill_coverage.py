"""mootdx_service 指数日线 + 因子表 + 空分区告警回源测试。"""
from __future__ import annotations

import datetime as _dt

import pandas as pd
import polars as pl
import pytest

from app.services import mootdx_service as ms


class _FakeSrc:
    """按 6 位代码返回目标日的单根指数日线帧（模拟 mootdx index_bars）。"""
    def __init__(self, day: _dt.date):
        self.day = day

    def get_daily(self, code, start, end):
        pure = code.split(".")[0]
        ts = pd.Timestamp(f"{self.day} 15:00:00")
        return pd.DataFrame(
            {"open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5],
             "volume": [1000.0], "amount": [10000.0]},
            index=pd.DatetimeIndex([ts]))


def _patch_index_universe(monkeypatch, syms):
    monkeypatch.setattr(ms, "_index_universe", lambda: syms)


def test_sync_index_daily_writes_partition(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeSrc(_dt.date(2026, 8, 5)))
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    _patch_index_universe(monkeypatch, ["000300.SH", "000510.SH", "899050.BJ"])

    res = ms.sync_index_daily(_dt.date(2026, 8, 5))

    assert res["written"] == 2  # 北交所跳过
    part = tmp_path / "kline_index_daily" / "date=2026-08-05" / "part.parquet"
    assert part.exists()
    df = pl.read_parquet(part)
    assert sorted(df["symbol"].to_list()) == ["000300.SH", "000510.SH"]


def test_index_universe_fallback_empty(monkeypatch, tmp_path):
    # 兜底路径：instruments_index 不存在 → 返回模拟盘 4 只
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path / "nova")
    out = ms._index_universe()
    assert out == ["000300.SH", "000510.SH", "399006.SZ", "399101.SZ"]
