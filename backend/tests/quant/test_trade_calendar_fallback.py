"""交易日历 fallback 回归测试。

2026-09-11 事故：mootdx 全局不可用时 `_trade_days_up_to` 退回工作日近似，
把端午休市日 2026-06-19（周五）误判为交易日，导致启动 backfill 与备用链
为空跑全市场。修复后应优先用本地 kline_index_daily 的 000300 bar 推导。
"""
from datetime import date

import polars as pl

import app.services.mootdx_service as ms

D618 = date(2026, 6, 18)  # 周四，交易日
D619 = date(2026, 6, 19)  # 周五，端午休市
D910 = date(2026, 9, 10)
D911 = date(2026, 9, 11)


class _BoomSource:
    """一律爆炸的 mootdx 源：模拟出口 IP 被限 / 全局无响应。"""

    def __init__(self, *a, **k):
        pass

    def get_daily(self, *a, **k):
        raise TimeoutError("mootdx down")


def _write_index_day(root, day):
    d = root / f"date={day.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["000300.SH", "000001.SH"],
        "date": [day, day],
        "open": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0],
        "close": [1.0, 1.0], "volume": [1.0, 1.0], "amount": [1.0, 1.0],
    }).write_parquet(d / "part.parquet")


def _local_cal(monkeypatch, tmp_path):
    idx = tmp_path / "kline_index_daily"
    for d in (D618, D910, D911):
        _write_index_day(idx, d)
    monkeypatch.setattr(ms, "MootdxSource", _BoomSource)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", idx)
    return idx


def test_up_to_excludes_holiday_without_mootdx(monkeypatch, tmp_path):
    _local_cal(monkeypatch, tmp_path)
    days = ms._trade_days_up_to(D911)
    assert D910 in days and D618 in days
    assert D619 not in days  # 端午休市：本地 000300 无 bar


def test_in_range_excludes_holiday_without_mootdx(monkeypatch, tmp_path):
    _local_cal(monkeypatch, tmp_path)
    days = ms._trade_days_in_range(D618, D911)
    assert D910 in days
    assert D619 not in days


def test_empty_local_falls_back_to_weekdays(monkeypatch, tmp_path):
    """无本地历史时仍退回工作日近似（已知局限，不断链）。"""
    monkeypatch.setattr(ms, "MootdxSource", _BoomSource)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "nope")
    assert D619 in ms._trade_days_up_to(D911)
