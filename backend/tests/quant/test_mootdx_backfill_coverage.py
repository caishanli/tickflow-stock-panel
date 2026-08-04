"""mootdx_service 指数日线 + 因子表 + 空分区告警回源测试。"""
from __future__ import annotations

import datetime as _dt

import pandas as pd
import polars as pl

from app.services import mootdx_service as ms


class _FakeSrc:
    """按 6 位代码返回目标日的单根指数日线帧（模拟 mootdx index_bars）。"""
    def __init__(self, day: _dt.date):
        self.day = day

    def get_daily(self, code, start, end):
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


def test_adj_factor_stale(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")

    # 因子表不存在 → stale
    assert ms._adj_factor_stale() is True

    # 因子表最新（ETF 日线也有同日分区）→ not stale
    (tmp_path / "kline_etf_daily").mkdir()
    (tmp_path / "kline_etf_daily" / "date=2026-08-05").mkdir()
    (tmp_path / "adj_factor_etf").mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["510300.XSHG"],
        "trade_date": [_dt.date(2026, 8, 5)],
        "ex_factor": [1.0],
    }).write_parquet(ms.ADJ_FACTOR_PATH)
    assert ms._adj_factor_stale() is False

    # 因子表落后 → stale
    pl.DataFrame({
        "symbol": ["510300.XSHG"],
        "trade_date": [_dt.date(2026, 8, 3)],
        "ex_factor": [1.0],
    }).write_parquet(ms.ADJ_FACTOR_PATH)
    assert ms._adj_factor_stale() is True


def test_backfill_to_now_includes_index_and_adj(monkeypatch, tmp_path):
    """空分区场景：因子表空 → 触发 sync_adj_factor；结果含 missing；触发钉钉。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    # %s 空 → 无分区，回源窗口为空
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: True)  # 空文件 → stale
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: 0)
    monkeypatch.setattr(ms, "sync_daily", lambda d: {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    adj = {"written_symbols": 1, "rows": 5, "total_symbols": 2}
    monkeypatch.setattr(ms, "sync_adj_factor", lambda: adj)
    sent = []
    monkeypatch.setattr(ms, "_notify_missing", lambda m: sent.append(m))

    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(lambda: _d(2026, 8, 5))})())

    res = ms.backfill_to_now()

    assert res["adj_factor"] == adj       # 因子表空 → 跑
    assert "index_daily_days" in res
    assert "missing" in res
    assert any(v["empty"] for v in res["missing"].values())
    assert sent, "空分区应触发钉钉告警"


def test_backfill_noop_when_all_current(monkeypatch, tmp_path):
    """全部数据最新（且非空）→ 不触发任何回源、无 missing。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(lambda: _d(2026, 8, 5))})())
    # 每类都给一个最新分区，使 empty=False
    for name in ["kline_etf_minute", "kline_daily", "kline_etf_daily", "kline_index_daily"]:
        (tmp_path / name / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    (tmp_path / "adj_factor_etf").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["510300.XSHG"],
        "trade_date": [_d(2026, 8, 4)],
        "ex_factor": [1.0],
    }).write_parquet(tmp_path / "adj_factor_etf" / "all.parquet")
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")

    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: False)
    calls = {"n": 0}
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: calls.__setitem__("n", calls["n"] + 1))
    monkeypatch.setattr(ms, "sync_daily", lambda d: calls.__setitem__("n", calls["n"] + 1))
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: calls.__setitem__("n", calls["n"] + 1))
    monkeypatch.setattr(ms, "sync_adj_factor", lambda: calls.__setitem__("n", calls["n"] + 1))
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "_notify_missing", lambda m: None)

    res = ms.backfill_to_now()
    assert calls["n"] == 0
    assert not any(st["missing"] or st["empty"] for st in res["missing"].values())


def test_backfill_runs_sync_per_gap_day(monkeypatch, tmp_path):
    """日线缺 2 个交易日 → 每个缺日都调 sync_daily。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(lambda: _d(2026, 8, 5))})())
    for name in ["kline_etf_minute", "kline_etf_daily", "kline_index_daily"]:
        (tmp_path / name / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    (tmp_path / "kline_daily" / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    (tmp_path / "adj_factor_etf").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["510300.XSHG"], "trade_date": [_d(2026, 8, 4)], "ex_factor": [1.0],
    }).write_parquet(tmp_path / "adj_factor_etf" / "all.parquet")
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    # 股票+ETF 日线都缺 8/5、8/6 两个交易日
    gap = [_d(2026, 8, 5), _d(2026, 8, 6)]
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: list(gap))
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: False)
    days = []
    monkeypatch.setattr(ms, "sync_daily", lambda d: days.append(d) or {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "_notify_missing", lambda m: None)

    res = ms.backfill_to_now()
    assert days == gap
    assert res["daily_days"] == ["2026-08-05", "2026-08-06"]


def test_backfill_seeds_window_when_root_empty(monkeypatch, tmp_path):
    """股票日线根目录为空 → 用 _trade_days_up_to 窗口 seed。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(lambda: _d(2026, 8, 5))})())
    for name in ["kline_etf_minute", "kline_etf_daily", "kline_index_daily"]:
        (tmp_path / name / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    (tmp_path / "adj_factor_etf").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["510300.XSHG"], "trade_date": [_d(2026, 8, 4)], "ex_factor": [1.0],
    }).write_parquet(tmp_path / "adj_factor_etf" / "all.parquet")
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")  # 该目录不创建 = 空
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])  # 空根返回 []（既有语义）
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [_d(2026, 8, 3), _d(2026, 8, 4)])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: False)
    days = []
    monkeypatch.setattr(ms, "sync_daily", lambda d: days.append(d) or {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "_notify_missing", lambda m: None)

    res = ms.backfill_to_now()
    assert days == [_d(2026, 8, 3), _d(2026, 8, 4)]
    assert res["daily_days"] == ["2026-08-03", "2026-08-04"]
