"""mootdx_service 指数日线 + 因子表 + 空分区告警回源测试。"""
from __future__ import annotations

import datetime as _dt
import os

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


def test_trade_days_in_range(monkeypatch):
    class _FakeSrc:
        def get_daily(self, code, start, end):
            idx = pd.DatetimeIndex([
                _dt.datetime(2026, 8, 3), _dt.datetime(2026, 8, 4),
                _dt.datetime(2026, 8, 5)])
            return pd.DataFrame({"open": [1.0] * 3}, index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeSrc())
    days = ms._trade_days_in_range(_dt.date(2026, 8, 1), _dt.date(2026, 8, 6))
    assert days == [_dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)]


def test_trade_days_in_range_filters_lower_bound(monkeypatch):
    """mootdx get_daily 忽略 start 返回全历史时，下界必须显式过滤（08-07 全量回源回归）。

    回归背景：get_daily 返回 2023 年起的全量索引，旧实现只过滤上界，
    空分区 seed / 00:00 巡检会把全历史交易日当缺失重拉（08-07 空 kline_daily
    目录触发 2023-04-20 起连续数日回源）。
    """
    class _FullHistSrc:
        def get_daily(self, code, start, end):
            idx = pd.DatetimeIndex(pd.date_range("2023-04-20", "2026-08-06", freq="B"))
            return pd.DataFrame({"open": [1.0] * len(idx)}, index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _FullHistSrc())
    days = ms._trade_days_in_range(_dt.date(2026, 8, 1), _dt.date(2026, 8, 6))
    assert days == [_dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5), _dt.date(2026, 8, 6)]


def test_trade_days_up_to_filters_lower_bound(monkeypatch):
    """_trade_days_up_to 窗口下界过滤：get_daily 返回全历史时只取最近窗口。"""
    class _FullHistSrc:
        def get_daily(self, code, start, end):
            idx = pd.DatetimeIndex(pd.date_range("2023-04-20", "2026-08-06", freq="B"))
            return pd.DataFrame({"open": [1.0] * len(idx)}, index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _FullHistSrc())
    monkeypatch.setattr(ms, "_DAILY_BACKFILL_LIMIT_DAYS", 10)
    days = ms._trade_days_up_to(_dt.date(2026, 8, 6))
    lo = _dt.date(2026, 8, 6) - _dt.timedelta(days=10)
    assert days, "窗口内应有交易日"
    assert all(lo <= d <= _dt.date(2026, 8, 6) for d in days)
    # 窗口 = 7-27(lo) ~ 8-06(end) 的全部工作日
    assert days == [
        _dt.date(2026, 7, 27), _dt.date(2026, 7, 28), _dt.date(2026, 7, 29),
        _dt.date(2026, 7, 30), _dt.date(2026, 7, 31), _dt.date(2026, 8, 3),
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5), _dt.date(2026, 8, 6),
    ]


def test_trade_days_in_range_fallback_weekday(monkeypatch):
    class _FailSrc:
        def get_daily(self, code, start, end):
            raise RuntimeError("boom")

    monkeypatch.setattr(ms, "MootdxSource", lambda: _FailSrc())
    days = ms._trade_days_in_range(_dt.date(2026, 8, 3), _dt.date(2026, 8, 5))
    # 周一(3)周二(4)周三(5)都是工作日
    assert days == [_dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)]


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


def _stub_etf_nav(monkeypatch, latest: list[str] | None = None):
    """etf_nav 全链路 stub：避免 backfill_to_now 读真实 data/etf_nav、触发
    akshare 网络回源或踩到被 monkeypatch 的 fake _date。"""
    from app.services import etf_nav_service
    monkeypatch.setattr(etf_nav_service, "_partition_dates",
                        lambda: (latest if latest is not None else []))
    monkeypatch.setattr(etf_nav_service, "_missing_etf_nav_days", lambda now=None: [])
    monkeypatch.setattr(etf_nav_service, "sync_etf_nav", lambda day=None: 0)


def test_backfill_to_now_includes_index_and_adj(monkeypatch, tmp_path):
    """空分区场景：因子表空 → 触发 sync_adj_factor；结果含 missing；触发钉钉。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    _stub_etf_nav(monkeypatch)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    # %s 空 → 无分区，回源窗口为空
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
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
    for name in ["kline_etf_minute", "kline_daily", "kline_etf_daily",
                 "kline_index_daily", "kline_minute"]:
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
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    _stub_etf_nav(monkeypatch, latest=["2026-08-04"])  # etf_nav 视为已最新，使 empty=False

    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [])
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
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    _stub_etf_nav(monkeypatch)
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
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
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    _stub_etf_nav(monkeypatch)
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
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


def test_sync_daily_writes_etf_not_filtered_by_stock_listing(monkeypatch, tmp_path):
    """sync_daily 的 ETF 部分不应被股票 instruments 表过滤。

    Bug：`_active` 用 `_listing_date_map()`（股票表，无 ETF）过滤 etfs，导致
    所有 ETF 被判 1970 占位退市 → `daily_written.etf=0`，ETF 日线从不落盘
    （模拟盘请求今日 ETF 日线 stale → 离线本地缺失）。
    """
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeSrc(_dt.date(2026, 8, 4)))
    # 股票表存在但无 ETF symbol（真实情形）
    monkeypatch.setattr(ms, "_listing_date_map",
                        lambda: {"000001.SZ": _dt.date(1991, 4, 3)})
    monkeypatch.setattr(ms, "_stock_universe", lambda: ["000001.SZ"])
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159518.XSHE", "510300.XSHG"])
    written = {}
    monkeypatch.setattr(ms, "_write_daily_partition", lambda df, root: written.setdefault(
        root.name, df["symbol"].to_list()))

    res = ms.sync_daily(_dt.date(2026, 8, 4))

    assert res["etf"] == 2, f"ETF 应写入 2 只，实际 {res}"
    assert written.get("kline_etf_daily") == ["159518.SZ", "510300.SH"]


def test_etf_universe_segment_missing_detects_missing_segments():
    """宇宙缺整个段（如 501/161）应被检出——服务器快照缺 501/161 的回归。

    旧校验只比「分区覆盖率 vs 当前宇宙」，而宇宙快照本身就缺段时覆盖率恒高，
    永远发现不了。本函数以权威段结构为基线，缺段必报。
    """
    full = [f"{seg}{100:03d}.XSHG" for seg in ms._ETF_UNIVERSE_EXPECTED_SEGMENTS]
    assert ms._etf_universe_segment_missing(full) == []

    # 缺 501 和 161 段（服务器快照实际形态）
    missing = [c for c in full if not c.startswith(("501", "161"))]
    out = ms._etf_universe_segment_missing(missing)
    assert "501" in out and "161" in out
    assert "159" not in out and "506" not in out


def test_etf_universe_segment_missing_empty():
    """空宇宙 → 全部段缺失（但调用方应更早拦截空宇宙）。"""
    out = ms._etf_universe_segment_missing([])
    assert set(out) == set(ms._ETF_UNIVERSE_EXPECTED_SEGMENTS)


def test_etf_universe_segment_missing_with_extra_segments():
    """非权威段出现不报缺失，也不误报权威段。"""
    codes = [f"{seg}{100:03d}.XSHG" for seg in ms._ETF_UNIVERSE_EXPECTED_SEGMENTS]
    codes.append("999999.XSHG")  # 非权威段
    assert ms._etf_universe_segment_missing(codes) == []


def test_incomplete_etf_daily_detects_sparse_partitions(tmp_path, monkeypatch):
    """残缺 ETF 日线分区（符号数 << 宇宙）应判为缺失，触发重写。

    回归：全市场 1600+ 只 ETF，某日只有 1 只落盘（回源中断/宇宙零星），
    分区目录存在 → 既有缺口判定视为"已覆盖"永不重写，收益曲线被残帧
    污染。此测试要求内容级校验兜住该形态。
    """
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "_etf_universe", lambda: [f"1599{dd:02d}.XSHE" for dd in range(90)])
    # 整天集合内：8/3 完整（90 只全有），8/4、8/5 残缺（各 1 只）
    root = tmp_path / "kline_etf_daily"
    for d in ["2026-08-03", "2026-08-04", "2026-08-05"]:
        (root / f"date={d}").mkdir(parents=True, exist_ok=True)
    full = pl.DataFrame({
        "symbol": [f"1599{dd:02d}.SZ" for dd in range(90)],
        "open": [1.0] * 90, "close": [1.0] * 90,
    })
    full.write_parquet(root / "date=2026-08-03" / "part.parquet")
    pl.DataFrame({"symbol": ["159900.SZ"], "open": [1.0], "close": [1.0]}).write_parquet(
        root / "date=2026-08-04" / "part.parquet")
    pl.DataFrame({"symbol": ["159901.SZ"], "open": [1.0], "close": [1.0]}).write_parquet(
        root / "date=2026-08-05" / "part.parquet")
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [_d(2026, 8, 3), _d(2026, 8, 4), _d(2026, 8, 5)])
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)

    leftovers = ms._incomplete_etf_daily_days()
    assert _d(2026, 8, 4) in leftovers, "1只残缺日应被识别"
    assert _d(2026, 8, 5) in leftovers, "1只残缺日应被识别"
    assert _d(2026, 8, 3) not in leftovers, "完整日不应被误判"


def test_incomplete_stock_minute_skips_today_intraday(tmp_path, monkeypatch):
    """盘中（<15:00）当日分区覆盖率天然偏低，不应误判为残缺触发全市场重拉。

    回归：修复"启动回源把今日盘中半程数据当残缺 → sync_stock_minute_range
    全市场重拉 ~2h 浪费"。收盘后才把当日纳入残缺校验。
    """
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(90)])
    root = tmp_path / "kline_minute"
    for d in ["2026-08-04", "2026-08-05"]:
        (root / f"date={d}").mkdir(parents=True, exist_ok=True)
    # 昨日 8/4 残缺、今日 8/5 也只有 1 只（盘中半程）
    for d in ["2026-08-04", "2026-08-05"]:
        pl.DataFrame({
            "symbol": ["600000.SH"], "datetime": [_dt.datetime(2026, 8, int(d[8:]), 9, 31)],
            "close": [1.0],
        }).write_parquet(root / f"date={d}" / "part.parquet")
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda: False)  # 盘中

    leftovers = ms._incomplete_stock_minute_days()
    assert _d(2026, 8, 4) in leftovers, "昨日残缺仍应被识别"
    assert _d(2026, 8, 5) not in leftovers, "盘中当日不应被误判"

    # 收盘后当日仍残缺 → 纳入校验
    monkeypatch.setattr(ms, "_market_closed", lambda: True)
    leftovers = ms._incomplete_stock_minute_days()
    assert _d(2026, 8, 5) in leftovers, "收盘后当日残缺应被识别"


def test_incomplete_etf_daily_ignores_when_no_universe(monkeypatch, tmp_path):
    """宇 宙为空时内容校验应跳过（避免误删/误判正常数据）。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "_etf_universe", lambda: [])
    root = tmp_path / "kline_etf_daily"
    (root / "date=2026-08-05").mkdir(parents=True)
    pl.DataFrame({"symbol": ["159900.SZ"], "open": [1.0], "close": [1.0]}).write_parquet(
        root / "date=2026-08-05" / "part.parquet")

    assert ms._incomplete_etf_daily_days() == []


def test_scan_missing_partitions_flags_sparse_etf_daily(tmp_path, monkeypatch):
    """scan_missing_partitions 应把「目录存在但只有 1 只 ETF」的分区判为缺失。"""
    import datetime as _dt
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_trade_days_in_range", lambda s, e: [
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [f"1599{dd:02d}.XSHE" for dd in range(90)])
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days",
                        lambda recent=None: [_dt.date(2026, 8, 4)])

    # ETF 分区 8/4 已存在（但残缺，被内容校验兜住）
    root = tmp_path / "kline_etf_daily"
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)

    missing = ms.scan_missing_partitions()
    assert _dt.date(2026, 8, 4) in missing["kline_etf_daily"]


def test_backfill_to_now_resyncs_sparse_etf_daily(tmp_path, monkeypatch):
    """残缺 ETF 日线应触发 sync_daily 重写，且缺日被记入 daily_days。"""
    import logging
    from datetime import date as _d
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    _stub_etf_nav(monkeypatch)
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(lambda: _d(2026, 8, 5))})())
    # 残缺日 = 8/5；其余判定给空，保证只有残缺触发 sync_daily
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days",
                        lambda recent=None: [_d(2026, 8, 5)])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: False)
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
    monkeypatch.setattr(ms, "_notify_missing", lambda m: None)
    days = []
    monkeypatch.setattr(ms, "sync_daily", lambda d: days.append(d) or {"stock": 1, "etf": 1000})

    res = ms.backfill_to_now()

    assert days == [_d(2026, 8, 5)], f"残缺日应触发 sync_daily, 实际 {days}"
    assert "2026-08-05" in res["daily_days"]
    assert res["missing"]["kline_etf_daily"]["missing"] is True


def test_sync_daily_warns_when_etf_zero(caplog, monkeypatch, tmp_path):
    """ETF 全部拉取失败时 sync_daily 应打 warning（防静默写 0）。"""
    import logging
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")

    class _EmptySrc:
        def get_daily(self, code, start, end):
            return None

    monkeypatch.setattr(ms, "MootdxSource", lambda: _EmptySrc())
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    monkeypatch.setattr(ms, "_stock_universe", lambda: ["000001.SZ"])
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159518.XSHE"])
    monkeypatch.setattr(ms, "_write_daily_partition", lambda df, root: None)

    with caplog.at_level(logging.WARNING, logger="app.services.mootdx_service"):
        res = ms.sync_daily(_dt.date(2026, 8, 4))

    assert res["etf"] == 0
    assert any("ETF" in r.message and "0" in r.message for r in caplog.records), \
        f"应告警 ETF 写 0，实际日志: {[r.message for r in caplog.records]}"


# ---------------------------------------------------------------------------
# 盘中守卫 + 收盘自愈（08-05 盘中写入坏分区的回归）
# ---------------------------------------------------------------------------

def _mk_daily_part(root, day, mtime_hour):
    pdir = root / f"date={day}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    pl.DataFrame({
        "symbol": ["000300.SH"], "open": [4550.0], "high": [4620.0],
        "low": [4550.0], "close": [4619.7], "volume": [1.0], "amount": [1.0],
    }).write_parquet(part)
    y, m, d = (int(x) for x in day.split("-"))
    os.utime(part, (_dt.datetime(y, m, d, mtime_hour, 0).timestamp(),) * 2)
    return part


def _mk_daily_part_mtime(root, day, mtime: _dt.datetime):
    """写一个日线分区并把 part.parquet 的 mtime 设为指定时刻（不受 08-05 写死限制）。"""
    pdir = root / f"date={day}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    pl.DataFrame({
        "symbol": ["000300.SH"], "open": [4550.0], "high": [4620.0],
        "low": [4550.0], "close": [4619.7], "volume": [1.0], "amount": [1.0],
    }).write_parquet(part)
    os.utime(part, (mtime.timestamp(),) * 2)
    return part


def test_market_closed_threshold(monkeypatch):
    assert ms._market_closed(_dt.datetime(2026, 8, 5, 14, 59)) is False
    assert ms._market_closed(_dt.datetime(2026, 8, 5, 15, 0)) is True
    assert ms._market_closed(_dt.datetime(2026, 8, 5, 18, 0)) is True


def test_missing_daily_guards_intraday_today(tmp_path, monkeypatch):
    """盘中（<15:00）：今日分区已存在 → 不把今天当缺失日，不回源。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_daily"
    _mk_daily_part(root, "2026-08-05", mtime_hour=10)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [_dt.date(2026, 8, 5)])

    days = ms._missing_daily_days(root, _dt.datetime(2026, 8, 5, 10, 55))
    assert days == []


def test_missing_daily_selfheals_stale_today_after_close(tmp_path, monkeypatch):
    """收盘后：今日分区为盘中快照（mtime<15:00）→ 强制重写今天。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_daily"
    _mk_daily_part(root, "2026-08-05", mtime_hour=10)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [_dt.date(2026, 8, 5)])

    days = ms._missing_daily_days(root, _dt.datetime(2026, 8, 5, 18, 0))
    assert days == [_dt.date(2026, 8, 5)]


def test_missing_daily_skips_clean_today_after_close(tmp_path, monkeypatch):
    """收盘后：今日分区为收盘后写入（mtime>=15:00）→ 不重写。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_daily"
    _mk_daily_part(root, "2026-08-05", mtime_hour=16)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [_dt.date(2026, 8, 5)])

    days = ms._missing_daily_days(root, _dt.datetime(2026, 8, 5, 18, 0))
    assert days == []


def test_missing_daily_includes_today_when_partition_absent_after_close(tmp_path, monkeypatch):
    """收盘后：今日分区缺失 → 今天算缺失日，回源补全。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_daily"
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [_dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])

    days = ms._missing_daily_days(root, _dt.datetime(2026, 8, 5, 18, 0))
    assert _dt.date(2026, 8, 5) in days


def test_missing_daily_detects_hole_before_latest(tmp_path, monkeypatch):
    """回归：latest 之前的历史中间洞也要报缺失（08-04/05 在 latest=08-06 前）。

    背景：路径 bug（dbbf940 前）导致 08-04/05 回源失败永久搁浅，随后
    backfill 补上 08-06/07，latest 跳到 08-07 后旧实现只查 latest 之后，
    中间洞永远漏检。此处用交易日历窗口内"分区目录不存在"的日期判缺失。
    """
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_daily"
    # 只有 08-06 一个分区（latest=08-06），08-04/08-05 是 latest 之前的洞
    _mk_daily_part(root, "2026-08-06", mtime_hour=16)
    monkeypatch.setattr(
        ms, "_trade_days_up_to",
        lambda end: [_dt.date(2026, 8, 4), _dt.date(2026, 8, 5), _dt.date(2026, 8, 6)])

    days = ms._missing_daily_days(root, _dt.datetime(2026, 8, 6, 18, 0))
    assert _dt.date(2026, 8, 4) in days
    assert _dt.date(2026, 8, 5) in days
    assert _dt.date(2026, 8, 6) not in days


def test_stale_today_daily_days_detects_intraday_write(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_index_daily"
    _mk_daily_part(root, "2026-08-05", mtime_hour=10)
    assert ms._stale_today_daily_days(root, _dt.datetime(2026, 8, 5, 18, 0)) \
        == [_dt.date(2026, 8, 5)]
    # 收盘后写入 → 不 stale
    _mk_daily_part(root, "2026-08-05", mtime_hour=16)
    assert ms._stale_today_daily_days(root, _dt.datetime(2026, 8, 5, 18, 0)) == []
    # 盘中判断（未收盘）不报 stale（只有收盘后触发重写）
    _mk_daily_part(root, "2026-08-05", mtime_hour=10)
    assert ms._stale_today_daily_days(root, _dt.datetime(2026, 8, 5, 10, 55)) \
        == [_dt.date(2026, 8, 5)]  # 仍返回（调用方只在收盘后使用）


def test_missing_daily_selfheals_yesterday_stale_partition(tmp_path, monkeypatch):
    """回归 08-11 半程快照：昨天盘中写入的日线分区，今天收盘后也应被强制重写。

    ``_stale_today_daily_days`` 只查"今天"，昨天（08-11 12:50）写入的半日
    快照一旦跨天就永远漏检。``_missing_daily_days`` 收盘后必须把「最近分区
    中任何早于该分区自身日期收盘时刻」写入的旧分区判为需重写。
    """
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_etf_daily"
    # 昨天 08-11 12:50（盘中）写入的分区 → 半程快照
    _mk_daily_part_mtime(root, "2026-08-11", _dt.datetime(2026, 8, 11, 12, 50))
    # 08-10 是正常收盘后写入
    _mk_daily_part_mtime(root, "2026-08-10", _dt.datetime(2026, 8, 10, 16, 0))
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 10), _dt.date(2026, 8, 11), _dt.date(2026, 8, 12)])

    # 今天（08-12）收盘后巡检 → 应把昨天 08-11 的盘中快照判为需重写
    days = ms._missing_daily_days(root, _dt.datetime(2026, 8, 12, 18, 0))
    assert _dt.date(2026, 8, 11) in days, f"应重写 08-11 半程快照, 实际 {days}"


def test_missing_daily_does_not_backfill_today_intraday_when_missing(tmp_path, monkeypatch):
    """盘中：今日分区缺失时也不该回源今天（否则写半程日线污染分区）。

    08-11 服务器 12:50 重启时 ``_missing_daily_days`` 把"今天"当成缺失日，
    触发 ``sync_daily(today)`` 拉回盘中半日数据落盘——这正是坏分区源头。
    盘中（<15:00）无论今日分区是否存在，都不把今天当缺失日。
    """
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_etf_daily"
    _mk_daily_part_mtime(root, "2026-08-10", _dt.datetime(2026, 8, 10, 16, 0))
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 10), _dt.date(2026, 8, 11)])

    # 08-11 12:50 盘中：今日（08-11）分区缺失 → 不该回源今天
    days = ms._missing_daily_days(root, _dt.datetime(2026, 8, 11, 12, 50))
    assert days == [], f"盘中不应回源今日半程数据, 实际 {days}"


def test_scan_missing_partitions_flags_stale_yesterday_etf_daily(tmp_path, monkeypatch):
    """00:00 全量巡检也应把「昨天盘中半程快照」的 ETF 日线判为缺失重写。"""
    import datetime as _dt
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_trade_days_in_range", lambda s, e: [
        _dt.date(2026, 8, 10), _dt.date(2026, 8, 11)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [f"1599{dd:02d}.XSHE" for dd in range(90)])
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])

    # 昨天 08-11 12:50 盘中写入的 ETF 日线分区（半程快照，symbol 全覆盖）
    root = tmp_path / "kline_etf_daily"
    _mk_daily_part_mtime(root, "2026-08-11", _dt.datetime(2026, 8, 11, 12, 50))

    missing = ms.scan_missing_partitions()
    assert _dt.date(2026, 8, 11) in missing["kline_etf_daily"], \
        f"00:00 巡检应重写昨日半程快照, 实际 {missing['kline_etf_daily']}"


def test_missing_minute_guards_intraday_today(tmp_path, monkeypatch):
    """盘中：ETF 分钟今日分区已存在 → 不回源今日。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_etf_minute"
    (root / "date=2026-08-05").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [_dt.date(2026, 8, 5)])

    # 盘中（10:55）：今日分区已存在 → 不回源今日
    assert ms._missing_minute_days(_dt.datetime(2026, 8, 5, 10, 55)) == []
    # 盘中但今日分区缺失 → 同样不回源（半日数据不落盘）
    (root / "date=2026-08-05").rmdir()
    assert ms._missing_minute_days(_dt.datetime(2026, 8, 5, 10, 55)) == []


def test_missing_minute_excludes_today_intraday_when_partition_absent(tmp_path, monkeypatch):
    """盘中：今日分区缺失但旧分区存在 → 仍不回源今天（08-18 盘中重启回归）。

    背景：原实现盘中保护只认 ``latest >= today``（今日分区已存在），当日分区
    缺失（15:35 收盘 cron 尚未跑，latest<today）时保护失效，把今天当缺失日
    盘中落盘半日数据（11:30 错标 13:00:00 一并污染）。盘中无论分区是否存在
    都排除今天；收盘后才把今天算缺失。
    """
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_etf_minute"
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", root)
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ms, "_trade_days_up_to",
                        lambda end: [_dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])

    # 盘中 10:55：今日分区缺失 → 仍不应把今天当缺失日
    assert ms._missing_minute_days(_dt.datetime(2026, 8, 5, 10, 55)) == []
    # 收盘后：今天算缺失，回源补全
    assert ms._missing_minute_days(_dt.datetime(2026, 8, 5, 16, 0)) == [_dt.date(2026, 8, 5)]


def test_missing_minute_still_backfills_past_day_intraday(tmp_path, monkeypatch):
    """盘中：latest 之后的真实历史缺失日仍要补（只排除今天）。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_etf_minute"
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", root)
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5), _dt.date(2026, 8, 6)])

    # 盘中 08-06：08-05 是真实缺失的历史交易日，应补；08-06 今天排除
    days = ms._missing_minute_days(_dt.datetime(2026, 8, 6, 10, 55))
    assert days == [_dt.date(2026, 8, 5)]


def test_sync_etf_minute_normalizes_phantom_noon_bar(tmp_path, monkeypatch):
    """mootdx 盘中把 11:30 错标 13:00:00 → 落盘归位为 11:30:00（不污染分区）。"""
    import datetime as _dt
    import pandas as pd
    import polars as pl
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159518.XSHE"])

    class _Src:
        def get_minute_recent(self, code, pages=1):
            # 源返回 10:30 + 被错标成 13:00:00 的 11:30（当天走 get_minute_recent 分支）
            idx = pd.DatetimeIndex([
                _dt.datetime(2026, 8, 5, 10, 30), _dt.datetime(2026, 8, 5, 13, 0)])
            idx.name = "datetime"
            return pd.DataFrame({"open": [1.0, 2.0], "close": [1.0, 2.0],
                                 "volume": [100.0, 200.0], "amount": [100.0, 200.0]},
                                index=idx)

    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _dt.date(2026, 8, 5))})())
    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    n = ms.sync_etf_minute(_dt.date(2026, 8, 5))
    part = tmp_path / "kline_etf_minute" / "date=2026-08-05" / "part.parquet"
    assert part.exists()
    df = pl.read_parquet(part)
    times = sorted(str(t) for t in df["datetime"].to_list())
    assert times == ["2026-08-05 10:30:00", "2026-08-05 11:30:00"], times
    assert n == 2


def test_clean_phantom_noon_partitions_relabels_etf(tmp_path, monkeypatch):
    """既有 ETF 分区里的 13:00:00 假bar → 归位 11:30:00（无数据丢失）。"""
    import datetime as _dt
    import polars as pl
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_etf_minute"
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", root)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    pdir = root / "date=2026-08-05"
    pdir.mkdir(parents=True)
    df = pl.DataFrame({
        "symbol": ["159518.XSHE"] * 3,
        "datetime": [_dt.datetime(2026, 8, 5, 10, 30), _dt.datetime(2026, 8, 5, 13, 0),
                     _dt.datetime(2026, 8, 5, 13, 1)],
        "open": [1.0, 2.0, 3.0], "high": [1.0, 2.0, 3.0],
        "low": [1.0, 2.0, 3.0], "close": [1.0, 2.0, 3.0],
        "volume": [100.0, 200.0, 300.0], "amount": [100.0, 200.0, 300.0],
    })
    df.write_parquet(pdir / "part.parquet")

    res = ms.clean_phantom_noon_partitions()
    assert res["partitions"] == ["kline_etf_minute/date=2026-08-05"]
    out = pl.read_parquet(pdir / "part.parquet")
    times = sorted(str(t) for t in out["datetime"].to_list())
    assert times == ["2026-08-05 10:30:00", "2026-08-05 11:30:00", "2026-08-05 13:01:00"]


def test_clean_phantom_noon_partitions_keeps_real_1130(tmp_path, monkeypatch):
    """同 symbol 已有真实 11:30 时，假bar 丢弃而非覆盖真实行。"""
    import datetime as _dt
    import polars as pl
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    pdir = root / "date=2026-08-05"
    pdir.mkdir(parents=True)
    df = pl.DataFrame({
        "symbol": ["000063.SZ"] * 3,
        "datetime": [_dt.datetime(2026, 8, 5, 11, 30), _dt.datetime(2026, 8, 5, 13, 0),
                     _dt.datetime(2026, 8, 5, 13, 1)],
        "open": [1.0, 2.0, 3.0], "high": [1.0, 2.0, 3.0],
        "low": [1.0, 2.0, 3.0], "close": [1.0, 2.0, 3.0],
        "volume": [111.0, 222.0, 300.0], "amount": [111.0, 222.0, 300.0],
    })
    df.write_parquet(pdir / "part.parquet")

    ms.clean_phantom_noon_partitions()
    out = pl.read_parquet(pdir / "part.parquet")
    assert out.height == 2
    row = out.filter(pl.col("datetime") == pl.lit(_dt.datetime(2026, 8, 5, 11, 30),
                                                  pl.Datetime("us")))
    assert row.height == 1
    assert row["volume"].to_list() == [111.0]  # 保留真实 11:30，不用假bar 覆盖


def test_clean_phantom_noon_partitions_recovers_swallowed_1130(tmp_path, monkeypatch):
    """11:30 被吞的 symbol（只有 13:00:00 幻影）→ 归位补回 11:30（000838.SZ 案例）。"""
    import datetime as _dt
    import polars as pl
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    pdir = root / "date=2026-08-05"
    pdir.mkdir(parents=True)
    df = pl.DataFrame({
        "symbol": ["000838.SZ"] * 2,
        "datetime": [_dt.datetime(2026, 8, 5, 10, 30), _dt.datetime(2026, 8, 5, 13, 0)],
        "open": [1.0, 2.0], "high": [1.0, 2.0],
        "low": [1.0, 2.0], "close": [1.0, 2.0],
        "volume": [100.0, 200.0], "amount": [100.0, 200.0],
    })
    df.write_parquet(pdir / "part.parquet")

    ms.clean_phantom_noon_partitions()
    out = pl.read_parquet(pdir / "part.parquet")
    times = sorted(str(t) for t in out["datetime"].to_list())
    assert times == ["2026-08-05 10:30:00", "2026-08-05 11:30:00"], times


def test_stock_minute_write_funnel_relabels_phantom(tmp_path, monkeypatch):
    """股票分钟写漏斗：新拉帧里的 13:00:00 假bar → 落盘归位 11:30:00。"""
    import datetime as _dt
    import polars as pl
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    chunk = [pl.DataFrame({
        "symbol": ["000063.SZ"] * 2,
        "datetime": [_dt.datetime(2026, 8, 5, 11, 30), _dt.datetime(2026, 8, 5, 13, 0)],
        "open": [1.0, 2.0], "high": [1.0, 2.0],
        "low": [1.0, 2.0], "close": [1.0, 2.0],
        "volume": [100.0, 200.0], "amount": [100.0, 200.0],
    })]
    ms._flush_stock_minute_chunk(chunk)
    out = pl.read_parquet(tmp_path / "kline_minute" / "date=2026-08-05" / "part.parquet")
    times = sorted(str(t) for t in out["datetime"].to_list())
    assert times == ["2026-08-05 11:30:00"], times  # 幻影归位后与真实 11:30 去重


def test_scan_missing_partitions_reports_universe_segments(tmp_path, monkeypatch):
    """scan_missing_partitions 应报告 ETF 宇宙缺失的代码段（快照缺段的回归）。

    旧巡检只跑 ``_incomplete_etf_daily_days``（分区覆盖率 vs 当前宇宙），
    宇宙快照缺整个 501/161 段时覆盖率恒高（缺 2/1662≈99.9%）永不告警。
    本测试要求 scan 输出 ``etf_universe_segments`` 键承载段缺失信息。
    """
    import datetime as _dt
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_trade_days_in_range", lambda s, e: [
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    # 宇宙缺 501/161 段（服务器快照实际形态）
    monkeypatch.setattr(ms, "_etf_universe", lambda: [
        "159985.XSHE", "510300.XSHG", "506000.XSHG", "169101.XSHE"])
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days", lambda recent=None: [])

    missing = ms.scan_missing_partitions()
    assert "501" in missing["etf_universe_segments"]
    assert "161" in missing["etf_universe_segments"]
    assert "159" not in missing["etf_universe_segments"]


def test_backfill_to_now_flags_missing_universe_segments(tmp_path, monkeypatch):
    """backfill_to_now 报告宇宙缺段且触发告警。"""
    from datetime import date as _d
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    _stub_etf_nav(monkeypatch)
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159985.XSHE", "510300.XSHG"])
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: False)
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: 0)
    monkeypatch.setattr(ms, "sync_daily", lambda d: {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
    sent = []
    monkeypatch.setattr(ms, "_notify_missing", lambda m: sent.append(m))

    res = ms.backfill_to_now()

    segs = res["missing"]["kline_etf_daily"].get("segment_missing", [])
    assert "501" in segs and "161" in segs, f"应报告缺段, 实际 {segs}"
    assert sent, "宇宙缺段应触发告警"


def test_scan_missing_partitions_finds_middle_gap(tmp_path, monkeypatch):
    import datetime as _dt
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_trade_days_in_range", lambda s, e: [
        _dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])

    # 只有 8/3、8/5 有分区，8/4 缺失（中间洞）
    for name in ["kline_daily", "kline_etf_daily", "kline_index_daily"]:
        for d in ["2026-08-03", "2026-08-05"]:
            (tmp_path / name / f"date={d}").mkdir(parents=True)
    # 分钟类 8/3、8/5 也有，8/4 缺失
    for name in ["kline_etf_minute", "kline_minute"]:
        for d in ["2026-08-03", "2026-08-05"]:
            (tmp_path / name / f"date={d}").mkdir(parents=True)

    missing = ms.scan_missing_partitions()
    assert missing["kline_daily"] == [_dt.date(2026, 8, 4)]
    assert missing["kline_etf_daily"] == [_dt.date(2026, 8, 4)]
    assert missing["kline_index_daily"] == [_dt.date(2026, 8, 4)]
    assert missing["kline_etf_minute"] == [_dt.date(2026, 8, 4)]
    assert missing["kline_minute"] == [_dt.date(2026, 8, 4)]


def test_sync_stock_minute_day_filters_listing_and_writes(tmp_path, monkeypatch):
    import datetime as _dt
    import pandas as pd
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [
        "000001.SZ", "600000.SH", "999999.SZ", "888888.SZ"])
    # 600000 上市晚于目标日 → 跳过不取数；000001 停牌（无该日 bar）；
    # 999999 有 6/15 数据；888888 上市日期占位（1970）= 退市 → 跳过不取数
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {
        "000001.SZ": _dt.date(2020, 1, 1),
        "600000.SH": _dt.date(2026, 8, 1),   # 上市晚于 6/15 → 跳过
        "999999.SZ": _dt.date(2020, 1, 1),
        "888888.SZ": _dt.date(1970, 1, 1),   # 退市/异常占位 → 跳过
    })

    fetched: list[str] = []
    failures: list[str] = []

    class _Src:
        def get_minute(self, sym, max_bars=40000):
            fetched.append(sym)
            if sym == "000001.SZ":  # 停牌：无该日 bar
                idx = pd.DatetimeIndex([_dt.datetime(2026, 6, 16, 9, 31)])
            else:  # 999999 有 6/15 的两根
                idx = pd.DatetimeIndex([
                    _dt.datetime(2026, 6, 15, 9, 31),
                    _dt.datetime(2026, 6, 15, 9, 32)])
            idx.name = "datetime"  # 与真实 MootdxSource.get_minute 一致
            return pd.DataFrame({"open": [1.0] * len(idx), "close": [1.0] * len(idx),
                                 "volume": [100.0] * len(idx), "amount": [100.0] * len(idx)},
                                index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    monkeypatch.setattr(ms, "_append_failure",
                        lambda sym, reason: failures.append(sym))
    n = ms.sync_stock_minute_day(_dt.date(2026, 6, 15))
    # 只有 999999 写入 2 根（600000 上市过滤跳过，000001 停牌该日无 bar，
    # 888888 退市占位跳过）
    assert n == 2
    assert "888888.SZ" not in fetched, "退市占位标的不应被取数"
    assert "888888.SZ" not in failures, "退市占位标的不应记失败"
    part = tmp_path / "kline_minute" / "date=2026-06-15" / "part.parquet"
    assert part.exists()


def test_sync_stock_minute_range_writes_all_missing_days(tmp_path, monkeypatch):
    import datetime as _dt
    import pandas as pd
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [
        "000001.SZ", "600000.SH", "999999.SZ"])
    # 600000 上市晚于缺失窗口起点（6/15）→ 跳过；000001/999999 窗口内两日都有 bar
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {
        "000001.SZ": _dt.date(2020, 1, 1),
        "600000.SH": _dt.date(2026, 8, 1),   # 上市晚于 min_day → 跳过
        "999999.SZ": _dt.date(2020, 1, 1),
    })

    class _Src:
        def get_minute(self, sym, max_bars=40000):
            idx = pd.DatetimeIndex([
                _dt.datetime(2026, 6, 15, 9, 31),
                _dt.datetime(2026, 6, 15, 9, 32),
                _dt.datetime(2026, 6, 16, 9, 31)])
            idx.name = "datetime"  # 与真实 MootdxSource.get_minute 一致
            return pd.DataFrame({"open": [1.0] * len(idx), "close": [1.0] * len(idx),
                                 "volume": [100.0] * len(idx), "amount": [100.0] * len(idx)},
                                index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    n = ms.sync_stock_minute_range([_dt.date(2026, 6, 15), _dt.date(2026, 6, 16)])
    # 2 只 × 3 根（两日）= 6 行；600000 上市过滤跳过
    assert n == 6
    for d in ["2026-06-15", "2026-06-16"]:
        part = tmp_path / "kline_minute" / f"date={d}" / "part.parquet"
        assert part.exists(), f"缺失日 {d} 应有分区"
    df15 = pl.read_parquet(tmp_path / "kline_minute" / "date=2026-06-15" / "part.parquet")
    df16 = pl.read_parquet(tmp_path / "kline_minute" / "date=2026-06-16" / "part.parquet")
    assert sorted(set(df15["symbol"].to_list())) == ["000001.SZ", "999999.SZ"]
    assert df15.height == 4  # 2 只 × 各 2 根
    assert sorted(set(df16["symbol"].to_list())) == ["000001.SZ", "999999.SZ"]
    assert df16.height == 2  # 2 只 × 各 1 根


def test_sync_etf_minute_historical_day_uses_get_minute(tmp_path, monkeypatch):
    import datetime as _dt
    import pandas as pd
    import polars as pl
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159518.XSHE"])

    class _Src:
        def get_minute(self, code, max_bars=30000):
            idx = pd.DatetimeIndex([
                _dt.datetime(2026, 6, 15, 10, 30), _dt.datetime(2026, 6, 15, 10, 31)])
            idx.name = "datetime"  # 与真实 MootdxSource.get_minute 一致（reset_index 得 datetime 列）
            return pd.DataFrame({"open": [1.0, 1.0], "close": [1.0, 1.0],
                                 "volume": [100.0, 100.0], "amount": [100.0, 100.0]},
                                index=idx)

    # 强制走历史日分支（day 距今天 > 5 天）
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _dt.date(2026, 8, 6))})())
    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    n = ms.sync_etf_minute(_dt.date(2026, 6, 15))
    assert n == 2
    part = tmp_path / "kline_etf_minute" / "date=2026-06-15" / "part.parquet"
    assert part.exists()
    df = pl.read_parquet(part)
    assert len(df) == 2


def test_backfill_missing_partitions_routes_to_sync(monkeypatch):
    import datetime as _dt
    from app.services import mootdx_service as ms

    calls = {"daily": [], "index": [], "etf_min": [], "stock_min": []}
    monkeypatch.setattr(ms, "sync_daily", lambda d: calls["daily"].append(d) or {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: calls["index"].append(d) or {"written": 1})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d: calls["etf_min"].append(d) or 5)
    monkeypatch.setattr(ms, "sync_stock_minute_range",
                        lambda days: calls["stock_min"].append(list(days)) or 7)

    missing = {
        "kline_daily":       [_dt.date(2026, 6, 15)],
        "kline_etf_daily":   [_dt.date(2026, 6, 16)],
        "kline_index_daily": [_dt.date(2026, 6, 17)],
        "kline_etf_minute":  [_dt.date(2026, 6, 18)],
        "kline_minute":      [_dt.date(2026, 6, 19)],
    }
    res = ms.backfill_missing_partitions(missing)
    assert calls["daily"] == [_dt.date(2026, 6, 15), _dt.date(2026, 6, 16)]
    assert calls["index"] == [_dt.date(2026, 6, 17)]
    assert calls["etf_min"] == [_dt.date(2026, 6, 18)]
    # 批量一次调用拿全缺失日列表
    assert calls["stock_min"] == [[_dt.date(2026, 6, 19)]]
    assert res["errors"] == []


def test_backfill_missing_partitions_survives_per_day_error(monkeypatch):
    import datetime as _dt
    from app.services import mootdx_service as ms

    def _boom(d):
        raise RuntimeError("sync failed")

    monkeypatch.setattr(ms, "sync_daily", _boom)
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: {"written": 1})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d: 0)
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)

    missing = {
        "kline_daily": [_dt.date(2026, 6, 15), _dt.date(2026, 6, 16)],
        "kline_index_daily": [_dt.date(2026, 6, 17)],
    }
    res = ms.backfill_missing_partitions(missing)
    assert len(res["errors"]) == 2  # 两日都失败，但 index 仍补了
    assert "2026-06-17" in res["index_daily_days"]


# ---------------------------------------------------------------------------
# 后台回源节流（客户端请求优先）
# ---------------------------------------------------------------------------

def test_throttle_backfill_sleeps_every_n(monkeypatch):
    """非盘中：每 _BACKFILL_THROTTLE_EVERY 个 symbol 后 sleep _BACKFILL_THROTTLE_SLEEP。"""
    sleeps = []
    monkeypatch.setattr(ms, "_is_market_open", lambda: False)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_EVERY", 3)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_SLEEP", 0.05)
    monkeypatch.setattr(ms.time, "sleep", lambda s: sleeps.append(s))
    for i in range(6):
        ms._throttle_backfill(i)
    assert sleeps == [0.05, 0.05]  # i=2(第3个) 和 i=5(第6个) 触发


def test_throttle_backfill_disabled_when_every_zero(monkeypatch):
    """非盘中：_BACKFILL_THROTTLE_EVERY<=0 时完全不禁流。"""
    sleeps = []
    monkeypatch.setattr(ms, "_is_market_open", lambda: False)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_EVERY", 0)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_SLEEP", 0.05)
    monkeypatch.setattr(ms.time, "sleep", lambda s: sleeps.append(s))
    for i in range(10):
        ms._throttle_backfill(i)
    assert sleeps == []


def test_throttle_backfill_intraday_slows_down(monkeypatch):
    """盘中：使用独立降速参数，每 _BACKFILL_INTRADAY_EVERY 个 symbol 睡 1s。"""
    sleeps = []
    monkeypatch.setattr(ms, "_is_market_open", lambda: True)
    monkeypatch.setattr(ms, "_BACKFILL_INTRADAY_EVERY", 2)
    monkeypatch.setattr(ms, "_BACKFILL_INTRADAY_SLEEP", 0.05)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_EVERY", 5)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_SLEEP", 0.2)
    monkeypatch.setattr(ms.time, "sleep", lambda s: sleeps.append(s))
    for i in range(4):
        ms._throttle_backfill(i)
    # 盘中每 2 个 symbol 触发（i=1, i=3），且不受非盘中参数影响
    assert sleeps == [0.05, 0.05]


def test_throttle_backfill_intraday_disabled_when_every_zero(monkeypatch):
    """盘中：_BACKFILL_INTRADAY_EVERY<=0 时盘中不禁流。"""
    sleeps = []
    monkeypatch.setattr(ms, "_is_market_open", lambda: True)
    monkeypatch.setattr(ms, "_BACKFILL_INTRADAY_EVERY", 0)
    monkeypatch.setattr(ms, "_BACKFILL_INTRADAY_SLEEP", 0.05)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_EVERY", 5)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_SLEEP", 0.2)
    monkeypatch.setattr(ms.time, "sleep", lambda s: sleeps.append(s))
    for i in range(4):
        ms._throttle_backfill(i)
    assert sleeps == []


def test_throttle_backfill_called_in_sync_daily_loop(tmp_path, monkeypatch):
    """sync_daily 循环逐 symbol 调用节流（证明接入点存在且生效）。"""
    calls = {"throttle": []}
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_EVERY", 2)
    monkeypatch.setattr(ms, "_BACKFILL_THROTTLE_SLEEP", 0.0)
    monkeypatch.setattr(ms.time, "sleep", lambda s: None)
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "_stock_universe", lambda: ["000001.SZ", "600000.SH", "000002.SZ"])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [])
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    monkeypatch.setattr(ms, "_write_daily_partition", lambda df, root: None)

    class _Src:
        def get_daily(self, code, start, end):
            import pandas as pd
            ts = pd.Timestamp("2026-08-05 15:00:00")
            return pd.DataFrame(
                {"open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
                 "volume": [1000.0], "amount": [10000.0]},
                index=pd.DatetimeIndex([ts]))

    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    # 统计 throttle 调用次数：monkeypatch 包装
    orig = ms._throttle_backfill
    def _spy(i):
        calls["throttle"].append(i)
        orig(i)
    monkeypatch.setattr(ms, "_throttle_backfill", _spy)

    ms.sync_daily(_dt.date(2026, 8, 5))
    assert len(calls["throttle"]) >= 3  # 每只股票都调用


# ---------------------------------------------------------------------------
# _put_daily_mem_protected：窄窗口请求不污染日线全量缓存
# ---------------------------------------------------------------------------

def test_put_daily_mem_protected_keeps_wider_cache():
    """已有缓存覆盖更广（起点更早）时，窄窗口新帧不覆盖。"""
    import pandas as _pd
    from app.quant.jqengine.datasource.manager import DataManager

    dm = DataManager.__new__(DataManager)
    dm._daily_mem = {}
    dm._daily_ver = 0

    full = _pd.DataFrame({"close": [1.0, 1.0]},
                         index=_pd.DatetimeIndex(["2025-07-31", "2026-08-06"]))
    narrow = _pd.DataFrame({"close": [1.0]},
                           index=_pd.DatetimeIndex(["2026-07-10"]))

    # 先写全量
    dm._put_daily_mem_protected("get_daily_000300.XSHG", full)
    assert len(dm._daily_mem["get_daily_000300.XSHG"]) == 2
    v0 = dm._daily_ver

    # 窄窗口请求 → 已有更全 → 不覆盖
    dm._put_daily_mem_protected("get_daily_000300.XSHG", narrow)
    assert len(dm._daily_mem["get_daily_000300.XSHG"]) == 2
    assert dm._daily_ver == v0  # 未写，版本号不变


def test_put_daily_mem_protected_writes_new_or_wider():
    """无缓存 / 新帧更全时正常写入。"""
    import pandas as _pd
    from app.quant.jqengine.datasource.manager import DataManager

    dm = DataManager.__new__(DataManager)
    dm._daily_mem = {}
    dm._daily_ver = 0

    full = _pd.DataFrame({"close": [1.0, 1.0]},
                         index=_pd.DatetimeIndex(["2025-07-31", "2026-08-06"]))

    # 无缓存 → 写入
    dm._put_daily_mem_protected("get_daily_000300.XSHG", full)
    assert len(dm._daily_mem["get_daily_000300.XSHG"]) == 2
    assert dm._daily_ver == 1

    # 已有窄缓存，新帧更全 → 覆盖
    narrow = _pd.DataFrame({"close": [1.0]}, index=_pd.DatetimeIndex(["2026-07-10"]))
    dm2 = DataManager.__new__(DataManager)
    dm2._daily_mem = {"get_daily_000300.XSHG": narrow}
    dm2._daily_ver = 5
    dm2._put_daily_mem_protected("get_daily_000300.XSHG", full)
    assert len(dm2._daily_mem["get_daily_000300.XSHG"]) == 2
    assert dm2._daily_ver == 6


# ---------------------------------------------------------------------------
# 股票分钟 kline_minute 内容级 symbol 覆盖率校验（pytdx 缺失事故的回归）
# ---------------------------------------------------------------------------

def test_incomplete_stock_minute_detects_sparse_partitions(tmp_path, monkeypatch):
    """残缺股票分钟分区（symbol 数 << 股票宇宙）应判为缺失，触发重写。

    回归：08-11 服务器重建 venv 丢弃 pytdx，回源全失败 5226 只 → 当日分区
    只写入极少数（如 1 只）甚至为空。分区目录存在 → 既有缺口判定视为
    "已覆盖"永不重写，最新交易日分钟数据永久缺失（回测/模拟盘 get_minute
    拿到昨天的数据）。此测试要求内容级校验兜住该形态。
    """
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(90)])
    # 整天集合内：8/3 完整（90 只全有），8/4、8/5 残缺（各 1 只）
    root = tmp_path / "kline_minute"
    for d in ["2026-08-03", "2026-08-04", "2026-08-05"]:
        (root / f"date={d}").mkdir(parents=True, exist_ok=True)
    full = pl.DataFrame({
        "symbol": [f"6000{dd:03d}.SH" for dd in range(90)],
        "datetime": [_dt.datetime(2026, 8, 3, 9, 31)] * 90,
        "close": [1.0] * 90,
    })
    full.write_parquet(root / "date=2026-08-03" / "part.parquet")
    pl.DataFrame({
        "symbol": ["6000000.SH"], "datetime": [_dt.datetime(2026, 8, 4, 9, 31)],
        "close": [1.0],
    }).write_parquet(root / "date=2026-08-04" / "part.parquet")
    # 覆盖率为 1/90，与 8/4 相同；用另一只保持独立断言
    pl.DataFrame({
        "symbol": ["6000001.SH"], "datetime": [_dt.datetime(2026, 8, 5, 9, 31)],
        "close": [1.0],
    }).write_parquet(root / "date=2026-08-05" / "part.parquet")
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda: True)

    leftovers = ms._incomplete_stock_minute_days()
    assert _d(2026, 8, 4) in leftovers, "1只残缺日应被识别"
    assert _d(2026, 8, 5) in leftovers, "1只残缺日应被识别"
    assert _d(2026, 8, 3) not in leftovers, "完整日不应被误判"


def test_incomplete_stock_minute_ignores_when_no_universe(monkeypatch, tmp_path):
    """股票宇宙为空时内容校验应跳过（避免误判正常数据）。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [])
    root = tmp_path / "kline_minute"
    (root / "date=2026-08-05").mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH"], "datetime": [_dt.datetime(2026, 8, 5, 9, 31)],
        "close": [1.0],
    }).write_parquet(root / "date=2026-08-05" / "part.parquet")

    assert ms._incomplete_stock_minute_days() == []


def test_incomplete_stock_minute_respects_recent_limit(tmp_path, monkeypatch):
    """内容校验只查最近 recent 个分区；更早的残缺不算（性能设计）。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(90)])
    root = tmp_path / "kline_minute"
    for d in ["2026-08-01", "2026-08-02", "2026-08-03", "2026-08-04", "2026-08-05"]:
        (root / f"date={d}").mkdir(parents=True, exist_ok=True)
    # 8/1、8/2 残缺，8/3/4/5 完整
    for d in ["2026-08-01", "2026-08-02"]:
        pl.DataFrame({
            "symbol": ["600000.SH"], "datetime": [_dt.datetime(2026, 8, int(d[8:]), 9, 31)],
            "close": [1.0],
        }).write_parquet(root / f"date={d}" / "part.parquet")
    for d in ["2026-08-03", "2026-08-04", "2026-08-05"]:
        full = pl.DataFrame({
            "symbol": [f"6000{dd:03d}.SH" for dd in range(90)],
            "datetime": [_dt.datetime(2026, 8, int(d[8:]), 9, 31)] * 90,
            "close": [1.0] * 90,
        })
        full.write_parquet(root / f"date={d}" / "part.parquet")

    leftovers = ms._incomplete_stock_minute_days(recent=3)
    assert _dt.date(2026, 8, 1) not in leftovers
    assert _dt.date(2026, 8, 2) not in leftovers


def test_scan_missing_partitions_flags_sparse_stock_minute(tmp_path, monkeypatch):
    """scan_missing_partitions 应把「目录存在但只写入 1 只」的股票分钟判为缺失。"""
    import datetime as _dt

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_trade_days_in_range", lambda s, e: [
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(90)])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days",
                        lambda recent=None: [_dt.date(2026, 8, 4)])

    # kline_minute 8/4 已存在（但残缺，被内容校验兜住）
    root = tmp_path / "kline_minute"
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)

    missing = ms.scan_missing_partitions()
    assert _dt.date(2026, 8, 4) in missing["kline_minute"]


def test_backfill_to_now_resyncs_sparse_stock_minute(tmp_path, monkeypatch):
    """残缺股票分钟应触发 sync_stock_minute_range 重写，且在增量慢跑前执行。"""
    from datetime import date as _d

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    _stub_etf_nav(monkeypatch)
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(lambda: _d(2026, 8, 5))})())
    # 残缺日 = 8/5；其余判定给空，保证只有残缺触发 range 重写
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [_d(2026, 8, 5)])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: False)
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: 0)
    monkeypatch.setattr(ms, "sync_daily", lambda d: {"stock": 1, "etf": 1000})
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "_notify_missing", lambda m: None)
    calls = []
    monkeypatch.setattr(ms, "sync_stock_minute_range",
                        lambda days: calls.append(list(days)) or 100)

    res = ms.backfill_to_now()

    assert calls == [[_d(2026, 8, 5)]], f"残缺日应触发 range 重写, 实际 {calls}"
    assert res["missing"]["kline_minute"]["missing"] is True


def test_sync_stock_minute_day_with_symbols_subset(tmp_path, monkeypatch):
    """symbols 参数只拉取给定子集（残片日只补缺失标的）。"""
    import datetime as _dt
    import pandas as pd
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [
        "000001.SZ", "600000.SH", "999999.SZ"])
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {
        "000001.SZ": _dt.date(2020, 1, 1),
        "600000.SH": _dt.date(2020, 1, 1),
        "999999.SZ": _dt.date(2020, 1, 1),
    })

    fetched: list[str] = []

    class _Src:
        def get_minute(self, sym, max_bars=40000):
            fetched.append(sym)
            idx = pd.DatetimeIndex([_dt.datetime(2026, 6, 15, 9, 31)])
            idx.name = "datetime"
            return pd.DataFrame({"open": [1.0], "close": [1.0],
                                 "volume": [100.0], "amount": [100.0]}, index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    n = ms.sync_stock_minute_day(_dt.date(2026, 6, 15),
                                 symbols=["999999.SZ"])

    assert n == 1
    assert fetched == ["999999.SZ"], f"只应拉取子集, 实际 {fetched}"


def test_sync_stock_minute_repairs_fragment_day(tmp_path, monkeypatch):
    """中间分区 symbol 数显著低于基线分区(回源中断残片) → 只补缺失标的。"""
    import datetime as _dt
    import pandas as pd
    from app.services import mootdx_service as ms

    ms.DATA_ROOT = tmp_path
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    universe = ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]
    monkeypatch.setattr(ms, "_stock_universe", lambda: list(universe))
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {
        s: _dt.date(2020, 1, 1) for s in universe})
    monkeypatch.setattr(ms, "_throttle_backfill", lambda i: None)
    monkeypatch.setattr(ms, "_append_failure", lambda s, r: None)
    # 测试规模小, 阈值降到 2
    monkeypatch.setattr(ms, "_MINUTE_FRAGMENT_THRESHOLD", 2)
    # 本测试只验证残片+resume，隔离新交易日 range 补全（latest=08-14 vs 真实今天）
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: [])

    def _write(day: str, syms: list[str]) -> None:
        pdir = root / f"date={day}"
        pdir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": syms,
            "datetime": [_dt.datetime.fromisoformat(f"{day}T09:31:00")] * len(syms),
            "open": [1.0] * len(syms),
            "high": [1.0] * len(syms),
            "low": [1.0] * len(syms),
            "close": [1.0] * len(syms),
            "volume": [100.0] * len(syms),
            "amount": [100.0] * len(syms),
        }).write_parquet(pdir / "part.parquet")

    # 残片日 08-12 只有 1 只；基线日 08-13 全量（最新日 08-14 只回源了 1 只,
    # 模拟新交易日回源未完成——基线必须取 08-13 而非最新分区）→ missing = 3 只
    _write("2026-08-11", ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"])
    _write("2026-08-12", ["000001.SZ"])
    _write("2026-08-13", list(universe))
    _write("2026-08-14", ["000001.SZ"])

    fetched: list[str] = []

    class _Src:
        def get_minute(self, sym, max_bars=40000):
            fetched.append(sym)
            idx = pd.DatetimeIndex([_dt.datetime(2026, 8, 12, 9, 31)])
            idx.name = "datetime"
            return pd.DataFrame({"open": [1.0], "high": [1.0], "low": [1.0],
                                 "close": [1.0], "volume": [100.0], "amount": [100.0]},
                                index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    n = ms.sync_stock_minute()

    # 残片补齐: 只拉 08-12 缺失的 3 只（000002/000003/000004），各 1 根；
    # resume 再补最新日 08-14 缺失的同 3 只 → 共拉 6 次
    assert n == 6
    assert sorted(fetched) == sorted(universe[1:] * 2), \
        f"残片+resume 应各拉缺失标的, 实际 {sorted(fetched)}"
    df12 = pl.read_parquet(root / "date=2026-08-12" / "part.parquet")
    assert sorted(set(df12["symbol"].to_list())) == list(universe)
    assert df12.height == 4


# ---------------------------------------------------------------------------
# 近一年内容校验：5 类统一（07-31 指数日线残帧回归）
# ---------------------------------------------------------------------------

def test_incomplete_index_daily_flags_fallback_fragment(tmp_path, monkeypatch):
    """07-31 回归：指数日线用兜底 4 只写入的残帧应被判残缺、完整日不误报。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "_index_universe",
                        lambda: [f"000{i:03d}.SH" for i in range(1, 61)] + ["399006.SZ"])
    root = tmp_path / "kline_index_daily"
    (root / "date=2026-07-31").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["000300.SH", "000510.SH", "399006.SZ", "399101.SZ"],
        "open": [1.0] * 4, "close": [1.0] * 4,
    }).write_parquet(root / "date=2026-07-31" / "part.parquet")
    (root / "date=2026-08-03").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"000{i:03d}.SH" for i in range(1, 61)] + ["399006.SZ"],
        "open": [1.0] * 61, "close": [1.0] * 61,
    }).write_parquet(root / "date=2026-08-03" / "part.parquet")
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)

    leftovers = ms._incomplete_index_daily_days()
    assert _d(2026, 7, 31) in leftovers, "4/61 残帧应被判残缺"
    assert _d(2026, 8, 3) not in leftovers, "完整日不应误报"


def test_incomplete_stock_daily_detects_sparse(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(90)])
    root = tmp_path / "kline_daily"
    for d in ["2026-08-03", "2026-08-04"]:
        (root / f"date={d}").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"6000{dd:03d}.SH" for dd in range(90)],
        "open": [1.0] * 90, "close": [1.0] * 90,
    }).write_parquet(root / "date=2026-08-03" / "part.parquet")
    pl.DataFrame({"symbol": ["600000.SH"], "open": [1.0], "close": [1.0]}).write_parquet(
        root / "date=2026-08-04" / "part.parquet")
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)

    leftovers = ms._incomplete_stock_daily_days()
    assert _d(2026, 8, 4) in leftovers
    assert _d(2026, 8, 3) not in leftovers


def test_incomplete_etf_minute_detects_sparse(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "_etf_universe",
                        lambda: [f"1599{dd:02d}.XSHE" for dd in range(90)])
    root = tmp_path / "kline_etf_minute"
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["159900.SZ"],
        "datetime": [_dt.datetime(2026, 8, 4, 9, 31)],
        "close": [1.0],
    }).write_parquet(root / "date=2026-08-04" / "part.parquet")
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)

    assert _d(2026, 8, 4) in ms._incomplete_etf_minute_days()


def test_incomplete_etf_daily_window_250_vs_7(tmp_path, monkeypatch):
    """残缺分区位于第 31~250 位之间：recent=250 识别、recent=7 不识别。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "_etf_universe",
                        lambda: [f"1599{dd:02d}.XSHE" for dd in range(90)])
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 1))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    root = tmp_path / "kline_etf_daily"
    for i in range(60):
        ds = (_d(2026, 8, 1) - _dt.timedelta(days=i)).isoformat()
        (root / f"date={ds}").mkdir(parents=True, exist_ok=True)
        if i == 40:  # 第 41 新的分区为残帧（1 只）
            df = pl.DataFrame({"symbol": ["159900.SZ"], "open": [1.0], "close": [1.0]})
        else:
            df = pl.DataFrame({
                "symbol": [f"1599{dd:02d}.SZ" for dd in range(90)],
                "open": [1.0] * 90, "close": [1.0] * 90,
            })
        df.write_parquet(root / f"date={ds}" / "part.parquet")

    assert ms._incomplete_etf_daily_days(recent=7) == [], "近 7 分区内无残缺"
    left = ms._incomplete_etf_daily_days(recent=250)
    assert len(left) == 1, f"recent=250 应识别 1 个残帧, 实际 {left}"


# ---------------------------------------------------------------------------
# 新交易日股票分钟当日落盘（resume 架空修复）
# ---------------------------------------------------------------------------

def test_missing_stock_minute_days_intraday_and_after_close(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    assert ms._missing_stock_minute_days(_dt.datetime(2026, 8, 5, 10, 55)) == [], \
        "盘中今天不补（防半程数据）"
    assert ms._missing_stock_minute_days(_dt.datetime(2026, 8, 5, 16, 0)) == [_dt.date(2026, 8, 5)]


def test_missing_stock_minute_days_past_gap(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    (root / "date=2026-08-03").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    days = ms._missing_stock_minute_days(_dt.datetime(2026, 8, 6, 10, 55))
    assert days == [_dt.date(2026, 8, 4), _dt.date(2026, 8, 5)], "盘中补真实历史缺失日，排除今天"


def test_sync_stock_minute_pulls_missing_day_first(monkeypatch, tmp_path):
    """回归：最新分区=昨天完整、今天无分区、已收盘 → 先 range 补今天再 resume。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(3)])
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"6000{dd:03d}.SH" for dd in range(3)],
        "datetime": [_dt.datetime(2026, 8, 4, 9, 31)] * 3, "close": [1.0] * 3,
    }).write_parquet(root / "date=2026-08-04" / "part.parquet")
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    monkeypatch.setattr(ms, "_minute_fragment_days", lambda: {})
    ranges = []
    monkeypatch.setattr(ms, "sync_stock_minute_range",
                        lambda days: ranges.append(list(days)) or 10)

    n = ms.sync_stock_minute(limit=None)

    assert ranges == [[_d(2026, 8, 5)]], "应先 range 补今天"
    assert n == 10, "返回值应包含 range 写入行数"


def test_sync_stock_minute_no_range_when_current(monkeypatch, tmp_path):
    """最新分区=今天 → 不触发 range，正常 resume。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(3)])
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 5)])
    (root / "date=2026-08-05").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"6000{dd:03d}.SH" for dd in range(3)],
        "datetime": [_dt.datetime(2026, 8, 5, 9, 31)] * 3, "close": [1.0] * 3,
    }).write_parquet(root / "date=2026-08-05" / "part.parquet")
    monkeypatch.setattr(ms, "_minute_fragment_days", lambda: {})
    ranges = []
    monkeypatch.setattr(ms, "sync_stock_minute_range",
                        lambda days: ranges.append(list(days)) or 10)

    n = ms.sync_stock_minute(limit=None)

    assert ranges == [], "最新分区已是今天，不应触发 range"
    assert n == 0, "resume todo 空 → 0 行"


# ---------------------------------------------------------------------------
# 内容校验接入巡检/启动回源
# ---------------------------------------------------------------------------

def test_scan_missing_partitions_content_recent_default(tmp_path, monkeypatch):
    """content_recent 默认近 1 周（7），显式传 250 时透传给全部 _incomplete_*。"""
    import datetime as _dt
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    for key in ["STOCK_DAILY_ROOT", "ETF_DAILY_ROOT", "INDEX_DAILY_ROOT",
                "ETF_MINUTE_ROOT", "STOCK_MINUTE_ROOT"]:
        monkeypatch.setattr(ms, key, tmp_path / key.lower())
    monkeypatch.setattr(ms, "_trade_days_in_range", lambda s, e: [_dt.date(2026, 8, 4)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159900.XSHE"])
    monkeypatch.setattr(ms, "_stale_daily_days", lambda root, now=None: [])
    seen: dict[str, int | None] = {}

    def _mk(name):
        def f(recent=None):
            seen[name] = recent
            return []
        return f

    monkeypatch.setattr(ms, "_incomplete_stock_daily_days", _mk("stock_daily"))
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days", _mk("etf_daily"))
    monkeypatch.setattr(ms, "_incomplete_index_daily_days", _mk("index_daily"))
    monkeypatch.setattr(ms, "_incomplete_etf_minute_days", _mk("etf_minute"))
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", _mk("stock_minute"))

    ms.scan_missing_partitions()
    assert set(seen.values()) == {ms._DAILY_CHECK_RECENT_PARTITIONS}, \
        f"默认应传近 1 周窗口, 实际 {seen}"

    seen.clear()
    ms.scan_missing_partitions(content_recent=250)
    assert set(seen.values()) == {250}, f"显式 250 应透传, 实际 {seen}"


def test_backfill_to_now_resyncs_content_flagged_index(monkeypatch, tmp_path):
    """启动回源用近 1 周窗口：内容残缺的指数日线触发 sync_index_daily 重写。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    _stub_etf_nav(monkeypatch)
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    for name in ["kline_etf_minute", "kline_etf_daily", "kline_index_daily", "kline_daily"]:
        (tmp_path / name / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    (tmp_path / "adj_factor_etf").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["510300.XSHG"], "trade_date": [_d(2026, 8, 4)], "ex_factor": [1.0],
    }).write_parquet(tmp_path / "adj_factor_etf" / "all.parquet")
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_incomplete_etf_minute_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_daily_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [])

    def _flagged_index(recent=None):
        w = recent or ms._CONTENT_CHECK_RECENT_DAYS
        return [_d(2026, 7, 31)] if w <= ms._DAILY_CHECK_RECENT_PARTITIONS else []

    monkeypatch.setattr(ms, "_incomplete_index_daily_days", _flagged_index)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: False)
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: 0)
    monkeypatch.setattr(ms, "sync_daily", lambda d: {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
    monkeypatch.setattr(ms, "_notify_missing", lambda m: None)
    days = []
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: days.append(d) or {"written": 1})

    res = ms.backfill_to_now()

    assert days == [_d(2026, 7, 31)], f"启动回源应重写内容残缺指数日线, 实际 {days}"
    assert res["missing"]["kline_index_daily"]["missing"] is True


# ---------------------------------------------------------------------------
# 单日 / 全量检验补齐
# ---------------------------------------------------------------------------

def test_check_and_repair_day_repairs_sparse_index(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [f"6000{dd:02d}.SH" for dd in range(3)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [f"1599{dd:02d}.XSHE" for dd in range(3)])
    monkeypatch.setattr(ms, "_index_universe", lambda: ["000001.SH", "000300.SH", "399006.SZ"])
    root = tmp_path / "kline_index_daily"
    (root / "date=2026-07-31").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["000300.SH"], "open": [1.0], "close": [1.0]}).write_parquet(
        root / "date=2026-07-31" / "part.parquet")
    calls = {"daily": 0, "index": [], "etf_minute": 0, "stock_minute": []}
    monkeypatch.setattr(ms, "sync_daily", lambda d: calls.__setitem__("daily", calls["daily"] + 1))
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: calls["index"].append(d) or {"written": 3})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: calls.__setitem__("etf_minute", calls["etf_minute"] + 1))
    monkeypatch.setattr(ms, "sync_stock_minute_day",
                        lambda day, symbols=None: calls["stock_minute"].append(sorted(symbols or [])) or 0)

    res = ms.check_and_repair_day(_d(2026, 7, 31))

    assert calls["index"] == [_d(2026, 7, 31)], "指数日线残缺应重写"
    assert res["results"]["index_daily"]["status"] == "repaired"
    # 其它类型该日无分区 → 覆盖率为 0 → 也重写（股票分钟只补缺失 symbol）
    assert calls["daily"] >= 1
    assert calls["etf_minute"] >= 1
    assert calls["stock_minute"] == [[
        "600000.SH", "600001.SH", "600002.SH"]], "股票分钟应只补缺失 symbol"


def test_check_and_repair_day_noop_when_complete(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [f"6000{dd:02d}.SH" for dd in range(3)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [f"1599{dd:02d}.XSHE" for dd in range(3)])
    monkeypatch.setattr(ms, "_index_universe", lambda: ["000001.SH", "000300.SH", "399006.SZ"])
    for sub, syms in [
        ("kline_daily", ["600000.SH", "600001.SH", "600002.SH"]),
        ("kline_etf_daily", ["159900.SZ", "159901.SZ", "159902.SZ"]),
        ("kline_index_daily", ["000001.SH", "000300.SH", "399006.SZ"]),
        ("kline_etf_minute", ["159900.SZ", "159901.SZ", "159902.SZ"]),
        ("kline_minute", ["600000.SH", "600001.SH", "600002.SH"]),
    ]:
        pdir = tmp_path / sub / "date=2026-08-04"
        pdir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": syms, "close": [1.0] * len(syms)}).write_parquet(
            pdir / "part.parquet")
    calls = {"n": 0}
    for fn in ["sync_daily", "sync_index_daily", "sync_etf_minute", "sync_stock_minute_day"]:
        monkeypatch.setattr(ms, fn, lambda *a, **k: calls.__setitem__("n", calls["n"] + 1))

    res = ms.check_and_repair_day(_d(2026, 8, 4))

    assert calls["n"] == 0, "全部完整时不应触发任何重写"
    assert all(v["status"] == "ok" for v in res["results"].values())


def test_check_and_repair_day_skips_today_intraday(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [f"6000{dd:02d}.SH" for dd in range(3)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [f"1599{dd:02d}.XSHE" for dd in range(3)])
    monkeypatch.setattr(ms, "_index_universe", lambda: ["000001.SH", "000300.SH", "399006.SZ"])
    # 今日盘中即使残缺也不应重写（防 08-18 半程污染写入）
    for sub in ["kline_daily", "kline_etf_daily", "kline_index_daily",
                "kline_etf_minute", "kline_minute"]:
        pdir = tmp_path / sub / "date=2026-08-05"
        pdir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": ["000300.SH"], "close": [1.0]}).write_parquet(
            pdir / "part.parquet")
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda: False)  # 盘中
    calls = {"n": 0}
    for fn in ["sync_daily", "sync_index_daily", "sync_etf_minute", "sync_stock_minute_day"]:
        monkeypatch.setattr(ms, fn, lambda *a, **k: calls.__setitem__("n", calls["n"] + 1))

    res = ms.check_and_repair_day(_d(2026, 8, 5))

    assert calls["n"] == 0, "盘中当日不应触发任何重写"
    assert all(v["status"] == "skip" for v in res["results"].values())


def test_check_and_repair_full_uses_250_window(monkeypatch, tmp_path):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    seen = {}
    monkeypatch.setattr(ms, "scan_and_backfill_full",
                        lambda content_recent=None: seen.setdefault(
                            "recent", content_recent)
                        and {"missing": {}, "backfilled": {}, "errors": []})

    res = ms.check_and_repair_full()

    assert seen["recent"] == ms._CONTENT_CHECK_RECENT_DAYS, "全量默认用近一年窗口"
    assert res["errors"] == []
