"""指数日线部分缺失的自动检测 + 跨源补齐测试。

背景：08-21 指数日线只有 555/599（前 5 天基线），绝对覆盖率 91% 越过
50% 残缺阈值检测不到；且缺的 44 只 000xxx.SH 中证系指数 mootdx 不提供，
须路由 TickFlow 源（index_sync）补齐。
"""
from __future__ import annotations

import datetime as _dt

import polars as pl

from app.services import mootdx_service as ms


def _write_index_part(root, day: _dt.date, syms: list[str]) -> None:
    pdir = root / f"date={day.isoformat()}"
    pdir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": syms}).write_parquet(pdir / "part.parquet")


def _syms(n: int, prefix: str) -> list[str]:
    return [f"{prefix}{i:04d}.SH" for i in range(n)]


def test_index_shortfall_detected_against_recent_baseline(tmp_path, monkeypatch):
    """近 7 分区基线 100 只，某日 90 只 → 判残缺并列出缺失清单。"""
    root = tmp_path / "kline_index_daily"
    base = _syms(100, "0000")
    d0 = _dt.date(2026, 8, 10)
    for i in range(5):  # 5 天完整基线
        _write_index_part(root, d0 + _dt.timedelta(days=i), base)
    short_day = d0 + _dt.timedelta(days=5)
    _write_index_part(root, short_day, base[:90])  # 缺 10 只

    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", root)
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    got = ms._index_shortfall_days()
    assert short_day in got
    assert sorted(got[short_day]) == _syms(100, "0000")[90:]


def test_index_no_shortfall_when_within_ratio(tmp_path, monkeypatch):
    """缺失在 5% 容忍带内（停牌/退市类正常波动）不误报。"""
    root = tmp_path / "kline_index_daily"
    base = _syms(100, "0000")
    d0 = _dt.date(2026, 8, 10)
    for i in range(5):
        _write_index_part(root, d0 + _dt.timedelta(days=i), base)
    ok_day = d0 + _dt.timedelta(days=5)
    _write_index_part(root, ok_day, base[:97])  # 缺 3% —— 容忍带内

    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", root)
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    assert ms._index_shortfall_days() == {}


def test_repair_index_day_falls_back_to_cross_source(tmp_path, monkeypatch):
    """mootdx 补不满基线时，剩余缺口路由 TickFlow 源（symbols_override）。"""
    root = tmp_path / "kline_index_daily"
    base = _syms(100, "0000") + _syms(45, "9000")  # 145 只基线
    d0 = _dt.date(2026, 8, 10)
    for i in range(5):
        _write_index_part(root, d0 + _dt.timedelta(days=i), base)
    day = d0 + _dt.timedelta(days=5)
    # 当日只有 mootdx 能供的 100 只；9000 系 45 只缺
    _write_index_part(root, day, _syms(100, "0000"))
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", root)
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: {"written": 0})

    calls = {}

    def fake_cross_src(d, missing):
        calls["day"] = d
        calls["missing"] = sorted(missing)
        # 模拟 tickflow 落盘：写全缺口
        _write_index_part(root, d, base)
        return len(missing)

    monkeypatch.setattr(ms, "_cross_source_index_repair", fake_cross_src)
    res = ms._repair_index_day(day)
    assert res["cross"] == 45
    assert calls["day"] == day
    assert calls["missing"] == _syms(45, "9000")


def test_repair_index_day_skips_cross_source_when_mootdx_covers(tmp_path, monkeypatch):
    """mootdx 一轮就补满（无缺口）时不触发跨源。"""
    root = tmp_path / "kline_index_daily"
    base = _syms(100, "0000")
    d0 = _dt.date(2026, 8, 10)
    for i in range(5):
        _write_index_part(root, d0 + _dt.timedelta(days=i), base)
    day = d0 + _dt.timedelta(days=5)
    _write_index_part(root, day, base[:80])
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", root)
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)

    def fake_mootdx(d):
        _write_index_part(root, d, base)  # mootdx 一轮补满
        return {"written": 20}

    monkeypatch.setattr(ms, "sync_index_daily", fake_mootdx)
    called = {"n": 0}
    monkeypatch.setattr(ms, "_cross_source_index_repair",
                        lambda d, m: called.__setitem__("n", called["n"] + 1))
    ms._repair_index_day(day)
    assert called["n"] == 0


# ---------------------------------------------------------------------------
# 泛化：其余数据集的相对基线检测
# ---------------------------------------------------------------------------

import pytest

from app.services import mootdx_service as ms


def _seed_baseline(root, base_syms, days=5, d0=_dt.date(2026, 8, 10)):
    for i in range(days):
        _write_index_part(root, d0 + _dt.timedelta(days=i), base_syms)


@pytest.mark.parametrize("attr", ["STOCK_DAILY_ROOT", "ETF_DAILY_ROOT",
                                  "ETF_MINUTE_ROOT"])
def test_shortfall_generalizes_to_other_datasets(tmp_path, monkeypatch, attr):
    """股票日线/ETF 日线/ETF 分钟：基线 100 只，某日 85 只(<95%) → 检出。"""
    root = tmp_path / attr.lower()
    monkeypatch.setattr(ms, attr, root)
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    base = [f"{600000 + i}.SH" for i in range(100)]
    _seed_baseline(root, base)
    short_day = _dt.date(2026, 8, 15)
    _write_index_part(root, short_day, base[:85])

    got = ms._shortfall_days(root)
    assert got and short_day in got
    assert len(got[short_day]) == 15


def test_scan_missing_partitions_merges_shortfall(tmp_path, monkeypatch):
    """巡检扫描把各数据集相对基线缺口并入对应缺失清单。"""
    import pandas as pd

    # 交易日历：只认 08-11~08-16
    cal = [_dt.date(2026, 8, d) for d in range(11, 17)]
    monkeypatch.setattr(ms, "_trade_days_in_range",
                        lambda start=None, end=None, **k: cal)

    roots = {}
    base = [f"{600000 + i}.SH" for i in range(100)]
    for attr in ("INDEX_DAILY_ROOT", "STOCK_DAILY_ROOT", "ETF_DAILY_ROOT",
                 "ETF_MINUTE_ROOT", "STOCK_MINUTE_ROOT"):
        r = tmp_path / attr.lower()
        monkeypatch.setattr(ms, attr, r)
        roots[attr] = r
        _seed_baseline(r, base, days=4, d0=_dt.date(2026, 8, 11))
        # 每类都写一个 08-14 的"完整"日 + 把 08-15 缺 20 只
        _write_index_part(r, _dt.date(2026, 8, 14), base)
        _write_index_part(r, _dt.date(2026, 8, 15), base[:80])
    # 股票分钟走残片逻辑（≥500），80 只缺失不触发——从其缺口中排除
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    import app.services.etf_nav_service as nav
    monkeypatch.setattr(nav, "_missing_etf_nav_days", lambda: [])
    monkeypatch.setattr(ms, "_safe_universe_segment_missing", lambda: [])

    missing = ms.scan_missing_partitions()
    assert _dt.date(2026, 8, 15) in [d for d in missing["kline_daily"]]
    assert _dt.date(2026, 8, 15) in [d for d in missing["kline_etf_daily"]]
    assert _dt.date(2026, 8, 15) in [d for d in missing["kline_etf_minute"]]


def test_backfill_missing_partitions_routes_shortfall_repair(tmp_path, monkeypatch):
    """00:00 巡检补全对缺口日调用 sync_daily / sync_etf_minute。"""
    called = {"daily": [], "etf_min": []}
    day = _dt.date(2026, 8, 15)
    monkeypatch.setattr(ms, "sync_daily",
                        lambda d: called["daily"].append(d) or {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: called["etf_min"].append(d))
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: {"written": 0})
    missing = {"kline_daily": [day], "kline_etf_daily": [],
               "kline_index_daily": [], "kline_etf_minute": [day],
               "kline_minute": [], "etf_nav": [], "etf_universe_segments": []}
    res = ms.backfill_missing_partitions(missing)
    assert called["daily"] == [day]
    assert called["etf_min"] == [day]
    assert res["errors"] == []
