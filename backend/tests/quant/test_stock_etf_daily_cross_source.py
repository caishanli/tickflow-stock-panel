"""股票/ETF 日线部分缺失的 TickFlow 跨源二级修复测试。

背景：2026-09-14 mootdx K 线熔断开路一整天，午夜巡检与收盘同步的
sync_daily 全失败（0 行），kline_etf_daily 只剩 774/1658 行，
次日晨间 ab_v56fix 报「成交额异常」。指数日线已有
_repair_index_day（mootdx + TickFlow 两级），stock/etf 只有 mootdx
单源——本文件锁定新加的同款两级修复。
"""
from __future__ import annotations

import datetime as _dt

import polars as pl

from app.services import mootdx_service as ms


def _write_part(root, day: _dt.date, syms: list[str]) -> None:
    pdir = root / f"date={day.isoformat()}"
    pdir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": syms}).write_parquet(pdir / "part.parquet")


def _syms(n: int, prefix: str) -> list[str]:
    return [f"{prefix}{i:04d}.SH" for i in range(n)]


def _seed(root, base, d0=_dt.date(2026, 9, 7), days=5):
    for i in range(days):
        _write_part(root, d0 + _dt.timedelta(days=i), base)


def _patch_roots(monkeypatch, tmp_path):
    sroot = tmp_path / "kline_daily"
    eroot = tmp_path / "kline_etf_daily"
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", sroot)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", eroot)
    return sroot, eroot


def _breaker_open(monkeypatch, is_open: bool):
    import app.quant.jqengine.datasource.mootdx_breaker as br
    # True = 熔断开路（kline 不可用）→ 允许跨源
    monkeypatch.setattr(br, "kline_allowed", lambda: not is_open)


def test_repair_routes_cross_source_when_breaker_open(tmp_path, monkeypatch):
    """mootdx 补不满 + 熔断开路 → stock/etf 缺口各走 TickFlow。"""
    sroot, eroot = _patch_roots(monkeypatch, tmp_path)
    base = _syms(100, "6000")
    _seed(sroot, base)
    _seed(eroot, base)
    day = _dt.date(2026, 9, 14)
    _write_part(sroot, day, base[:80])
    _write_part(eroot, day, base[:80])
    monkeypatch.setattr(ms, "sync_daily", lambda d: {"stock": 0, "etf": 0})
    _breaker_open(monkeypatch, True)

    calls = {}

    def fake_stock(d, missing):
        calls["stock"] = sorted(missing)
        _write_part(sroot, d, base)
        return len(missing)

    def fake_etf(d, missing):
        calls["etf"] = sorted(missing)
        _write_part(eroot, d, base)
        return len(missing)

    monkeypatch.setattr(ms, "_cross_source_stock_repair", fake_stock)
    monkeypatch.setattr(ms, "_cross_source_etf_repair", fake_etf)

    res = ms._repair_stock_etf_day(day)
    assert res["cross"] == {"stock": 20, "etf": 20}
    assert calls["stock"] == base[80:]
    assert calls["etf"] == base[80:]


def test_repair_skips_cross_source_when_breaker_closed(tmp_path, monkeypatch):
    """熔断关闭（mootdx 健康）→ 不耗 TickFlow 配额。"""
    sroot, eroot = _patch_roots(monkeypatch, tmp_path)
    base = _syms(100, "6000")
    _seed(sroot, base)
    _seed(eroot, base)
    day = _dt.date(2026, 9, 14)
    _write_part(sroot, day, base[:80])
    _write_part(eroot, day, base[:80])
    monkeypatch.setattr(ms, "sync_daily", lambda d: {"stock": 0, "etf": 0})
    _breaker_open(monkeypatch, False)

    called = {"n": 0}

    def _boom(d, missing):
        called["n"] += 1
        return 0

    monkeypatch.setattr(ms, "_cross_source_stock_repair", _boom)
    monkeypatch.setattr(ms, "_cross_source_etf_repair", _boom)
    res = ms._repair_stock_etf_day(day)
    assert called["n"] == 0
    assert res["cross"] == {"stock": 0, "etf": 0}


def test_repair_skips_cross_source_when_mootdx_covers(tmp_path, monkeypatch):
    """mootdx 一轮补满 → 无缺口，不跨源。"""
    sroot, eroot = _patch_roots(monkeypatch, tmp_path)
    base = _syms(100, "6000")
    _seed(sroot, base)
    _seed(eroot, base)
    day = _dt.date(2026, 9, 14)
    _write_part(sroot, day, base[:80])
    _write_part(eroot, day, base[:80])

    def fake_mootdx(d):
        _write_part(sroot, d, base)
        _write_part(eroot, d, base)
        return {"stock": 20, "etf": 20}

    monkeypatch.setattr(ms, "sync_daily", fake_mootdx)
    _breaker_open(monkeypatch, True)
    called = {"n": 0}
    monkeypatch.setattr(ms, "_cross_source_stock_repair",
                        lambda d, m: called.__setitem__("n", called["n"] + 1))
    monkeypatch.setattr(ms, "_cross_source_etf_repair",
                        lambda d, m: called.__setitem__("n", called["n"] + 1))
    ms._repair_stock_etf_day(day)
    assert called["n"] == 0


def test_backfill_missing_partitions_routes_two_level_repair(tmp_path, monkeypatch):
    """00:00 巡检补全对 kline_daily/kline_etf_daily 走两级修复而非裸 sync_daily。"""
    day = _dt.date(2026, 9, 14)
    called = {"repair": [], "sync": []}
    monkeypatch.setattr(ms, "_repair_stock_etf_day",
                        lambda d: called["repair"].append(d) or {"cross": {}})
    monkeypatch.setattr(ms, "sync_daily",
                        lambda d: called["sync"].append(d) or {"stock": 0, "etf": 0})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: None)
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: {"written": 0})
    missing = {"kline_daily": [day], "kline_etf_daily": [day],
               "kline_index_daily": [], "kline_etf_minute": [],
               "kline_minute": [], "etf_nav": [], "etf_universe_segments": []}
    ms.backfill_missing_partitions(missing)
    assert called["repair"] == [day]
    assert called["sync"] == []
