"""启动 backfill 持锁与中断感知日志测试。"""
from __future__ import annotations

import time

import polars as pl

from app.services import mootdx_service as ms


def test_interrupted_partition_detected_by_mtime(tmp_path, monkeypatch):
    """最新分区 mtime 距今 <10min → 视为上次回源中断（覆盖率判定保留）。"""
    root = tmp_path / "kline_minute"
    pdir = root / "date=2026-08-21"
    pdir.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600000.SH"]}).write_parquet(pdir / "part.parquet")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    stocks = [f"{600000 + i}.SH" for i in range(10)]
    monkeypatch.setattr(ms, "_stock_universe", lambda: stocks)
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    partial = ms._stock_minute_latest_partial({"600000.SH"}, stocks)
    assert partial is True  # 覆盖率 1/10 < 0.95


def test_sync_lock_shared_between_service_and_scheduler():
    """scheduler._sync_lock 与 mootdx_service._SYNC_LOCK 是同一把锁。"""
    from app.services.stockdata import scheduler

    assert scheduler._sync_lock() is ms._SYNC_LOCK


def test_backfill_to_now_holds_sync_lock(tmp_path, monkeypatch):
    """backfill_to_now 全程持 _SYNC_LOCK：锁被占时 body 阻塞不执行。"""
    import threading

    started = threading.Event()

    # 最小化副作用：全部扫描返回空、nav 服务打桩
    for name in ["_incomplete_etf_minute_days", "_incomplete_stock_daily_days",
                 "_incomplete_etf_daily_days", "_incomplete_index_daily_days",
                 "_incomplete_stock_minute_days", "_missing_stock_minute_days",
                 "_missing_minute_days", "_missing_index_daily_days",
                 "_safe_universe_segment_missing"]:
        monkeypatch.setattr(ms, name, lambda *a, **k: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root, now=None: [])
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj.parquet")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kd")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "ed")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "idx")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "em")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "sm")
    from app.services import etf_nav_service
    monkeypatch.setattr(etf_nav_service, "_partition_dates", lambda: [])
    monkeypatch.setattr(etf_nav_service, "_missing_etf_nav_days", lambda: [])
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
    monkeypatch.setattr(ms, "sync_daily", lambda day: {"stock": 0, "etf": 0})
    monkeypatch.setattr(ms, "sync_index_daily", lambda day: {"written": 0})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda day=None: 0)
    monkeypatch.setattr(ms, "sync_adj_factor", lambda: {"rows": 0})

    def runner():
        ms.backfill_to_now()
        started.set()

    t = threading.Thread(target=runner)
    assert ms._SYNC_LOCK.acquire()
    t.start()
    time.sleep(0.3)
    assert not started.is_set(), "锁被占时 backfill_to_now 应阻塞"
    ms._SYNC_LOCK.release()
    # 全量套件下 scheduler 用例可能遗留持锁后台线程，宽限到 60s
    assert started.wait(timeout=60), "放锁后应完成"
    t.join(timeout=1)


def test_etf_daily_content_scan_called_once(tmp_path, monkeypatch):
    """backfill_to_now 内 kline_etf_daily 250 分区内容校验只跑一次。"""
    calls = {"n": 0}

    def fake_scan(recent=None):
        calls.setdefault(recent, 0)
        calls[recent] += 1
        return []

    for name in ["_incomplete_stock_daily_days", "_incomplete_index_daily_days",
                 "_incomplete_stock_minute_days", "_incomplete_etf_minute_days"]:
        monkeypatch.setattr(ms, name, lambda *a, **k: [])
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days", fake_scan)
    _orig_missing_etf_daily = ms._missing_daily_days
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: set())
    for name in ["_missing_minute_days", "_missing_index_daily_days"]:
        monkeypatch.setattr(ms, name, lambda *a, **k: set())
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root, now=None: set())
    monkeypatch.setattr(ms, "_stale_daily_days", lambda root, now=None: set())
    monkeypatch.setattr(ms, "_safe_universe_segment_missing", lambda: [])
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj.parquet")
    for attr, name in [("STOCK_DAILY_ROOT", "kd"), ("ETF_DAILY_ROOT", "ed"),
                       ("INDEX_DAILY_ROOT", "idx"), ("ETF_MINUTE_ROOT", "em"),
                       ("STOCK_MINUTE_ROOT", "sm")]:
        monkeypatch.setattr(ms, attr, tmp_path / name)
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
    from app.services import etf_nav_service
    monkeypatch.setattr(etf_nav_service, "_partition_dates", lambda: [])
    monkeypatch.setattr(etf_nav_service, "_missing_etf_nav_days", lambda: [])
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
    monkeypatch.setattr(ms, "sync_daily", lambda day: {"stock": 0, "etf": 0})
    monkeypatch.setattr(ms, "sync_index_daily", lambda day: {"written": 0})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda day=None: 0)
    monkeypatch.setattr(ms, "sync_adj_factor", lambda: {"rows": 0})

    ms.backfill_to_now()
    # 全量校验窗口（recent=None→默认250）只跑一次；7 日窗口调用是另一语义，不计
    assert calls.get(None, 0) == 1, f"250 窗口校验应只跑一次，实际 {calls}"
