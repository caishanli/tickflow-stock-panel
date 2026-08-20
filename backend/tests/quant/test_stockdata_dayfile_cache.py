"""DayFileCache 单测：命中/未命中/并发 single-flight/超时卸载/容量踢旧。

spec: docs/superpowers/specs/2026-08-21-stockdata-daily-dayfile-lru-design.md
"""
import threading
import time

import polars as pl

from app.services.stockdata.sources import DayFileCache


def _frame():
    return pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": ["2026-08-19"],
        "open": [10.0], "high": [11.0], "low": [9.0],
        "close": [10.5], "volume": [1000], "amount": [105000.0],
    })


def test_get_miss_returns_none():
    c = DayFileCache()
    assert c.get("kline_daily", "2026-08-19") is None
    assert len(c) == 0


def test_get_or_load_stores_and_hits():
    c = DayFileCache()
    f = _frame()
    assert c.get_or_load("kline_daily", "2026-08-19", lambda: f) is f
    assert c.get("kline_daily", "2026-08-19") is f
    assert len(c) == 1


def test_loader_skipped_when_hit():
    calls = []
    c = DayFileCache()
    f = _frame()
    c.get_or_load("kline_daily", "2026-08-19", lambda: (calls.append(1), f)[1])
    c.get_or_load("kline_daily", "2026-08-19", lambda: (calls.append(1), f)[1])
    assert len(calls) == 1


def test_concurrent_load_single_flight():
    """同键并发 get_or_load 只读盘一次。"""
    calls = []
    c = DayFileCache()
    f = _frame()

    def loader():
        calls.append(1)
        time.sleep(0.05)
        return f

    results = []
    threads = [threading.Thread(target=lambda: results.append(
        c.get_or_load("kline_daily", "2026-08-19", loader))) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(calls) == 1
    assert all(r is f for r in results)


def test_sweep_evicts_expired():
    c = DayFileCache(ttl=0.1, cap=10)
    c.get_or_load("kline_daily", "2026-08-19", lambda: _frame())
    time.sleep(0.15)
    assert c.sweep() == 1
    assert len(c) == 0


def test_sweep_touch_keeps_alive():
    """60s 内被访问过的文件不被卸载。"""
    c = DayFileCache(ttl=0.2, cap=10)
    c.get_or_load("kline_daily", "2026-08-19", lambda: _frame())
    time.sleep(0.1)
    c.get("kline_daily", "2026-08-19")  # 刷新最后访问时间
    time.sleep(0.1)
    assert c.sweep() == 0
    assert len(c) == 1


def test_sweep_caps_size():
    """超容量上限按最后访问时间从旧到新踢。"""
    c = DayFileCache(ttl=60.0, cap=3)
    for i in range(5):
        c.get_or_load("kline_daily", f"2026-08-{10 + i}", lambda: _frame())
        time.sleep(0.01)
    assert c.sweep() == 2
    assert len(c) == 3


def test_loader_empty_not_cached():
    c = DayFileCache()
    assert c.get_or_load("kline_daily", "2026-08-19", lambda: pl.DataFrame()) is None
    assert c.get_or_load("kline_daily", "2026-08-19", lambda: None) is None
    assert len(c) == 0