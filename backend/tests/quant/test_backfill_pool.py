"""BackfillPool 并发/批回调/盘中减半测试。"""
from __future__ import annotations

import time

from app.services.stockdata.backfill_pool import BackfillPool


def test_map_runs_all_symbols_and_batches():
    seen = []
    batches = []

    def fn(src, sym):
        seen.append(sym)
        return f"v-{sym}"

    pool = BackfillPool(workers=3)
    res = pool.map(fn, ["a", "b", "c", "d", "e"], batch_size=2,
                   on_batch_done=batches.append)
    assert sorted(seen) == list("abcde")
    assert sorted(res["ok"]) == ["v-a", "v-b", "v-c", "v-d", "v-e"]
    assert [len(b) for b in batches] == [2, 2, 1]


def test_map_records_failures_without_blocking():
    def fn(src, sym):
        if sym == "bad":
            raise ValueError("boom")
        return sym

    pool = BackfillPool(workers=2)
    res = pool.map(fn, ["x", "bad", "y"])
    assert res["ok"] == ["x", "y"]
    assert res["failed"] == {"bad": "boom"}


def test_workers_halved_intraday_large_task(monkeypatch):
    from app.services.stockdata import backfill_pool as bp
    monkeypatch.setattr(bp, "_is_market_open", lambda: True)
    pool = bp.BackfillPool(workers=6)
    assert pool.effective_workers(task_size=501) == 3
    assert pool.effective_workers(task_size=500) == 6


def test_thread_local_sources_distinct():
    sources = set()

    def fn(src, sym):
        sources.add(id(src))
        time.sleep(0.01)
        return sym

    pool = BackfillPool(workers=4)
    pool.map(fn, list("abcdefgh"))
    assert len(sources) > 1  # 每 worker 独立 source 实例
