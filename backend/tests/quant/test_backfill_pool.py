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


def test_keep_frames_false_does_not_retain():
    """keep_frames=False：池不驻留帧，ok 为空、ok_count 计数正确。"""
    held = []

    def fn(src, sym):
        return {"sym": sym, "payload": [0] * 1000}  # 模拟大帧

    pool = BackfillPool(workers=2)
    res = pool.map(fn, list("abc"), batch_size=2,
                   on_batch_done=lambda b: held.extend(b), keep_frames=False)
    assert res["ok"] == []            # 池未驻留
    assert res["ok_count"] == 3
    assert len(held) == 3             # 帧仍经批回调全量送达


def test_flushed_frames_are_released_before_map_finishes():
    """Future 自身也持有结果；仅返回 ok=[] 不能证明批量落盘后内存释放。"""
    import weakref

    class Frame:
        pass

    first_batch = []
    retained_at_flush = []

    def flush(batch):
        if not first_batch:
            first_batch.extend(weakref.ref(frame) for frame in batch)
        else:
            retained_at_flush.append(sum(ref() is not None for ref in first_batch))

    pool = BackfillPool(workers=2, source_factory=object)
    result = pool.map(lambda _src, _sym: Frame(), range(30), batch_size=2,
                      on_batch_done=flush, keep_frames=False)
    assert result["ok_count"] == 30
    assert retained_at_flush == [0] * 14


def test_completed_symbols_flush_while_first_symbol_is_slow():
    import threading

    release = threading.Event()
    flushed = threading.Event()
    result = []

    def fetch(_src, symbol):
        if symbol == "slow":
            assert release.wait(5)
        return symbol

    def flush(batch):
        if "fast" in batch:
            flushed.set()

    pool = BackfillPool(workers=2, source_factory=object)
    thread = threading.Thread(target=lambda: result.append(pool.map(
        fetch, ["slow", "fast"], batch_size=1, on_batch_done=flush
    )), daemon=True)
    thread.start()
    try:
        assert flushed.wait(2), "已完成数据不应等待队首慢标的才落盘"
    finally:
        release.set()
        thread.join(timeout=5)
    assert not thread.is_alive()
    # 返回结果保留调用方输入顺序，落盘回调按完成顺序流式消费。
    assert result[0]["ok"] == ["slow", "fast"]
