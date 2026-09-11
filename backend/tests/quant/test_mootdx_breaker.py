"""mootdx K 线熔断器回归测试。

背景：2026-09-10 起 TDX K 线全局返回空，stockdata 逐只烧满 17 台轮换
（实时每只卡 30s、批量 0.2 只/s、日志风暴）——熔断器让开路期调用快速
失败、直走腾讯/新浪。
"""
import threading
import time

import pandas as pd
import pytest

from app.quant.jqengine.datasource import mootdx_breaker as mbr
from app.quant.jqengine.datasource import mootdx_src as msrc


@pytest.fixture(autouse=True)
def _clean_breaker(monkeypatch):
    monkeypatch.setenv("MOOTDX_BREAKER_THRESHOLD", "3")
    monkeypatch.setenv("MOOTDX_BREAKER_COOLDOWN", "600")
    monkeypatch.delenv("MOOTDX_BREAKER_DISABLED", raising=False)
    mbr._reset_for_tests()
    yield
    mbr._reset_for_tests()


def _open_breaker(n=3):
    for _ in range(n):
        mbr.kline_record_fail()


def test_closed_allows_and_ok_resets():
    assert mbr.kline_allowed()
    mbr.kline_record_fail()
    mbr.kline_record_fail()
    assert mbr.breaker_state() == {"open": False, "fail_streak": 2}
    mbr.kline_record_ok()
    assert mbr.breaker_state() == {"open": False, "fail_streak": 0}


def test_threshold_opens_and_fast_fails():
    _open_breaker(3)
    assert mbr.breaker_state()["open"] is True
    assert not mbr.kline_allowed()


def test_half_open_single_probe_and_recovery(monkeypatch):
    monkeypatch.setenv("MOOTDX_BREAKER_COOLDOWN", "1")
    _open_breaker(3)
    assert not mbr.kline_allowed()
    time.sleep(1.05)
    assert mbr.kline_allowed()  # 半开探测放行
    assert not mbr.kline_allowed()  # 探测在途，其余拒绝
    mbr.kline_record_ok()
    assert mbr.breaker_state() == {"open": False, "fail_streak": 0}
    assert mbr.kline_allowed()


def test_half_open_probe_fail_reopens(monkeypatch):
    monkeypatch.setenv("MOOTDX_BREAKER_COOLDOWN", "1")
    _open_breaker(3)
    time.sleep(1.05)
    assert mbr.kline_allowed()  # 探测
    mbr.kline_record_fail()  # 探测失败 → 续开
    assert mbr.breaker_state()["open"] is True
    assert not mbr.kline_allowed()


def test_disabled_always_allows(monkeypatch):
    monkeypatch.setenv("MOOTDX_BREAKER_DISABLED", "1")
    _open_breaker(10)
    assert mbr.kline_allowed()
    assert mbr.breaker_state()["open"] is False


class _EmptyClient:
    """bars 恒返回空的伪客户端；rotate 计数。"""

    def __init__(self):
        self.calls = 0

    def bars(self, **kwargs):
        self.calls += 1
        return pd.DataFrame()


def test_retry_aborts_rotation_on_global_empty(monkeypatch):
    src = msrc.MootdxSource()
    client = _EmptyClient()
    monkeypatch.setattr(src, "_api", lambda: client)
    monkeypatch.setattr(src, "_rotate_server", lambda *a, **k: None)
    df, err = src._with_server_retry(lambda c: c.bars(symbol="600000"))
    assert df is None
    assert "超时/无数据" in err
    # 早停：5 次连续空即结束，而非烧满 17 台
    assert client.calls == msrc._RETRY_ABORT_EMPTY


def test_retry_entry_gate_when_open_no_network():
    _open_breaker(3)
    src = msrc.MootdxSource()
    called = []

    def _fn(c):
        called.append(True)
        return pd.DataFrame({"close": [1.0]})

    df, err = src._with_server_retry(_fn)
    assert df is None
    assert mbr.BREAKER_OPEN_MSG in err
    assert called == []


def test_retry_success_resets_streak(monkeypatch):
    src = msrc.MootdxSource()
    monkeypatch.setattr(
        src, "_api", lambda: _EmptyClient())  # 仅占位，fn 不经 client
    monkeypatch.setattr(src, "_rotate_server", lambda *a, **k: None)
    mbr.kline_record_fail()
    df, err = src._with_server_retry(
        lambda c: pd.DataFrame({"close": [1.0]}))
    assert err is None and df is not None
    assert mbr.breaker_state() == {"open": False, "fail_streak": 0}


def test_concurrent_access_no_deadlock():
    """并发 hammer 熔断器不死锁（daemon 线程 + join 超时断言，见 AGENTS.md 纪律）。"""
    errors = []

    def _worker():
        try:
            for _ in range(200):
                mbr.kline_allowed()
                mbr.kline_record_fail()
                mbr.kline_record_ok()
                mbr.breaker_state()
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=_worker, daemon=True) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
        assert not t.is_alive(), "breaker 并发死锁"
    assert errors == []


def test_backfill_pool_skips_mootdx_when_open():
    from app.services.stockdata.backfill_pool import BackfillPool

    from app.quant.jqengine.datasource.mootdx_src import MootdxSource
    _open_breaker(3)
    for factory in (None, MootdxSource):
        pool = BackfillPool(workers=2, source_factory=factory)
        called = []
        res = pool.map(lambda src, s: called.append(s), ["a", "b"])
        assert called == [], "开路期不应触网"
        assert res["ok_count"] == 0
        assert set(res["failed"]) == {"a", "b"}
        assert all(mbr.BREAKER_OPEN_MSG in v for v in res["failed"].values())


def test_backfill_pool_fake_factory_not_gated():
    from app.services.stockdata.backfill_pool import BackfillPool

    pool = BackfillPool(workers=2, source_factory=lambda: object())
    res = pool.map(lambda src, s: {"sym": s}, ["a", "b"])
    assert res["ok_count"] == 2
    assert res["failed"] == {}
