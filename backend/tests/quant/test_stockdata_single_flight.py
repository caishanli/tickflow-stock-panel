import threading
import time

from app.services.stockdata.single_flight import DedupCache, SingleFlight, TTLCache


def test_single_flight_only_fetches_once():
    sf = SingleFlight()
    calls = []
    entered = threading.Event()
    release = threading.Event()

    def loader():
        calls.append(1)
        entered.set()  # 通知 leader 已进入 loader
        release.wait(5)  # 等所有并发请求都到达 single-flight 再返回
        return 42

    results = []
    def worker():
        results.append(sf.run("k", loader))

    ts = [threading.Thread(target=worker) for _ in range(5)]
    for t in ts: t.start()
    assert entered.wait(5)  # leader 已到达 loader
    time.sleep(0.05)  # 留时间让其余 4 个并发请求到达 single-flight 的等待点
    release.set()
    for t in ts: t.join()
    assert results == [42] * 5
    assert len(calls) == 1  # 只回源一次


def test_single_flight_releases_on_error():
    sf = SingleFlight()
    def bad():
        raise RuntimeError("boom")
    for _ in range(2):
        try:
            sf.run("k", bad)
            assert False
        except RuntimeError:
            pass


def test_ttl_cache_hit():
    c = DedupCache()
    calls = []
    for _ in range(3):
        c.get_or_fetch("k", ttl=10, loader=lambda: calls.append(1) or "v")
    assert len(calls) == 1
    assert c.get_or_fetch("k", ttl=10, loader=lambda: "x") == "v"


def test_ttl_cache_expires():
    c = DedupCache()
    calls = []
    def loader():
        calls.append(1)
        return "v"
    c.get_or_fetch("k", ttl=0.1, loader=loader)
    time.sleep(0.15)
    c.get_or_fetch("k", ttl=0.1, loader=loader)
    assert len(calls) == 2


def test_ttl_cache_purge_expired_and_set_returns_value():
    """purge_expired 主动清理过期键(无流量时由后台清扫调用); set 恒返回 value。"""
    c = DedupCache()
    assert c.get_or_fetch("big", ttl=0.1, loader=lambda: "frame") == "frame"
    assert c.get_or_fetch("live", ttl=60, loader=lambda: "keep") == "keep"
    time.sleep(0.15)
    assert c.purge_expired() == 1  # 过期大帧被清, 无人访问也回收
    assert c.get_or_fetch("live", ttl=60, loader=lambda: "other") == "keep"
    # set 返回值链路: get_or_fetch 必须拿到 loader 的结果而非 None
    assert c.get_or_fetch("k2", ttl=60, loader=lambda: {"v": 1}) == {"v": 1}


def test_ttl_cache_set_triggered_purge_does_not_deadlock():
    """回归 e583b73：set() 持有锁时摊销触发 purge_expired()，后者再次 `with
    self._lock`。普通 threading.Lock 不可同线程重入 → 第 32 次 set 自死锁、
    永久卡死该缓存并连带全部 get_minute 请求；锁必须可重入(RLock)。"""
    c = TTLCache()
    done = {}

    def hammer():
        for i in range(64):  # 超过 32，摊销清理触发两次
            c.set(f"k{i}", i, ttl=60)
        done["ok"] = True

    t = threading.Thread(target=hammer)
    t.start()
    t.join(timeout=5)
    assert not t.is_alive(), "set 触发 purge 自死锁（锁须可重入）"
    assert done.get("ok")
    # 锁未卡死：其它线程仍能正常访问
    assert c.get("k0") == 0
