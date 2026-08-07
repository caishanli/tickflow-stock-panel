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
