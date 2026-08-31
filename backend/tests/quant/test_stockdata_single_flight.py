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

    t = threading.Thread(target=hammer, daemon=True)
    t.start()
    t.join(timeout=5)
    assert not t.is_alive(), "set 触发 purge 自死锁（锁须可重入）"
    assert done.get("ok")
    # 锁未卡死：其它线程仍能正常访问
    assert c.get("k0") == 0


def test_dedup_cache_concurrent_stress_past_purge_threshold():
    """回归 e583b73：多线程并发多键 get_or_fetch，跨过摊销清理阈值（32 次 set），
    且 leader 在 set→purge 死锁时不得连带卡死所有 follower（生产 13:10 事故场景）。"""
    c = DedupCache()
    n = 40  # > 32，触发 purge_expired
    results = [None] * n

    def worker(i):
        try:
            results[i] = c.get_or_fetch(f"key{i}", ttl=60, loader=lambda k=i: k)
        except Exception as e:
            results[i] = e

    ts = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=5)
    assert all(not t.is_alive() for t in ts), "并发 get_or_fetch 触发 purge 死锁"
    assert [r for r in results if isinstance(r, int)] == list(range(n))


def test_stockdata_locks_no_reentrant_deadlock_pattern():
    """结构不变量：stockdata 各锁类不得在持有锁时调用本类另一个取同一把锁的方法
    （普通 threading.Lock 不可重入 → 自死锁）。若确有这种嵌套，锁必须用 RLock。

    回归 e583b73：TTLCache.set 持锁调 purge_expired 再取锁。此用例用 AST 静态
    扫描，防止将来有人把"锁内逻辑抽成加锁 helper"再次引入同类死锁。"""
    import ast
    import pathlib

    src_root = pathlib.Path(__file__).resolve().parents[2] / "app" / "services" / "stockdata"
    modules = ["single_flight.py", "sources.py", "rt_sources.py"]
    violations = []

    def _locked_methods(cls) -> set[str]:
        """方法体内含 `with self.<x>lock:` / `with self.<x>lock():` 视为加锁方法。"""
        out = set()
        for m in cls.body:
            if not isinstance(m, ast.FunctionDef):
                continue
            for sub in ast.walk(m):
                if not isinstance(sub, ast.With):
                    continue
                for item in sub.items:
                    ctx = item.context_expr
                    attr = None
                    if isinstance(ctx, ast.Attribute) and isinstance(ctx.value, ast.Name) \
                            and ctx.value.id == "self":
                        attr = ctx.attr
                    elif isinstance(ctx, ast.Call) and isinstance(ctx.func, ast.Attribute) \
                            and isinstance(ctx.func.value, ast.Name) \
                            and ctx.func.value.id == "self":
                        attr = ctx.func.attr
                    if attr and attr.endswith("lock"):
                        out.add(m.name)
        return out

    def _lock_is_rlock(cls) -> bool:
        for m in cls.body:
            if not isinstance(m, ast.FunctionDef) or m.name != "__init__":
                continue
            for sub in ast.walk(m):
                if not isinstance(sub, ast.Assign):
                    continue
                for tgt in sub.targets:
                    if isinstance(tgt, ast.Attribute) and tgt.attr.endswith("lock"):
                        val = sub.value
                        if isinstance(val, ast.Call) and isinstance(val.func, ast.Attribute):
                            if val.func.attr == "RLock":
                                return True
                            if val.func.attr == "Lock":
                                return False
        return False

    for fname in modules:
        tree = ast.parse((src_root / fname).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            locked = _locked_methods(node)
            if not locked:
                continue
            for m in node.body:
                if not isinstance(m, ast.FunctionDef) or m.name not in locked:
                    continue
                for sub in ast.walk(m):
                    if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                            and isinstance(sub.func.value, ast.Name)
                            and sub.func.value.id == "self" and sub.func.attr in locked
                            and sub.func.attr != m.name and not _lock_is_rlock(node)):
                        violations.append(
                            f"{fname}.{node.name}.{m.name} -> self.{sub.func.attr}（锁非 RLock）")
    assert not violations, f"锁内调用本类加锁方法须用 RLock: {violations}"
