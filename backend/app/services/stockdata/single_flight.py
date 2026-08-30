"""标的级并发去重：同一键的并发回源只执行一次；短 TTL 缓存共享结果。

去重粒度是**键**（调用方把键做成 per-symbol，如 ``rt:{code}``），
两个交叉批量请求在重叠 symbol 上自然共享 in-flight/缓存结果。
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any


class _Flight:
    __slots__ = ("ev", "started", "result", "error")

    def __init__(self) -> None:
        self.ev = threading.Event()
        self.started = False
        self.result: Any = None
        self.error: BaseException | None = None


class SingleFlight:
    """同键并发请求只执行一次 loader，其余请求等待共享结果。失败即抛，允许下次重试。"""

    def __init__(self) -> None:
        self._inflight: dict[str, _Flight] = {}
        self._lock = threading.Lock()

    def run(self, key: str, loader: Callable[[], Any]) -> Any:
        with self._lock:
            flight = self._inflight.get(key)
            if flight is None:
                flight = _Flight()
                self._inflight[key] = flight
            if not flight.started:
                flight.started = True  # 原子选举 leader：并发只有一个真 leader
                is_leader = True
            else:
                is_leader = False
        if is_leader:
            try:
                flight.result = loader()
            except BaseException as e:
                flight.error = e
            finally:
                with self._lock:
                    if self._inflight.get(key) is flight:
                        del self._inflight[key]
                flight.ev.set()
            if flight.error is not None:
                raise flight.error
            return flight.result
        flight.ev.wait()
        if flight.error is not None:
            raise flight.error
        return flight.result


class TTLCache:
    """线程安全短 TTL 内存缓存。"""

    def __init__(self) -> None:
        self._items: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._sets_since_purge = 0

    def get(self, key: str) -> Any | None:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                return None
            exp, val = item
            if time.monotonic() > exp:
                del self._items[key]
                return None
            return val

    def set(self, key: str, value: Any, ttl: float) -> Any:
        with self._lock:
            self._items[key] = (time.monotonic() + ttl, value)
            # 过期条目原先只在同 key 再次 get 时才删除；key 空间随天数/请求
            # 组合增长（如 min:{codes}:{lo}:{hi} 每日每池一键、值含全池分钟
            # 窗口帧）时，无人再访问的键连同大帧一起永久驻留 → 服务 RSS 随
            # 运行时长膨胀到数 GB。每 32 次 set 摊销清理一次过期键。
            self._sets_since_purge += 1
            if self._sets_since_purge >= 32:
                self._sets_since_purge = 0
                now = time.monotonic()
                expired = [k for k, (exp, _v) in self._items.items() if now > exp]
                for k in expired:
                    del self._items[k]
        return value


class DedupCache:
    """TTL 命中直接返回；未命中 single-flight 拉取（并发只拉一次）。"""

    def __init__(self) -> None:
        self._single = SingleFlight()
        self._cache = TTLCache()

    def get_or_fetch(self, key: str, ttl: float, loader: Callable[[], Any]) -> Any:
        hit = self._cache.get(key)
        if hit is not None:
            return hit
        # 双检：并发时 single-flight 保证 loader 只执行一次
        return self._single.run(
            key, lambda: self._cache.set(key, loader(), ttl))
