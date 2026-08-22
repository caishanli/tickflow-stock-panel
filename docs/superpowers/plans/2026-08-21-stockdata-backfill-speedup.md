# stockdata 回源提速 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** stockdata 回源全面提速——盘后当日数据 ≤30min 落盘、重启断点续传不重拉。

**Architecture:** 三组件：`mootdx_src.py` 按需分页+短页自愈+延迟感知选服；新 `backfill_pool.py` 并发池（每线程独立 MootdxSource）；`mootdx_service.py` sync_* 接池 + manifest 断点续传。实时路径 NetworkPuller 不动。Spec: `docs/superpowers/specs/2026-08-21-stockdata-backfill-speedup-design.md`

**Tech Stack:** Python 3.11 / pytdx / pandas / polars / pytest（asyncio_mode=auto）

## Global Constraints

- 测试从 `backend/` 运行：`uv run --extra dev pytest tests/... -q`
- lint: `uv run --extra dev ruff check app`（line-length 100）；类型: `uv run --extra dev mypy app`
- 不引入 pandas 之外的新依赖；不改实时路径（sources.NetworkPuller/handlers）
- 删除 `_throttle_backfill` 及 `_BACKFILL_THROTTLE_EVERY/SLEEP`、`_BACKFILL_INTRADAY_EVERY/SLEEP`
- 北交所 (.BJ) 标的继续跳过；盘中不写当日半程数据的既有约定保持

---

### Task 1: mootdx_src — get_minute 按需分页（since 参数）

**Files:**
- Modify: `backend/app/quant/jqengine/datasource/mootdx_src.py`（get_minute，约 L321-371）
- Test: `backend/tests/quant/test_mootdx_paging.py`（新建）

**Interfaces:**
- Produces: `get_minute(code, date="", max_bars=30000, since: datetime.date | None = None)`——since 给定时只拉覆盖 `[since, today]` 的页；`_weekday_days(since, today) -> int`（含两端的工作日数）

- [ ] **Step 1: 写失败测试**

```python
"""get_minute 按需分页（since）测试。"""
from __future__ import annotations

import datetime as _dt
import sys

import pandas as pd

from app.quant.jqengine.datasource.mootdx_src import MootdxSource, _weekday_days


def _bars_page(start_ts: _dt.datetime, n: int) -> pd.DataFrame:
    idx = pd.DatetimeIndex([start_ts - _dt.timedelta(minutes=i) for i in range(n)])
    return pd.DataFrame({"close": [1.0] * n}, index=idx)


class _FakeClient:
    """start=N 返回从最新往回第 N*800 根起的 800 根（模拟 pytdx 分页语义）。"""

    def __init__(self, total: int):
        self.total = total
        self.calls: list[int] = []

    def bars(self, symbol, frequency, start=0, offset=800):
        self.calls.append(start)
        newest = _dt.datetime(2026, 8, 21, 15, 0)
        first = newest - _dt.timedelta(minutes=max(0, start))
        n = min(offset, max(0, self.total - start))
        if n == 0:
            return None
        return _bars_page(first, n)


def test_weekday_days_counts_both_ends():
    mon = _dt.date(2026, 8, 17)
    fri = _dt.date(2026, 8, 21)
    assert _weekday_days(mon, fri) == 5


def test_get_minute_since_limits_pages():
    """since=3 个交易日前 → 预计 2 页即停，不再拉满全历史。"""
    src = MootdxSource()
    client = _FakeClient(total=240 * 30)  # 30 个交易日全量
    src._client = client
    df = src.get_minute("600519", since=_dt.date(2026, 8, 19))
    assert not df.empty
    # 3 个交易日 ×240 bar ÷800/页 ≈1 → 估算上限小；且最老 bar 已 ≤since 提前停
    assert len(client.calls) <= 3
    assert df.index.min() <= pd.Timestamp("2026-08-19 09:31:00")


def test_get_minute_no_since_pulls_all():
    src = MootdxSource()
    client = _FakeClient(total=1000)  # 2 页拉完
    src._client = client
    src.get_minute("600519")
    assert len(client.calls) == 2  # 第二页 <800 根触发停止
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_paging.py -q`
Expected: FAIL（`_weekday_days` 不存在 / get_minute 无 since 参数）

- [ ] **Step 3: 实现**

```python
# mootdx_src.py 模块级新增（_TDX_FETCH_GUARD_TIMEOUT 之后）
import datetime as _dt_mod


def _weekday_days(since, today):
    """[since, today] 两端含的工作日数（分页初始估算用；日历误差由循环终止条件兜底）。"""
    if today < since:
        return 0
    days = 0
    d = since
    while d <= today:
        if d.weekday() < 5:
            days += 1
        d += _dt_mod.timedelta(days=1)
    return days
```

`get_minute` 改为（保留原列名归一化尾部不动）：

```python
    def get_minute(self, code, date="", max_bars=30000, since=None):
        """历史 1 分钟 K 线分页拉取。

        ``since`` 给定时只回看到覆盖 [since, today]：按工作日×240÷800 估算
        初始页数上限，并在累计帧最老 bar ≤ since 时提前停止（节假日/停牌
        导致估算偏少时由该精确条件兜底）。None = 全量（首次初始化用）。
        """
        sym = _to_symbol(code)
        box = {}
        def _fn(c):
            first = c.bars(symbol=sym, frequency=8, start=0, offset=800)
            if first is None or first.empty:
                raise DataSourceError("mootdx 无分钟数据")
            box["c"] = c
            return first
        first, err = self._with_server_retry(_fn)
        if first is None:
            raise DataSourceError(f"mootdx 无分钟数据 ({err})")
        c = box.get("c")
        frames = [first]
        fetched = len(first)
        oldest_seen = first.index.min()
        start = 800
        offset = 800
        if since is not None:
            est_pages = (_weekday_days(since, _dt_mod.date.today()) * 240) // 800 + 2
            max_pages = min(int(max_bars // offset), est_pages)
        else:
            max_pages = int(max_bars // offset)
        for _ in range(min(399, max_pages)):
            if fetched >= max_bars:
                break
            if since is not None and pd.Timestamp(oldest_seen).date() <= since:
                break
            try:
                df = c.bars(symbol=sym, frequency=8, start=start, offset=offset)
            except Exception:
                break
            if df is None or df.empty:
                break
            frames.append(df)
            fetched += len(df)
            oldest_seen = min(oldest_seen, df.index.min())
            if len(df) < offset:
                break
            start += offset
        # ……以下与原实现一致（concat/去重/列名归一化），不变
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_paging.py -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add app/quant/jqengine/datasource/mootdx_src.py tests/quant/test_mootdx_paging.py
git commit -m "feat(mootdx): get_minute 支持 since 按需分页"
```

---

### Task 2: mootdx_src — 分页短页自愈

**Files:**
- Modify: `backend/app/quant/jqengine/datasource/mootdx_src.py`（get_minute 分页循环）
- Test: `backend/tests/quant/test_mootdx_paging.py`（追加）

**Interfaces:**
- Consumes: Task 1 的分页循环结构
- Produces: 短页探测语义——`0<len(df)<offset` 时补发一页 `start+=len(df)` 验证；有数据继续拉，连续空才停（防限速截断误判历史尽头）

- [ ] **Step 1: 写失败测试**

```python
class _TruncatingClient:
    """第一次请求某 offset 时返回短页（模拟限速截断），重试同位置返回满页。"""

    def __init__(self, total: int):
        self.total = total
        self.truncated_once: set[int] = set()
        self.calls: list[int] = []

    def bars(self, symbol, frequency, start=0, offset=800):
        self.calls.append(start)
        n = min(offset, max(0, self.total - start))
        if n == 0:
            return None
        newest = _dt.datetime(2026, 8, 21, 15, 0)
        first = newest - _dt.timedelta(minutes=start + n - 1)
        if start in self.truncated_once:
            return _bars_page(first, n)
        self.truncated_once.add(start)
        return _bars_page(first, max(1, n // 2))  # 截断：半页


def test_short_page_heals_and_continues():
    """限速截断的短页不应被当作历史尽头——补发验证后继续拉满。"""
    src = MootdxSource()
    client = _TruncatingClient(total=1600)  # 正常应为 2 满页
    src._client = client
    df = src.get_minute("600519")
    assert len(df) >= 1600  # 若旧逻辑遇短页即 break 只会拿到 ~400+800
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_paging.py::test_short_page_heals_and_continues -q`
Expected: FAIL（旧逻辑 break 后只拉到部分数据）

- [ ] **Step 3: 实现（替换 Task 1 循环中的短页处理）**

```python
            if df is None or df.empty:
                break
            if len(df) < offset:
                # 短页二义性：历史尽头 OR 限速截断。补发一页验证：
                probe = None
                try:
                    probe = c.bars(symbol=sym, frequency=8,
                                   start=start + len(df), offset=offset)
                except Exception:
                    probe = None
                if probe is not None and not probe.empty:
                    frames.append(df)
                    frames.append(probe)
                    fetched += len(df) + len(probe)
                    oldest_seen = min(oldest_seen, probe.index.min())
                    start += len(df) + len(probe)
                    continue
                frames.append(df)
                fetched += len(df)
                break  # 真·历史尽头
            frames.append(df)
            fetched += len(df)
            oldest_seen = min(oldest_seen, df.index.min())
            start += offset
```

- [ ] **Step 4: 跑全部 paging 测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_paging.py -q`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add app/quant/jqengine/datasource/mootdx_src.py tests/quant/test_mootdx_paging.py
git commit -m "fix(mootdx): 分页短页自愈——补发验证区分限速截断与历史尽头"
```

---

### Task 3: mootdx_src — rank_servers 延迟排名 + 选服接入

**Files:**
- Modify: `backend/app/quant/jqengine/datasource/mootdx_src.py`
- Test: `backend/tests/quant/test_mootdx_ranking.py`（新建）

**Interfaces:**
- Consumes: 现有 `probe_servers()`、`_probe()`、`_TDX_SERVERS`
- Produces: `rank_servers(force=False) -> list[tuple[str, int]]`（最优在前，模块级 TTL 30min 缓存；空响应节点沉底）；`MootdxSource(server=(ip,port))` 可选固定服务器参数；`_make_client`/`_rotate_server` 按 ranking 顺序选取

- [ ] **Step 1: 写失败测试**

```python
"""rank_servers 延迟排名测试。"""
from __future__ import annotations

from app.quant.jqengine.datasource import mootdx_src as msrc


def test_rank_servers_orders_by_latency(monkeypatch):
    monkeypatch.setattr(msrc, "_TDX_SERVERS", [("a", 1), ("b", 2), ("c", 3)])
    monkeypatch.setattr(msrc, "probe_servers",
                        lambda timeout=1.5: [
                            {"ip": "a", "port": 1, "ok": True, "latency_ms": 50},
                            {"ip": "b", "port": 2, "ok": True, "latency_ms": 10},
                            {"ip": "c", "port": 3, "ok": False, "latency_ms": None}])
    lat = {"a": 500.0, "b": 60.0}  # 实测请求延迟：a 慢 b 快

    def fake_req(ip, port):
        import time
        t0 = time.perf_counter()
        rows = 0 if ip == "a" else 10  # a 还返回空 → 双重降权
        return rows

    monkeypatch.setattr(msrc, "_measure_server", lambda ip, port, timeout=5.0: (
        lat.get(ip, 9e9), fake_req(ip, port)))
    msrc._RANK_CACHE["ts"] = 0.0
    msrc._RANK_CACHE["data"] = None
    ranked = msrc.rank_servers()
    assert ranked[0] == ("b", 2)
    assert ("c", 3) not in ranked      # TCP 不可达剔除
    assert ranked[-1] == ("a", 1)      # 空+慢沉底


def test_rank_servers_cache_ttl(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(msrc, "_TDX_SERVERS", [("a", 1)])
    monkeypatch.setattr(msrc, "probe_servers",
                        lambda timeout=1.5: [{"ip": "a", "port": 1, "ok": True,
                                              "latency_ms": 5}])
    monkeypatch.setattr(msrc, "_measure_server", lambda ip, port, timeout=5.0: (5.0, 10))

    def counting(*a, **k):
        calls["n"] += 1
        return msrc._rank_servers_uncached()

    monkeypatch.setattr(msrc, "_rank_servers_uncached", counting)
    msrc._RANK_CACHE["ts"] = 0.0
    msrc._RANK_CACHE["data"] = None
    msrc.rank_servers()
    msrc.rank_servers()  # TTL 内第二次走缓存
    assert calls["n"] == 1
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_ranking.py -q`
Expected: FAIL（`rank_servers`/`_measure_server`/`_RANK_CACHE` 不存在）

- [ ] **Step 3: 实现**

```python
# mootdx_src.py 模块级新增
_RANK_TTL = 1800.0
_RANK_CACHE: dict = {"ts": 0.0, "data": None}


def _measure_server(ip, port, timeout=5.0):
    """对服务器发一次真实日线请求：(延迟 ms, 行数)。失败返回 (inf, 0)。"""
    import time as _t
    from pytdx.hq import TdxHq_API
    api = TdxHq_API()
    try:
        t0 = _t.perf_counter()
        if not api.connect(ip, port, time_out=timeout):
            return float("inf"), 0
        df = api.get_security_bars(9, 1, "600519", 0, 10)
        dt_ms = (_t.perf_counter() - t0) * 1000
        rows = 0 if df is None else len(df)
        return dt_ms, rows
    except Exception:
        return float("inf"), 0
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


def _rank_servers_uncached():
    probes = {p["ip"]: p for p in probe_servers()}
    scored = []
    for ip, port in _TDX_SERVERS:
        p = probes.get(ip)
        if not p or not p.get("ok"):
            continue
        lat_ms, rows = _measure_server(ip, port)
        if rows == 0:
            lat_ms += 10_000.0  # 空响应节点沉底（不硬黑名单：空可能瞬时）
        scored.append((lat_ms, ip, port))
    scored.sort()
    return [(ip, port) for _, ip, port in scored]


def rank_servers(force: bool = False) -> list[tuple[str, int]]:
    """按实测请求延迟的服务器排名（进程级缓存 TTL 30min）。"""
    import time as _t
    now = _t.monotonic()
    if not force and _RANK_CACHE["data"] and now - _RANK_CACHE["ts"] < _RANK_TTL:
        return list(_RANK_CACHE["data"])
    data = _rank_servers_uncached()
    if data:
        _RANK_CACHE["ts"] = now
        _RANK_CACHE["data"] = list(data)
    return list(data)
```

`MootdxSource.__init__` 加可选参数并改选服顺序：

```python
    def __init__(self, token="", server=None):
        self._client = None
        self._server_idx = -1
        self._pinned_server = server   # 显式指定则不参与自动排名
        self._xdxr_cache = {}
        self._page_latencies: list[float] = []
        self._empty_streak = 0
```

`_make_client(server=None)`：server 为 None 时改为遍历 `rank_servers()`（而非裸列表）取第一个可建连者：

```python
        servers = [server] if server else (
            [tuple(x) for x in rank_servers()] or list(_TDX_SERVERS))
        for ip, port in servers:
            # 原 probe/factory/_patch 逻辑不变，只是候选源换成 ranked 列表
```

`_rotate_server`：轮换序列改为 ranking 顺序（idx 对 ranked 列表递增，用尽回绕 -1，其余逻辑保持）。

**运行时切换**（健康统计 + 降级切换）：

```python
    # MootdxSource 方法新增
    _PAGE_LAT_WINDOW = 8          # 滚动窗口页数
    _PAGE_SLOW_MS = 800.0         # 健康值 ~55ms 的 ~15 倍即判劣化
    _EMPTY_STREAK_LIMIT = 3       # 连续空响应次数阈值

    def note_page(self, latency_ms: float, empty: bool) -> None:
        """分页循环每页调用：记录延迟与空响应连击。"""
        self._page_latencies.append(latency_ms)
        self._page_latencies = self._page_latencies[-self._PAGE_LAT_WINDOW:]
        self._empty_streak = self._empty_streak + 1 if empty else 0

    def unhealthy(self) -> bool:
        """最近 8 页均速 >800ms 或连续 ≥3 次空响应 → 应切换服务器。"""
        if self._empty_streak >= self._EMPTY_STREAK_LIMIT:
            return True
        if len(self._page_latencies) >= self._PAGE_LAT_WINDOW:
            avg = sum(self._page_latencies) / len(self._page_latencies)
            return avg > self._PAGE_SLOW_MS
        return False

    def rotate_if_unhealthy(self) -> bool:
        """不健康则按排名重建连接；返回是否发生了切换。"""
        if not self.unhealthy():
            return False
        logger.warning("mootdx 服务器劣化(均速%.0fms/空%d)，切换",
                       sum(self._page_latencies) / max(1, len(self._page_latencies)),
                       self._empty_streak)
        self._page_latencies = []
        self._empty_streak = 0
        try:
            self._rotate_server()
        except Exception:  # noqa: BLE001
            self._client = None
        return True
```

get_minute 分页循环内每页计时并检查（Task 2 循环基础上插入）：

```python
            t0 = time.perf_counter()
            df = c.bars(symbol=sym, frequency=8, start=start, offset=offset)
            src_self.note_page((time.perf_counter() - t0) * 1000,
                               df is None or df.empty)
            if src_self.rotate_if_unhealthy():
                c = src_self._api()   # 已切新服务器，继续同一 start 分页
```

（get_minute 内通过 `box` 持有的 client 在切换后需刷新——实现时把 `c` 的获取收进循环体或以 `self` 直访 `_api()`；测试用 FakeClient 注入 `src._client` 时 rotate 需被 monkeypatch 掉。）

- [ ] **Step 4: 跑测试 + 既有 socket timeout 测试回归**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_ranking.py tests/quant/test_mootdx_socket_timeout.py tests/test_mootdx_servers.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/quant/jqengine/datasource/mootdx_src.py tests/quant/test_mootdx_ranking.py
git commit -m "feat(mootdx): 实测延迟服务器排名 + 选服接入（空响应节点沉底）"
```

---

### Task 4: backfill_pool.py — 并发池 + 盘中减半

**Files:**
- Create: `backend/app/services/stockdata/backfill_pool.py`
- Test: `backend/tests/quant/test_backfill_pool.py`（新建）

**Interfaces:**
- Consumes: `MootdxSource(server=...)`（Task 3）、`mootdx_service._is_market_open`
- Produces: `BackfillPool(workers=None).map(fn, symbols, batch_size=100, on_batch_done=None) -> dict`；`fn(src: MootdxSource, symbol: str) -> object | None`；返回 `{"ok": [...], "failed": {sym: reason}}`；`on_batch_done(results: list)` 在主线程按批回调

- [ ] **Step 1: 写失败测试**

```python
"""BackfillPool 并发/批回调/盘中减半测试。"""
from __future__ import annotations

import threading
import time

import pytest

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
```

注意：BackfillPool 构造时不得真的创建 MootdxSource（测试环境无网络）——source 工厂需可注入：

```python
    def __init__(self, workers=None, source_factory=None):
        ...
        self._factory = source_factory or (lambda: MootdxSource())
```

测试里传 `source_factory=lambda: object()` 或默认即可（上面 fn 不使用 src，默认工厂在 map 时才调用——但无网络环境 MootdxSource() 构造本身不发网络请求，安全）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_backfill_pool.py -q`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现**

```python
# backend/app/services/stockdata/backfill_pool.py
"""回源并发池：N worker 线程、线程独立 MootdxSource、主线程批量 flush。

与实时路径 NetworkPuller 物理隔离（独立连接/独立限速策略）；场景天然错开：
回源任务盘后 ~20 分钟内完成，盘中只剩罕见的历史缺口小任务。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("app.services.stockdata.backfill_pool")

BACKFILL_WORKERS_DEFAULT = 6
_INTRADAY_HALVE_THRESHOLD = 500


def _is_market_open(now=None) -> bool:
    now = now or _dt.datetime.now()
    t = now.time()
    return (now.weekday() < 5
            and (_dt.time(9, 30) <= t <= _dt.time(11, 30)
                 or _dt.time(13, 0) <= t <= _dt.time(15, 0)))


class BackfillPool:
    def __init__(self, workers: int | None = None, source_factory=None):
        if workers is None:
            try:
                workers = int(os.getenv("BACKFILL_WORKERS", "") or BACKFILL_WORKERS_DEFAULT)
            except ValueError:
                workers = BACKFILL_WORKERS_DEFAULT
        self._configured_workers = max(1, workers)
        self._factory = source_factory
        self._local = threading.local()

    def effective_workers(self, task_size: int) -> int:
        """交易时段的大任务减半（轻量保险；正常盘后任务不受影响）。"""
        w = self._configured_workers
        if _is_market_open() and task_size > _INTRADAY_HALVE_THRESHOLD:
            w = max(1, w // 2)
        return w

    def _source(self):
        src = getattr(self._local, "src", None)
        if src is None:
            if self._factory is not None:
                src = self._factory()
            else:
                from app.quant.jqengine.datasource.mootdx_src import MootdxSource
                src = MootdxSource()
            self._local.src = src
        return src

    def _reset_source(self):
        self._local.src = None

    def map(self, fn, symbols, batch_size=100, on_batch_done=None) -> dict:
        """逐 symbol 执行 fn(src, symbol)；批满主线程回调 on_batch_done。

        失败语义：单只异常记 failed 不阻断；TimeoutError/连续异常时重建该
        worker 的 source（坏 socket 不残留）。返回 {"ok":[...], "failed":{}}。
        """
        symbols = list(symbols)
        workers = self.effective_workers(len(symbols))
        results: list = []
        failed: dict[str, str] = {}
        batch: list = []
        lock = threading.Lock()
        progress = {"done": 0}

        def _one(sym):
            try:
                out = fn(self._source(), sym)
                err = None
            except Exception as e:  # noqa: BLE001
                out, err = None, e
                self._reset_source()
            return sym, out, err

        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix="backfill") as ex:
            futures = [ex.submit(_one, s) for s in symbols]
            for fut in futures:
                sym, out, err = fut.result()
                if err is not None:
                    failed[sym] = str(err)[:120]
                    continue
                if out is not None:
                    results.append(out)
                    batch.append(out)
                progress["done"] += 1
                if on_batch_done is not None and len(batch) >= batch_size:
                    on_batch_done(batch)
                    batch = []
        if on_batch_done is not None and batch:
            on_batch_done(batch)
        logger.info("backfill pool done: ok=%d failed=%d workers=%d",
                    len(results), len(failed), workers)
        return {"ok": results, "failed": failed}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_backfill_pool.py -q`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add app/services/stockdata/backfill_pool.py tests/quant/test_backfill_pool.py
git commit -m "feat(stockdata): BackfillPool 并发回源池（盘中大任务减半保险）"
```

---

### Task 5: mootdx_service — manifest 断点续传

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（模块常量区 + 新函数）
- Test: `backend/tests/quant/test_backfill_manifest.py`（新建）

**Interfaces:**
- Produces: `MANIFEST_PATH = DATA_ROOT/"backfill_state.json"`；`_manifest_load() -> dict`；`_manifest_reset(dataset: str, targets: list[str], mode: str) -> None`；`_manifest_mark_done(dataset: str, symbols: list[str]) -> None`；`_manifest_done(dataset: str) -> set[str]`

- [ ] **Step 1: 写失败测试**

```python
"""backfill manifest 断点续传测试。"""
from __future__ import annotations

import json

from app.services import mootdx_service as ms


def test_manifest_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "MANIFEST_PATH", tmp_path / "m.json")
    ms._manifest_reset("stock_minute", ["a", "b", "c"], mode="full")
    ms._manifest_mark_done("stock_minute", ["a", "b"])
    assert ms._manifest_done("stock_minute") == {"a", "b"}
    raw = json.loads((tmp_path / "m.json").read_text())
    assert raw["stock_minute"]["mode"] == "full"


def test_manifest_reset_clears_stale_done(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "MANIFEST_PATH", tmp_path / "m.json")
    ms._manifest_reset("stock_minute", ["x"], mode="recent")
    ms._manifest_mark_done("stock_minute", ["x"])
    ms._manifest_reset("stock_minute", ["y", "z"], mode="full")  # 新一轮清空 done
    assert ms._manifest_done("stock_minute") == set()


def test_manifest_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "MANIFEST_PATH", tmp_path / "nope.json")
    assert ms._manifest_done("stock_minute") == set()
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_backfill_manifest.py -q`
Expected: FAIL

- [ ] **Step 3: 实现（mootdx_service.py 常量区后新增）**

```python
# 回源断点续传 manifest：任务启动写 targets，每批 flush 后追加 done。
# 重启后 todo = targets − done − 最新分区已有 → 精确续跑。
MANIFEST_PATH = DATA_ROOT / "backfill_state.json"


def _manifest_load() -> dict:
    import json
    try:
        return json.loads(MANIFEST_PATH.read_text())
    except Exception:  # noqa: BLE001
        return {}


def _manifest_save(data: dict) -> None:
    import json
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False))
    tmp.rename(MANIFEST_PATH)


def _manifest_reset(dataset: str, targets: list[str], mode: str) -> None:
    data = _manifest_load()
    data[dataset] = {"targets": list(targets), "done": [], "mode": mode,
                     "updated_at": _dt.datetime.now().isoformat()}
    _manifest_save(data)


def _manifest_mark_done(dataset: str, symbols: list[str]) -> None:
    data = _manifest_load()
    entry = data.setdefault(dataset, {"targets": [], "done": [], "mode": ""})
    entry["done"] = sorted(set(entry.get("done") or []) | set(symbols))
    entry["updated_at"] = _dt.datetime.now().isoformat()
    _manifest_save(data)


def _manifest_done(dataset: str) -> set[str]:
    return set(_manifest_load().get(dataset, {}).get("done") or [])
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_backfill_manifest.py -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add app/services/mootdx_service.py tests/quant/test_backfill_manifest.py
git commit -m "feat(mootdx): 回源 manifest 断点续传（原子写）"
```

---

### Task 6: mootdx_service — sync_stock_minute 三路径接池 + since 分页 + 删限速 + 时间日志

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（sync_stock_minute / sync_stock_minute_day / sync_stock_minute_range / 删 _throttle_backfill 区块）
- Test: `backend/tests/quant/test_sync_stock_minute_pool.py`（新建）

**Interfaces:**
- Consumes: `BackfillPool.map(fn, symbols, batch_size, on_batch_done)`（Task 4）、`get_minute(since=...)`（Task 1）、`_manifest_*`（Task 5）
- Produces: 三个 sync 函数行为签名不变（返回行数）；内部走池；进度日志时间驱动（60s）

- [ ] **Step 1: 写失败测试**

```python
"""sync_stock_minute 接入并发池 + since 分页 + manifest 记账。"""
from __future__ import annotations

import datetime as _dt

import pandas as pd
import polars as pl

from app.services import mootdx_service as ms


class _FakePagedSrc:
    """返回覆盖 since 的最近两天分钟帧；记录每次调用的 since 参数。"""

    def __init__(self):
        self.since_calls: list = []

    def get_minute(self, code, date="", max_bars=30000, since=None):
        self.since_calls.append((code, since))
        idx = pd.DatetimeIndex([
            pd.Timestamp("2026-08-20 09:31:00"),
            pd.Timestamp("2026-08-21 09:31:00")])
        return pd.DataFrame({"open": [1.0] * 2, "high": [1.0] * 2,
                             "low": [1.0] * 2, "close": [1.0] * 2,
                             "volume": [1.0] * 2, "amount": [1.0] * 2}, index=idx)


def _setup_env(tmp_path, monkeypatch, universe):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "MANIFEST_PATH", tmp_path / "backfill_state.json")
    monkeypatch.setattr(ms, "_stock_universe", lambda: universe)
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda now=None: [])
    monkeypatch.setattr(ms, "_minute_fragment_days", lambda: {})
    monkeypatch.setattr(ms, "_existing_minute_symbols", lambda: set())
    monkeypatch.setattr(ms, "_market_closed", lambda: True)


def test_sync_stock_minute_uses_recent_pages_and_manifest(tmp_path, monkeypatch):
    universe = ["600000.SH", "600001.SH"]
    _setup_env(tmp_path, monkeypatch, universe)
    fake = _FakePagedSrc()
    monkeypatch.setattr(ms, "BackfillPool",
                        lambda workers=None, source_factory=None: _StubPool(fake))
    n = ms.sync_stock_minute(limit=None)
    assert n == 4  # 2 只 × 2 天
    codes = {c for c, _ in fake.since_calls}
    assert codes == {"600000.SH", "600001.SH"}
    # recent 模式：since=最新分区之后的目标日（这里缺整月→since=STOCK_MINUTE_START）
    assert all(s is not None for _, s in fake.since_calls)
    assert ms._manifest_done("stock_minute") == set(universe)


class _StubPool:
    """串行执行的单 worker 假池（保持 map 接口契约）。"""

    def __init__(self, src):
        self.src = src

    def effective_workers(self, task_size):
        return 1

    def map(self, fn, symbols, batch_size=100, on_batch_done=None):
        ok, failed, batch = [], {}, []
        for s in symbols:
            try:
                out = fn(self.src, s)
            except Exception as e:  # noqa: BLE001
                failed[s] = str(e)
                continue
            if out is not None:
                ok.append(out)
                batch.append(out)
            if on_batch_done is not None and len(batch) >= batch_size:
                on_batch_done(batch)
                batch = []
        if on_batch_done is not None and batch:
            on_batch_done(batch)
        return {"ok": ok, "failed": failed}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_sync_stock_minute_pool.py -q`
Expected: FAIL（仍走旧串行路径/无 since）

- [ ] **Step 3: 实现**

(a) 删除限速区块：`_BACKFILL_THROTTLE_*`、`_BACKFILL_INTRADAY_*`、`_throttle_backfill` 整体移除；各调用点（sync_etf_minute/sync_daily/sync_index_daily/sync_stock_minute*/check_day 内循环里的 `_throttle_backfill(i)` 行）随 Task 6/7 重构一并消失。

(b) 新增模块级导入与共享工具：

```python
from app.services.stockdata.backfill_pool import BackfillPool

_PROGRESS_LOG_INTERVAL_S = 60.0


def _mk_progress_logger(total: int, label: str):
    """时间驱动的进度日志器：每 60s 打一条（处理数/速率/ETA），替代按只数打点。"""
    state = {"t0": time.time(), "last": time.time(), "n": 0}

    def tick(done_now: int, current: str = "") -> None:
        state["n"] = done_now
        now = time.time()
        if now - state["last"] < _PROGRESS_LOG_INTERVAL_S:
            return
        state["last"] = now
        rate = done_now / max(1e-9, now - state["t0"])
        eta = (total - done_now) / rate if rate > 0 else 0
        logger.info("%s 进度 %d/%d (%s) 速率 %.1f只/s ETA %.0fmin",
                    label, done_now, total, current, rate, eta / 60)

    return tick
```

(c) `_guarded_get_minute` 增加 `since` 透传：

```python
def _guarded_get_minute(src, sym, max_bars=40000,
                        timeout: float | None = None,
                        since: _date | None = None) -> pd.DataFrame | None:
    ...
    def _run() -> None:
        try:
            box["df"] = src.get_minute(sym, max_bars=max_bars, since=since)
        except Exception as e:  # noqa: BLE001
            box["err"] = e
    ...
```

(d) `sync_stock_minute` 主体改造（保留 range/fragment/resume 判定骨架，逐只循环换成池）。进度打点用具体闭包计数器，不简写：

```python
    from app.services.stockdata.backfill_pool import BackfillPool

    # resume 过滤追加 manifest 维度（targets − done − 最新分区已有）
    done_syms = _existing_minute_symbols() | _manifest_done("stock_minute")
    todo = [s for s in stocks if s not in done_syms]
    ...
    _manifest_reset("stock_minute", todo, mode="full")
    tick = _mk_progress_logger(len(todo), "股票分钟回源")
    keep = ["symbol", "datetime", "open", "high", "low", "close",
            "volume", "amount"]
    counter = {"n": 0}
    pending: list[pl.DataFrame] = []

    def _fetch_one(src, sym):
        counter["n"] += 1
        tick(counter["n"], sym)
        sym_start = STOCK_MINUTE_START
        ld = listing.get(sym)
        if ld is not None and ld > sym_start:
            sym_start = ld
        try:
            df = _guarded_get_minute(src, sym, max_bars=40000, since=sym_start)
        except TimeoutError:
            _append_failure(sym, "timeout")
            raise                      # pool 捕获 → failed + 该 worker 重建 source
        except Exception as e:  # noqa: BLE001
            _append_failure(sym, f"exception:{str(e)[:60]}")
            return None
        if df is None or df.empty:
            _append_failure(sym, "empty")
            return None
        df = df.copy()
        df["symbol"] = sym
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date >= sym_start]
        if not _market_closed():
            df = df[pd.to_datetime(df["datetime"]).dt.date < _date.today()]
        if df.empty:
            return None
        return pl.from_pandas(df).with_columns(
            pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))

    def _on_batch(batch_frames: list[pl.DataFrame]) -> None:
        pending.extend(batch_frames)
        if len(pending) >= _STOCK_MINUTE_BATCH:
            _flush_stock_minute_chunk(pending.copy())
            _manifest_mark_done(
                "stock_minute", [f["symbol"][0] for f in pending])
            pending.clear()

    result = BackfillPool().map(_fetch_one, todo,
                                batch_size=_STOCK_MINUTE_BATCH,
                                on_batch_done=_on_batch)
    if pending:
        _flush_stock_minute_chunk(pending)
        _manifest_mark_done("stock_minute", [f["symbol"][0] for f in pending])
    total = sum(f.height for f in result["ok"])
    logger.info("mootdx_service: 股票分钟回源完成 %d 行 (failed=%d)",
                total, len(result["failed"]))
    return range_rows + total + fragment_rows
```

(e) `sync_stock_minute_range` / `sync_stock_minute_day` 同模式改写：range 取 `since=min(day_set)`、manifest `mode="recent"`；day/残片修复同样记账（统一机制）。

注意：原实现里"每批 100 只重建 src / 每 500 只重置连接"的手工维护随池内 per-worker source 消亡；TimeoutError 由 pool 统一捕获进 failed 并重置该 worker source，语义等价。

- [ ] **Step 4: 跑测试 + 相关回归**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_sync_stock_minute_pool.py tests/quant/test_mootdx_backfill_coverage.py tests/quant/test_scheduler_scan.py -q`
Expected: PASS（旧测试如依赖 _throttle_backfill/串行路径，按新行为修正断言——删限速是 spec 明确项）

- [ ] **Step 5: Commit**

```bash
git add app/services/mootdx_service.py tests/quant/test_sync_stock_minute_pool.py
git commit -m "perf(mootdx): 股票分钟三路径接入并发池+since分页+manifest，删除盘中限速"
```

---

### Task 7: mootdx_service — daily/index/etf-minute 路径接池

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（sync_daily / sync_index_daily / sync_etf_minute）
- Test: `backend/tests/quant/test_sync_daily_pool.py`（新建）

**Interfaces:**
- Consumes: `BackfillPool.map`（Task 4）
- Produces: 三函数签名/返回值不变；内部并发

- [ ] **Step 1: 写失败测试**

```python
"""sync_daily/sync_index_daily/sync_etf_minute 并发池改造测试。"""
from __future__ import annotations

import datetime as _dt

import pandas as pd

from app.services import mootdx_service as ms


class _FakeDailySrc:
    def get_daily(self, code, start, end):
        ts = pd.Timestamp(f"{end[:4]}-{end[4:6]}-{end[6:]} 15:00:00")
        return pd.DataFrame({"open": [1.0], "high": [2.0], "low": [0.5],
                             "close": [1.5], "volume": [100.0],
                             "amount": [1000.0]},
                            index=pd.DatetimeIndex([ts]))

    def get_minute_recent(self, code, pages=1):
        idx = pd.DatetimeIndex([pd.Timestamp("2026-08-21 15:00:00")])
        return pd.DataFrame({"open": [1.0], "high": [1.0], "low": [1.0],
                             "close": [1.0], "volume": [1.0], "amount": [1.0]},
                            index=idx)


def test_sync_daily_concurrent_writes_partition(tmp_path, monkeypatch):
    day = _dt.date(2026, 8, 21)
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "_stock_universe", lambda: ["600000.SH"])
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["510300.XSHG"])
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {})
    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeDailySrc())

    from tests.quant.test_sync_stock_minute_pool import _StubPool
    monkeypatch.setattr(ms, "BackfillPool",
                        lambda workers=None, source_factory=None: _StubPool(_FakeDailySrc()))
    written = ms.sync_daily(day)
    assert written["stock"] == 1 and written["etf"] == 1
    assert (tmp_path / "kline_daily" / f"date={day}" / "part.parquet").exists()


def test_sync_etf_minute_concurrent(tmp_path, monkeypatch):
    day = _dt.date(2026, 8, 21)
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["510300.XSHG"])
    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeDailySrc())

    from tests.quant.test_sync_stock_minute_pool import _StubPool
    monkeypatch.setattr(ms, "BackfillPool",
                        lambda workers=None, source_factory=None: _StubPool(_FakeDailySrc()))
    n = ms.sync_etf_minute(day)
    assert n == 1
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_sync_daily_pool.py -q`
Expected: FAIL

- [ ] **Step 3: 实现**

`sync_daily` 的 `_fetch` 内层循环换成池提交（逐 symbol 单请求 `_guarded_get_daily`，结果聚合后写分区逻辑不变）：

```python
    def _fetch_one(src, sym):
        try:
            df = _guarded_get_daily(src, sym, day_str, day_str)
        except Exception:  # noqa: BLE001
            return None
        if df is None or df.empty:
            return None
        hit = df[[x.date() == day for x in df.index]]
        if hit.empty:
            return None
        row = hit.iloc[-1]
        return pl.DataFrame({
            "symbol": [sym], "date": [day],
            "open": [float(row["open"])], "high": [float(row["high"])],
            "low": [float(row["low"])], "close": [float(row["close"])],
            "volume": [float(row["volume"])], "amount": [float(row["amount"])],
        })

    pool = BackfillPool()
    stock_res = pool.map(lambda src, s: _fetch_one(src, s), stocks)
    etf_res = pool.map(lambda src, s: _fetch_one(src, s), etfs)
    sdf = pl.concat(stock_res["ok"]) if stock_res["ok"] else None
    edf = pl.concat(etf_res["ok"]) if etf_res["ok"] else None
    # ……后续 volume/100、warning、写分区逻辑与原实现一致
```

`sync_index_daily`、`sync_etf_minute` 同模式（etf_minute 用 `src.get_minute_recent(jq, pages=2)`，historical 分支用 `src.get_minute(jq, max_bars=40000, since=day)`）。500 只周期重建连接的旧逻辑随池内 per-worker source 自然消亡，删除。

- [ ] **Step 4: 跑测试 + 回归**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_sync_daily_pool.py tests/quant/test_mootdx_backfill_coverage.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/mootdx_service.py tests/quant/test_sync_daily_pool.py
git commit -m "perf(mootdx): 日线/指数/ETF分钟回源接入并发池"
```

---

### Task 8: 中断感知日志 + 启动 backfill 持锁 + 内容校验去重

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（backfill_to_now / _stock_minute_latest_partial 日志措辞 / kline_etf_daily 扫描复用）
- Modify: `backend/app/services/mootdx_service.py`（backfill_to_now 包 _sync_lock）
- Test: `backend/tests/quant/test_startup_backfill_guard.py`（新建）

**Interfaces:**
- Consumes: 现有 `_sync_lock`（scheduler.py 定义，经 import 共享——注意它定义在 scheduler 模块，mootdx_service 需延迟导入避免环）
- Produces: `backfill_to_now()` 全程持锁；mtime<10min 的分区打「中断续跑」日志；250 分区扫描单次复用

- [ ] **Step 1: 写失败测试**

```python
"""启动 backfill 持锁与中断感知日志测试。"""
from __future__ import annotations

import datetime as _dt
import time

from app.services import mootdx_service as ms


def test_interrupted_partition_message(tmp_path, monkeypatch, caplog):
    """最新分区 mtime 距今 <10min → 日志为「中断续跑」而非「残缺」。"""
    import polars as pl
    root = tmp_path / "kline_minute"
    pdir = root / "date=2026-08-21"
    pdir.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600000.SH"]}).write_parquet(pdir / "part.parquet")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    monkeypatch.setattr(ms, "_stock_universe", lambda: [f"{600000+i}.SH" for i in range(10)])
    with caplog.at_level("INFO", logger="app.services.mootdx_service"):
        partial = ms._stock_minute_latest_partial({"600000.SH"},
                                                  [f"{600000+i}.SH" for i in range(10)])
    assert partial is True  # 判定保留


def test_backfill_to_now_holds_sync_lock(tmp_path, monkeypatch):
    """backfill_to_now 全程持 _SYNC_LOCK：锁被占时 body 不执行。"""
    import threading
    from app.services import mootdx_service as ms

    started = threading.Event()

    # 最小化副作用：全部扫描返回空、nav 服务打桩
    scan_fns = ["_incomplete_etf_minute_days", "_incomplete_stock_daily_days",
                "_incomplete_etf_daily_days", "_incomplete_index_daily_days",
                "_incomplete_stock_minute_days", "_missing_stock_minute_days",
                "_missing_minute_days", "_missing_index_daily_days",
                "_safe_universe_segment_missing"]
    for name in scan_fns:
        monkeypatch.setattr(ms, name, lambda *a, **k: [])
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root, now=None: [])
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj.parquet")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kd")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "ed")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "id")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "em")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "sm")
    from app.services import etf_nav_service
    monkeypatch.setattr(etf_nav_service, "_partition_dates", lambda: [])
    monkeypatch.setattr(etf_nav_service, "_missing_etf_nav_days", lambda: [])

    def runner():
        ms.backfill_to_now()
        started.set()

    lock = ms._SYNC_LOCK
    t = threading.Thread(target=runner)
    lock.acquire()
    t.start()
    time.sleep(0.3)
    assert not started.is_set()      # 锁被占 → body 阻塞未完成
    lock.release()
    assert started.wait(timeout=10)  # 放锁后跑完
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_startup_backfill_guard.py -q`
Expected: FAIL

- [ ] **Step 3: 实现**

(a) 共享锁 + backfill_to_now 加锁。mootdx_service.py 模块级新增：

```python
import threading

# 15:35 cron / 00:00 巡检 / 启动 backfill 共用的互斥锁（原 scheduler._sync_lock
# 上移至此，scheduler 反向导入同一对象，消除启动 backfill 不持锁的并发轰击）
_SYNC_LOCK = threading.Lock()
```

`backfill_to_now()` 首尾：

```python
def backfill_to_now() -> dict[str, Any]:
    """启动回源：补齐到当前时间缺失的全部数据集（幂等，持 _SYNC_LOCK）。"""
    with _SYNC_LOCK:
        return _backfill_to_now_locked()
```

（原函数体整体改名为 `_backfill_to_now_locked()`，内部逻辑不变。）

scheduler.py 头部替换：

```python
# 旧：_sync_lock = threading.Lock()  # 15:35 cron 与手动 trigger 串行
from app.services.mootdx_service import _SYNC_LOCK as _sync_lock
```

(b) `_stock_minute_latest_partial` 命中分支日志改为中断语义：

```python
        if _stock_minute_latest_partial(done_syms, stocks):
            mt_age = 1e9
            days = sorted(STOCK_MINUTE_ROOT.glob("date=*"))
            if days:
                part = days[-1] / "part.parquet"
                if part.exists():
                    mt_age = time.time() - part.stat().st_mtime
            if mt_age < 600:
                logger.info("mootdx_service: 上次回源中断于 %s（%.0fmin 前，覆盖率 "
                            "%.1f%%），从断点继续补齐 %d 只",
                            days[-1].name, mt_age / 60,
                            len(done_syms & set(stocks)) / len(stocks) * 100, len(todo))
            else:
                logger.info("mootdx_service: 最新分钟分区覆盖率 %.1f%%，全量补齐 %d 只",
                            len(done_syms & set(stocks)) / len(stocks) * 100, len(todo))
```

(c) `backfill_to_now` 内 `incomplete_etf_daily` 与 `daily_days` 集合构造中对 `_incomplete_etf_daily_days()` 的两次调用合并为一次复用变量。

- [ ] **Step 4: 跑测试 + scheduler 回归**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_startup_backfill_guard.py tests/quant/test_stockdata_scheduler.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/mootdx_service.py app/services/stockdata/scheduler.py tests/quant/test_startup_backfill_guard.py
git commit -m "feat(stockdata): 启动回源持锁+中断感知日志+内容校验去重"
```

---

### Task 9: 全量回归 + lint + mypy

**Files:**
- 无新改动（修复回归暴露的问题）

- [ ] **Step 1: 全量测试**

Run: `cd backend && uv run --extra dev pytest -q`
Expected: 全部 PASS（如有历史用例依赖已删除的 `_throttle_backfill`/串行行为，按 spec 语义更新用例并在 commit message 注明）

- [ ] **Step 2: lint + 类型**

Run: `cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app`
Expected: 无错误

- [ ] **Step 3: 真机验收（可选，盘后）**

手动触发 `trigger_sync(kind="backfill")` 观察日志：worker 数、速率、ETA、总耗时；重启服务验证 manifest 续跑日志「上次回源中断于…从断点继续」。

- [ ] **Step 4: Commit（如有修复）**

```bash
git add -A && git commit -m "fix: 回归修复（回源提速收尾）"
```
