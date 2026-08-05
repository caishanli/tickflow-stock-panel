# stock data 服务（网络行情数据服务）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 mootdx intraday 实时分钟服务进化为独立网络 stock data 服务，量化回测/模拟盘全部通过网络获取行情数据（零本地 parquet 读取、零 mootdx/astock 直连），服务自治完成全部回源落盘。

**Architecture:** 单进程 TCP 服务（ThreadingTCPServer + msgpack 帧），服务端多源聚合（本地分区 / mootdx / astock）+ 当日分钟内存库（纯 lazy，网络数据才驻留，00:00 清空）+ 共享网络拉取线程池 + 标的级 single-flight 去重回源 + 自治调度（启动 backfill / 15:35 收盘同步 / 00:00 清空）。量化侧经 jqdata 风格客户端 SDK 取数，DataManager / QuantDataProvider / live_feed 换成网络取数对象。FastAPI 主后端只做守护。

**Tech Stack:** Python 3.11+ / socketserver / threading / msgpack / polars / pandas / mootdx。前端、DuckDB、kline_sync、quote_service 不动。

## Global Constraints

- 量化侧（回测/模拟盘）**不从任何本地文件或其他网络接口获取行情数据**：DataManager / QuantDataProvider / live_feed 只能通过 `network_client` 取数。
- **前端展示不动**：DuckDB 直读共享 `data/`、kline_sync/quote_service/financial_sync 一律不改。
- **主后端只做守护**：删除主后端启动 backfill 调用与 15:35 mootdx_sync cron；回源落盘全在服务内。
- 前复权（fq='pre'）在**客户端**计算，服务端只出原始价。
- 去重粒度=**标的级**：两个交叉批量请求重叠部分只回源一次；实时键 `rt:{code}` 不含时间分量 + ~10s TTL 缓存。
- baostock 目前不是依赖；数据源做成可插拔注册表，未来加源只注册一个实现。
- 依赖新增仅 `msgpack`（后端 `uv add msgpack`）。
- 服务端单只失败只标记该标的，不拖垮整批；墙钟守护（分钟 30s、日线 20s），超时重建 `MootdxSource`。
- 验证命令（`backend/` 下）：`uv run --extra dev pytest` / `uv run --extra dev ruff check app` / `uv run --extra dev mypy app`。

---

## 文件结构

| 文件 | 职责 |
|------|------|
| `backend/app/services/stockdata/__init__.py` | 包 |
| `backend/app/services/stockdata/protocol.py` | 4 字节长度前缀 + msgpack 帧编解码 |
| `backend/app/services/stockdata/single_flight.py` | 标的级 single-flight 去重 + TTL 缓存 |
| `backend/app/services/stockdata/sources.py` | 分区读取 / mootdx / astock 聚合，带 single-flight、墙钟守护、失败统计 |
| `backend/app/services/stockdata/handlers.py` | method → handler 分发（json / parquet 响应） |
| `backend/app/services/stockdata/server.py` | ThreadingTCPServer + 帧循环 + 调度器装配 |
| `backend/app/services/stockdata/scheduler.py` | 启动 backfill + 15:35 收盘同步 + 00:00 清空当日分钟内存（线程） |
| `backend/scripts/run_stockdata_service.py` | 服务进程入口 |
| `backend/app/services/stockdata_guardian.py` | 泛化自 intraday_guardian 的进程守护（PID 锁 + 3s 自愈） |
| `backend/app/quant/datasource/network_client.py` | jqdata 风格客户端 SDK（重连/超时/请求 id） |
| `backend/app/quant/jqengine/datasource/network_source.py` | 实现 DataSource 接口的网络源适配器（喂给 DataManager.fetch） |

**修改：** `main.py`（守护）、`daily_pipeline.py`（删 cron）、`quant/jqengine/datasource/manager.py`（DataManager 换源）、`quant/simulate/live_feed.py`、`quant/simulate/runner.py`、`quant/datasource/manager.py`（QuantDataProvider）、`quant/rqalpha_bridge.py`（sources 键名）、`tests/quant/*`。

---

## 阶段一：分支与移植

### Task 1: 建分支 + 移植 intraday 代码

**Files:** 仓库级操作

**Interfaces:**
- Produces: `feature/stockdata-service` 分支，含 spec 提交 + 移植的 `mootdx_service.py` 回源覆盖提交（`sync_index_daily` 及 staleness/告警检查）。**不移植** `mootdx_intraday.py`/`run_mootdx_intraday.py`/`intraday_guardian.py`（主动盘中轮询已取消，实时改按需回源）。

- [ ] **Step 1: 记录 spec/计划提交**

Run: `git log --oneline --reverse custom-main..feature/mootdx-intraday -- docs/superpowers/specs/2026-08-05-stockdata-service-design.md docs/superpowers/plans/2026-08-05-stockdata-service.md`
Expected（依赖序，当前全部）：spec `243a7c9` `7e49b9e` `9657252` `1c8838f` `2947afe`；计划 `a7fa283` `1f96363` `5858207` `6e14edb` `8770a05`。若执行时又新增了文档提交，一并记下。

- [ ] **Step 2: 从 custom-main 建分支**

```bash
git checkout custom-main
git switch -c feature/stockdata-service
```

- [ ] **Step 3: 移植 spec/计划提交（依赖序，Step 1 列出的全部）**

```bash
git cherry-pick 243a7c9 7e49b9e 9657252 1c8838f 2947afe a7fa283 1f96363 5858207 6e14edb 8770a05
```

- [ ] **Step 4: 移植 mootdx_service 回源覆盖提交**

只移植 `mootdx_service.py` 相关（backfill 覆盖 + 指数日线 + staleness + 写0告警），**不移植** intraday 循环/守护/模拟盘实时回源提交：

```bash
git log --oneline custom-main..feature/mootdx-intraday -- backend/app/services/mootdx_service.py backend/app/jobs/daily_pipeline.py | cat
```
输出应含：`d98a2ec` `067b4f9` `1dfaf9f` `8226d79` `e5f2ee0` `97ad919` `20f84a4`（依赖序）。

按依赖序 cherry-pick：`git cherry-pick d98a2ec 067b4f9 1dfaf9f 8226d79 e5f2ee0 97ad919 20f84a4`
（冲突时解决后 `git cherry-pick --continue`。注意 `20f84a4` 也改了 `daily_pipeline.py` 的告警——该 cron 随后在 Task 9 整体删除，先保留。）

- [ ] **Step 5: 确认未误移植 intraday/模拟盘回源**

Run: `git diff custom-main..HEAD --stat | grep -iE "mootdx_intraday|intraday_guardian|minute_realtime|sim_accounts|live_feed"`
Expected: 无输出（这些文件都不应在本次移植中出现；若出现，`git revert` 掉）。

- [ ] **Step 6: 验证移植后代码可导入**

```bash
cd backend
uv run --extra dev python -c "from app.services import mootdx_service; print('ok')"
```
Expected: `ok`

- [ ] **Step 7: 运行既有测试确认移植无回归**

```bash
uv run --extra dev pytest tests/quant/test_sync_adj_factor.py -q
```
Expected: PASS（与移植前一致；`test_mootdx_intraday.py` 不存在于本分支，跳过）。

- [ ] **Step 8: Commit（若 cherry-pick 已产生提交则跳过）**

```bash
git status --short
```

---

## 阶段二：服务骨架（协议 + 去重 + 源 + handler + server + 客户端）

### Task 2: 协议层 protocol.py

**Files:**
- Create: `backend/app/services/stockdata/__init__.py`
- Create: `backend/app/services/stockdata/protocol.py`
- Test: `backend/tests/quant/test_stockdata_protocol.py`

**Interfaces:**
- Produces: `encode_request(req_id: int, method: str, params: dict) -> bytes`；`decode_frame(conn) -> dict`（阻塞读完整一帧，返回 `{"v","id","m","p"}`）；`encode_response(req_id, ok, t, data) -> bytes`；`decode_response(data: bytes) -> dict`。

- [ ] **Step 1: 写失败测试**

```python
# backend/tests/quant/test_stockdata_protocol.py
import socket

from app.services.stockdata import protocol


def test_request_roundtrip():
    raw = protocol.encode_request(7, "get_price", {"security": "512670.XSHG", "frequency": "daily"})
    c, s = socket.socketpair()
    s.sendall(raw)
    got = protocol.decode_frame(c)
    c.close(); s.close()
    assert got["v"] == 1 and got["id"] == 7
    assert got["m"] == "get_price"
    assert got["p"]["security"] == "512670.XSHG"


def test_response_roundtrip_json():
    raw = protocol.encode_response(3, True, "json", {"pong": True})
    assert protocol.decode_response(raw)["ok"] is True
    assert protocol.decode_response(raw)["d"] == {"pong": True}


def test_response_parquet():
    import io
    import polars as pl
    df = pl.DataFrame({"symbol": ["a"], "close": [1.0]})
    raw = protocol.encode_response(1, True, "parquet", df)
    msg = protocol.decode_response(raw)
    assert msg["t"] == "parquet"
    back = pl.read_parquet(io.BytesIO(msg["d"]))
    assert back["close"].to_list() == [1.0]
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_protocol.py -q`
Expected: FAIL（`ModuleNotFoundError`）。

- [ ] **Step 2b: 添加 msgpack 依赖**

Run: `uv add msgpack`
Expected: 成功更新 `pyproject.toml` 与 `uv.lock`（`msgpack` 为唯一新增依赖）。

- [ ] **Step 3: 实现 protocol.py**

```python
# backend/app/services/stockdata/__init__.py
"""stock data 网络行情数据服务包。"""
```

```python
# backend/app/services/stockdata/protocol.py
"""TCP 帧编解码：4 字节大端长度前缀 + msgpack 负载。

请求：  {"v":1, "id":<int>, "m":<method>, "p":{params}}
响应：  {"v":1, "id":<int>, "ok":<bool>, "t":"parquet"|"json", "d":<bytes|dict>}
parquet 响应的 ``d`` 为原始 parquet 字节（嵌入 msgpack 二进制），
json 响应的 ``d`` 为 dict/list。
"""
from __future__ import annotations

import io

import msgpack
import polars as pl

VERSION = 1
_HEADER = 4


def encode_request(req_id: int, method: str, params: dict) -> bytes:
    payload = msgpack.packb(
        {"v": VERSION, "id": req_id, "m": method, "p": params},
        use_bin_type=True,
    )
    return len(payload).to_bytes(_HEADER, "big") + payload


def _recv_exact(conn, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError("连接已关闭")
        buf.extend(chunk)
    return bytes(buf)


def decode_frame(conn) -> dict:
    """从 socket 阻塞读一帧并解码（线程内调用）。"""
    header = _recv_exact(conn, _HEADER)
    n = int.from_bytes(header, "big")
    payload = _recv_exact(conn, n)
    msg = msgpack.unpackb(payload, raw=False)
    if msg.get("v") != VERSION:
        raise ValueError(f"协议版本不匹配: {msg.get('v')}")
    return msg


def encode_response(req_id: int, ok: bool, t: str, data) -> bytes:
    if t == "parquet":
        buf = io.BytesIO()
        data.write_parquet(buf)
        body = buf.getvalue()
        payload = msgpack.packb(
            {"v": VERSION, "id": req_id, "ok": ok, "t": "parquet", "d": body},
            use_bin_type=True,
        )
    else:
        payload = msgpack.packb(
            {"v": VERSION, "id": req_id, "ok": ok, "t": "json", "d": data},
            use_bin_type=True,
        )
    return len(payload).to_bytes(_HEADER, "big") + payload


def decode_response(data: bytes) -> dict:
    payload = data[_HEADER:]
    return msgpack.unpackb(payload, raw=False)
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_protocol.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/stockdata backend/tests/quant/test_stockdata_protocol.py
git commit -m "feat(stockdata): TCP 协议层（4字节长度前缀 + msgpack 帧）"
```

---

### Task 3: 标的级 single-flight + TTL 缓存

**Files:**
- Create: `backend/app/services/stockdata/single_flight.py`
- Test: `backend/tests/quant/test_stockdata_single_flight.py`

**Interfaces:**
- Produces: `SingleFlight.run(key: str, loader: Callable) -> Any`（同键并发只执行一次 loader）；`TTLCache.get(key) -> Any|None` / `TTLCache.set(key, value, ttl)`；`DedupCache.get_or_fetch(key, ttl, loader) -> Any`（TTL 命中直接返回，未命中走 single-flight）。

- [ ] **Step 1: 写失败测试**

```python
# backend/tests/quant/test_stockdata_single_flight.py
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
        entered.set()       # leader 进入 loader
        release.wait(5)     # 保持 loader 挂起，等 4 个 follower 阻塞在 ev.wait
        return 42

    results = []
    def worker():
        results.append(sf.run("k", loader))

    ts = [threading.Thread(target=worker) for _ in range(5)]
    for t in ts: t.start()
    entered.wait(2)         # 等 leader 已进 loader
    time.sleep(0.05)        # 让 follower 全部到达 sf.run 并 park
    release.set()
    for t in ts: t.join()
    assert results == [42] * 5
    assert len(calls) == 1  # 只回源一次
    # 说明：不能用 Barrier(5) 在 loader 里等——single-flight 下 loader 只执行一次，
    # 永远凑不齐 5 人必死锁；用事件保持 + 短暂 sleep 保证 follower 并发到达。


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
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_single_flight.py -q`
Expected: FAIL

- [ ] **Step 3: 实现 single_flight.py**

```python
# backend/app/services/stockdata/single_flight.py
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
            except BaseException as e:  # noqa: BLE001
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
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_single_flight.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/stockdata/single_flight.py backend/tests/quant/test_stockdata_single_flight.py
git commit -m "feat(stockdata): 标的级 single-flight + 短TTL缓存（交叉请求重叠只回源一次）"
```

---

### Task 4: 数据源 sources.py（分区读 + mootdx 实时 + 聚合去重）

**Files:**
- Create: `backend/app/services/stockdata/sources.py`
- Test: `backend/tests/quant/test_stockdata_sources.py`

**Interfaces:**
- Produces（供 handlers 调用）：
  - `class DataSources`；`get_or_fetch(key, ttl, loader)`（DedupCache 透传）
  - `preload_daily(lookback_days, asof=None) -> pl.DataFrame`（全市场日线，归一化后 [symbol,date,open,high,low,close,volume,amount]）
  - `get_daily(codes, start_date, end_date) -> pl.DataFrame`
  - `get_minute(codes, lo_ts, hi_ts) -> pl.DataFrame`（含 kline_etf_minute + kline_minute）
  - `get_realtime_snapshot(codes, as_of=None) -> pl.DataFrame`（当日分区 + 未覆盖标的 mootdx 并发补实时，per-symbol single-flight）
  - `get_trade_days(start_date, end_date) -> list[str]`
  - `get_all_securities(types, date) -> pl.DataFrame`
  - `get_security_info(code) -> dict`
  - `get_index_stocks(index_code, date) -> list[str]`
  - `get_stock_names(codes) -> dict`
  - `get_adj_factors() -> pl.DataFrame`
- 复用既有：`_to_tf_symbol`/`_etf_universe`/`_write_minute_partition` 等从 `mootdx_service` 导入。

- [ ] **Step 1: 写失败测试**

```python
# backend/tests/quant/test_stockdata_sources.py
import datetime as _dt

import polars as pl
import pytest

from app.services.stockdata.sources import DataSources, MinuteMemoryStore


def _write_daily(root, day, rows):
    import os
    d = os.path.join(root, "kline_daily", f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame(rows).write_parquet(os.path.join(d, "part.parquet"))


def _write_minute(root, sub, day, rows):
    import os
    d = os.path.join(root, sub, f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame(rows).write_parquet(os.path.join(d, "part.parquet"))


@pytest.fixture
def src(tmp_path):
    import os
    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    day = _dt.date.today().isoformat()
    _write_daily(str(tmp_path), day, [
        {"symbol": "600000.SH", "date": day, "open": 10.0, "high": 11.0,
         "low": 9.0, "close": 10.5, "volume": 1000, "amount": 105000.0},
    ])
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=2)
    yield s
    os.environ.pop("PARTITION_DATA_ROOT", None)


def test_preload_daily_reads_partitions(src):
    df = src.preload_daily(lookback_days=400)
    assert not df.is_empty()
    assert "symbol" in df.columns and "close" in df.columns
    assert df["symbol"].to_list() == ["600000.SH"]
    # 股票日线 volume 手 → 股（×100）
    assert df["volume"].to_list() == [100000]


def test_minute_memory_store_lazy_and_clear():
    ms = MinuteMemoryStore()
    day = _dt.date.today()
    assert ms.day() is None  # 未请求标的：无日期、无内存
    df = pl.DataFrame({"symbol": ["600000.SH"], "datetime": [f"{day} 10:00:00"],
                       "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
                       "volume": [100], "amount": [100.0]})
    ms.update(f"{day} 00:00:00", df)
    assert ms.day() == day
    got = ms.get_slice({"600000.SH"}, "2000-01-01 00:00:00", f"{day} 15:00:00")
    assert not got.is_empty()
    # 换日 lazy 清空：ensure_day(次日) 后旧帧全部清空
    nxt = day + _dt.timedelta(days=1)
    ms.ensure_day(nxt)
    assert ms.day() == nxt
    assert ms.get_slice({"600000.SH"}, "2000-01-01 00:00:00", f"{day} 15:00:00").is_empty()
    # clear() 显式清空后回到初始态
    ms.clear()
    assert ms.day() is None


def test_realtime_snapshot_serves_from_memory(src, monkeypatch):
    day = _dt.date.today().isoformat()
    _write_minute(str(src.data_root), "kline_etf_minute", day, [
        {"symbol": "512670.SH", "datetime": f"{day} 09:31:00", "open": 1.0,
         "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1000, "amount": 1000.0},
    ])
    # 非交易时段：只读内存库，不触网
    monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: False)
    df = src.get_realtime_snapshot(["512670.XSHG"])
    assert not df.is_empty()
    assert df["close"].to_list() == [1.0]
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py -q`
Expected: FAIL

- [ ] **Step 3: 实现 sources.py**

```python
# backend/app/services/stockdata/sources.py
"""数据源聚合：本地分区 / mootdx / astock + 当日分钟内存库 + 共享网络拉取线程池。

内存策略（重要）：
- 本地分区已有的历史数据 → 每次按需读盘，**不常驻内存**（短 TTL 仅突发去重）；
- 本地没有、需网络拿的数据（当日实时分钟）→ 拿到后进**当日分钟内存库**，
  当日驻留，次日 00:00 清空，避免重复回源。
- 当日分钟内存库是纯 lazy dict：服务启动不预载、不预分配，未请求标的零内存。
"""
from __future__ import annotations

import datetime as _dt
import itertools
import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import polars as pl

from .single_flight import DedupCache, SingleFlight

logger = logging.getLogger("app.services.stockdata.sources")

_HIST_TTL = 60.0  # 历史日线/分钟短 TTL（仅突发去重，不驻留）


def _tf_symbol(code: str) -> str:
    """平台代码(.XSHG/.XSHE/.SH/.SZ) -> 分区符号(.SH/.SZ)。"""
    pure, _, suf = code.rpartition(".")
    if not pure:
        return code
    return pure + (".SH" if suf in ("XSHG", "SH") else ".SZ")


def _to_jq(code: str) -> str:
    pure, _, suf = code.rpartition(".")
    if not pure:
        return code
    return pure + (".XSHG" if suf in ("SH", "XSHG") else ".XSHE")


def _is_index(code: str) -> bool:
    pure = code.split(".", 1)[0]
    return pure.startswith("399") or (pure.startswith("000") and len(pure) == 6
                                      and not pure.startswith("0000"))


def _in_trading(now: _dt.datetime | None = None) -> bool:
    """交易时段判定（口径同 quant.simulate.runner.in_trading）。"""
    now = now or _dt.datetime.now()
    t = now.time()
    return (now.weekday() < 5
            and (_dt.time(9, 30) <= t <= _dt.time(11, 30)
                 or _dt.time(13, 0) <= t <= _dt.time(15, 0)))


def _normalize_etf_volume_unit(df: pl.DataFrame) -> pl.DataFrame:
    """ETF 日线 volume 归一为「股」（同 DataManager._normalize_etf_volume_unit）。"""
    if df is None or df.is_empty() or "volume" not in df.columns:
        return df
    ratio = (pl.col("amount") / (pl.col("volume") * pl.col("close"))).alias("_ratio")
    per_sym = df.group_by("symbol", maintain_order=True).agg(ratio.first())
    hand_syms = per_sym.filter(pl.col("_ratio") > 50).select("symbol")
    if hand_syms.is_empty():
        return df
    hand_set = set(hand_syms["symbol"].to_list())
    return df.with_columns(
        pl.when(pl.col("symbol").is_in(hand_set))
        .then(pl.col("volume") * 100)
        .otherwise(pl.col("volume"))
        .alias("volume")
    )


_MINUTE_COLS = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]


class MinuteMemoryStore:
    """当日分钟内存库：纯 lazy dict，只存「客户端请求过、经网络拉到」的当日实时分钟。

    不预载、不预分配；换日 lazy 清空（scheduler 在 00:00 主动清一次）。
    """

    def __init__(self) -> None:
        self._frames: dict[str, pl.DataFrame] = {}
        self._day: _dt.date | None = None
        self._lock = threading.Lock()

    def day(self) -> _dt.date | None:
        with self._lock:
            return self._day

    def ensure_day(self, day: _dt.date) -> None:
        """换日 lazy 清空：内存库只保留 `day` 当天的数据。"""
        with self._lock:
            if self._day != day:
                self._frames.clear()
                self._day = day

    def clear(self) -> None:
        with self._lock:
            self._frames.clear()
            self._day = None

    def update(self, day: str, frames: list[pl.DataFrame]) -> None:
        """把网络拉到/当日分区的分钟帧并入内存库（same-day）。"""
        if not frames:
            return
        with self._lock:
            self._day = _dt.date.fromisoformat(day)
            for df in frames:
                if df.is_empty():
                    continue
                syms = set(df["symbol"].to_list())
                for sym in syms:
                    sub = df.filter(pl.col("symbol") == sym)
                    old = self._frames.get(sym)
                    merged = pl.concat([old, sub]).unique(
                        subset=["datetime"], keep="last").sort("datetime") if old is not None \
                        else sub
                    self._frames[sym] = merged

    def get_slice(self, symbols: set[str], lo_ts: str, hi_ts: str) -> pl.DataFrame:
        """取内存库中指定标的在 [lo_ts, hi_ts] 的当日分钟（空帧当无数据）。"""
        with self._lock:
            parts = [self._frames[s] for s in symbols if s in self._frames]
            if not parts:
                return pl.DataFrame(schema={c: pl.Utf8 for c in _MINUTE_COLS})
            df = pl.concat(parts).filter(
                (pl.col("datetime") >= pd_to_ts(lo_ts)) & (pl.col("datetime") <= pd_to_ts(hi_ts)))
            return df


class NetworkPuller:
    """服务端共享网络拉取线程池：有界并发 + 每线程独立数据源 + 标的级 single-flight。

    所有 handler 线程的实时回源都提交到此池：并发客户端请求的重叠标的内在
    ``rt:{code}`` 键上只拉一次，对 mootdx 的并发 socket 数被池上限约束。
    """

    def __init__(self, factory: Callable | None = None, workers: int = 16):
        self._factory = factory
        self._workers = max(1, workers)
        self._single = SingleFlight()
        self._local = threading.local()
        self._pool = ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="stockdata-pull")

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

    def _fetch_one(self, code: str) -> pl.DataFrame:
        df = _pull_recent_guarded(self._source(), code)
        if df is None or df.empty:
            return pl.DataFrame()
        pdf = df.reset_index()
        pdf["symbol"] = _tf_symbol(code)
        for c in _MINUTE_COLS:
            if c not in pdf.columns:
                pdf[c] = None
        return pl.from_pandas(pdf[_MINUTE_COLS])

    def fetch_minute(self, code: str) -> pl.DataFrame:
        """单只标的实时分钟（per-symbol 去重：同一分钟多请求只回源一次）。"""
        return self._single.run(f"rt:{code}", lambda: self._fetch_one(code))

    def fetch_many(self, codes: list[str]) -> list[pl.DataFrame]:
        futures = {self._pool.submit(self.fetch_minute, c): c for c in codes}
        out = []
        for f in futures:
            try:
                df = f.result()
            except Exception as e:  # noqa: BLE001
                logger.warning("[sources] 实时回源异常 %s: %s", f, e)
                continue
            if df is not None and not df.is_empty():
                out.append(df)
        return out

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False)


def _pull_recent_guarded(src, code: str, timeout: float = 30.0):
    """墙钟守护的单只 mootdx 实时分钟拉取；超时/异常返回空帧。"""
    import threading as _th
    box: dict = {}

    def _run():
        try:
            box["df"] = src.get_minute_recent(code, pages=1)
        except Exception as e:  # noqa: BLE001
            box["err"] = e

    t = _th.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        logger.warning("[sources] %s 实时回源超时(%ss)", code, timeout)
        return None
    if "err" in box:
        logger.warning("[sources] %s 实时回源失败: %s", code, box["err"])
        return None
    df = box.get("df")
    if df is None or df.empty:
        return None
    return df


class DataSources:
    """聚合源：本地分区读取为主 + 当日分钟内存库 + 共享网络拉取池。"""

    def __init__(self, data_root: str | None = None, mootdx_factory: Callable | None = None,
                 fetch_workers: int | None = None):
        self.data_root = data_root or os.getenv(
            "PARTITION_DATA_ROOT",
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "data"),
        )
        if fetch_workers is None:
            try:
                fetch_workers = int(os.getenv("STOCKDATA_FETCH_WORKERS", "") or 16)
            except (TypeError, ValueError):
                fetch_workers = 16
        self.dedup = DedupCache()
        self.minute_store = MinuteMemoryStore()
        self.puller = NetworkPuller(factory=mootdx_factory, workers=fetch_workers)
        self.fail_counts: dict[str, int] = {}
        self._fail_lock = threading.Lock()

    # ---- 去重透传 ----
    def get_or_fetch(self, key: str, ttl: float, loader: Callable) -> object:
        return self.dedup.get_or_fetch(key, ttl, loader)

    # ---- 分区扫描 ----
    def _scan_partitions(self, subdir: str, day_lo: str | None, day_hi: str | None,
                         symbols: set[str] | None, cols: list[str]) -> pl.DataFrame:
        root = os.path.join(self.data_root, subdir)
        if not os.path.isdir(root):
            return pl.DataFrame()
        paths = []
        for name in sorted(os.listdir(root)):
            if not name.startswith("date="):
                continue
            ds = name[len("date="):]
            if day_lo and ds < day_lo:
                continue
            if day_hi and ds > day_hi:
                continue
            import glob as _glob
            paths.extend(_glob.glob(os.path.join(root, name, "*.parquet")))
        if not paths:
            return pl.DataFrame()
        lf = pl.scan_parquet(paths, hive_partitioning=True)
        if symbols:
            lf = lf.filter(pl.col("symbol").is_in(list(symbols)))
        return lf.select(cols).collect()

    def _daily_days(self, lookback_days: int, asof: _dt.date | None) -> tuple[str | None, str | None]:
        end = asof or _dt.date.today()
        lo = end - _dt.timedelta(days=lookback_days * 2)  # 余量覆盖非交易日
        return lo.isoformat(), end.isoformat()

    def _load_daily(self, lookback_days: int, asof: _dt.date | None) -> pl.DataFrame:
        lo, hi = self._daily_days(lookback_days, asof)
        cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        parts = []
        for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False),
                                 ("kline_index_daily", False)):
            df = self._scan_partitions(subdir, lo, hi, None, cols)
            if df.is_empty():
                continue
            if is_stock:
                df = df.with_columns((pl.col("volume") * 100).alias("volume"))
            parts.append(df)
        if not parts:
            return pl.DataFrame()
        out = pl.concat(parts)
        out = _normalize_etf_volume_unit(out)
        if asof is not None:
            out = out.filter(pl.col("date") <= asof)
        return out

    def preload_daily(self, lookback_days: int = 400, asof: _dt.date | None = None) -> pl.DataFrame:
        key = f"preload_daily:{lookback_days}:{asof or ''}"
        return self.get_or_fetch(key, _HIST_TTL,
                                 lambda: self._load_daily(lookback_days, asof))

    def get_daily(self, codes: list[str], start_date: str, end_date: str) -> pl.DataFrame:
        syms = {_tf_symbol(c) for c in codes}
        cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        parts = []
        for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False),
                                 ("kline_index_daily", False)):
            df = self._scan_partitions(subdir, start_date, end_date, syms, cols)
            if df.is_empty():
                continue
            if is_stock:
                df = df.with_columns((pl.col("volume") * 100).alias("volume"))
            parts.append(df)
        if not parts:
            return pl.DataFrame()
        return _normalize_etf_volume_unit(pl.concat(parts))

    def get_minute(self, codes: list[str], lo_ts, hi_ts) -> pl.DataFrame:
        """历史分钟读分区；若请求范围包含今日，叠加当日分钟内存库（网络数据）。"""
        lo_d = str(pd_to_date(lo_ts)) if lo_ts is not None else None
        hi_d = str(pd_to_date(hi_ts)) if hi_ts is not None else None
        syms = {_tf_symbol(c) for c in codes}
        parts = []
        for subdir in ("kline_etf_minute", "kline_minute"):
            df = self._scan_partitions(subdir, lo_d, hi_d, syms, _MINUTE_COLS)
            if not df.is_empty():
                parts.append(df)
        today = _dt.date.today()
        if (lo_d is None or lo_d <= today.isoformat()) and (hi_d is None or hi_d >= today.isoformat()):
            mem = self.minute_store.get_slice(syms, str(lo_ts or today), str(hi_ts or f"{today} 15:00:00"))
            if not mem.is_empty():
                parts.append(mem)
        if not parts:
            return pl.DataFrame()
        out = pl.concat(parts).unique(subset=["symbol", "datetime"], keep="last")
        if lo_ts is not None:
            out = out.filter(pl.col("datetime") >= pd_to_ts(lo_ts))
        if hi_ts is not None:
            out = out.filter(pl.col("datetime") <= pd_to_ts(hi_ts))
        return out

    def _bump_fail(self, code: str) -> None:
        with self._fail_lock:
            self.fail_counts[code] = self.fail_counts.get(code, 0) + 1

    def get_realtime_snapshot(self, codes: list[str], as_of=None) -> pl.DataFrame:
        """当日分钟内存库 + 未覆盖标的共享拉取池按需补实时（per-symbol 去重）。

        实时回源只在交易时段执行；非交易时段只读内存库 + 当日分区（不触网）。
        指数标的（仅用日线）不参与实时回源。
        """
        asof_ts = pd_to_ts(as_of) if as_of is not None else _dt.datetime.now()
        today = asof_ts.date()
        tf_syms = {_tf_symbol(c) for c in codes}
        self.minute_store.ensure_day(today)

        # 基础帧：当日分区（收盘同步/重启场景）+ 内存库（网络实时）
        base_parts = []
        part = self._scan_partitions("kline_etf_minute", today.isoformat(),
                                     today.isoformat(), tf_syms, _MINUTE_COLS)
        if not part.is_empty():
            base_parts.append(part)
        mem = self.minute_store.get_slice(tf_syms, f"{today} 00:00:00", str(asof_ts))
        if not mem.is_empty():
            base_parts.append(mem)
        base = pl.concat(base_parts).unique(subset=["symbol", "datetime"], keep="last") \
            if base_parts else pl.DataFrame(schema={c: pl.Utf8 for c in _MINUTE_COLS})

        # 未覆盖：内存缺失，或内存最新 bar < asof（过期）。指数跳过。非交易时段不拉。
        latest_by_sym = {}
        for sym, mx in base.group_by("symbol").agg(pl.col("datetime").max()).iter_rows():
            latest_by_sym[sym] = mx
        todo = [c for c in codes
                if _in_trading(asof_ts) and not _is_index(c)
                and (_tf_symbol(c) not in latest_by_sym
                     or latest_by_sym[_tf_symbol(c)] < asof_ts - _dt.timedelta(minutes=3))]
        fills: list[pl.DataFrame] = []
        if todo:
            pulls = self.puller.fetch_many(todo)
            for df in pulls:
                if not df.is_empty():
                    fills.append(df)
            # 更新当日分钟内存库（网络数据才驻留）
            if fills:
                self.minute_store.update(today.isoformat(), fills)

        out = pl.concat(base_parts + fills).unique(subset=["symbol", "datetime"], keep="last") \
            if (fills or base_parts) else base
        out = out.filter(pl.col("datetime") <= asof_ts)
        return out.sort(["symbol", "datetime"])

    # ---- 元数据 ----
    def get_trade_days(self, start_date: str, end_date: str) -> list[str]:
        # 交易日历：从 kline_index_daily 分区索引推（沪深300 恒有数据）
        df = self._scan_partitions("kline_index_daily", start_date, end_date, None,
                                   ["date"]).unique(subset=["date"])
        return sorted(str(d) for d in df["date"].to_list())

    def get_all_securities(self, types: list[str] | None, date: str | None) -> pl.DataFrame:
        cols = ["symbol", "name", "list_date"]
        parts = []
        if types is None or "stock" in types:
            df = self._scan_partitions("kline_daily", None, None, None, cols) if os.path.isdir(
                os.path.join(self.data_root, "kline_daily")) else pl.DataFrame()
            if not df.is_empty():
                parts.append(df.select(["symbol", "name", "list_date"])
                              .unique(subset=["symbol"]).with_columns(pl.lit("stock").alias("type")))
        if types is None or "etf" in types:
            df = self._scan_partitions("kline_etf_daily", None, None, None, cols) if os.path.isdir(
                os.path.join(self.data_root, "kline_etf_daily")) else pl.DataFrame()
            if not df.is_empty():
                parts.append(df.select(["symbol", "name", "list_date"])
                              .unique(subset=["symbol"]).with_columns(pl.lit("etf").alias("type")))
        if types is None or "index" in types:
            df = self._scan_partitions("kline_index_daily", None, None, None, cols) if os.path.isdir(
                os.path.join(self.data_root, "kline_index_daily")) else pl.DataFrame()
            if not df.is_empty():
                parts.append(df.select(["symbol", "name", "list_date"])
                              .unique(subset=["symbol"]).with_columns(pl.lit("index").alias("type")))
        if not parts:
            return pl.DataFrame(schema={"symbol": pl.Utf8, "name": pl.Utf8,
                                        "list_date": pl.Utf8, "type": pl.Utf8})
        return pl.concat(parts)

    def get_security_info(self, code: str) -> dict:
        df = self.get_all_securities(None, None)
        sym = _tf_symbol(code)
        row = df.filter(pl.col("symbol") == sym)
        if row.is_empty():
            return {}
        r = row.to_dicts()[0]
        return {"code": code, "name": r.get("name"), "type": r.get("type"),
                "start_date": r.get("list_date"), "end_date": None}

    def get_index_stocks(self, index_code: str, date: str | None) -> list[str]:
        # 成分股暂以全市场股票日线标的近似（不维护成分表）；有成分表后替换
        df = self._scan_partitions("kline_daily", None, None, None, ["symbol"])
        return sorted(set(df["symbol"].to_list()))

    def get_stock_names(self, codes: list[str] | None = None) -> dict:
        df = self._scan_partitions("kline_daily", None, None, None,
                                   ["symbol", "name"]).unique(subset=["symbol"])
        out = {}
        for r in df.iter_rows(named=True):
            if codes is None or _tf_symbol(r["symbol"]) in {_tf_symbol(c) for c in codes}:
                out[_tf_symbol(r["symbol"])] = r.get("name")
        return out

    def get_adj_factors(self) -> pl.DataFrame:
        root = os.path.join(self.data_root, "adj_factor_etf")
        if not os.path.isdir(root):
            return pl.DataFrame()
        import glob as _glob
        paths = _glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True)
        if not paths:
            return pl.DataFrame()
        lf = pl.scan_parquet(paths, hive_partitioning=True)
        cols = lf.columns
        if "symbol" not in cols:
            lf = lf.with_columns(pl.lit("").alias("symbol"))
        return lf.select(["symbol", "trade_date", "ex_factor"]).collect()


def pd_to_ts(x):
    import pandas as pd
    return pd.Timestamp(x)


def pd_to_date(x):
    import pandas as pd
    return pd.Timestamp(x).date()
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/stockdata/sources.py backend/tests/quant/test_stockdata_sources.py
git commit -m "feat(stockdata): 数据源聚合 + 当日分钟内存库 + 共享网络拉取线程池"

> **Task 4 review 修复（已并入实现，本代码块为设计底稿、已过时）**：commit `5d35d5f` 修复——(1) 空 base 帧 Utf8 vs datetime 过滤崩溃（返回空帧短路）；(2) `_is_index` 加后缀判断（仅沪市 000xxx 是指数，深市 000001.XSHE 是股票）；(3) `get_all_securities`/`get_security_info`/`get_stock_names` 只 select 分区实际有的 `symbol` 列（name/list_date 置空）；(4) `_pull_recent_guarded` 超时抛 `TimeoutError`，`_fetch_one` 重建线程局部源；(5) `get_daily`/`get_minute` 包 `get_or_fetch`（`daily:`/`min:` 键，minute 短 TTL 10s）；(6) 删未用 `itertools` 与死代码 `fail_counts`（`h_status` 不再引用）。Task 5 `h_status` 代码块已按此更新。
```

---

### Task 5: handlers.py（method 分发）

**Files:**
- Create: `backend/app/services/stockdata/handlers.py`
- Test: `backend/tests/quant/test_stockdata_handlers.py`

**Interfaces:**
- Consumes: `DataSources`（Task 4 全部方法）。
- Produces: `HANDLERS: dict[str, Callable[[dict], tuple[str, object]]]`——每个 handler 返回 `("json"|"parquet", data)`。`get_price`/`preload_daily`/`get_minute`/`current_snapshot`/`get_all_securities`/`get_adj_factors` 返回 parquet；其余 json。

- [ ] **Step 1: 写失败测试**

```python
# backend/tests/quant/test_stockdata_handlers.py
import datetime as _dt
import os

import pytest

from app.services.stockdata.handlers import HANDLERS, handle
from app.services.stockdata.sources import DataSources


@pytest.fixture
def src(tmp_path):
    import polars as pl
    day = _dt.date.today().isoformat()
    d = os.path.join(str(tmp_path), "kline_etf_minute", f"date={day}")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame({
        "symbol": ["512670.SH"], "datetime": [f"{day} 10:00:00"],
        "open": [1.0], "high": [1.1], "low": [0.9], "close": [1.05],
        "volume": [1000], "amount": [1050.0],
    }).write_parquet(os.path.join(d, "part.parquet"))
    return DataSources(data_root=str(tmp_path), mootdx_factory=None)


def test_handlers_registered():
    for m in ("ping", "status", "get_price", "current_snapshot", "preload_daily",
              "get_minute", "get_trade_days", "get_all_securities",
              "get_security_info", "get_index_stocks", "get_stock_names",
              "get_adj_factors", "trigger_sync"):
        assert m in HANDLERS, m


def test_get_price_minute(src):
    t, data = handle("get_price", {"security": "512670.XSHG",
                                   "frequency": "1m"}, src)
    assert t == "parquet"
    assert data["close"].to_list() == [1.05]


def test_ping(src):
    t, data = handle("ping", {}, src)
    assert t == "json" and data["pong"] is True
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_handlers.py -q`
Expected: FAIL

- [ ] **Step 3: 实现 handlers.py**

```python
# backend/app/services/stockdata/handlers.py
"""method → handler 分发。每个 handler 返回 ("json"|"parquet", data)，
parquet 的 data 是 polars DataFrame，由 server 编码为 parquet 字节。"""
from __future__ import annotations

import datetime as _dt
import logging
from typing import Any

import polars as pl

from .sources import DataSources, _is_index, _tf_symbol, _to_jq

logger = logging.getLogger("app.services.stockdata.handlers")


def _norm_code(code: str) -> str:
    """客户端可能传 .XSHG/.XSHE/.SH/.SZ/裸 6 位，统一为 .XSHG/.XSHE。"""
    pure = code.split(".", 1)[0]
    return _to_jq(pure) if "." not in code else _to_jq(code)


def _norm_codes(security) -> list[str]:
    if isinstance(security, str):
        return [_norm_code(security)]
    return [_norm_code(c) for c in security]


def h_ping(p, s: DataSources):
    return "json", {"pong": True, "ts": _dt.datetime.now().isoformat()}


def h_status(p, s: DataSources):
    # 注：DataSources 不再维护 fail_counts（Task4 review 已移除），状态仅回显基础信息
    return "json", {"ok": True, "ts": _dt.datetime.now().isoformat()}


def h_get_price(p, s: DataSources):
    codes = _norm_codes(p["security"])
    freq = p.get("frequency", "daily")
    start = p.get("start_date")
    end = p.get("end_date")
    if freq == "daily":
        df = s.get_daily(codes, start or "2000-01-01", end or _dt.date.today().isoformat())
        return "parquet", df
    # 分钟：区间内（或当日）1m
    if not start or not end:
        today = _dt.date.today().isoformat()
        start, end = start or today, end or today
    df = s.get_minute(codes, start + " 00:00:00", end + " 15:00:00")
    return "parquet", df


def h_preload_daily(p, s: DataSources):
    lookback = int(p.get("lookback_days", 400))
    asof = p.get("asof")
    df = s.preload_daily(lookback, _dt.date.fromisoformat(asof) if asof else None)
    return "parquet", df


def h_get_minute(p, s: DataSources):
    codes = _norm_codes(p["security"])
    df = s.get_minute(codes, p.get("lo_ts"), p.get("hi_ts"))
    return "parquet", df


def h_current_snapshot(p, s: DataSources):
    codes = _norm_codes(p["security"])
    df = s.get_realtime_snapshot(codes, p.get("as_of"))
    return "parquet", df


def h_get_trade_days(p, s: DataSources):
    return "json", s.get_trade_days(p.get("start_date", "2000-01-01"),
                                    p.get("end_date", _dt.date.today().isoformat()))


def h_get_all_securities(p, s: DataSources):
    types = p.get("types")
    df = s.get_all_securities(types, p.get("date"))
    return "parquet", df


def h_get_security_info(p, s: DataSources):
    return "json", s.get_security_info(_norm_code(p["code"]))


def h_get_index_stocks(p, s: DataSources):
    return "json", s.get_index_stocks(_norm_code(p["index_code"]), p.get("date"))


def h_get_stock_names(p, s: DataSources):
    codes = p.get("codes")
    return "json", s.get_stock_names(codes)


def h_get_adj_factors(p, s: DataSources):
    return "parquet", s.get_adj_factors()


def h_trigger_sync(p, s: DataSources):
    from .scheduler import trigger_sync
    kind = p.get("kind", "backfill")
    trigger_sync(kind)
    return "json", {"ok": True, "kind": kind}


HANDLERS = {
    "ping": h_ping,
    "status": h_status,
    "get_price": h_get_price,
    "preload_daily": h_preload_daily,
    "get_minute": h_get_minute,
    "current_snapshot": h_current_snapshot,
    "get_trade_days": h_get_trade_days,
    "get_all_securities": h_get_all_securities,
    "get_security_info": h_get_security_info,
    "get_index_stocks": h_get_index_stocks,
    "get_stock_names": h_get_stock_names,
    "get_adj_factors": h_get_adj_factors,
    "trigger_sync": h_trigger_sync,
}


def handle(method: str, params: dict, src: DataSources):
    fn = HANDLERS.get(method)
    if fn is None:
        raise ValueError(f"未知 method: {method}")
    return fn(params, src)
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_handlers.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/stockdata/handlers.py backend/tests/quant/test_stockdata_handlers.py
git commit -m "feat(stockdata): method 分发 handlers（json/parquet 响应）"
```

---

### Task 6: scheduler.py（自治调度 + trigger_sync 全局入口）

**Files:**
- Create: `backend/app/services/stockdata/scheduler.py`

**Interfaces:**
- Consumes: `mootdx_service.backfill_to_now`、`mootdx_service.sync_etf_minute`、`sync_adj_factor`、`sync_stock_minute`、`STOCK_MINUTE_BATCH_LIMIT`；`DataSources.minute_store`（00:00 清空）。
- Produces: `start_scheduler()`（启动 backfill 线程 + intraday 线程 + 15:35 cron 线程）；`stop_scheduler()`；`trigger_sync(kind)`（进程级单例，供 handler 手动触发）；`_scheduler_state: dict`（回源进度，供 status）。

- [ ] **Step 1: 实现 scheduler.py（无测试，随 Task 8 集成验证）**

```python
# backend/app/services/stockdata/scheduler.py
"""自治调度：启动 backfill + 15:35 收盘批量同步 + 00:00 清空当日分钟内存（线程）。

无主动盘中全市场轮询——实时分钟只在客户端请求时按需回源（见 sources.get_realtime_snapshot）。"""
from __future__ import annotations

import datetime as _dt
import logging
import threading
import time

logger = logging.getLogger("app.services.stockdata.scheduler")

_scheduler_state: dict = {"last_backfill": None, "last_sync": None, "sync_job": None}
_lock = threading.Lock()
_stop = threading.Event()
_threads: list[threading.Thread] = []
_sync_lock = threading.Lock()  # 15:35 cron 与手动 trigger 串行


def _backfill_loop():
    try:
        from app.services import mootdx_service
        res = mootdx_service.backfill_to_now()
        with _lock:
            _scheduler_state["last_backfill"] = str(_dt.datetime.now())
            _scheduler_state["backfill_result"] = res
        logger.info("stockdata startup backfill done: %s", res)
    except Exception:  # noqa: BLE001
        logger.exception("stockdata startup backfill failed")


def _run_sync():
    with _sync_lock:
        try:
            from app.services import mootdx_service
            minutes = mootdx_service.sync_etf_minute()
            adj = mootdx_service.sync_adj_factor()
            stock = mootdx_service.sync_stock_minute(
                limit=mootdx_service.STOCK_MINUTE_BATCH_LIMIT)
            with _lock:
                _scheduler_state["last_sync"] = str(_dt.datetime.now())
                _scheduler_state["sync_result"] = {
                    "minute_rows": minutes, "adj": adj, "stock_minute_rows": stock}
            logger.info("scheduled mootdx sync done: minute=%d rows, adj=%s, stock_minute_rows=%d",
                        minutes, adj, stock)
        except Exception:  # noqa: BLE001
            logger.exception("scheduled mootdx sync failed")


def _sync_cron_loop():
    """15:35（工作日）触发 _run_sync；非交易日不触发。"""
    while not _stop.is_set():
        now = _dt.datetime.now()
        if (now.weekday() < 5 and now.time() >= _dt.time(15, 35)
                and now.time() < _dt.time(15, 36)):
            with _lock:
                last = _scheduler_state.get("sync_job")
            if last != now.date().isoformat():
                _scheduler_state["sync_job"] = now.date().isoformat()
                threading.Thread(target=_run_sync, daemon=True).start()
        time.sleep(30)


def _midnight_clear_loop(data_sources) -> None:
    """次日 00:00 清空当日分钟内存库（前一日网络实时数据不跨日驻留）。"""
    last_date = _dt.date.today()
    while not _stop.is_set():
        time.sleep(30)
        today = _dt.date.today()
        if today != last_date:
            if today > last_date:  # 日期前跳（改系统时钟）时也兜底
                data_sources.minute_store.clear()
                logger.info("stockdata minute store cleared at midnight (new day %s)", today)
            last_date = today


def trigger_sync(kind: str) -> dict:
    """手动触发同步（供 handler 调用）。kind: backfill|daily|etf_minute|stock_minute|adj_factor"""
    if kind == "backfill":
        threading.Thread(target=_backfill_loop, daemon=True).start()
    else:
        threading.Thread(target=_run_sync, daemon=True).start()
    return {"ok": True}


def start_scheduler(data_sources=None) -> None:
    if _threads:
        return
    _stop.clear()
    targets = [_backfill_loop, _sync_cron_loop]
    if data_sources is not None:
        targets.append(lambda: _midnight_clear_loop(data_sources))
    for target in targets:
        t = threading.Thread(target=target, name=f"stockdata-{target.__name__}", daemon=True)
        t.start()
        _threads.append(t)
    logger.info("stockdata scheduler started")


def stop_scheduler() -> None:
    _stop.set()
```
（说明：无主动 intraday 全市场轮询——实时分钟只在客户端请求时按需回源，见 Task 4 `get_realtime_snapshot`。）

- [ ] **Step 2: Commit**

```bash
git add backend/app/services/stockdata/scheduler.py
git commit -m "feat(stockdata): 自治调度（启动 backfill + 15:35 收盘同步 + 00:00 内存清空）"
```

---

### Task 7: server.py + run_stockdata_service.py 入口

**Files:**
- Create: `backend/app/services/stockdata/server.py`
- Create: `backend/scripts/run_stockdata_service.py`

**Interfaces:**
- Consumes: `protocol.encode_response/decode_frame`、`handle(method, params, src)`、`DataSources`、`scheduler.start_scheduler/stop_scheduler/trigger_sync`。
- Produces: `StockDataServer`（ThreadingTCPServer 子类，`serve_forever`）；`scripts/run_stockdata_service.py` 进程入口（端口 env `STOCKDATA_PORT` 默认 3322、`STOCKDATA_HOST` 默认 127.0.0.1，SIGTERM 优雅退出）。

- [ ] **Step 1: 实现 server.py**

```python
# backend/app/services/stockdata/server.py
"""ThreadingTCPServer：每连接一线程，连接内循环读帧→分发→响应。"""
from __future__ import annotations

import logging
import socketserver

from . import protocol
from .handlers import handle
from .sources import DataSources

logger = logging.getLogger("app.services.stockdata.server")


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:  # noqa: D102
        src: DataSources = self.server.data_sources  # type: ignore[attr-defined]
        try:
            while True:
                msg = protocol.decode_frame(self.request)
                req_id = msg["id"]
                try:
                    t, data = handle(msg["m"], msg.get("p") or {}, src)
                    resp = protocol.encode_response(req_id, True, t, data)
                except Exception as e:  # noqa: BLE001
                    logger.warning("[stockdata] method=%s 失败: %s", msg.get("m"), e)
                    resp = protocol.encode_response(
                        req_id, False, "json",
                        {"code": type(e).__name__, "msg": str(e)})
                self.request.sendall(resp)
        except (EOFError, ConnectionError):
            return
        except Exception:  # noqa: BLE001
            logger.exception("[stockdata] 连接处理异常")


class StockDataServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: tuple[str, int], data_sources: DataSources):
        self.data_sources = data_sources
        super().__init__(addr, _Handler)
```

- [ ] **Step 2: 实现进程入口**

```python
# backend/scripts/run_stockdata_service.py
"""stock data 服务独立进程：TCP server + 自治调度（FastAPI 主进程托管守护）。"""
from __future__ import annotations

import logging
import os
import signal
import threading

from app.services.stockdata import scheduler
from app.services.stockdata.server import StockDataServer
from app.services.stockdata.sources import DataSources

HOST = os.getenv("STOCKDATA_HOST", "127.0.0.1")


def _port() -> int:
    try:
        return int(os.getenv("STOCKDATA_PORT", "") or 3322)
    except (TypeError, ValueError):
        return 3322


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    stop = threading.Event()

    def _sig(_signum, _frame) -> None:
        stop.set()
        scheduler.stop_scheduler()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    src = DataSources()
    server = StockDataServer((HOST, _port()), src)
    scheduler.start_scheduler(data_sources=src)  # backfill + 15:35 + 00:00 内存清空
    logger = logging.getLogger("stockdata")
    logger.info("stockdata service listening on %s:%s", HOST, _port())
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: 冒烟启动服务**

Run: `STOCKDATA_PORT=3333 uv run --extra dev python scripts/run_stockdata_service.py > /tmp/stockdata-smoke.log 2>&1 & sleep 3; head -5 /tmp/stockdata-smoke.log`
Expected: 日志含 `stockdata service listening on 127.0.0.1:3333`。然后 `kill %1`。

- [ ] **Step 4: Commit**

```bash
git add backend/app/services/stockdata/server.py backend/scripts/run_stockdata_service.py
git commit -m "feat(stockdata): TCP server 进程入口"
```

---

### Task 8: 客户端 SDK network_client.py + 集成测试

**Files:**
- Create: `backend/app/quant/datasource/network_client.py`
- Test: `backend/tests/quant/test_network_client.py`

**Interfaces:**
- Produces（量化侧唯一取数入口；本 task 后 Task 9-16 全部消费它）：
  - `class StockDataClient`；`__init__(host=None, port=None, timeout=120, connect_timeout=5)`
  - `ping() -> dict`；`status() -> dict`；`trigger_sync(kind) -> dict`
  - `get_price(security, start_date=None, end_date=None, frequency='daily', fields=None) -> dict[str, pd.DataFrame]`（key 为 jq 代码；df 为 DatetimeIndex + OHLCV/volume/amount/trade_dt）
  - `current_snapshot(codes, as_of=None) -> dict[str, pd.DataFrame]`
  - `preload_daily(lookback_days=400, asof=None) -> dict[str, pd.DataFrame]`
  - `get_minute_pool(codes, lo_ts, hi_ts) -> dict[str, pd.DataFrame]`
  - `get_trade_days(start_date, end_date) -> list[str]`
  - `get_all_securities(types=None, date=None) -> pd.DataFrame`
  - `get_security_info(code) -> dict`
  - `get_index_stocks(index_code, date=None) -> list[str]`
  - `get_stock_names(codes=None) -> dict`
  - `get_adj_factors() -> pd.DataFrame`
  - `close()`

- [ ] **Step 1: 写失败测试（进程内起真实 server）**

```python
# backend/tests/quant/test_network_client.py
import datetime as _dt
import os
import threading

import pandas as pd
import pytest
import polars as pl

from app.quant.datasource.network_client import StockDataClient
from app.services.stockdata.server import StockDataServer
from app.services.stockdata.sources import DataSources


@pytest.fixture
def server_and_client(tmp_path, monkeypatch):
    import socket
    root = tmp_path / "sd"
    day = _dt.date.today().isoformat()
    for sub, sym, ts, close in (
        ("kline_etf_minute", "512670.SH", f"{day} 10:00:00", 1.05),
        ("kline_etf_minute", "159919.SZ", f"{day} 10:00:00", 3.10),
    ):
        d = os.path.join(str(root), sub, f"date={day}")
        os.makedirs(d, exist_ok=True)
        pl.DataFrame({
            "symbol": [sym], "datetime": [ts], "open": [close], "high": [close],
            "low": [close], "close": [close], "volume": [1000], "amount": [close * 1000.0],
        }).write_parquet(os.path.join(d, "part.parquet"))
    # 非交易时段门控：current_snapshot 不触网（fixture 无 mootdx，测试只验读路径）
    monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: False)
    srv = StockDataServer(("127.0.0.1", 0), DataSources(data_root=str(root), mootdx_factory=None))
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    cli = StockDataClient(port=port)
    yield cli, port
    cli.close()
    srv.shutdown()
    srv.server_close()


def test_ping(server_and_client):
    cli, _ = server_and_client
    assert cli.ping()["pong"] is True


def test_get_price_minute_multi(server_and_client):
    cli, _ = server_and_client
    out = cli.get_price(["512670.XSHG", "159919.XSHE"], frequency="1m",
                        start_date=_dt.date.today().isoformat(),
                        end_date=_dt.date.today().isoformat())
    assert set(out) == {"512670.XSHG", "159919.XSHE"}
    assert out["512670.XSHG"]["close"].iloc[0] == 1.05
    assert isinstance(out["512670.XSHG"].index, pd.DatetimeIndex)


def test_current_snapshot(server_and_client):
    cli, _ = server_and_client
    snap = cli.current_snapshot(["512670.XSHG", "159919.XSHE"])
    assert "512670.XSHG" in snap
    assert snap["512670.XSHG"]["close"].iloc[-1] == 1.05
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_network_client.py -q`
Expected: FAIL（`ModuleNotFoundError`）

- [ ] **Step 3: 实现 network_client.py**

```python
# backend/app/quant/datasource/network_client.py
"""jqdata 风格网络行情客户端（stock data 服务唯一取数入口）。

量化侧（回测/模拟盘）只能经此类取数：不读本地 parquet、不直连 mootdx/astock。
返回帧均为 DatetimeIndex + OHLCV/volume/amount(+trade_dt)，按 jq 代码分帧。
"""
from __future__ import annotations

import io
import itertools
import logging
import os
import socket
import threading
import time

import msgpack
import pandas as pd
import polars as pl

log = logging.getLogger("app.quant.datasource.network_client")

_HEADER = 4


def _default_host() -> str:
    return os.getenv("STOCKDATA_HOST", "127.0.0.1")


def _default_port() -> int:
    try:
        return int(os.getenv("STOCKDATA_PORT", "") or 3322)
    except (TypeError, ValueError):
        return 3322


def _to_jq(code: str) -> str:
    pure, _, suf = code.rpartition(".")
    if not pure:
        return code
    return pure + (".XSHG" if suf in ("SH", "XSHG") else ".XSHE")


class StockDataClient:
    def __init__(self, host: str | None = None, port: int | None = None,
                 timeout: float = 120.0, connect_timeout: float = 5.0):
        self.host = host or _default_host()
        self.port = port or _default_port()
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self._ids = itertools.count(1)
        self._sock: socket.socket | None = None
        self._sock_lock = threading.Lock()

    # ---- 连接管理 ----
    def _connect(self) -> socket.socket:
        s = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        s.settimeout(self.timeout)
        self._sock = s
        return s

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("连接已关闭")
            buf.extend(chunk)
        return bytes(buf)

    def _request(self, method: str, params: dict, retry: int = 3):
        payload = msgpack.packb(
            {"v": 1, "id": next(self._ids), "m": method, "p": params},
            use_bin_type=True)
        frame = len(payload).to_bytes(_HEADER, "big") + payload
        last: Exception | None = None
        for attempt in range(retry):
            with self._sock_lock:
                try:
                    if self._sock is None:
                        self._connect()
                    self._sock.sendall(frame)
                    n = int.from_bytes(self._recv_exact(_HEADER), "big")
                    resp = msgpack.unpackb(self._recv_exact(n), raw=False)
                    if not resp.get("ok"):
                        d = resp.get("d") or {}
                        raise RuntimeError(f"{method} 失败: {d.get('msg')} ({d.get('code')})")
                    return resp
                except Exception as e:  # noqa: BLE001
                    last = e
                    try:
                        if self._sock is not None:
                            self._sock.close()
                    except Exception:
                        pass
                    self._sock = None
            if attempt < retry - 1:
                time.sleep(min(0.5 * (2 ** attempt), 5.0))
        raise ConnectionError(f"stock data 服务不可达 ({self.host}:{self.port}): {last}")

    def _parquet_to_dict(self, resp: dict) -> dict[str, pd.DataFrame]:
        """parquet 响应 → {jq_code: DatetimeIndex df}。"""
        if resp["t"] != "parquet":
            return {}
        raw = resp["d"]
        if not raw:
            return {}
        df = pl.read_parquet(io.BytesIO(raw))
        if df.is_empty():
            return {}
        pdf = df.to_pandas()
        has_date = "date" in pdf.columns
        ts_col = "date" if has_date else "datetime"
        pdf = pdf.set_index(pd.to_datetime(pdf[ts_col]))
        pdf.index.name = None
        drop = ["symbol", ts_col]
        out: dict[str, pd.DataFrame] = {}
        for sym, g in pdf.groupby("symbol"):
            sub = g.drop(columns=[c for c in drop if c in g.columns]).copy()
            if has_date:
                sub["trade_dt"] = pd.to_datetime(g.index.normalize()).values
            out[_to_jq(sym)] = sub.sort_index()
        return out

    # ---- 行情 ----
    def get_price(self, security, start_date=None, end_date=None, frequency="daily",
                  fields=None) -> dict[str, pd.DataFrame]:
        resp = self._request("get_price", {
            "security": security, "start_date": str(start_date) if start_date else None,
            "end_date": str(end_date) if end_date else None,
            "frequency": frequency, "fields": fields})
        return self._parquet_to_dict(resp)

    def get_minute_pool(self, codes, lo_ts, hi_ts) -> dict[str, pd.DataFrame]:
        resp = self._request("get_minute", {
            "security": list(codes),
            "lo_ts": str(lo_ts) if lo_ts is not None else None,
            "hi_ts": str(hi_ts) if hi_ts is not None else None})
        return self._parquet_to_dict(resp)

    def current_snapshot(self, codes, as_of=None) -> dict[str, pd.DataFrame]:
        resp = self._request("current_snapshot", {
            "security": list(codes),
            "as_of": str(as_of) if as_of is not None else None})
        return self._parquet_to_dict(resp)

    def preload_daily(self, lookback_days: int = 400, asof=None) -> dict[str, pd.DataFrame]:
        resp = self._request("preload_daily", {
            "lookback_days": lookback_days,
            "asof": str(asof) if asof is not None else None})
        return self._parquet_to_dict(resp)

    def get_adj_factors(self) -> pd.DataFrame:
        resp = self._request("get_adj_factors", {})
        if resp["t"] != "parquet" or not resp["d"]:
            return pd.DataFrame()
        return pl.read_parquet(io.BytesIO(resp["d"])).to_pandas()

    # ---- 列表/元数据 ----
    def get_trade_days(self, start_date, end_date) -> list[str]:
        return self._request("get_trade_days", {
            "start_date": str(start_date), "end_date": str(end_date)})["d"]

    def get_all_securities(self, types=None, date=None) -> pd.DataFrame:
        resp = self._request("get_all_securities", {"types": types, "date": date})
        if resp["t"] != "parquet" or not resp["d"]:
            return pd.DataFrame()
        return pl.read_parquet(io.BytesIO(resp["d"])).to_pandas()

    def get_security_info(self, code) -> dict:
        return self._request("get_security_info", {"code": code})["d"]

    def get_index_stocks(self, index_code, date=None) -> list[str]:
        return self._request("get_index_stocks", {"index_code": index_code, "date": date})["d"]

    def get_stock_names(self, codes=None) -> dict:
        return self._request("get_stock_names", {"codes": codes})["d"]

    # ---- 运维 ----
    def ping(self) -> dict:
        return self._request("ping", {})

    def status(self) -> dict:
        return self._request("status", {})["d"]

    def trigger_sync(self, kind: str) -> dict:
        return self._request("trigger_sync", {"kind": kind})["d"]

    def close(self) -> None:
        with self._sock_lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_network_client.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/datasource/network_client.py backend/tests/quant/test_network_client.py
git commit -m "feat(stockdata): jqdata 风格客户端 SDK + server 集成测试"
```

---

## 阶段三：主后端集成

### Task 9: guardian 泛化 + main.py 守护 + 删除回源 cron

**Files:**
- Create: `backend/app/services/stockdata_guardian.py`
- Modify: `backend/app/main.py`（替换 intraday guardian + 删除启动 backfill）
- Modify: `backend/app/jobs/daily_pipeline.py`（删除 mootdx_sync cron，`_mootdx_sync` 函数）
- Modify: `backend/scripts/run_quant_sim.py`（确认无 mootdx 依赖）
- Test: `backend/tests/test_stockdata_guardian.py`

**Interfaces:**
- Consumes: `scripts/run_stockdata_service.py` 路径。
- Produces: `StockDataGuardian`（`start()`/`stop()`，PID 锁 + 3s 自愈）。

- [ ] **Step 1: 写失败测试**

```python
# backend/tests/test_stockdata_guardian.py
import os
import subprocess
import sys
import time
from pathlib import Path

from app.services.stockdata_guardian import StockDataGuardian


def test_guardian_restarts_died_process(tmp_path):
    script = tmp_path / "sleepy.py"
    script.write_text("import time, sys\ntime.sleep(60)\n")
    pidfile = tmp_path / "proc.pid"
    g = StockDataGuardian(pidfile=pidfile, script=script, logfile=tmp_path / "p.log")
    g.start()
    try:
        pid = int(pidfile.read_text().strip())
        assert os.path.exists(f"/proc/{pid}")
        os.kill(pid, 9)
        time.sleep(4.5)  # 3s poll + 余量
        new_pid = int(pidfile.read_text().strip())
        assert new_pid != pid
        assert os.path.exists(f"/proc/{new_pid}")
    finally:
        g.stop()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/test_stockdata_guardian.py -q`
Expected: FAIL（`ModuleNotFoundError`）

- [ ] **Step 3: 实现 stockdata_guardian.py（泛化自 intraday_guardian）**

```python
# backend/app/services/stockdata_guardian.py
"""FastAPI 主进程托管 stock data 服务子进程：单实例 PID 锁 + 3s 守护自愈。"""
from __future__ import annotations

import contextlib
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

logger = logging.getLogger("app.services.stockdata_guardian")


class StockDataGuardian:
    """托管 ``scripts/run_stockdata_service.py``：崩了 3s 内自动重启。"""

    def __init__(self, pidfile: Path, script: Path, logfile: Path | None = None,
                 poll_interval: float = 3.0):
        self.pidfile = Path(pidfile)
        self.script = Path(script)
        self.logfile = Path(logfile) if logfile else Path(pidfile.parent) / "stockdata.log"
        self._poll_interval = poll_interval
        self.proc: subprocess.Popen | None = None
        self._stop = threading.Event()

    def _kill_orphan(self) -> None:
        if not self.pidfile.exists():
            return
        try:
            old = int(self.pidfile.read_text().strip())
        except (ValueError, OSError):
            self.pidfile.unlink(missing_ok=True)
            return
        if old and os.path.exists(f"/proc/{old}"):
            try:
                os.killpg(old, signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                pass
            for _ in range(50):
                if not os.path.exists(f"/proc/{old}"):
                    break
                time.sleep(0.1)
            if os.path.exists(f"/proc/{old}"):
                try:
                    os.killpg(old, signal.SIGKILL)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
                for _ in range(50):
                    if not os.path.exists(f"/proc/{old}"):
                        break
                    time.sleep(0.1)
        self.pidfile.unlink(missing_ok=True)

    def _spawn(self) -> None:
        self.pidfile.parent.mkdir(parents=True, exist_ok=True)
        logf = open(self.logfile, "a")
        self.proc = subprocess.Popen(
            [sys.executable, str(self.script)],
            stdout=logf, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.pidfile.write_text(str(self.proc.pid))

    def _watch(self) -> None:
        while not self._stop.is_set():
            time.sleep(self._poll_interval)
            if self._stop.is_set():
                return
            if self.proc is None or self.proc.poll() is not None:
                if self._stop.is_set():
                    return
                logger.warning("stockdata service died, respawning")
                self._spawn()

    def start(self) -> None:
        self._kill_orphan()
        self._spawn()
        threading.Thread(target=self._watch, name="stockdata-guard",
                         daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        if self.proc is not None and self.proc.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
                os.killpg(self.proc.pid, signal.SIGTERM)
        self.pidfile.unlink(missing_ok=True)
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/test_stockdata_guardian.py -q`
Expected: PASS

- [ ] **Step 5: 改 main.py（守护服务，删 backfill 与旧 intraday guardian）**

把 `main.py:106-139` 整段（mootdx 启动 backfill + intraday guardian 托管）替换为：

```python
    # stock data 服务：主进程只做守护（PID 锁 + 3s 自愈），
    # 回源落盘（启动 backfill / intraday / 15:35 同步）全在服务内自治。
    try:
        if os.getenv("STOCKDATA_ENABLED", "true").lower() not in ("0", "false", "no"):
            from app.services.stockdata_guardian import StockDataGuardian
            _script_path = (Path(__file__).resolve().parent.parent
                            / "scripts" / "run_stockdata_service.py")
            _guardian = StockDataGuardian(
                pidfile=store.data_dir / ".stockdata.pid",
                script=_script_path,
            )
            _guardian.start()
            app.state.stockdata_guardian = _guardian
            logger.info("stockdata guardian started")
    except Exception:  # noqa: BLE001
        logger.warning("stockdata guardian not started: %s", exc_info=True)
```

同步检查 `main.py` shutdown 段是否 stop 旧 guardian；若有 `app.state.mootdx_intraday_guardian.stop()`，改为 `app.state.stockdata_guardian.stop()`。

- [ ] **Step 6: 删 daily_pipeline 的 mootdx_sync cron**

把 `daily_pipeline.py:894-918`（`_mootdx_sync` 函数 + `scheduler.add_job(... id="mootdx_sync")`）整段删除。

- [ ] **Step 7: 验证主后端可启动（不连真实服务）**

```bash
uv run --extra dev python -c "import app.main; print('main ok')"
```
Expected: `main ok`（应用对象可导入；guardian 由 lifespan 才启动，不影响导入）。

- [ ] **Step 8: 跑主后端既有测试**

```bash
uv run --extra dev pytest tests/test_sync_adj_factor.py -q
```
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add backend/app/services/stockdata_guardian.py backend/tests/test_stockdata_guardian.py backend/app/main.py backend/app/jobs/daily_pipeline.py
git commit -m "feat(stockdata): 主后端守护 stockdata 服务，删除 mootdx 启动回源/15:35 cron"
```

---

## 阶段四：DataManager 换源（回测 + 模拟盘策略模式核心路径）

### Task 10: NetworkSource 适配器 + DataManager 接入

**Files:**
- Create: `backend/app/quant/jqengine/datasource/network_source.py`
- Modify: `backend/app/quant/jqengine/datasource/manager.py`（`__init__`、`SOURCES`、`_priority`、`fetch`、`_adj_factor_map`、`preload_daily`、`_load_minute_from_partitions`、`_load_minute_pool_from_partitions`、`_load_real_minute`）

**Interfaces:**
- Consumes: `StockDataClient`（Task 8）。
- Produces: `NetworkSource`（实现 DataSource：`get_daily`/`get_minute`/`get_stock_list`/`get_etf_list`/`get_stock_names`）；`DataManager.client` 属性。

- [ ] **Step 1: 写失败测试（fake client 注入 DataManager）**

```python
# backend/tests/quant/test_network_source.py
import datetime as _dt

import pandas as pd
import pytest

from app.quant.jqengine.datasource.manager import DataManager
from app.quant.jqengine.datasource.network_source import NetworkSource
from app.quant.datasource.cache import DataCache


def _df(code, closes):
    idx = pd.date_range(_dt.date.today() - pd.Timedelta(days=len(closes) - 1),
                        periods=len(closes))
    return pd.DataFrame({"open": closes, "high": closes, "low": closes,
                         "close": closes, "volume": 1000.0, "amount": 1e5,
                         "trade_dt": idx.normalize().values}, index=idx)


class FakeClient:
    def __init__(self):
        self.calls = []

    def preload_daily(self, lookback_days=400, asof=None):
        self.calls.append("preload_daily")
        return {"512670.XSHG": _df("512670.XSHG", [1.0, 1.1])}

    def get_price(self, security, start_date=None, end_date=None,
                  frequency="daily", fields=None):
        self.calls.append(("get_price", frequency, security))
        codes = security if isinstance(security, list) else [security]
        return {c: _df(c, [1.0, 1.1]) for c in codes}

    def get_adj_factors(self):
        return pd.DataFrame()


def test_network_source_get_daily():
    src = NetworkSource(FakeClient())
    df = src.get_daily("512670.XSHG", "2026-01-01", "2026-02-01")
    assert df["close"].iloc[-1] == 1.1


def test_datamanager_preload_via_client(tmp_path):
    dm = DataManager(cache=DataCache(root=str(tmp_path / "cache")),
                     client=FakeClient())
    dm.preload_daily()
    assert "get_daily_512670.XSHG" in dm._daily_mem
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_network_source.py -q`
Expected: FAIL

- [ ] **Step 3: 实现 network_source.py**

```python
# backend/app/quant/jqengine/datasource/network_source.py
"""网络数据源：把 StockDataClient 适配成 DataSource 接口（喂给 DataManager.fetch）。"""
from __future__ import annotations

import logging

import pandas as pd

from ...datasource.base import DataSource, DataSourceError

log = logging.getLogger("app.quant.jqengine.datasource.network_source")


class NetworkSource(DataSource):
    name = "network"

    def __init__(self, client=None):
        from app.quant.datasource.network_client import StockDataClient
        self.client = client or StockDataClient()

    def _fetch_daily(self, code, start, end) -> pd.DataFrame:
        out = self.client.get_price(code, start_date=start, end_date=end,
                                    frequency="daily")
        df = out.get(code)
        if df is None or df.empty:
            raise DataSourceError(f"网络无日线数据: {code}")
        return df

    def get_daily(self, code, start, end):
        return self._fetch_daily(code, start, end)

    def get_minute(self, code, date=""):
        end = date or pd.Timestamp.now().normalize().date()
        start = (pd.Timestamp(end) - pd.Timedelta(days=15)).date()
        out = self.client.get_price(code, start_date=start, end_date=end,
                                    frequency="1m")
        df = out.get(code)
        if df is None or df.empty:
            raise DataSourceError(f"网络无分钟数据: {code}")
        return df

    def get_stock_list(self):
        df = self.client.get_all_securities(types=["stock"])
        return [f"{r['symbol'].split('.')[0]}.{r['symbol'].split('.')[1]}"
                for r in df.to_dict("records")]

    def get_etf_list(self):
        df = self.client.get_all_securities(types=["etf"])
        return [f"{r['symbol'].split('.')[0]}.{r['symbol'].split('.')[1]}"
                for r in df.to_dict("records")]

    def get_stock_names(self):
        return self.client.get_stock_names() or {}

    def test_connection(self):
        try:
            self.client.ping()
            return True, "stockdata 服务连接正常"
        except Exception as e:  # noqa: BLE001
            return False, str(e)
```

- [ ] **Step 4: 改 DataManager（manager.py）**

4a. 模块级 `SOURCES`（manager.py:19）改为：

```python
SOURCES = {"network": NetworkSource}
```
并确保 `from .network_source import NetworkSource` 在文件头 import。

4b. `__init__` 增建客户端，source 键兼容旧引用（runner/rqalpha_bridge 用 `dm.sources["mootdx"]` 的读取要适配，见 Task 13）：

```python
        self.client = kwargs.pop("client", None)
        if self.client is None:
            from app.quant.datasource.network_client import StockDataClient
            self.client = StockDataClient()
        self.sources = {k: v(self.client) for k, v in SOURCES.items()}
        # 网络单一数据源：把优先级固定为 network（避免旧 mootdx/astock 键
        # 参与 _maybe_demote / _priority 导致空列表）
        CONFIG["DATASOURCE_PRIORITY"] = ["network"]
```

4c. `fetch` 的 `get_daily` 分支：删掉 `self.cache.get("daily", ...)` 缓存写路径，直接调源并进 `_daily_mem`：

```python
            try:
                if method in ("get_daily",):
                    code = args[0]
                    df = getattr(self.sources["network"], method)(*args, **kwargs)
                    if df is None or (hasattr(df, "empty") and df.empty):
                        raise DataSourceError(f"network 空数据")
                    self._daily_mem[cache_key] = df
                    self._daily_ver += 1
                    self._src_fail["network"] = 0
                    return df
                result = getattr(self.sources["network"], method)(*args, **kwargs)
                self._daily_mem[cache_key] = result
                self._daily_ver += 1
                return result
            except Exception as e:
                ...
```
（其余 `except`/`_maybe_demote` 结构保留；`_maybe_demote` 对单源 network 无副作用。）

4d. `_adj_factor_map`（manager.py:218-246）加载改走客户端：

```python
    def _adj_factor_map(self) -> dict[str, dict]:
        if self._adj_factor is not None:
            return self._adj_factor
        self._adj_factor = {}
        try:
            df = self.client.get_adj_factors()
            if df is not None and not df.empty:
                for row in df.to_dict("records"):
                    jq = self._to_jq_code(row["symbol"])
                    self._adj_factor.setdefault(jq, {})[pd.Timestamp(row["trade_date"])] = float(row["ex_factor"])
        except Exception as e:
            logger.warning("[DataManager] adj_factor 加载失败: %s", e)
        return self._adj_factor
```

4e. `preload_daily`（manager.py:387-413）主体改为：

```python
    def preload_daily(self, force: bool = False):
        if getattr(self, "_daily_preloaded", False) and not force:
            return
        try:
            from_part = self.client.preload_daily(
                lookback_days=self._DAILY_LOOKBACK_DAYS,
                asof=pd.Timestamp.now().normalize().date() - pd.Timedelta(days=1))
            if from_part:
                for jq, df in from_part.items():
                    self._daily_mem[f"get_daily_{jq}"] = df
                self._daily_ver += 1
                self._daily_preloaded = True
                return
        except Exception as e:
            logger.warning("[DataManager] preload_daily 网络取数失败: %s", e)
        print("[preload] 日线缓存为空")
```
注：`asof=昨日` 保证盘中/盘后都只取到昨收为止（与模拟盘「到昨收为止」语义一致；盘后 15:35 同步后次日 force 重载含今日）。

4f. `_load_minute_from_partitions`（manager.py:1063-1112）改为：

```python
    def _load_minute_from_partitions(self, code, lo_ts, hi_ts):
        try:
            out = self.client.get_price(code, frequency="1m",
                                        start_date=str(lo_ts) if lo_ts is not None else None,
                                        end_date=str(hi_ts) if hi_ts is not None else None)
            return out.get(code)
        except Exception as e:
            logger.warning("[DataManager] 分钟网络取数失败 %s: %s", code, e)
            return None
```

4g. `_load_minute_pool_from_partitions`（manager.py:824-885）改为：

```python
    def _load_minute_pool_from_partitions(self, codes, lo_ts, hi_ts):
        if not codes:
            return {}
        try:
            return self.client.get_minute_pool(codes, lo_ts, hi_ts)
        except Exception as e:
            logger.warning("[DataManager] 分钟池网络取数失败: %s", e)
            return {}
```

4h. `_load_real_minute`（manager.py:1114-1203）改为纯网络（删本地 real_ 缓存与 mootdx 缺口逻辑）：

```python
    def _load_real_minute(self, code, lo_ts, hi_ts, all_min):
        df = self._load_minute_from_partitions(code, lo_ts, hi_ts)
        if df is not None and not df.empty:
            self._minute_real_cov[code] = (df.index.min(), df.index.max())
            return df
        if getattr(self, "_offline_missing_warn", False):
            logger.warning("[DataManager] 离线分钟缺失（网络无数据）: %s", code)
        return None
```

- [ ] **Step 5: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_network_source.py -q`
Expected: PASS

- [ ] **Step 6: 确认既有测试改动点**

Run: `uv run --extra dev pytest tests/quant/test_fix_datamanager.py tests/quant/test_partition_load.py tests/quant/test_live_feed.py -q`
Expected: 记录失败清单（这些测试仍直接测分区读，将在 Task 14 改为注入 fake client；本 task 允许失败，但不允许新增**未记录**的失败）。

- [ ] **Step 7: Commit**

```bash
git add backend/app/quant/jqengine/datasource/network_source.py backend/app/quant/jqengine/datasource/manager.py backend/tests/quant/test_network_source.py
git commit -m "feat(quant): DataManager 改走网络数据源（network_source + client）"
```

---

## 阶段五：量化侧其余调用面换源

### Task 11: QuantDataProvider（看护模式）换源

**Files:**
- Modify: `backend/app/quant/datasource/manager.py`
- Test: `backend/tests/quant/test_quant_data_provider.py`

**Interfaces:**
- Consumes: `StockDataClient`。
- Produces: `QuantDataProvider`（`get_daily`/`get_minute`/`get_stock_list`/`get_etf_list`）改走客户端。

- [ ] **Step 1: 写失败测试**

```python
# backend/tests/quant/test_quant_data_provider.py
import datetime as _dt

import pandas as pd
import pytest

from app.quant.datasource.manager import QuantDataProvider


def _df(closes):
    idx = pd.date_range(_dt.date.today() - pd.Timedelta(days=len(closes) - 1),
                        periods=len(closes))
    return pd.DataFrame({"open": closes, "high": closes, "low": closes,
                         "close": closes, "volume": 1000.0, "amount": 1e5}, index=idx)


class FakeClient:
    def get_price(self, security, start_date=None, end_date=None, frequency="daily", fields=None):
        codes = security if isinstance(security, list) else [security]
        return {c: _df([1.0, 1.1]) for c in codes}

    def current_snapshot(self, codes, as_of=None):
        return {c: _df([1.05]) for c in codes}


def test_provider_get_daily():
    p = QuantDataProvider(client=FakeClient())
    df = p.get_daily("512670.XSHG", "2026-01-01", "2026-02-01")
    assert df["close"].iloc[-1] == 1.1


def test_provider_get_minute():
    p = QuantDataProvider(client=FakeClient())
    df = p.get_minute("512670.XSHG", str(_dt.date.today()))
    assert df["close"].iloc[-1] == 1.05
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_quant_data_provider.py -q`
Expected: FAIL

- [ ] **Step 3: 改 QuantDataProvider**

把 `backend/app/quant/datasource/manager.py` 的 `QuantDataProvider` 改为：

```python
class QuantDataProvider:
    """网络数据源看护模式适配：一切数据走 StockDataClient（零本地文件/零直连）。"""

    def __init__(self, client=None):
        from app.quant.datasource.network_client import StockDataClient
        self.client = client or StockDataClient()

    def get_daily(self, code, start, end):
        out = self.client.get_price(code, start_date=start, end_date=end, frequency="daily")
        df = out.get(code)
        if df is None or df.empty:
            raise DataSourceError(f"网络无日线数据: {code}")
        return df

    def get_minute(self, code, date):
        out = self.client.current_snapshot([code], as_of=f"{date} 15:00:00")
        df = out.get(code)
        if df is None or df.empty:
            return pd.DataFrame()
        return df

    def get_stock_list(self):
        df = self.client.get_all_securities(types=["stock"])
        return [f"{r['symbol'].split('.')[0]}.{r['symbol'].split('.')[1]}"
                for r in df.to_dict("records")]

    def get_etf_list(self):
        df = self.client.get_all_securities(types=["etf"])
        return [f"{r['symbol'].split('.')[0]}.{r['symbol'].split('.')[1]}"
                for r in df.to_dict("records")]
```
（`QuantDataProvider()` 无参构造仍可用；`runner._run_watcher_loop` 传 `provider or QuantDataProvider()` 不变。）

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_quant_data_provider.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/datasource/manager.py backend/tests/quant/test_quant_data_provider.py
git commit -m "feat(quant): QuantDataProvider（看护模式）改走网络客户端"
```

---

### Task 12: live_feed + runner 换源（模拟盘盘中分钟）

**Files:**
- Modify: `backend/app/quant/simulate/live_feed.py`（loader 换 `client.current_snapshot`；删 mootdx 实时回源；`persist_real` 变 no-op）
- Modify: `backend/app/quant/simulate/runner.py`（`_make_dm`、`_is_trading_day` 的 `sources["mootdx"]`、`_pre_market` 的 `preload_daily(force)` 已随 Task 10 生效、删 `minute_realtime_backfill` 开关分支）
- Test: `backend/tests/quant/test_live_feed.py`（改造现有为 fake client）

**Interfaces:**
- Consumes: `DataManager.client`（Task 10）、`StockDataClient`。
- Produces: `live_feed.refresh(dm, codes, now=None, fresh_acc=None, loader=None, enabled=False)` 行为不变，但 loader 默认改为 `dm.client.current_snapshot`。

- [ ] **Step 1: 改 live_feed.py**

1a. 删除 `_pull_recent_guarded`、`_load_recent_with_network_fallback`、`_load_recent_from_partitions`（分区直读已由客户端承接）。

1b. `refresh` 默认 loader 改为：

```python
def _load_recent_via_client(dm, codes, now):
    client = getattr(dm, "client", None)
    if client is None:
        return {}
    return client.current_snapshot(codes, as_of=now)
```

并把 `refresh` 中 `if loader is None:` 分支改为：

```python
    if loader is None:
        loader = _load_recent_via_client
```
（`enabled` 参数保留仅向后兼容，不再启用 mootdx 网络路径。）

1c. `persist_real` 改为 no-op（落盘归服务）：

```python
def persist_real(dm, fresh_frames):
    """落盘已移交 stock data 服务，本函数保留为空操作（兼容调用方）。"""
    return
```

- [ ] **Step 2: 改 runner.py**

2a. `_make_dm`（runner.py:177-193）保持 `get_data_manager()` + `_offline` 设置（`_offline` 语义现在是「网络缺失视为缺失」，不再触网），确认 `dm._use_real_minute = True` 保留。

2b. `_is_trading_day`（runner.py:210-217）改：

```python
        dm_client = getattr(dm, "client", None)
        if dm_client is not None:
            try:
                out = dm_client.get_price("000300.XSHG", start_date=start, end_date=str(today),
                                          frequency="daily")
                df = out.get("000300.XSHG")
            except Exception:
                df = None
        if df is None:
            df = dm.fetch("get_daily", "000300.XSHG", start, str(today))
```

2c. `_run_strategy_loop`（runner.py:698-708）删 `minute_realtime_backfill` 开关分支：

```python
    feed = feed or live_feed.refresh
    # 实时由 stock data 服务保证，feed 恒走网络客户端（无 mootdx 直连路径）。
```

2d. `_eod`（runner.py:476）的 `live_feed.persist_real(...)` 保留调用（现为 no-op）。

- [ ] **Step 3: 改造既有测试注入 fake client**

`backend/tests/quant/test_live_feed.py`、`test_runner_strategy.py` 中构造 `dm` 处注入带 `client` 属性的 fake：

```python
class _FakeClient:
    def current_snapshot(self, codes, as_of=None):
        idx = pd.DatetimeIndex([pd.Timestamp(as_of)])
        return {c: pd.DataFrame({"open": [1.0], "high": [1.0], "low": [1.0],
                                 "close": [1.0], "volume": [100], "amount": [100.0]},
                                index=idx) for c in codes}
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_live_feed.py tests/quant/test_runner_strategy.py -q`
Expected: PASS（`test_minute_realtime_backfill.py` 不存在于本分支——该功能未移植，跳过）。

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/simulate/live_feed.py backend/app/quant/simulate/runner.py backend/tests/quant/test_live_feed.py backend/tests/quant/test_runner_strategy.py
git commit -m "feat(quant): live_feed/runner 换网络客户端，删除 mootdx 直连路径"
```

---

### Task 13: rqalpha_bridge 等 sources 键名适配

**Files:**
- Modify: `backend/app/quant/rqalpha_bridge.py`（`dm.sources["mootdx"]` → `dm.sources["network"]`）

**Interfaces:**
- Consumes: Task 10 的 `DataManager.sources`（现为 `{"network": ...}`）。

- [ ] **Step 1: 替换引用**

`rqalpha_bridge.py:1236` `dm.sources["mootdx"].get_stock_names()` → `dm.sources["network"].get_stock_names()`。
全文搜 `sources["mootdx"]` / `sources.get("mootdx")`，量化侧一律改为 `sources["network"]`（或 `dm.client`）。

Run: `grep -rn 'sources\["mootdx"\]\|sources.get("mootdx")' backend/app/quant/`

- [ ] **Step 2: 跑相关测试**

Run: `uv run --extra dev pytest tests/quant/test_fix_bridge_unit.py tests/quant/test_fix_compat2.py -q`
Expected: PASS（或记录并修复 fake 构造点）。

- [ ] **Step 3: Commit**

```bash
git add backend/app/quant/rqalpha_bridge.py
git commit -m "fix(quant): rqalpha_bridge sources 键名适配 network"
```

---

## 阶段六：测试补全 + wufu_v52 回归验收

### Task 14: 既有测试改造 + 全量回归

**Files:**
- Modify: `backend/tests/quant/test_fix_datamanager.py`、`test_partition_load.py`、`test_fix_money_units.py`（改为注入 fake client 或断言走网络）
- Test: 全量

**Interfaces:**
- Consumes: Task 10-13 的最终行为。

- [ ] **Step 1: 改造分区读相关测试**

`test_partition_load.py` 直接测 `_load_minute_from_partitions` 的分区读取 → 改为构造 `DataManager(client=FakeClient(...))` 断言走 client。`test_fix_datamanager.py` 同理。
（具体每处改造跟随 Task 10 Step 6 记录的失败清单逐一处理。）

- [ ] **Step 2: 全量 pytest**

Run: `uv run --extra dev pytest -q`
Expected: 新增失败数 = 0；对比改造前基线（记录在案）无新增失败类别。

- [ ] **Step 3: lint + mypy**

Run: `uv run --extra dev ruff check app && uv run --extra dev mypy app`
Expected: 无新增错误类别（repo 基线脏，以 base/head 对比）。

- [ ] **Step 4: Commit**

```bash
git add backend/tests/quant
git commit -m "test(quant): 既有测试改走 fake network client"
```

---

### Task 15: wufu_v52 回测对齐验收（backtest_260401-260716）

**Files:**
- 运行：`backend/scripts/run_quant_backtest.py`（wufu-v5.2，区间 2026-04-01~2026-07-16）+ `scripts/diff_jq_vs_local.py`
- 参考 fixture：`backend/tests/fixtures/wufu_v52/backtest_260401-260716/`

**Interfaces:**
- Consumes: 完整网络数据路径（Task 10-13）。

- [ ] **Step 1: 启动 stock data 服务（真实分区数据）**

```bash
cd backend
STOCKDATA_PORT=3322 nohup uv run --extra dev python scripts/run_stockdata_service.py > /tmp/stockdata.log 2>&1 &
```

- [ ] **Step 2: 回测**

```bash
uv run --extra dev python scripts/run_quant_backtest.py \
  --strategy tests/fixtures/wufu_v52/wufu-v5.2.py \
  --start 2026-04-01 --end 2026-07-16 \
  --out data/quant_sim/jqwufu_network
```

- [ ] **Step 3: 对比 fixture（回测对齐）**

```bash
uv run --extra dev python scripts/diff_jq_vs_local.py \
  --local data/quant_sim/jqwufu_network \
  --fixture tests/fixtures/wufu_v52 \
  --ret 20260401-20260716收益.csv \
  --trd 20260401-20260716交易记录.csv
```
Expected: 与改造前基线一致（收益逐日差 ≤0.05% 交易日集合、交易组对齐口径与基线 diff 相同）。**先跑改造前基线保存 `diff/` 输出**（本分支 Task 15 前先在 custom-main 跑一次存档）。

- [ ] **Step 4: 与基线对比**

对比改造前后 `return_diff.csv` / `trade_diff.csv` 逐位一致。

- [ ] **Step 5: Commit（若需要记录基线/脚本）**

---

### Task 16: wufu_v52 模拟盘对齐验收（sim_260710）

**Files:**
- 运行：`backend/scripts/run_quant_sim.py`（wufu-v5.2 账户，对齐 2026-07-10）
- 参考 fixture：`backend/tests/fixtures/wufu_v52/sim_260710/live_transaction_list.csv`

**Interfaces:**
- Consumes: 完整网络数据路径。

- [ ] **Step 1: 启动服务 + 建模拟盘账户并启动**

```bash
cd backend
uv run --extra dev python -m app.quant.simulate.scripts.run_quant_sim --account wufu_v52_sim --strategy tests/fixtures/wufu_v52/wufu-v5.2.py --date 2026-07-10
```
（`sim_260710` 为当日数据；若当日已过，用回放 `_replay_partial_day` 路径重放 2026-07-10 当日分钟，见 AGENTS.md「模拟盘重启标准操作」。）

- [ ] **Step 2: 对比交易**

把 `data/quant_kline/sim_logs/<aid>.log` 中的成交与 `tests/fixtures/wufu_v52/sim_260710/live_transaction_list.csv` 对比（对齐口径同现状：按 date+code+side+qty 聚合）。
Expected: 与改造前基线一致。

- [ ] **Step 3: 盘中零直连验证**

```bash
grep -rn "import mootdx\|from mootdx\|baostock\|import baostock" backend/app/quant/ | grep -v network_client || echo "quant 侧无 mootdx/baostock 直连"
grep -rn "_partition_root\|kline_etf_minute\|read_parquet" backend/app/quant/quant/simulate backend/app/quant/quant/jqengine/datasource/manager.py | grep -v "client" || echo "quant 侧无本地 parquet 读取"
```
Expected: quant 侧仅 `network_client` 与 mootdx 无直接引用。

- [ ] **Step 4: 全量回归**

Run: `uv run --extra dev pytest -q`
Expected: 与基线一致。

- [ ] **Step 5: 收尾 commit + AGENTS.md 记录**

更新 `AGENTS.md`：记录 stock data 服务（启动方式、STOCKDATA_PORT/HOST、守护、量化侧唯一取数入口、服务自治回源清单、wufu_v52 验收命令）。

```bash
git add AGENTS.md
git commit -m "docs: AGENTS.md 记录 stock data 服务架构与验收命令"
```

---

## Self-Review

**Spec 覆盖核对**
- 网络服务 + 多源聚合：Task 2-7 ✓
- 全部走网络（DataManager/QuantDataProvider/live_feed）：Task 10-13 ✓
- 服务自治回源（启动 backfill / 15:35 收盘同步 / 00:00 清空，无主动盘中轮询）：Task 6、9 ✓
- 主后端只做守护 + 删回源：Task 9 ✓
- jqdata 风格 API：Task 8 ✓
- 客户端算 fq（get_adj_factors）：Task 10（_adj_factor_map）✓
- 标的级去重 + 当日分钟内存库（纯 lazy、按需回源、00:00 清空）+ 共享拉取线程池：Task 3、4 ✓
- 历史数据不驻留（仅短 TTL 突发去重）：Task 4（_HIST_TTL）+ Task 3 ✓
- 前端/DuckDB/主后端其余不动：Global Constraints + Task 9 只删回源 ✓
- wufu_v52 回测/模拟盘对齐：Task 15、16 ✓
- 测试/回归/ruff/mypy：Task 14 ✓

**Placeholder 扫描**：无 TBD/TODO；每个改动步骤均给出代码/命令。

**类型一致性**：客户端方法名（get_price/current_snapshot/preload_daily/get_minute_pool/get_adj_factors/get_all_securities/get_trade_days/get_security_info/get_index_stocks/get_stock_names/ping/status/trigger_sync）在 Task 8 定义、Task 5 handler、Task 4 sources、Task 10-13 消费处完全一致。`DataManager.sources` 键为 `network`，`dm.client` 为 `StockDataClient`，Task 12/13 引用一致。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-05-stockdata-service.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
