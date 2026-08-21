# stockdata 日线日期文件 LRU 缓存 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 stockdata 服务内新增 `DayFileCache`（日线日期文件 LRU 缓存），重写 `get_daily`/`preload_daily` 走 LRU，删除模拟盘在线 preload 调用，把服务 RSS 从 ~1.16GB 降至 <600MB。

**Architecture:** 日线分区按日存储、每日期文件含全市场标的。`DayFileCache`（dict + `threading.Lock` + single-flight 载入）以 `(subdir, date)` 为键缓存整文件 8 列原始单位帧；读取侧（get_daily/preload_daily）从缓存过滤 symbols 拼帧返回，帧不驻留。scheduler 后台线程每 10s `sweep()`（先踢 >60s 未访问，再按最后访问踢旧至 ≤60 文件）。模拟盘在线删除 preload 调用；离线回测四处 preload 保留。客户端/协议/返回帧结构零改动。

**Tech Stack:** Python 3.11+ / Polars（pl.scan_parquet 读分区）/ threading / 既有 `SingleFlight`（`backend/app/services/stockdata/single_flight.py`）

## Global Constraints

（来自 spec `docs/superpowers/specs/2026-08-21-stockdata-daily-dayfile-lru-design.md`，逐条原文语义）

- 键：`(subdir, date)`，`subdir ∈ {kline_daily, kline_etf_daily, kline_index_daily}`，`date = YYYY-MM-DD`（ISO 分区名）。
- 值：该日分区文件全市场整帧，固定 8 列 `symbol/date/open/high/low/close/volume/amount`，**原始单位**（股票 volume 为「手」，不预换算）。换算/投影统一在读取侧做，缓存不存变体。
- `get(subdir, date)`：命中返回帧并刷新最后访问时间；未命中返回 None（不加载）。
- `load`：日期文件级 single-flight 读盘（同键并发只读一次），写入缓存。
- 卸载规则（纯后台清扫，访问时不清理）：每 10s 一次；先卸载 `最后访问时间 > 60s` 的文件；仍超容量上限（60 文件）按最后访问时间从旧到新踢，直到 ≤60。
- 并发：dict + `threading.Lock`（读写都加锁）。清扫与载入不互斥：清扫只删键。
- 启动/换日：无预载无预分配；不跨日清理。
- `preload_daily` 维持现语义：只含股票+ETF（不含 `kline_index_daily`），按 asof 截断。
- `get_daily` 日期规范化保留（%Y%m%d → ISO 与 ISO 两种格式兼容）。
- 删除原 `daily:` 与 `preload_daily:` 请求级 60s DedupCache 缓存（LRU 已覆盖）。
- 模拟盘在线：删除 `runner.py` 两处 `dm.preload_daily()`（`_make_dm` 启动处 + `_pre_market` 盘前处），注释写明原因防回退。
- 离线回测：`rqalpha_bridge.py` 四处 `dm.preload_daily()` 不动。
- `network_client.py` / `DataManager` / `jqcompat`：零改动。
- 客户端 `_daily_mem` 会话驻留：不动。
- YAGNI：分钟分区不做日文件 LRU；容量上限不参数化（60 固定）；不做访问时懒卸载。
- 测试命令（必须从 `backend/` 目录）：`uv run --extra dev pytest <file> -q`（dev 依赖含 pytest）。
- lint：`uv run --extra dev ruff check app`（line-length 100，select E,F,I,N,UP,B,SIM,RUF，忽略 E501）。
- 提交：每个任务末尾独立 commit，中文 message，风格参考仓库 `git log`。

---

### Task 1: `DayFileCache` 类 + 单元测试

**Files:**
- Modify: `backend/app/services/stockdata/sources.py`（新增类，放在 `MinuteMemoryStore` 之后、`NetworkPuller` 之前；文件顶部 import 区加 `import time`）
- Test: 新建 `backend/tests/quant/test_stockdata_dayfile_cache.py`

**Interfaces:**
- Consumes: `SingleFlight`（`from .single_flight import SingleFlight`，已导入）；`pl.DataFrame`；`Callable`（`from collections.abc import Callable`，已导入）；`threading`（已导入）；需新增 `import time`。
- Produces:
  - `class DayFileCache`，构造 `DayFileCache(ttl: float = 60.0, cap: int = 60)`（ttl/cap 可注入，供测试用小值）。
  - `get(subdir: str, date: str) -> pl.DataFrame | None` — 命中刷新最后访问时间。
  - `get_or_load(subdir: str, date: str, loader: Callable[[], pl.DataFrame | None]) -> pl.DataFrame | None` — 命中直接返回；未命中经 `self._single.run(f"{subdir}:{date}", ...)` single-flight 载入；loader 返回 None/空帧不写入缓存。
  - `sweep() -> int` — 卸载过期 + 超容量踢旧，返回卸载文件数。
  - `__len__() -> int` — 当前缓存文件数（测试用）。

- [ ] **Step 1: 写失败测试**（新建 `backend/tests/quant/test_stockdata_dayfile_cache.py`）

```python
# backend/tests/quant/test_stockdata_dayfile_cache.py
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_dayfile_cache.py -q`
Expected: FAIL，`ImportError: cannot import name 'DayFileCache'`（类还不存在）。

- [ ] **Step 3: 实现 `DayFileCache`**

在 `backend/app/services/stockdata/sources.py`：
1. 顶部 import 区（`import os` 之后）加 `import time`。
2. 在 `MinuteMemoryStore` 类结束之后、`NetworkPuller` 类之前插入（`_MINUTE_COLS` 常量在 87 行附近、`NetworkPuller` 约 173 行起）：

```python
class DayFileCache:
    """日线日期文件缓存：键=(subdir, date) → 该日全市场整帧（原始单位）。

    日线分区按日存储、每日期文件含全市场标的：读取时整文件载入内存，同文件
    其他标的的后续请求直接命中。后台清扫线程每 10s 卸载超时（默认 60s）未
    访问的文件，并执行容量上限（默认 60 文件）淘汰。不预载、不驻留 400 天
    全市场整帧（spec 2026-08-21-stockdata-daily-dayfile-lru-design）。
    """

    def __init__(self, ttl: float = 60.0, cap: int = 60) -> None:
        self._ttl = ttl
        self._cap = cap
        self._items: dict[tuple[str, str], tuple[float, pl.DataFrame]] = {}
        self._lock = threading.Lock()
        self._single = SingleFlight()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def get(self, subdir: str, date: str) -> pl.DataFrame | None:
        """命中返回帧并刷新该文件最后访问时间；未命中返回 None（不加载）。"""
        with self._lock:
            item = self._items.get((subdir, date))
            if item is None:
                return None
            _ts, frame = item
            self._items[(subdir, date)] = (time.monotonic(), frame)
            return frame

    def get_or_load(self, subdir: str, date: str,
                    loader: Callable[[], pl.DataFrame | None]) -> pl.DataFrame | None:
        """缓存命中直接返回；未命中日期文件级 single-flight 读盘（同键并发只读一次）。"""
        hit = self.get(subdir, date)
        if hit is not None:
            return hit
        return self._single.run(
            f"{subdir}:{date}",
            lambda: self._insert(subdir, date, loader()))

    def _insert(self, subdir: str, date: str,
                frame: pl.DataFrame | None) -> pl.DataFrame | None:
        if frame is None or frame.is_empty():
            return frame
        with self._lock:
            # double-check：single-flight 期间可能已有其他线程载入
            if (subdir, date) not in self._items:
                self._items[(subdir, date)] = (time.monotonic(), frame)
        return frame

    def sweep(self) -> int:
        """卸载超时未访问文件；仍超容量上限时按最后访问时间从旧到新踢。返回卸载数。"""
        now = time.monotonic()
        evicted = 0
        with self._lock:
            for k in [k for k, (ts, _f) in self._items.items() if now - ts > self._ttl]:
                del self._items[k]
                evicted += 1
            if len(self._items) > self._cap:
                oldest = sorted(self._items.items(), key=lambda kv: kv[1][0])
                for k, _v in oldest[: len(self._items) - self._cap]:
                    del self._items[k]
                    evicted += 1
        return evicted
```

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_dayfile_cache.py -q`
Expected: 8 passed。

- [ ] **Step 5: lint + 提交**

```bash
cd backend && uv run --extra dev ruff check app/services/stockdata/sources.py
git add backend/app/services/stockdata/sources.py backend/tests/quant/test_stockdata_dayfile_cache.py
git commit -m "feat(stockdata): DayFileCache 日线日期文件 LRU 缓存（spec 2026-08-21）"
```

---

### Task 2: `get_daily`/`preload_daily` 重写走 LRU + 黄金对比 + 单测

**Files:**
- Modify: `backend/app/services/stockdata/sources.py`
  - `DataSources.__init__`（约 262-274 行）：加 `self.dayfile_cache = DayFileCache()`
  - `_load_daily`（313-333 行）：**删除**（preload 不再用它）
  - `preload_daily`（335-338 行）：重写
  - `get_daily`（340-367 行）：重写
  - 新增私有方法 `_read_day_file`、`_existing_day_files`（放在 `_daily_days` 之后）
- Test: `backend/tests/quant/test_stockdata_sources.py`（新增 2 个测试）
- 临时对比产物（不入库）：`/tmp/opencode/daily_golden/`（黄金 parquet）

**Interfaces:**
- Consumes: `DayFileCache.get_or_load`（Task 1）；`_daily_days`（既有，返回 `(lo, hi)` ISO 字符串）；`_tf_symbol`、`_normalize_etf_volume_unit`、`_as_datetime`、`pd_to_date`（既有）；`_HIST_TTL`（既有常量，重写后 get_daily/preload_daily 不再使用）。
- Produces:
  - `_read_day_file(subdir: str, date: str) -> pl.DataFrame | None`：读单个日期分区（含全市场标的）→ 8 列原始帧；分区不存在/无 parquet 返回 None。
  - `_existing_day_files(subdir: str, lo: str | None, hi: str | None) -> list[str]`：区间内已存在的日期分区名（升序，ISO）。
  - 重写后的 `preload_daily(lookback_days: int = 400, asof: _dt.date | None = None) -> pl.DataFrame`：逐日文件经 LRU → 股票 volume×100 → `_normalize_etf_volume_unit` → concat → asof 截断；只含 kline_daily + kline_etf_daily。
  - 重写后的 `get_daily(codes: list[str], start_date: str, end_date: str) -> pl.DataFrame`：日期规范化保留 → 逐日 × 3 subdir 经 LRU → 过滤 symbols → 股票 volume×100 → concat → `_normalize_etf_volume_unit`；不再用 `get_or_fetch`/`daily:` 缓存。

- [ ] **Step 1: 用旧实现捕获黄金输出（重构前先定格旧行为）**

Run（从 `backend/` 目录，用当前旧代码直接实例化 DataSources 读真实 `data/` 分区）：

```bash
cd backend && uv run --extra dev python - <<'EOF'
import datetime as _dt, os
import polars as pl
from app.services.stockdata.sources import DataSources

s = DataSources(mootdx_factory=None, fetch_workers=2)
os.makedirs("/tmp/opencode/daily_golden", exist_ok=True)
got = s.get_daily(["600000.XSHG", "512670.SH", "000300.XSHG"], "2026-07-01", "2026-07-16")
got.write_parquet("/tmp/opencode/daily_golden/get_daily_600000_512670_000300_260701-260716.parquet")
pre = s.preload_daily(lookback_days=400, asof=_dt.date(2026, 7, 16))
pre.write_parquet("/tmp/opencode/daily_golden/preload_400_asof_20260716.parquet")
print("golden rows:", got.height, pre.height)
print("cache files:", len(s.dedup._cache))
EOF
```

Expected: 打印 golden rows（记录行数备用）；`cache files` 非零（证明旧实现确实驻留了缓存）。

- [ ] **Step 2: 新增单测（先写测试，跑通旧实现）**

在 `backend/tests/quant/test_stockdata_sources.py` 末尾追加：

```python
def test_get_daily_serves_via_dayfile_cache(src):
    """get_daily 走日线日期文件 LRU：结果与直读分区一致，且日期文件入缓存。"""
    day = _dt.date.today().isoformat()
    df = src.get_daily(["600000.XSHG"], day, day)
    assert df["symbol"].to_list() == ["600000.SH"]
    assert df["volume"].to_list() == [100000]  # 股票手→股 ×100
    assert len(src.dayfile_cache) >= 1
    assert src.dayfile_cache.get("kline_daily", day) is not None


def test_preload_daily_uses_dayfile_cache(src):
    df = src.preload_daily(lookback_days=400)
    assert not df.is_empty()
    assert df["symbol"].to_list() == ["600000.SH"]
    assert df["volume"].to_list() == [100000]
    assert len(src.dayfile_cache) >= 1
```

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py -q`
Expected: 这两个新测试 **FAIL**（`src.dayfile_cache` 属性不存在），其余既有测试 **PASS**（旧实现行为被现有测试锁定）。

- [ ] **Step 3: 重写实现**

`backend/app/services/stockdata/sources.py` 改动：

1. `DataSources.__init__` 中 `self.minute_store = MinuteMemoryStore()` 之后加：

```python
        self.dayfile_cache = DayFileCache()
```

2. `_daily_days` 之后新增两个私有方法（原 `_load_daily` 位置）：

```python
    def _read_day_file(self, subdir: str, date: str) -> pl.DataFrame | None:
        """读单个日期分区（含全市场标的）→ 8 列原始帧；分区不存在返回 None。"""
        root = os.path.join(self.data_root, subdir, f"date={date}")
        if not os.path.isdir(root):
            return None
        import glob as _glob
        paths = _glob.glob(os.path.join(root, "*.parquet"))
        if not paths:
            return None
        cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        lf = pl.scan_parquet(paths, hive_partitioning=True)
        return _as_datetime(lf.select(cols).collect())

    def _existing_day_files(self, subdir: str, lo: str | None,
                            hi: str | None) -> list[str]:
        """区间内已存在的日期分区名（升序，ISO 字符串）。"""
        root = os.path.join(self.data_root, subdir)
        if not os.path.isdir(root):
            return []
        out = []
        for name in sorted(os.listdir(root)):
            if not name.startswith("date="):
                continue
            ds = name[len("date="):]
            if lo and ds < lo:
                continue
            if hi and ds > hi:
                continue
            out.append(ds)
        return out
```

3. **删除**整个 `_load_daily` 方法（313-333 行，preload 不再使用）。

4. `preload_daily` 重写为：

```python
    def preload_daily(self, lookback_days: int = 400, asof: _dt.date | None = None) -> pl.DataFrame:
        """预载全市场日线（只含股票+ETF，不含指数）：逐日文件经 LRU 拼帧返回。

        帧不驻留（LRU 按 60s/60 文件自然淘汰）——spec
        2026-08-21-stockdata-daily-dayfile-lru-design 第 3 节。
        """
        lo, hi = self._daily_days(lookback_days, asof)
        parts = []
        for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False)):
            for day in self._existing_day_files(subdir, lo, hi):
                frame = self.dayfile_cache.get_or_load(
                    subdir, day, lambda s=subdir, d=day: self._read_day_file(s, d))
                if frame is None or frame.is_empty():
                    continue
                if is_stock:
                    frame = frame.with_columns((pl.col("volume") * 100).alias("volume"))
                parts.append(frame)
        if not parts:
            return pl.DataFrame()
        out = _normalize_etf_volume_unit(pl.concat(parts))
        if asof is not None:
            out = out.filter(pl.col("date") <= asof)
        return out
```

5. `get_daily` 重写为：

```python
    def get_daily(self, codes: list[str], start_date: str, end_date: str) -> pl.DataFrame:
        # 日期规范化：兼容 %Y%m%d（模拟盘 jqcompat _DayBarStore 传入）与 ISO
        # （rqalpha_bridge 传入）两种格式。分区名恒为 ISO（date=YYYY-MM-DD），
        # 字符串比较；'20260601' 与 '2026-06-01' 比较恒 False 会把全部分区跳过。
        # 统一转 ISO 再比较。
        start_date = str(pd_to_date(start_date)) if start_date else None
        end_date = str(pd_to_date(end_date)) if end_date else None

        syms = {_tf_symbol(c) for c in codes}
        parts = []
        for subdir, is_stock in (("kline_daily", True), ("kline_etf_daily", False),
                                 ("kline_index_daily", False)):
            for day in self._existing_day_files(subdir, start_date, end_date):
                frame = self.dayfile_cache.get_or_load(
                    subdir, day, lambda s=subdir, d=day: self._read_day_file(s, d))
                if frame is None or frame.is_empty():
                    continue
                sub = frame.filter(pl.col("symbol").is_in(syms))
                if sub.is_empty():
                    continue
                if is_stock:
                    sub = sub.with_columns((pl.col("volume") * 100).alias("volume"))
                parts.append(sub)
        if not parts:
            return pl.DataFrame()
        return _normalize_etf_volume_unit(pl.concat(parts))
```

- [ ] **Step 4: 跑单测确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py tests/quant/test_stockdata_dayfile_cache.py -q`
Expected: 全部 PASS（含既有 3 个日线测试：`test_preload_daily_reads_partitions`、`test_preload_daily_excludes_index_but_get_daily_serves`、`test_get_daily_accepts_compact_date_format`）。

- [ ] **Step 5: 黄金对比（新旧实现逐行一致）**

Run（用新代码重跑同一实例化逻辑，与 Step 1 黄金 parquet 对比）：

```bash
cd backend && uv run --extra dev python - <<'EOF'
import datetime as _dt, os
import polars as pl
from app.services.stockdata.sources import DataSources

s = DataSources(mootdx_factory=None, fetch_workers=2)
got = s.get_daily(["600000.XSHG", "512670.SH", "000300.XSHG"], "2026-07-01", "2026-07-16")
pre = s.preload_daily(lookback_days=400, asof=_dt.date(2026, 7, 16))
gold = pl.read_parquet("/tmp/opencode/daily_golden/get_daily_600000_512670_000300_260701-260716.parquet")
goldp = pl.read_parquet("/tmp/opencode/daily_golden/preload_400_asof_20260716.parquet")
assert got.equals(gold), f"get_daily 不一致: {got.height} vs {gold.height}"
assert pre.equals(goldp), f"preload_daily 不一致: {pre.height} vs {goldp.height}"
print("get_daily rows:", got.height, "preload rows:", pre.height, "-> 与旧实现逐行一致")
print("LRU files:", len(s.dayfile_cache))
EOF
```

Expected: 打印 `-> 与旧实现逐行一致`（`pl.DataFrame.equals` 全列逐行精确相等）。若断言失败：对比 schema/列顺序/volume 口径，回到 Step 3 检查。

- [ ] **Step 6: 全文件回归 + lint + 提交**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_stockdata_sources.py tests/quant/test_stockdata_dayfile_cache.py -q
cd backend && uv run --extra dev ruff check app/services/stockdata/sources.py
git add backend/app/services/stockdata/sources.py backend/tests/quant/test_stockdata_sources.py
git commit -m "feat(stockdata): get_daily/preload_daily 重写走日线日期文件 LRU（删请求级 60s 缓存）"
```

---

### Task 3: scheduler 清扫线程

**Files:**
- Modify: `backend/app/services/stockdata/scheduler.py`（`_midnight_clear_loop` 之后新增 `_dayfile_sweep_loop`；`start_scheduler` 224-236 行注册）
- Test: `backend/tests/quant/test_stockdata_scheduler.py`（新增 1 个测试）

**Interfaces:**
- Consumes: `data_sources.dayfile_cache`（Task 1/2 产物）；`_stop`（模块级 `threading.Event`，既有）；`logger`（既有）；`time`（已导入）。
- Produces: `_dayfile_sweep_loop(data_sources, interval: float = 10.0) -> None`——每 `interval` 秒调 `data_sources.dayfile_cache.sweep()`，evicted>0 时 debug 日志，异常 warning 不退出。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_stockdata_scheduler.py` 末尾追加：

```python
def test_dayfile_sweep_loop_evicts_expired(monkeypatch):
    """清扫线程循环：每 interval 秒 sweep 一次，过期文件被卸载。"""
    import threading as _th
    import time as _t
    import polars as pl
    from app.services.stockdata import scheduler as sch
    from app.services.stockdata.sources import DayFileCache

    c = DayFileCache(ttl=0.02, cap=10)
    c.get_or_load("kline_daily", "2026-08-19", lambda: pl.DataFrame({
        "symbol": ["600000.SH"], "date": ["2026-08-19"],
        "open": [10.0], "high": [11.0], "low": [9.0],
        "close": [10.5], "volume": [1000], "amount": [105000.0],
    }))
    assert len(c) == 1

    stop = _th.Event()
    monkeypatch.setattr(sch, "_stop", stop)
    ds = type("DS", (), {"dayfile_cache": c})()
    t = _th.Thread(target=sch._dayfile_sweep_loop, args=(ds, 0.01), daemon=True)
    t.start()
    _t.sleep(0.1)
    stop.set()
    t.join(timeout=2)
    assert len(c) == 0
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_scheduler.py::test_dayfile_sweep_loop_evicts_expired -q`
Expected: FAIL，`AttributeError: module '...scheduler' has no attribute '_dayfile_sweep_loop'`。

- [ ] **Step 3: 实现**

`backend/app/services/stockdata/scheduler.py`，`_midnight_clear_loop` 之后插入：

```python
def _dayfile_sweep_loop(data_sources, interval: float = 10.0) -> None:
    """每 10s 清扫日线日期文件缓存：先踢超 60s 未访问，再按最后访问踢旧至 ≤60。

    纯后台清扫、访问时不清理（spec 2026-08-21-stockdata-daily-dayfile-lru-design
    第 1 节卸载规则）；异常只记日志不退出，下次循环继续。
    """
    while not _stop.is_set():
        time.sleep(interval)
        try:
            evicted = data_sources.dayfile_cache.sweep()
            if evicted:
                logger.debug("stockdata dayfile cache sweep evicted %d files", evicted)
        except Exception:  # noqa: BLE001
            logger.warning("stockdata dayfile cache sweep failed", exc_info=True)
```

`start_scheduler` 中：

```python
    if data_sources is not None:
        targets.append(lambda: _midnight_clear_loop(data_sources))
        targets.append(lambda: _dayfile_sweep_loop(data_sources))
```

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_scheduler.py -q`
Expected: 全部 PASS。

- [ ] **Step 5: lint + 提交**

```bash
cd backend && uv run --extra dev ruff check app/services/stockdata/scheduler.py
git add backend/app/services/stockdata/scheduler.py backend/tests/quant/test_stockdata_scheduler.py
git commit -m "feat(stockdata): scheduler 后台清扫线程卸载日线日期文件缓存"
```

---

### Task 4: 模拟盘在线删除 preload 调用

**Files:**
- Modify: `backend/app/quant/simulate/runner.py`（`_make_dm` 306-310 行；`_pre_market` 盘前块 555-561 行）

**Interfaces:**
- Consumes: 无（删除调用，不动 `DataManager`）。
- Produces: `_make_dm()` 不再调 `dm.preload_daily()`；`_pre_market` 不再调 `dm.preload_daily()`。注释写明原因防回退。

- [ ] **Step 1: 先跑相关测试锁定基线**

Run: `uv run --extra dev pytest tests/quant/test_runner_strategy.py tests/quant/test_fix_sim.py tests/quant/test_sim_daemon.py -q`
Expected: 全部 PASS（`test_runner_strategy.py` 的 `_FakeDM.preload_daily` 是 `pass` 桩，删除调用不影响任何断言）。

- [ ] **Step 2: 删除 `_make_dm` 内 preload 调用**

`backend/app/quant/simulate/runner.py`，`_make_dm` 中：

```python
    dm._diag_minute = os.getenv("SIM_MINUTE_DIAG", "") == "1"
    try:
        dm.preload_daily()
    except Exception as e:  # noqa: BLE001
        log.warning("[runner] 日线预加载失败（策略取数将按需回源）: %s", e)
    return dm
```

替换为：

```python
    dm._diag_minute = os.getenv("SIM_MINUTE_DIAG", "") == "1"
    # 在线模式不预载全市场日线：日线按需读，stockdata 服务端经日线日期文件 LRU
    # 命中后逐块 chunk 请求秒级（spec 2026-08-21-stockdata-daily-dayfile-lru-design
    # 第 3 节）。离线回测才需要 preload_daily（rqalpha_bridge，offline 未命中即抛错）。
    return dm
```

- [ ] **Step 3: 删除 `_pre_market` 内 preload 调用**

```python
    # 盘前刷新最新交易日的日线数据（策略 get_price('1d') 会用到）
    dm = aux.get("dm") if aux is not None else None
    if dm is not None:
        try:
            dm.preload_daily()
        except Exception:  # noqa: BLE001
            pass
```

替换为：

```python
    # 盘前日线新鲜度由按需取数保证（get_price/get_history 走网络批量读最新分区，
    # 服务端 LRU 命中），不再整体预载全市场日线（见 _make_dm 注释）。
    dm = aux.get("dm") if aux is not None else None
```

- [ ] **Step 4: 跑相关测试确认通过**

Run: `uv run --extra dev pytest tests/quant/test_runner_strategy.py tests/quant/test_fix_sim.py tests/quant/test_sim_daemon.py -q`
Expected: 全部 PASS。

- [ ] **Step 5: lint + 提交**

```bash
cd backend && uv run --extra dev ruff check app/quant/simulate/runner.py
git add backend/app/quant/simulate/runner.py
git commit -m "feat(simulate): 在线模拟盘删除日线预载，改服务端 LRU 按需读（spec 2026-08-21）"
```

---

### Task 5: 全量回归 + 内存实测（验证任务，无代码改动）

**Files:**
- 无代码改动。运行验证命令。

**Interfaces:**
- Consumes: Task 1-4 全部产物；已运行的 stockdata 服务（pid 47369，旧代码）；模拟盘账户 `960366ab`（五福v5.4-ptrade对齐，运行中）；`data/quant.db`（仓库根）。
- Produces: 验收结论（对齐零差 + RSS <600MB）。

- [ ] **Step 1: 重启 stockdata 服务加载新代码**

```bash
# 先找到 stockdata 服务实际 pid（ps -o pid,rss,etime -C python | grep -i stockdata），
# 用实际 pid 替换下面的 PID 再 kill
ps -o pid,rss,etime,cmd -C python | grep stockdata
kill <PID> && sleep 1 && ss -tlnp | grep -E ":3322|:3018|:3011"
```

Expected: guardian 3s 内自动拉起新进程；`:3322` 监听恢复。若 10s 内未恢复，手动 `setsid python scripts/run_stockdata_service.py ...`（参照 AGENTS.md「stock data 服务」节）。

- [ ] **Step 2: 记录新 RSS 基线**

```bash
ps -o pid,rss,etime,cmd -C python | grep stockdata
```

Expected: 新进程 RSS 远低于旧 ~1.16GB（冷启动后应 ~100-200MB）。

- [ ] **Step 3: 全量单元回归**

```bash
cd backend && uv run --extra dev pytest tests/quant/ -q --ignore=tests/quant/test_fix_bridge_runtime.py
```

Expected: 全部 PASS（既有 flaky `test_h4_h5_universe_writable_and_run_daily_fires_in_daily_mode` 若再现为允许跳过；记录 pass/fail 数）。

- [ ] **Step 4: 对齐回测（走新 get_daily/preload_daily 路径）**

```bash
cd backend && uv run --extra dev pytest -m integration tests/quant/test_70978ed5_ptrade_alignment.py -q
```

Expected: PASS（jq vs ptrade 逐日收益差 ≤0.05%、交易组对齐）。顺带：`test_wufu_backtest_perf.py -m integration`（性能门禁 ≤120s）。

- [ ] **Step 5: 内存实测（回测触发全路径后观察 RSS 峰值与回落）**

回测 Step 4 运行期间及结束后各采一次：

```bash
ps -o rss= -p <stockdata_pid>   # 换算 MB：rss/1024
```

Expected: 峰值显著低于旧 1.16GB；目标 **<600MB**。若 >600MB：检查是否 `preload_daily` 仍在驻留（看 60s 后是否回落）或分钟库/去重缓存异常膨胀，回头查 Task 2 实现。

- [ ] **Step 6: 模拟盘重启前后对齐**

```bash
# 1) 停掉旧模拟盘进程，导出重启前 sim_trades/净值快照（API：GET /api/quant/sim/accounts/960366ab/trades 等）
# 2) 用同一策略 70978ed5.ptrade.py 重启账户 960366ab（在线模式）
# 3) 对比重启前后 sim_trades 逐笔 + 每日净值逐日
```

Expected: 重启前后零差（spec 第 4 节 3）。若在线按需读路径有缺数据/时差问题，回查 Task 2/4。

- [ ] **Step 7: 提交验证结论到 spec 状态（如无问题）**

```bash
git add docs/superpowers/specs/2026-08-21-stockdata-daily-dayfile-lru-design.md
git commit -m "docs(stockdata): 日线日期文件 LRU 设计验证通过（RSS 基线 -> 实测峰值）"
```

（仅当 Step 4/5/6 全过；RSS 数字填入 commit message 或 spec 状态行。）

---

## Self-Review（计划完成时逐项核对）

1. **Spec 覆盖**：第 1 节缓存语义→Task 1（get/load/sweep/并发/原始单位 8 列）；第 2 节服务端→Task 2（get_daily 删 daily: 缓存、preload 语义不变、清扫线程→Task 3、单测）；第 3 节客户端→Task 4（模拟盘删两处、rqalpha_bridge 不动由 Task 5 Step 4 回归验证）；第 4 节验证→Task 5。
2. **占位符扫描**：无 TBD/TODO；每步含完整代码与命令。
3. **类型一致性**：`DayFileCache.get/get_or_load/sweep` 签名在 Task 1 定义、Task 2/3 消费处一致；`_read_day_file`/`_existing_day_files` Task 2 内定义并只用在同一 Task；`_dayfile_sweep_loop(data_sources, interval=10.0)` Task 3 定义与测试调用一致；`preload_daily(lookback_days=400, asof=None)` 与 `get_daily(codes, start_date, end_date)` 签名未变（客户端零改动约束）。