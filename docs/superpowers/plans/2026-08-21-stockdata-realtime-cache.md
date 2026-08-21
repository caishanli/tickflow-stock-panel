# stockdata 实时路径缓存 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 stockdata 服务两处无缓存重复全扫——`get_realtime_snapshot` 的当日 ETF 分钟分区扫描改走既有 `DayFileCache`，`get_adj_factors` 走既有 DedupCache（TTL 300s）。

**Architecture:** 零新类。`_read_day_file` 泛化 `cols` 参数（默认日线 8 列不变），realtime 当日分区经 `dayfile_cache.get_or_load("kline_etf_minute", today, ...)` 缓存全市场当日帧、读侧 filter symbols（同 get_daily 模式）；adj_factors 抽出 `_load_adj_factors` 私有方法后用 `get_or_fetch("adj_factors", 300.0, ...)` 包装。

**Tech Stack:** Python 3.12 / Polars / 既有 DayFileCache（sources.py）与 DedupCache.get_or_fetch

## Global Constraints

（来自 spec `docs/superpowers/specs/2026-08-21-stockdata-realtime-cache-design.md`）

- 零新类：复用 `DayFileCache` 与 `get_or_fetch`（DedupCache），不引入新缓存结构。
- 缓存存**全市场当日帧**，读侧 filter（同 get_daily 模式）；键 `("kline_etf_minute", date)`。
- `_read_day_file` 默认行为不变（日线 8 列），既有 get_daily/preload_daily 调用点零改动。
- adj_factors TTL **300.0** 秒；key 为 `"adj_factors"`。
- 时效性：ETF 分钟分区仅 15:35 收盘同步写入，盘中通常不存在（loader 返回 None 不缓存，行为同现状）；TTL 60s 兜底陈旧窗口。
- 不做：get_minute 多日范围缓存、MinuteMemoryStore 改造、分配器调优、15:35 同步任务优化。
- 测试命令从 `backend/` 目录：`uv run --extra dev pytest <file> -q`（pyproject 已配 addopts 排除 integration）。
- 工作区有无关未提交改动时，`git add` 只加本任务文件。

---

### Task 1: realtime 当日分钟分区走 DayFileCache

**Files:**
- Modify: `backend/app/services/stockdata/sources.py`
  - `_read_day_file`（381 行）：加 `cols` 参数
  - `get_realtime_snapshot`（~521-526 行）：当日分区扫描改走 dayfile_cache
- Test: `backend/tests/quant/test_stockdata_sources.py`（新增 1 个测试）

**Interfaces:**
- Consumes: `self.dayfile_cache.get_or_load(subdir, date, loader)`（Task LRU-1 产物，签名不变）；`_MINUTE_COLS`（模块常量）；`_as_datetime`；`_in_trading`。
- Produces:
  - `_read_day_file(self, subdir: str, date: str, cols: list[str] | None = None) -> pl.DataFrame | None`——cols=None 时行为与旧版完全一致（日线 8 列）。
  - `get_realtime_snapshot(codes, as_of=None)` 对外签名与返回帧结构不变（缓存透明）。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_stockdata_sources.py` 中 `test_realtime_snapshot_serves_from_memory` 之后追加：

```python
def test_realtime_snapshot_uses_dayfile_cache(src, monkeypatch):
    """当日分钟分区经 DayFileCache：二次调用不再读盘（删源文件仍命中）。"""
    import os
    import shutil

    day = _dt.date.today().isoformat()
    _write_minute(str(src.data_root), "kline_etf_minute", day, [
        {"symbol": "512670.SH", "datetime": f"{day} 00:00:01", "open": 1.0,
         "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1000, "amount": 1000.0},
    ])
    # 非交易时段门控：不触网，只验读路径
    monkeypatch.setattr("app.services.stockdata.sources._in_trading", lambda *a, **k: False)
    got1 = src.get_realtime_snapshot(["512670.XSHE"])
    assert not got1.is_empty()
    assert src.dayfile_cache.get("kline_etf_minute", day) is not None
    # 删掉底层分区文件后二次调用仍命中缓存（证明未重扫）
    shutil.rmtree(os.path.join(str(src.data_root), "kline_etf_minute", f"date={day}"))
    got2 = src.get_realtime_snapshot(["512670.XSHE"])
    assert not got2.is_empty()
    assert got2["close"].to_list() == got1["close"].to_list()
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py::test_realtime_snapshot_uses_dayfile_cache -q`
Expected: FAIL——第二次调用返回空帧（旧实现每次重扫已删除的分区文件）。

- [ ] **Step 3: 实现**

3a. `_read_day_file`（381 行）改为：

```python
    def _read_day_file(self, subdir: str, date: str,
                       cols: list[str] | None = None) -> pl.DataFrame | None:
        """读单个日期分区（含全市场标的）→ 原始帧；分区不存在返回 None。

        cols 缺省为日线 8 列；分钟分区传 _MINUTE_COLS。
        """
        root = os.path.join(self.data_root, subdir, f"date={date}")
        if not os.path.isdir(root):
            return None
        import glob as _glob
        paths = _glob.glob(os.path.join(root, "*.parquet"))
        if not paths:
            return None
        if cols is None:
            cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        lf = pl.scan_parquet(paths, hive_partitioning=True)
        return _as_datetime(lf.select(cols).collect())
```

3b. `get_realtime_snapshot` 中（~521-526 行）：

```python
        # 基础帧：当日分区（收盘同步/重启场景）+ 内存库（网络实时）
        base_parts = []
        part = self._scan_partitions("kline_etf_minute", today.isoformat(),
                                     today.isoformat(), tf_syms, _MINUTE_COLS)
        if not part.is_empty():
            base_parts.append(part)
```

替换为：

```python
        # 基础帧：当日分区（收盘同步/重启场景，经日期文件 LRU 缓存避免逐请求重扫）
        # + 内存库（网络实时）。spec 2026-08-21-stockdata-realtime-cache-design。
        base_parts = []
        part = self.dayfile_cache.get_or_load(
            "kline_etf_minute", today.isoformat(),
            lambda: self._read_day_file("kline_etf_minute", today.isoformat(), _MINUTE_COLS))
        if part is not None and not part.is_empty():
            base_parts.append(part.filter(pl.col("symbol").is_in(tf_syms)))
```

- [ ] **Step 4: 跑测试确认通过 + 既有 realtime 测试不回归**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py -q`
Expected: 全部 PASS（含既有 test_realtime_snapshot_serves_from_memory / mixed_ns_us_datetime / empty_no_crash / empty_in_trading_no_crash 及新测试）。

- [ ] **Step 5: lint + 提交**

```bash
cd backend && uv run --extra dev ruff check app/services/stockdata/sources.py
git add backend/app/services/stockdata/sources.py backend/tests/quant/test_stockdata_sources.py
git commit -m "feat(stockdata): realtime 当日分钟分区走 DayFileCache（消除逐请求重扫）"
```

---

### Task 2: get_adj_factors 走 DedupCache（TTL 300s）

**Files:**
- Modify: `backend/app/services/stockdata/sources.py`
  - `get_adj_factors`（~683 行）：拆为 TTL 包装 + `_load_adj_factors` 私有方法
- Test: `backend/tests/quant/test_stockdata_sources.py`（新增 1 个测试）

**Interfaces:**
- Consumes: `self.get_or_fetch(key: str, ttl: float, loader: Callable)`（DataSources 既有方法，get_etf_nav 同款）。
- Produces: `get_adj_factors() -> pl.DataFrame` 对外签名与返回结构不变（["symbol", "trade_date", "ex_factor"]）；新增私有 `_load_adj_factors(self) -> pl.DataFrame`。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_stockdata_sources.py` 末尾追加：

```python
def test_get_adj_factors_cached(tmp_path):
    """adj_factors 走 DedupCache：TTL 内二次调用不重扫（删源文件仍返回）。"""
    import os
    import shutil

    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    d = os.path.join(str(tmp_path), "adj_factor_etf")
    os.makedirs(d, exist_ok=True)
    pl.DataFrame({"symbol": ["512670.SH"], "trade_date": ["2026-08-20"],
                  "ex_factor": [1.05]}).write_parquet(os.path.join(d, "all.parquet"))
    s = DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=2)
    try:
        got1 = s.get_adj_factors()
        assert got1["symbol"].to_list() == ["512670.SH"]
        assert got1["ex_factor"].to_list() == [1.05]
        # 删源文件后 TTL 内二次调用仍命中缓存
        shutil.rmtree(d)
        got2 = s.get_adj_factors()
        assert got2["symbol"].to_list() == ["512670.SH"]
    finally:
        os.environ.pop("PARTITION_DATA_ROOT", None)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py::test_get_adj_factors_cached -q`
Expected: FAIL——旧实现无缓存，删源文件后二次调用抛异常或返回空（glob 无 paths → 空 DataFrame，`got2["symbol"]` 抛 ColumnNotFoundError 或 to_list 为空断言失败）。

- [ ] **Step 3: 实现**

`get_adj_factors`（~683 行）整体替换为：

```python
    def get_adj_factors(self) -> pl.DataFrame:
        # 因子表仅除权事件/15:35 同步后变化：TTL 300s 去重即可，
        # 避免每次调用 recursive glob + 全量 scan_parquet（含 lf.columns schema 解析）。
        return self.get_or_fetch("adj_factors", 300.0, self._load_adj_factors)

    def _load_adj_factors(self) -> pl.DataFrame:
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
```

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run --extra dev pytest tests/quant/test_stockdata_sources.py -q`
Expected: 全部 PASS。

- [ ] **Step 5: lint + 提交**

```bash
cd backend && uv run --extra dev ruff check app/services/stockdata/sources.py
git add backend/app/services/stockdata/sources.py backend/tests/quant/test_stockdata_sources.py
git commit -m "feat(stockdata): get_adj_factors 走 DedupCache TTL 300s（消除重复全扫）"
```

---

### Task 3: 回归验证 + 服务重启实测（无代码改动）

**Files:**
- 无代码改动。运行验证命令。

**Interfaces:**
- Consumes: Task 1-2 产物；运行中的 stockdata 服务（改代码不热重载需重启，guardian 自动拉起）。
- Produces: 验收结论。

- [ ] **Step 1: 全量单元回归**

```bash
cd backend && uv run --extra dev pytest tests/quant/ -q
```

Expected: 全绿或仅既有 flaky（`test_h4_h5_universe_writable_and_run_daily_fires_in_daily_mode`）；integration 已由 addopts 默认排除。

- [ ] **Step 2: 重启 stockdata 服务加载新代码**

```bash
ps -eo pid,rss,etime,cmd | grep run_stockdata_service | grep -v grep   # 找实际 pid
kill <PID>; sleep 5; ss -tlnp | grep ":3322"   # guardian 自动拉起
```

Expected: 新进程监听恢复。

- [ ] **Step 3: 实测 repeated realtime 请求命中缓存**

```bash
cd backend && uv run python -c "
import time
from app.quant.datasource.network_client import StockDataClient
c = StockDataClient()
t0=time.time(); c.current_snapshot(['518880.XSHG','512670.XSHG']); t1=time.time()
for _ in range(5): c.current_snapshot(['518880.XSHG','512670.XSHG'])
t2=time.time()
print(f'first={t1-t0:.2f}s next5={t2-t1:.2f}s')
"
```

Expected: next5 明显快于 first（缓存命中；非交易时段分区可能不存在则两者都极快，亦属正常——盘中场景才体现差异）。

- [ ] **Step 4: 观察服务日志不再反复出现 adj_factors 的 PerformanceWarning**

```bash
tail -50 data/stockdata.log | grep -c "lf.columns" || true
```

Expected: 触发过一次 get_adj_factors 后 300s 内不再新增该 warning。

- [ ] **Step 5: 结论记入 ledger（.superpowers/sdd/progress.md），无需提交代码**
