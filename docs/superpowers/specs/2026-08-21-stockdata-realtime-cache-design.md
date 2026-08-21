# stockdata 实时路径缓存设计

日期：2026-08-21
状态：已批准（2026-08-21，方案 A 用户确认）

## 背景与问题

日线日期文件 LRU 缓存（`2026-08-21-stockdata-daily-dayfile-lru-design.md`）落地后，stockdata 服务空闲 RSS 从 ~1.16GB 降至 **289MB**，核心目标达成。剩余两处**无缓存的重复全扫**：

1. `get_realtime_snapshot`（sources.py ~523）：每次请求都 `_scan_partitions("kline_etf_minute", today, today, tf_syms, _MINUTE_COLS)` 重扫当日 ETF 分钟分区。模拟盘每 bar 调用，重复读同一文件。
2. `get_adj_factors`（sources.py ~683）：每次调用 recursive glob + `scan_parquet` 全部 adj_factor_etf 文件 + `lf.columns`（schema 解析，PerformanceWarning 已观测 93 次）+ collect，完全无缓存。

### CPU 根因调查结论（py-spy 实证，2026-08-21）

服务 CPU 高有两个独立原因，**均非 bug、非 LRU 回归**：
- 盘中模拟盘实时补跑取数（kill 模拟盘后 CPU 归零实证）；
- 15:35 收盘批量同步 cron（`sync_stock_minute_range` pandas/polars 重转换，py-spy 栈实证），设计内计划任务。

本设计消除的是上述两处稳态重复扫描开销（CPU + 瞬时内存），不针对同步窗口峰值。

## 方案（已批准：方案 A——复用既有结构，零新类）

### 1. realtime 今日分钟分区走 DayFileCache

- `_read_day_file(subdir, date)` 泛化：加 `cols` 参数（默认日线 8 列不变；分钟传 `_MINUTE_COLS`），`_as_datetime` 处理保持（分钟 datetime 列 ns/us 统一必需）。
- `get_realtime_snapshot` 的当日分区扫描改为：

```python
part = self.dayfile_cache.get_or_load(
    "kline_etf_minute", today.isoformat(),
    lambda: self._read_day_file("kline_etf_minute", today.isoformat(), _MINUTE_COLS))
if part is not None and not part.is_empty():
    base_parts.append(part.filter(pl.col("symbol").is_in(tf_syms)))
```

- 缓存存**全市场当日帧**，读侧 filter（同 get_daily 模式）；键 `("kline_etf_minute", date)` 与日线键天然隔离；今日文件高频访问在 60 槽 LRU 中自然保活。
- **时效性**：ETF 分钟分区仅 15:35 收盘同步写入，盘中通常不存在（loader 返回 None 不缓存，行为同现状）；若分区存在后被同步重写，TTL 60s 兜底陈旧窗口，收盘后场景可接受。

### 2. get_adj_factors 走既有 DedupCache

```python
def get_adj_factors(self) -> pl.DataFrame:
    return self.get_or_fetch("adj_factors", 300.0, self._load_adj_factors)
```

- `_load_adj_factors` 为现函数体抽出的私有方法。
- TTL 300s：因子表仅除权事件 / 15:35 同步后变化，5 分钟新鲜度足够；复用 `get_or_fetch` 既有模式（同 get_etf_nav/get_minute 风格），零新增结构。

## 不做的事（YAGNI）

- get_minute 多日范围扫描不走缓存（方案 C 已否决：非单日文件，改造大风险高）。
- MinuteMemoryStore 不动（当日驻留是设计内语义）。
- 分配器调优不做（实测 glibc 空闲能归还内存：1.2GB 峰值停载后自然回落 289MB）。
- 15:35 同步任务的 CPU/内存不优化（设计内计划任务，超范围）。

## 测试方案

1. 既有 `test_realtime_snapshot_*` 测试必须继续绿（缓存对调用方透明）。
2. 新增：realtime 二次调用 loader 只执行一次（经 dayfile_cache 验证命中）；`get_adj_factors` 二次调用只 scan 一次（TTL 内命中 dedup）。
3. 全量回归 `-m "not integration"` + lint。

## 验收标准

- 单测全绿、0 新增失败；
- 模拟盘运行时 repeated realtime 请求不再逐请求读盘（日志/计数验证）；
- `lf.columns` PerformanceWarning 不再随 get_adj_factors 调用反复出现（TTL 内仅一次）。
