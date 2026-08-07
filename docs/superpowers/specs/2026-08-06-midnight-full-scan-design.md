# 00:00 全量数据缺失检测 + 自动补全设计

- 日期：2026-08-06
- 状态：已批准
- 关联：`app/services/stockdata/scheduler.py`、`app/services/mootdx_service.py`

## 背景与动机

08-05 曾发生盘中写入半日日线快照（`kline_daily`/`kline_index_daily` 分区 mtime 早于 15:00），
且 `_missing_daily_days` 因 `latest==today` 判定"已覆盖"，导致该坏数据被永久跳过、永不修正。

现有检测仅覆盖「最新分区 → 今天」的尾部缺口，**不检测 4/1 至今的中间洞**
（如某交易日分区整体缺失但 latest 已是最新），也**没有定时全量巡检**。

本设计新增每日 00:00 的全量分区检测 + 自动补全任务：
- 检测：4/1（`STOCK_MINUTE_START`）至今，5 类数据按交易日历逐日比对分区是否存在（中间洞也检）。
- 补全：逐缺失日复用现有 `sync_*` 回源函数，**有数据才回**（上市日晚于目标日 / 当日停牌 → 跳过，不落盘）。

## 目标与非目标

### 目标
1. 每日 00:00 自动巡检 5 类数据集的**分区缺失**（4/1 → 今天）。
2. 缺失日自动回源补全（日线/指数/ETF 日线/ETF 分钟/股票分钟）。
3. 补全遵循「有数据才回」：不无脑拉取上市日之后/停牌期间的空数据。
4. 复用现有 `sync_daily` / `sync_index_daily` / `sync_etf_minute` / `sync_stock_minute`，不重复实现取数。

### 非目标
- 不逐 symbol 校验分钟数据完整性（仅分区级检测；分区存在即视为该日已覆盖）。
- 不修复已存在但内容损坏的分区（如盘中半日快照）——该能力已由 `_stale_today_daily_days` 自愈覆盖。
- 不做「交易日历缺失」的兜底修正（`_trade_days_in_range` 失败时回退工作日近似，不阻断）。

## 架构与组件

### 组件 1：`mootdx_service._trade_days_in_range(start, end) -> list[_date]`
完整交易日历（绕开 `_DAILY_BACKFILL_LIMIT_DAYS=90` 窗口）：
- `MootdxSource().get_daily("000300.XSHG", start, end)` 推导交易日。
- 失败时回退工作日近似（沿用 `_trade_days_up_to` 的兜底逻辑）。
- 输入 `start` 默认 `STOCK_MINUTE_START`（2026-04-01），`end` 默认今天。

### 组件 2：`mootdx_service.scan_missing_partitions(start=None) -> dict[str, list[_date]]`
分区级缺失检测，对 5 类数据返回缺失交易日列表：
- `kline_daily`（股票日线）
- `kline_etf_daily`（ETF 日线）
- `kline_index_daily`（指数日线）
- `kline_etf_minute`（ETF 分钟）
- `kline_minute`（股票分钟）

实现：
```python
calendar = _trade_days_in_range(start or STOCK_MINUTE_START, today)
missing = {
    "kline_daily":       _missing_days_in(calendar, STOCK_DAILY_ROOT),
    "kline_etf_daily":   _missing_days_in(calendar, ETF_DAILY_ROOT),
    "kline_index_daily": _missing_days_in(calendar, INDEX_DAILY_ROOT),
    "kline_etf_minute":  _missing_days_in(calendar, ETF_MINUTE_ROOT),
    "kline_minute":      _missing_days_in(calendar, STOCK_MINUTE_ROOT),
}
```
其中 `_missing_days_in(calendar, root)` = `[d for d in calendar if d 不在 root 的 date= 分区]`。

### 组件 3：`mootdx_service.sync_stock_minute_day(day) -> int`
按缺失日补全股票分钟（新增）：
- 从 `_stock_universe()` 取全市场股票，跳过北交所。
- `_listing_date_map()` 过滤：`listing_date > day` 的 symbol 跳过（该日尚未上市，无数据）。
- 逐 symbol `_guarded_get_minute(src, sym, max_bars=40000)`（全量历史）→ 过滤 `datetime.date == day`。
- 当日有数据的 bar 写入 `kline_minute/date={day}/part.parquet`（读旧→concat→unique→原子替换，复用 `_flush_stock_minute_chunk` 的分区写逻辑）；无数据（停牌/无记录）自然跳过。
- 失败 symbol 追加 `mootdx_sync_failures.csv`（复用 `_append_failure`）。
- 返回写入行数。

### 组件 4：`mootdx_service.backfill_missing_partitions(missing) -> dict`
逐缺失日复用现有 sync 函数：
| 数据集 | 缺失日处理 | 复用函数 |
|---|---|---|
| `kline_daily` | 逐日 | `sync_daily(day)` |
| `kline_etf_daily` | 逐日 | `sync_daily(day)`（内部含 ETF） |
| `kline_index_daily` | 逐日 | `sync_index_daily(day)` |
| `kline_etf_minute` | 逐日 | `sync_etf_minute(day)`（历史日走 `get_minute` 分支，见下） |
| `kline_minute` | 逐日 | `sync_stock_minute_day(day)` |

ETF 分钟历史日适配：
- 现有 `sync_etf_minute(day)` 用 `get_minute_recent`（只覆盖近几日）。为支持 4/1 起的历史缺失日，
  `sync_etf_minute` 增加按 `day` 判断：`day` 距今较远（>~5 交易日）时改用 `get_minute(code)` 全量拉 + 过滤当日，
  否则沿用 `get_minute_recent`。此改动保持现有调用（`sync_etf_minute()` 默认今天）行为不变。

单日失败捕获进 `errors`，不阻断其它缺失日。

### 组件 5：`mootdx_service.scan_and_backfill_full() -> dict`
组合：
```python
missing = scan_missing_partitions()
backfilled = backfill_missing_partitions(missing)
return {"missing": missing, "backfilled": backfilled, "errors": [...]}
```
- 结束打 INFO 日志汇总各数据集补全日。
- 若任一分区有缺失且补全后有残留错误 → 打 WARNING（可复用于钉钉通知）。

### 组件 6：`stockdata/scheduler.py._midnight_scan_loop`
- 00:00 触发一次 `scan_and_backfill_full()`（后台线程）。
- 用 `_sync_lock` 与 15:35 cron 串行，避免并发回源。
- 记录 `_scheduler_state["last_full_scan"]` 与 `["full_scan_result"]`。
- 接入 `start_scheduler` 的 `targets`。

## 数据流

```
00:00  --scheduler-->  _midnight_scan_loop
                        └─ mootdx_service.scan_and_backfill_full()
                            ├─ scan_missing_partitions()
                            │    └─ _trade_days_in_range(4/1, today)  （交易日历）
                            │    └─ _missing_days_in(calendar, root)  （5 类数据集）
                            ├─ backfill_missing_partitions()
                            │    ├─ kline_daily      → sync_daily(day)
                            │    ├─ kline_etf_daily  → sync_daily(day)
                            │    ├─ kline_index_daily→ sync_index_daily(day)
                            │    ├─ kline_etf_minute → sync_etf_minute(day)（历史日 get_minute）
                            │    └─ kline_minute     → sync_stock_minute_day(day)（上市日过滤）
                            └─ 日志 / 汇总
```

## 错误处理

- 交易日历获取失败 → 回退工作日近似，检测继续。
- 单个缺失日回源失败 → 记 `errors`，继续其它缺失日。
- 单 symbol 分钟拉取超时/异常/空 → `_append_failure` + 跳过，不阻断。
- 与 15:35 cron / 启动 backfill 的并发 → `_sync_lock` 串行化。

## 测试

1. `_trade_days_in_range`：返回 4/1~今天交易日（mock `MootdxSource.get_daily`）；失败回退工作日。
2. `scan_missing_partitions`：tmp 分区造中间洞（如缺 6/15），断言该日出现在对应数据集缺失列表；其余数据集不受影响。
3. `_missing_days_in`：日历全有 → 空；中间缺一天 → 只报那一天。
4. `sync_stock_minute_day`：mock `get_minute` 返回含目标日 bar 的帧 → 写入该日分区；上市日晚于目标日的 symbol 不调用取数；当日无 bar（停牌）不写。
5. `backfill_missing_partitions`：mock 各 `sync_*` 记录调用 → 断言逐缺失日正确路由；单日抛异常记 errors 不阻断。
6. `_midnight_scan_loop`：mock 时间触发一次，断言调 `scan_and_backfill_full` 且与 15:35 串行。

## 兼容性

- `sync_etf_minute` / `sync_daily` / `sync_index_daily` / `sync_stock_minute` 签名与默认行为不变（仅 `sync_etf_minute` 内部增加历史日分支）。
- `_missing_daily_days` / `_missing_minute_days` 保留（供启动 backfill 与检测逻辑复用）。
- 新代码不引入 pandas 之外的依赖。
