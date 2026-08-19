# 本地数据内容校验（近 1 周自动 + 手动单日/全量补齐）设计

日期：2026-08-19
分支：`fix/content-validation-1y`（基于 `custom-main`）

## 背景与目标

mootdx 自治回源服务（`mootdx_service.py`）按交易日写 5 类 Parquet 分区：
`kline_daily`（股票日线）、`kline_minute`（股票分钟）、`kline_etf_daily`（ETF日线）、
`kline_etf_minute`（ETF分钟）、`kline_index_daily`（指数日线）。

存在两类已观察到的缺陷：

1. **内容残缺残帧永不修复**：分区目录存在即被 `_missing_days_in`（分区级）视为"已覆盖"。
   例：`kline_index_daily/date=2026-07-31` 因当时 `instruments_index` 缺失而用兜底 4 只指数
   （000300/000510/399006/399101）写入，此后分区级扫描永远跳过它，4/600 残帧永久残留。
   目前仅有 ETF 日线（`_incomplete_etf_daily_days`）与股票分钟（`_incomplete_stock_minute_days`）
   有内容级校验，且回看窗口仅 30 个分区。
2. **新交易日股票分钟当日不落盘**：`sync_stock_minute` 的 resume 逻辑只读**最新分区**的
   symbol 集合，最新分区（昨天）完整时 `todo` 为空 → 15:35 收盘 cron 与启动回源都跳过
   今天，当天分钟要拖到次日 00:00 巡检才补。

目标：

- 5 类数据统一内容级校验（symbol 覆盖率 vs 基准宇宙），自动巡检回看**近 1 周交易日**，
  手动全量校验回看**近一年（250 个交易分区）**。
- 修复 resume 架空：新交易日收盘后当天即可拉取（15:35 cron / 启动回源）。
- 前端「本地股市数据」页：每行（日期）一个「检验」按钮做单日检验补齐；顶部一个
  「全量检验补齐」按钮做全量检验补齐。

## 需求

### 自动校验窗口

- 每日自动（00:00 巡检 `scan_and_backfill_full`、启动回源 `backfill_to_now`）：
  内容校验只看最近 `_DAILY_CHECK_RECENT_PARTITIONS`（默认 7）个交易分区。
- 手动全量：内容校验窗口 `_CONTENT_CHECK_RECENT_DAYS`（默认 250，≈1 年交易日）。
- 分区级缺失（`_missing_days_in`，目录不存在）始终全窗口（从 `STOCK_MINUTE_START` 4/1 起），不随内容窗口收窄。

### 内容校验逻辑

对某数据类型的某日分区：读该分区内全部 `*.parquet` 的 `symbol` 列并集，与基准宇宙取交集，
覆盖率 = `|∩| / |宇宙|`，`< _CONTENT_CHECK_MIN_COVERAGE`（默认 0.5）即判残缺，需重写。

- 宇宙为空 / 分区根为空 → 跳过（无基线可比，不误判）。
- 盘中（<15:00）跳过当日分区（当日分钟/日线本就未走完，覆盖率天然偏低，误判会触发
  全市场重拉且写回半程数据）；收盘后纳入。
- 基准宇宙与分区 symbol 格式归一化：
  - 股票日线 / 股票分钟 / 指数日线：直接 `.SH/.SZ` 对比（`_stock_universe` / `_index_universe`）。
  - ETF 日线 / ETF 分钟：`_etf_universe()` 返回 JQ 格式 `.XSHG/.XSHE`，经 `_to_tf_symbol` 归一为 `.SH/.SZ`。

### 修复动作（重写）

| 数据类型 | 重写函数 |
|---|---|
| 股票日线 / ETF日线 | `sync_daily(day)` |
| 指数日线 | `sync_index_daily(day)` |
| ETF分钟 | `sync_etf_minute(day)` |
| 股票分钟（单日） | `sync_stock_minute_day(day, symbols=missing)`，只补缺失 symbol |
| 股票分钟（缺失交易日） | `sync_stock_minute_range(days)`（整日缺失/多日） |

## 后端核心（`backend/app/services/mootdx_service.py`）

### 常量

- `_CONTENT_CHECK_RECENT_DAYS = int(os.getenv("CONTENT_CHECK_RECENT_DAYS", "250"))`
- `_DAILY_CHECK_RECENT_PARTITIONS = int(os.getenv("DAILY_CHECK_RECENT_PARTITIONS", "7"))`
- `_CONTENT_CHECK_MIN_COVERAGE = float(os.getenv("CONTENT_CHECK_MIN_COVERAGE", "0.5"))`
- 保留 `_STOCK_MINUTE_RECENT_LIMIT`（env 默认改为 250）与 `_STOCK_MINUTE_MIN_COVERAGE`（env 默认 0.5）
  作为股票分钟 legacy 覆盖；删除 `_ETF_DAILY_RECENT_LIMIT` / `_ETF_DAILY_MIN_COVERAGE`
  （由共享常量取代）。

### 通用 helper

```python
def _incomplete_partition_days(root, target, recent, min_coverage,
                               skip_today_intraday=True) -> list[_date]:
    """最近 recent 个分区 symbol 覆盖率 < min_coverage 即判残缺。"""
```

- `_partition_dates(root)` 为空 / `target` 为空 → `[]`。
- 只查 `existing[-recent:]`；`recent` 大于分区数时取全量。
- 盘中且当日 → 跳过（`skip_today_intraday=True` 时）。
- 汇总日志对齐现有 `_incomplete_*_days` 风格。

### 5 类包装（保留既有函数名与 `recent=None` 签名，测试兼容）

| 函数 | root | 宇宙基线 | 状态 |
|---|---|---|---|
| `_incomplete_stock_daily_days` | `STOCK_DAILY_ROOT` | `_stock_universe()` | 新增 |
| `_incomplete_etf_daily_days` | `ETF_DAILY_ROOT` | `_etf_universe()`→`_to_tf_symbol` | 重构为 helper |
| `_incomplete_index_daily_days` | `INDEX_DAILY_ROOT` | `_index_universe()` | 新增 |
| `_incomplete_etf_minute_days` | `ETF_MINUTE_ROOT` | `_etf_universe()`→`_to_tf_symbol` | 新增 |
| `_incomplete_stock_minute_days` | `STOCK_MINUTE_ROOT` | `_stock_universe()` | 重构为 helper |

`recent=None` 时各函数用 `_CONTENT_CHECK_RECENT_DAYS`（全量窗口）作为默认；显式传
`recent` 时以参数为准。

### 扫描 / 巡检

- `scan_missing_partitions(start=None, content_recent=None)`：新增 `content_recent` 参数，
  默认 `_DAILY_CHECK_RECENT_PARTITIONS`（7），透传给全部 `_incomplete_*_days(recent=...)`。
  5 类均做 `_missing_days_in(calendar, root) | _incomplete_*_days(recent=content_recent)`，
  并保留 ETF 日线 `_stale_daily_days` / 股票日线 `_stale_daily_days` 的既有并入。
- `scan_and_backfill_full(content_recent=None)`：透传给 `scan_missing_partitions`，
  其余不变（`backfill_missing_partitions` 复用现有）。
- `backfill_to_now()`：各段缺失判定并入对应 `_incomplete_*_days(recent=_DAILY_CHECK_RECENT_PARTITIONS)`；
  股票分钟改为 `min_days = _missing_stock_minute_days() | _incomplete_stock_minute_days(recent=7)` →
  `sync_stock_minute_range(min_days)` 后再 `sync_stock_minute(limit=STOCK_MINUTE_BATCH_LIMIT)`；
  `result["missing"][*]["missing"]` 布尔并入内容校验结果（保证 `_notify_missing` 告警）。

### 单日 / 全量手动入口（新增）

```python
def check_and_repair_day(day: _date) -> dict:
    """单日检验补齐：对该日 5 类逐类查内容，残缺/缺失则重写。

    返回 {"day": str, "results": {type: {"status": "ok"|"repaired"|"skip"|"failed",
                                          "coverage": float|None, "symbols": int}}}
    """

def check_and_repair_full(content_recent: int | None = None) -> dict:
    """全量检验补齐：scan_and_backfill_full(content_recent=...) 的汇总。

    content_recent 默认 _CONTENT_CHECK_RECENT_DAYS（250）。
    """
```

`check_and_repair_day` 的股票分钟：先读当日分区 symbol，与 `_stock_universe()` 求差集得
`missing`，`sync_stock_minute_day(day, symbols=missing)`（残缺少时快；当日停牌标的天然
无 bar，不在 missing 集合内即可，差集取分区 symbol 缺失的部分，停牌标的在宇宙里但分区
无 → 会被重拉一次，属可接受成本）。

### 新日当日拉取（resume 架空修复）

- 新增 `_missing_stock_minute_days(now=None)`：镜像 `_missing_minute_days`，读
  `STOCK_MINUTE_ROOT`（`_partition_dates` 最新分区 < d ≤ today 的交易日；盘中排除今天）。
  `_missing_minute_days(now)` 签名保持不动（现有测试按位置传 `now`）。
- `sync_stock_minute()` 开头（残片自愈之前）：
  ```python
  missing_days = _missing_stock_minute_days()
  if missing_days:
      total += sync_stock_minute_range(missing_days)
  ```
  之后照常残片自愈 + resume 增量。效果：
  - 15:35 cron `sync_stock_minute(limit=None)`：最新分区是昨天、今天无分区 → range 全市场
    拉今天（~2h）→ 当天落盘，不再拖次日 00:00。
  - 启动回源 `sync_stock_minute(limit=20)` 同样先补缺失交易日。
  - 盘中守卫：`_missing_stock_minute_days` 盘中排除今天，不写半程数据。

## stockdata 服务（`backend/app/services/stockdata/`）

- `scheduler.trigger_sync(kind, **params)` 新增 kinds：
  - `check_day`：`params["day"]`（YYYY-MM-DD）→ 后台线程跑 `check_and_repair_day(day)`。
  - `check_full`：后台线程跑 `check_and_repair_full()`（content_recent=250）。
  - 与 `_sync_lock` 串行（避免与 15:35 cron / 00:00 巡检争 mootdx socket）。
  - 结果写入 `_scheduler_state`（如 `last_check_day` / `check_result`）并打 INFO 汇总日志。
- `handlers.h_trigger_sync`：`kind` 之外透传 `day` 等 params（`trigger_sync(kind, **{k: v for k, v in p.items() if k != "kind"})`）。

## 主后端 API（`backend/app/api/data.py`）

- `POST /api/data/check-day`：body `{"date": "YYYY-MM-DD"}` → `StockDataClient.trigger_sync("check_day", day=...)`，返回 `{"ok": True}`。日期非法 → 400。
- `POST /api/data/check-full`：→ `StockDataClient.trigger_sync("check_full")`，返回 `{"ok": True}`。
- 需鉴权（与 data.py 其它端点一致）。stockdata 服务不可达时返回 503。

## 前端（`frontend/src/pages/LocalData.tsx` + `lib/api.ts`）

- `api.ts` 新增：
  - `checkDay: (date: string) => request<{ ok: boolean }>('/api/data/check-day', { method: 'POST', body: JSON.stringify({ date }) })`
  - `checkFull: () => request<{ ok: boolean }>('/api/data/check-full', { method: 'POST' })`
- `LocalData.tsx`：
  - 顶部（表格上方）「全量检验补齐」按钮：点击调 `checkFull`，loading 态，成功后 Toast
    提示并刷新当前页统计。
  - 每行末新增一列「操作」：单个「检验」按钮调 `checkDay(row.date)`，成功后 Toast +
    刷新当前页统计。
  - 触发为异步（后台线程执行），前端不做结果轮询；统计刷新靠
    `invalidateQueries(QK.localMarketStats(...))` 后在按钮侧短暂延时（如 3s）再刷一次。

## 错误处理

- 后端核心：单类失败不阻断其它类（`check_and_repair_day` 逐类 try/except，`status=failed`）。
- 内容校验：宇宙读取失败降级为空 → 跳过该校验（不误报）。
- 前端：接口失败走全局 401 / Toast；按钮 disabled 于进行中。

## 测试（`backend/tests/quant/test_mootdx_backfill_coverage.py` 追加）

- `_incomplete_index_daily_days` 识别 4/600 残帧（07-31 回归）。
- `_incomplete_stock_daily_days` / `_incomplete_etf_minute_days` 残缺识别。
- 窗口：残缺分区位于第 31~250 位之间时，`recent=250` 能识别、`recent=7` 不识别。
- `scan_missing_partitions(content_recent=7)` 只报近 1 周内容残缺；`content_recent=250` 报更早残帧。
- `_missing_stock_minute_days`：盘中排除今天 / 收盘含今天 / 补昨天缺失交易日。
- resume 修复：latest 分区=昨天且完整、今天无分区、已收盘 → `sync_stock_minute(limit=None)`
  先触发 `sync_stock_minute_range([today])` 再 resume。
- `check_and_repair_day`：单日残缺触发对应 sync 重写；股票分钟只补缺失 symbol；
  全部正常 → 不触发任何 sync。
- `check_and_repair_full`：汇总 missing/backfilled。
- 既有测试回归：`_missing_minute_days(now)` 位置参数、`_incomplete_*` recent=3、
  backfill 全家桶、scan 中间洞。

## 验证命令

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -q
cd backend && uv run --extra dev ruff check app
cd backend && uv run --extra dev mypy app
cd frontend && pnpm lint && pnpm build
```

## 非目标

- 不含 baostock `kline_5min` 管线（独立一次性回源）。
- 不做指数分钟线（`kline_index_minute`）采集与校验（目录不存在，表格列恒 0）。
- 不做前端结果轮询/进度条（触发式异步，结果在日志与 `_scheduler_state`）。
- 不改动回测/模拟盘对分区数据的读取语义。