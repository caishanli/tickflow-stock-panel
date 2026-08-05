# baostock 全市场近 3 年回源脚本 — 设计

日期：2026-08-05
分支：`feat/baostock-3y-backfill`（自 `custom-main` 派生）
状态：已获用户批准（2026-08-05，用户直接要求进入实施）

## 背景与约束（实测验证）

用户需求：从 baostock 回源全市场近 3 年「1 分钟」数据（股票/ETF/指数）+ 分红/缩股等公司行动数据，运行时报告进度，支持断点续传，本地落 `data/`，parquet 格式与现有分区一致。

实测结论（baostock 0.9.3 服务端验证，2026-08-05）：

| 数据 | baostock 支持 | 实测结果 |
|------|--------------|---------|
| 1min K 线 | ❌ | `frequency="1"` → 错误 `10004012 请求数据类型不正确`（官方文档仅 d/w/m/5/15/30/60） |
| 股票 5min | ✅ | 单次请求可返回 3 年全部 ~34,848 根；耗时波动大（47s~>100s/只，服务器负载相关） |
| ETF 分钟 | ❌ | 返回 0 行 |
| 指数分钟 | ❌ | 返回 0 行（官方文档：「分钟线不包含指数」） |
| ETF 日线 | ⚠️ | 仅 2026-01-05 至今（更早返回 0 行） |
| 指数日线 | ✅ | 3 年可用（sh.000001/sh.000300 等） |
| 复权因子 | ✅ | `query_adjust_factor(code, start, end)` → 每除权日一行：`code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor` |
| 分红送转 | ✅ | `query_dividend_data(code, year, yearType="operate")` → 每股派息/送股/转增/除权日等 |

其它候选源（实测）：新浪 scale=1 只回最近 ~4.3 交易日；腾讯 m1 只回最近 320 根且本机 DNS 不通；mootdx 1min 只覆盖 ~3 个月；tushare 需 token/积分未配置；a-stock-data skill 无深历史 1min。**无免费源可提供全市场 3 年真实 1min**，用户确认降级为「股票 5min 真实数据」。

存储估算：现有 ETF 分钟分区实测 ~7.6 字节/行（parquet 压缩），3 年全市场 5min（~5200 只 × ~34,560 根 ≈ 1.8 亿行）约 1.4-2GB，磁盘余量 8.4G 足够。

## 目标

独立脚本 `backend/scripts/backfill_baostock_3y.py`，baostock 回源：

1. **股票 5min**（2023-08-06 ~ 今）→ `data/kline_5min/date=YYYY-MM-DD/part.parquet`
2. **指数日线**（3 年）→ `data/kline_index_daily/date=YYYY-MM-DD/part.parquet`
3. **ETF 日线**（仅 2026-01-05 ~ 今，baostock 覆盖上限）→ `data/kline_etf_daily/date=YYYY-MM-DD/part.parquet`
4. **复权因子**（分红/送转/配股/缩股等全部除权事件净效果，3 年）→ `data/adj_factor/all.parquet`
5. **分红送转明细**（3 年）→ `data/dividends/all.parquet`

## 落盘格式（与现有分区一致）

| 表 | 路径 | schema | 说明 |
|----|------|--------|------|
| 股票 5min | `data/kline_5min/date=YYYY-MM-DD/part.parquet` | `symbol: str(.SH/.SZ), datetime: dt[us], open/high/low/close/volume/amount: f64` | 新建目录（`kline_minute` 是 1min 语义，不混写）；volume 单位股、amount 单位元（与现有分区一致） |
| 指数日线 | `data/kline_index_daily/date=.../part.parquet` | `symbol, date, open, high, low, close, volume, amount` | 读旧→concat→unique→原子写，与现有分区合并 |
| ETF 日线 | `data/kline_etf_daily/date=.../part.parquet` | 同上 | 同上 |
| 复权因子 | `data/adj_factor/all.parquet` | `symbol, trade_date, ex_factor` | 与 `adj_factor_etf/all.parquet` 同构；DataManager `_adj_factor_map` 自动 glob `adj_factor_*` 目录加载 |
| 分红送转 | `data/dividends/all.parquet` | `symbol, ex_date, cash_ps, stock_ps, reserve_to_stock_ps, ...` | 原始明细，不参与 DataManager |

- 全部原子写：tmp 文件 + rename（现有分区写盘模式）
- symbol 规范：与现有分区一致 `.SH`/`.SZ`；指数 `.SH`/`.SZ`（如 000001.SH）；跳过北交所 `.BJ`（baostock 无数据）

## 复权因子转换（关键算法）

- 数据源：`query_adjust_factor(code, start, end)` 每除权日一行，含 `backAdjustFactor`（后复权因子，只随除权事件跳变，单调累积）
- 转换：`ex_factor(t) = backAdjustFactor(t) / backAdjustFactor(latest)` —— 动态前复权累计因子，锚定最新交易日 = 1.0，与 `adj_factor_etf` 语义一致（DataManager `_adj_events` 用相邻行比例重建事件因子）
- 只写除权事件日行（事件行即全部行，无需逐交易日展开）
- 验证：实现时用已知事件核对（如某股 10 送 10 → 因子跳变 0.5）

## 断点续传

- 状态文件 `data/baostock_backfill_state.json`：`{start, end, minute_done: [...], daily_done: [...], adj_done: [...], dividends_done: [...], failed: {...}}`
- 每只完成即原子更新（tmp + rename）
- 重启跳过 done；`--retry-failed` 重试 failed 标的；写分区幂等（读旧→concat→unique keep=last→tmp→rename）

## 进度报告

- 每只/每批 stdout（flush）：`[stage] i/total done= fail= 速率=x只/min ETA=xxh 累计行数`
- 每攒满 100 只批量 flush 分区（复用 mootdx_service `_flush_stock_minute_chunk` 模式，降 IO 一个量级）
- 失败标的追加 `data/baostock_backfill_failures.csv`（symbol, 原因, 时间）

## 可靠性

- 每请求墙钟超时（默认 300s，`--timeout` 可调）→ 重试 3 次递增退避（2s/5s/10s）→ 仍失败记 failed + CSV 继续
- **串行执行**：baostock 连接是进程级全局，多线程共享会坏；服务器吞吐波动大（实测 47s~>100s/只），总时长可能几十小时，靠 resume 分多轮跑完
- 每批 flush 后重连（复用 mootdx_service 模式）

## Universe

- 股票：`data/instruments/instruments.parquet`（排除 `.BJ`）；回退 `query_all_stock(最新交易日)`
- 指数：现有 `data/instruments_index` 或 `kline_index_daily` 已有 symbol；回退 `query_all_stock` 过滤指数码
- ETF：`data/instruments_etf` / `quant_kline/etf_universe_snapshot.json`；回退 `query_all_stock`
- 新股：按 listing_date 起步（复用 mootdx_service `_listing_date_map` 模式），避免上市前空窗拉取

## CLI

```
python scripts/backfill_baostock_3y.py [--start 2023-08-06] [--end 2026-08-05]
    [--stage minute|daily|corporate|all] [--reset-state] [--retry-failed]
    [--timeout 300] [--flush-batch 100]
```

三个 stage 可分别运行（minute / daily / corporate=因子+分红），互不阻塞。

## 测试

- 新增 `backend/tests/test_backfill_baostock.py`（`uv run --extra dev pytest`）：
  - 复权因子转换公式（back → ex_factor，事件日跳变）
  - 分区合并幂等（读旧→concat→unique 不重复）
  - state 读写（原子更新、resume 跳过 done、failed 处理）
  - CLI 参数校验
- 手动冒烟：1 股票 + 1 指数 + 1 ETF + 1 分红，核对 schema/行数/复权事件

## 范围外（后续可做）

- DataManager 接入 `kline_5min` 分区作为真实 5min 缓存源（现仅落盘，不接线）
- 多进程并发提速（baostock QPS ~20 限制，需逐进程独立 login，风险高，暂不做）
- ETF 3 年前日线（baostock 无数据，需换源如 mootdx）
