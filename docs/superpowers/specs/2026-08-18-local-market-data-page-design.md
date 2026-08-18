# 本地股市数据页设计

日期：2026-08-18
分支：`feat/local-market-data-counts`（基于 `custom-main`）

## 背景与目标

前端新增「本地股市数据」页面，展示本地 Parquet 数据仓库（`data/`）中按日期统计的标的数量，用于直观了解本地行情数据的覆盖情况。

## 需求

- 左侧菜单在「量化模拟盘」（`/quant-sim`）下方新增「本地股市数据」。
- 页面显示一个表格：
  - 表头：股市日线 | 股市分钟线 | ETF日线 | ETF分钟线 | 指数日线 | 指数分钟线（另有首列「日期」）。
  - 行 = 本地数据中实际存在的日期（各表日期分区并集），降序排列（最新在前）。
  - 单元格 = 该日期该表的去重 `symbol` 数（标的数）。
  - 指数分钟线本地暂无 `kline_index_minute` 目录，该列恒为 0，保留列。
- 分页：每页 15 行，控件在表格下方（上一页/下一页 + 共 X 天 · 第 X/Y 页），服务端分页。
- 页面风格与系统其他页面保持一致。

## 数据源目录

| 列 | 目录 | 备注 |
|---|---|---|
| 股市日线 | `data/kline_daily/date=YYYY-MM-DD/` | 分区含 `part.parquet` |
| 股市分钟线 | `data/kline_minute/date=YYYY-MM-DD/` | |
| ETF日线 | `data/kline_etf_daily/date=YYYY-MM-DD/` | |
| ETF分钟线 | `data/kline_etf_minute/date=YYYY-MM-DD/` | |
| 指数日线 | `data/kline_index_daily/date=YYYY-MM-DD/` | |
| 指数分钟线 | `data/kline_index_minute/date=YYYY-MM-DD/` | 目录当前不存在 → 计 0 |

所有分区文件均含 `symbol` 列（已实测验证：kline_daily/kline_minute/kline_etf_daily/kline_etf_minute/kline_index_daily）。

## 后端

新增端点 `GET /api/data/local-market-stats?page=1&page_size=15`，放在 `backend/app/api/data.py`：

- 列定义（key → 目录映射），指数分钟线目录不存在时计 0。
- 日期集合 = 各目录下 `date=*` 分区目录名并集，解析为日期后降序排序；`total` = 并集大小。
- 服务端分页：按 page/page_size 截取当前页日期。
- 对当前页每个日期、每个存在的目录：`SELECT COUNT(DISTINCT symbol) FROM read_parquet('{data_dir}/{dir}/date={d}/**/*.parquet')`。
- 响应：
  ```json
  {
    "total": 5230,
    "page": 1,
    "page_size": 15,
    "rows": [
      { "date": "2026-08-17", "stock_daily": 5214, "stock_minute": 4987, "etf_daily": 865, "etf_minute": 0, "index_daily": 562, "index_minute": 0 }
    ]
  }
  ```
- 短 TTL 缓存（30s，复用 `_get_table_stats` 缓存模式），键含 page/page_size，避免同步期间频繁重复读。
- 目录缺失/无文件 → 该单元格 0；无任何日期 → `rows: []`。

## 前端

- `frontend/src/lib/api.ts`：新增 `LocalMarketStats` 接口与 `localMarketStats(page, pageSize)` 函数（走 `request<T>` 包装，`/api/data/local-market-stats`）。
- `frontend/src/pages/LocalData.tsx`：新页面组件：
  - `useQuery(['local-market-stats', page, pageSize], ...)`（query key 挂到 `lib/queryKeys.ts`）。
  - 表格样式复用系统现有表格（`rounded-card border border-border bg-surface` 容器 + 原生 `<table>` thead/tbody），参考 `QuantSim.tsx` / `QuantBacktest.tsx`。
  - 数字千分位显示。
  - 底部翻页：上一页/下一页按钮 + 「共 X 天 · 第 X/Y 页」，参照 `QuantBacktest.tsx`（PAGE_SIZE=15，安全 page 钳制）。
  - 无数据时显示 EmptyState；加载中显示 Skeleton。
  - 页面顶部用 PageHeader 组件，风格与其它页面一致。
- `frontend/src/router.tsx`：新增懒加载路由 `local-data` → `/local-data`。
- `frontend/src/components/Layout.tsx`：`nav` 数组在 `/quant-sim` 之后插入 `{ to: '/local-data', label: '本地股市数据', icon: Database }`。

## 错误处理

- 后端：目录不存在/无 parquet → 计 0；DuckDB 查询异常按表降级为 0 并记录日志（不影响整页）。
- 前端：接口失败走全局 401 处理与 Toast；查询失败显示错误态。

## 测试

- 后端 pytest：新端点返回的某日各列数量与直接 `SELECT COUNT(DISTINCT symbol)` 一致；分页正确（total 与日期数一致、page 越界返回空 rows 或钳制）；无数据目录时全 0。
- 前端无测试脚本：`pnpm lint` + `pnpm build` 验证。

## 非目标

- 不做数据导出/详情下钻/图表。
- 不统计行数（记录数），只统计去重标的数。
- 不新增 `kline_index_minute` 数据采集（列保留显示 0）。