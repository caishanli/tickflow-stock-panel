# 量化模拟盘交易/持仓显示标的名字（方案 C：落库时存名称）

- 日期：2026-08-07
- 分支：`feat/sim-trade-name`（基于 `custom-main`）

## 背景与目标

量化模拟盘页面的「成交记录」tab 只显示标的代码（如 `159985.XSHE`），持仓表也只显示
代码，用户难以辨认标的是什么。目标：成交记录与持仓都**同时显示标的名字**。

采用方案 C（一劳永逸）：在模拟盘进程落库时就把名称存进 `sim_trades.name` 与
`sim_state.positions_json` 的每个持仓 dict（加 `name` 键），读取/展示侧只透传，
不做运行时名称解析。

## 名称来源

模拟盘是独立子进程（`scripts/run_quant_sim.py`），唯一取数入口是 StockDataClient。
**根因**：stockdata 服务的 `DataSources.get_stock_names()`（`stockdata/sources.py:495`）
目前返回空（名称分区未落盘）。这导致整条名称管道失效：

- 策略侧 `get_security_name`/`get_all_securities`（`jqengine/engine/jq/api.py:810,865`）
  从 `mgr.sources["network"].get_stock_names()` 取名称 → 拿到空，名称退化为代码，
  **五福 v5.2 的标的名称分组功能实际失效**（`wufu-v5.2.py:497` 的 `etf_names_dict`
  里 display_name = 代码）
- 模拟盘无名称可落库

因此本次在**服务端实现 `get_stock_names`**，一个根因修复同时恢复策略名称分组 +
为模拟盘提供落库名称：

- 客户端现成接口 `StockDataClient.get_stock_names()`（`network_client.py:182`）不变
- 服务端实现（`DataSources.get_stock_names(codes=None)`）：
  - 先读共享 `data/instruments/**/instruments.parquet`（股票名称已在本地，实测
    `600000.SH → 浦发银行`）
  - ETF 名称本地 parquet（`instruments_etf`）可能为空 → 用免费 TickFlow instruments
    API `_fetch_instruments_by_type("etf","etf")`（`index_sync.py:77`，免费档可用，
    实测返回全量 ETF 名称）补
  - 合并构建 `{纯6位代码: 名称}` 映射（与 `get_all_securities` 里
    `mootdx_names.get(pure, ...)` 的查找约定一致），进程内缓存（服务长驻，
    首次构建后复用），可落到 `data/.stock_names_cache.json`
  - 异常兜底返回空映射
- 模拟盘子进程侧：`names.py` 调 `client.get_stock_names()` 拿 `{纯代码: 名称}`，
  进程内缓存，转码 `.SH→.XSHG`/`.SZ→.XSHE` 成 `{JQ码: 名称}`
- 取不到名称的标的回退为代码本身

## 改动清单

### 1. stock data 服务端 `get_stock_names` 实现（`backend/app/services/stockdata/sources.py`）

`DataSources.get_stock_names(codes=None)`：

- 懒构建 + 进程内缓存的 `{纯6位代码: 名称}` 映射：
  - 读共享 `data/instruments/instruments.parquet` 的股票名称（本地已有）
  - ETF 名称：`instruments_etf` parquet 若有则读，否则免费 TickFlow
    `_fetch_instruments_by_type("etf","etf")` 补（`index_sync` 免费档）
  - 结果写 `data/.stock_names_cache.json`（下次启动命中，免网络）
- `codes` 非空时只返回命中的子集
- 异常兜底：返回空映射，不影响行情路径
- **副产物**：此实现使 `jqengine` 的 `get_all_securities`/`get_security_name`
  （`api.py:810,865`）自动拿到真名称，恢复五福 v5.2 等策略的标的名称分组

### 2. 名称解析模块（新增 `backend/app/quant/simulate/names.py`）

- `get_name_map() -> dict[str, str]`：进程内缓存 `{JQ码: 名称}`；
  调 `StockDataClient.get_stock_names()` 拿 `{纯代码: 名称}`，后缀转码
  （`.SH→.XSHG`、`.SZ→.XSHE`）
- `resolve_name(code: str) -> str`：查映射，缺失回退 `code`
- 异常兜底：服务不可达/返回空 → 全部回退代码，不影响行情正确性

### 3. DB schema（`backend/app/quant/db.py`）

- `sim_trades` 建表 SQL 加 `name TEXT` 列
- `init_db` 兼容旧库：`PRAGMA table_info(sim_trades)` 无 `name` 列则
  `ALTER TABLE sim_trades ADD COLUMN name TEXT`（沿用 db.py:60-83 的迁移模式）
- `insert_sim_trade(...)` 增加 `name` 参数；`batch_insert_trades` 的行格式增加 name
- `get_sim_trades` / `get_sim_trades_after` 的 SELECT 加 `name`

### 4. 模拟盘写入侧（`backend/app/quant/simulate/runner.py`）

- `_persist`（:429）：trade_row 增加 `names.resolve_name(t["code"])`
- `_state_from_portfolio`（:305）：持仓 dict 加 `"name": resolve_name(code)`
- `save_state`（protocol.py）无需改（positions_json 序列化整体 dict）

### 5. 迁移脚本（新增）

`backend/scripts/backfill_sim_names.py`：

- 遍历所有 sim_accounts：
  - `UPDATE sim_trades SET name=? WHERE code=? AND (name IS NULL OR name='')`
  - 重写 `sim_state.positions_json`：每个持仓补 `name` 键（缺失才补）
- 幂等，可重复执行

### 6. API 透传（`backend/app/quant/api/quant.py`）

- `GET /sim/accounts/{aid}/trades`：`get_sim_trades` 已含 name，直接透传
- `sim_stream` 的 `trade` 事件：`d` dict 增加 `"name"`（sim_trades 表加列后
  `get_sim_trades_after` 返回行含 name）
- `sim_status` 的 state.positions 已含 name（来自 read_sim_state）

### 7. 前端（`frontend/src/quant/pages/QuantSim.tsx`）

- 成交记录标的列（:519）：显示 `{t.name} {t.code}`（name 为空则只显示 code）
- 持仓表标的列（:470）：显示 `{p.name} {sym}`（同样兜底）
- SSE `onTrade` 的字段已含 name（backend 透传），无需额外处理

## 影响面

- 只改量化模拟盘（sim_trades / sim_state）+ stock data 服务的 `get_stock_names`
  实现，不动回测、不动主面板取数路径
- 历史数据靠迁移脚本回填；`init_db` 自动补列，旧库升级平滑
- 名称解析：
  - 服务端首次构建 `{纯代码: 名称}` 时拉一次免费 TickFlow API，落盘
    `data/.stock_names_cache.json`，之后命中本地
  - 模拟盘子进程每启动首次调 `client.get_stock_names()`，进程内缓存，之后零网络往返
- 免费 TickFlow API 失败时降级：服务端返回已有本地名称（或空），标的名称为空回退代码，
  不影响行情正确性

## 测试

- `backend/tests/quant/test_db.py`：`insert_sim_trade` 带 name → `get_sim_trades` 返回含 name；
  `init_db` 对无 name 列旧表能补列（模拟临时库）
- stock data 服务 `get_stock_names`：mock 免费 API 失败时返回本地 instruments 名称
  （`600000` → `浦发银行`）、ETF 走 API、`codes` 子集过滤、缓存落盘
- 策略名称分组恢复验证：`get_all_securities(['etf'])` 返回的 `display_name`
  是真实名称而非代码（间接验证服务端修复生效）
- runner `_persist` 构造的 trade_row / `_state_from_portfolio` 持仓含 name（可用现有
  sim 相关测试或新增单测）
- 前端无测试脚本；`pnpm lint` + `pnpm build` 通过即可

## 验收

- 启动模拟盘跑出成交后，前端「成交记录」标的列显示 `豆粕ETF华夏 159985.XSHE` 等
- 持仓表显示名称
- 跑 `backfill_sim_names.py` 后历史记录也显示名称
