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

模拟盘是独立子进程（`scripts/run_quant_sim.py`），其唯一取数入口是 StockDataClient，
但 stockdata 服务的 `get_stock_names` 目前返回空（名称分区未落盘）。因此名称解析在
**子进程内**用免费 TickFlow instruments API：

- `_fetch_instruments_by_type("stock", "stock")` / `("etf", "etf")`（`app/services/index_sync.py:77`）
  - 免费档可用，无需 token；实测返回全量 stock（5540）+ ETF（1651）名称
- 返回 `.SH`/`.SZ` 符号格式 → 转成 JQ 码（`.SH→.XSHG`、`.SZ→.XSHE`），构建
  `{JQ码: 名称}` 映射，进程内缓存（模块级，跨补跑/实时复用）
- 取不到名称的标的回退为代码本身

## 改动清单

### 1. 名称解析模块（新增）

`backend/app/quant/simulate/names.py`：

- `get_name_map() -> dict[str, str]`：进程内缓存 `{JQ码: 名称}`；来源
  `_fetch_instruments_by_type` 合并 stock+ETF，做后缀转码
- `resolve_name(code: str) -> str`：查映射，缺失回退 `code`
- 异常兜底：API 失败返回空映射，全部回退代码，不影响行情正确性

### 2. DB schema（`backend/app/quant/db.py`）

- `sim_trades` 建表 SQL 加 `name TEXT` 列
- `init_db` 兼容旧库：`PRAGMA table_info(sim_trades)` 无 `name` 列则
  `ALTER TABLE sim_trades ADD COLUMN name TEXT`（沿用 db.py:60-83 的迁移模式）
- `insert_sim_trade(...)` 增加 `name` 参数；`batch_insert_trades` 的行格式增加 name
- `get_sim_trades` / `get_sim_trades_after` 的 SELECT 加 `name`

### 3. 模拟盘写入侧（`backend/app/quant/simulate/runner.py`）

- `_persist`（:429）：trade_row 增加 `names.resolve_name(t["code"])`
- `_state_from_portfolio`（:305）：持仓 dict 加 `"name": resolve_name(code)`
- `save_state`（protocol.py）无需改（positions_json 序列化整体 dict）

### 4. 迁移脚本（新增）

`backend/scripts/backfill_sim_names.py`：

- 遍历所有 sim_accounts：
  - `UPDATE sim_trades SET name=? WHERE code=? AND (name IS NULL OR name='')`
  - 重写 `sim_state.positions_json`：每个持仓补 `name` 键（缺失才补）
- 幂等，可重复执行

### 5. API 透传（`backend/app/quant/api/quant.py`）

- `GET /sim/accounts/{aid}/trades`：`get_sim_trades` 已含 name，直接透传
- `sim_stream` 的 `trade` 事件：`d` dict 增加 `"name"`（sim_trades 表加列后
  `get_sim_trades_after` 返回行含 name）
- `sim_status` 的 state.positions 已含 name（来自 read_sim_state）

### 6. 前端（`frontend/src/quant/pages/QuantSim.tsx`）

- 成交记录标的列（:519）：显示 `{t.name} {t.code}`（name 为空则只显示 code）
- 持仓表标的列（:470）：显示 `{p.name} {sym}`（同样兜底）
- SSE `onTrade` 的字段已含 name（backend 透传），无需额外处理

## 影响面

- 只改量化模拟盘（sim_trades / sim_state），不动回测、不动主面板
- 历史数据靠迁移脚本回填；`init_db` 自动补列，旧库升级平滑
- 名称解析只在模拟盘子进程落库时发生（每笔成交/每轮持仓），进程内缓存，无额外网络往返
  （首次构建拉一次全量名录，进程内复用）

## 测试

- `backend/tests/quant/test_db.py`：`insert_sim_trade` 带 name → `get_sim_trades` 返回含 name；
  `init_db` 对无 name 列旧表能补列（模拟临时库）
- runner `_persist` 构造的 trade_row / `_state_from_portfolio` 持仓含 name（可用现有
  sim 相关测试或新增单测）
- 前端无测试脚本；`pnpm lint` + `pnpm build` 通过即可

## 验收

- 启动模拟盘跑出成交后，前端「成交记录」标的列显示 `豆粕ETF华夏 159985.XSHE` 等
- 持仓表显示名称
- 跑 `backfill_sim_names.py` 后历史记录也显示名称
