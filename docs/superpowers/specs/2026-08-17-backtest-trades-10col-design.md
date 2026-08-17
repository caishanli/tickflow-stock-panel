# 回测成交表对齐模拟盘 10 列 — 设计

日期：2026-08-17

## 背景

量化回测（QuantBacktest.tsx `TradeTable`）成交表目前 6 列（时间/标的/方向/价格/数量/手续费），量化模拟盘（QuantSim.tsx:734-747）为 10 列（时间/名称/代码/持仓时长/方向/价格/数量/手续费/盈亏/收益率）。用户要求回测成交表列数与模拟盘对齐。

数据差异核查（data/quant.db 实查）：

- `backtest_trades` 表：`run_id, ts, code, action, price, amount, pnl, pnl_pct, commission`——**无 `name` 列**（`sim_trades` 有 `name`）。
- `pnl`/`pnl_pct` 已存在，盈亏/收益率两列数据可用。
- 持仓时长需前端按分标的 FIFO 配对计算（模拟盘 `computeHoldDays` 同款），适配 `SIDE.BUY`/`SIDE.SELL` 动作值。
- 后端已有 `resolve_name(code)`（`backend/app/quant/simulate/names.py:49`，通达信名优先、聚宽快照回退，缺名回退代码本身）——可在 API 读取时补齐 `name`，无需 DB schema 变更、历史行同样生效。

## 范围

- 后端 `backend/app/quant/api/quant.py`：成交读取接口补 `name`。
- 前端 `frontend/src/quant/pages/QuantBacktest.tsx`：`TradeTable` 重建为 10 列。

## 改动

### 1. 后端：成交接口补 name（quant.py）

- `GET /backtest/{run_id}/trades`（:130-132）返回的每行加 `name: resolve_name(row["code"])`。`resolve_name` 从 `..simulate.names` 导入（`from ..simulate.names import resolve_name`，quant.py 位于 `app/quant/api/`，相对导入为 `..simulate.names`）。
- SSE `backtest_stream` 的 `trade` 事件（:192-195）同样为每行加 `name: resolve_name(row["code"])`，保证实时增量行也有名称。
- 非目标：`trades.csv` 导出（:208-218）列不变。

### 2. 前端：TradeTable 重建为 10 列（QuantBacktest.tsx:800-830）

移植模拟盘 `buildDayLookup` + `computeHoldDays`（QuantSim.tsx:56-117），`tradeDays` 传 `[]`（回测无账户交易日历，走工作日兜底）；买卖判定用 `/BUY/i.test(String(t.action))`（兼容 `SIDE.BUY`/`SIDE.SELL`）。

列结构（与 QuantSim.tsx:734-747 一致）：

| 列 | 渲染 |
|---|---|
| 时间 | `t.ts` |
| 名称 | `t.name ?? ''`（后端已补，resolve_name 缺名回退代码） |
| 代码 | `t.code` |
| 持仓时长 | 移植 `computeHoldDays`：买入 `持仓中`；卖出按 FIFO 配对 `N个交易日` / `<1天` / `—`（无配对） |
| 方向 | 沿用已对齐的 `/BUY/i` 归一化「买入/卖出」+ text-bull/text-bear |
| 价格 | `fmtNum(t.price, 3)` |
| 数量 | `t.amount` |
| 手续费 | `fmtNum(t.commission, 2)` |
| 盈亏 | `pnl` 为 number 且 ≠0 → `fmtNum(t.pnl)`，否则 `—`（text-bull/text-bear/muted 同模拟盘 :771-772） |
| 收益率 | 买入行 `—`（muted）；卖出行 `fmtPct(t.pnl_pct)`，≥0 红 <0 绿（模拟盘 :774-775 同款，BUY 判断用 `/BUY/i`） |

展示顺序：与模拟盘一致 `[...trades].sort(按 ts).reverse()`（最新成交在上）。

## 验证

- 后端：`cd backend && uv run --extra dev pytest -q`（新增/确认无回归；本机全量 OOM 时以 `pytest tests/quant -q` 为准）。
- 前端：`cd frontend && pnpm build`（tsc -b && vite build）通过；`pnpm lint` 仓库级预置失败跳过。

## 非目标

- 不改 `backtest_trades` 表结构（名称在读取层补齐，历史行同样有名字）。
- 不改 `trades.csv` 导出列。
- 不改 `BacktestResult.tsx`（经典回测页）。
- 不改模拟盘任何代码。