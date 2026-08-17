# 回测列表/详情展示策略字母 id 设计

日期：2026-08-16
分支：feat/backtest-strategy-id（基于 custom-main）

## 背景

量化回测列表（`frontend/src/quant/pages/QuantBacktest.tsx` 的 `StrategyList`）目前只有 `#` 序号，没有像量化模拟盘（`QuantSim.tsx`）那样的「编号」列；回测详情页（`StrategyEditor`）顶栏也只显示策略名称，不显示 id。

模拟盘已有模式（本次完全复用）：
- 列表「编号」列：`font-mono` 灰字展示 8 位字母数字 id，点击复制（textarea + `document.execCommand`，toast「已复制」），`stopPropagation` 避免触发行进详情。
- 详情页顶栏：名称旁展示 id，`font-mono`、`title="点击复制账户ID"`、点击复制 + toast。

## 目标

给量化回测列表加「编号」列、详情页顶栏加策略 id 展示，交互与模拟盘一致。展示的是**策略 id**（`s.id`，与模拟盘展示账户自身 id 口径一致），不做 run_id。

## 改动

仅前端，`frontend/src/quant/pages/QuantBacktest.tsx`，无后端改动（接口已返回 `s.id`）。

1. **列表页（StrategyList）**：checkbox 列之后、策略名称之前插入「编号」表头；每行加 `<td>` 展示 `s.id`，样式/复制交互仿 `QuantSim.tsx:235-255`（`font-mono` 灰字、点击复制、toast「已复制」、`stopPropagation` 不触发行点击）。
2. **详情页（StrategyEditor 顶栏）**：名称输入框后加 `<span>` 展示 `strategyId`，样式/复制交互仿 `QuantSim.tsx:559-571`（`font-mono` 灰字、`title="点击复制策略ID"`、点击复制 + toast「策略ID已复制」）。

## 验证

- `cd frontend && pnpm lint`（eslint）
- `pnpm build`（tsc -b && vite build）
- 浏览器手测：列表编号列可复制且不触发进入详情；详情页 id 可复制。
