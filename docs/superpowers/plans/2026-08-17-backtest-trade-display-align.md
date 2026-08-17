# 回测成交方向/价格显示对齐模拟盘 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 量化回测成交表的「方向」列显示中文 买入/卖出（归一化 `BUY`/`buy`/`SIDE.BUY` 等），「价格」列用 3 位小数，与量化模拟盘一致。

**Architecture:** 纯前端单文件改动——只改 `frontend/src/quant/pages/QuantBacktest.tsx` 的 `TradeTable` 组件（:820 方向列、:821 价格列）。方向列用 `BUY` 大小写不敏感正则做归一化判断（真值来源：`t.action ?? t.side`），价格列复用已导入的 `fmtNum`。

**Tech Stack:** React 18 + TypeScript + Tailwind CSS。

## Global Constraints

- 只改 `frontend/src/quant/pages/QuantBacktest.tsx`，不改其它文件。
- 方向列目标代码（与 QuantSim.tsx:765-767 同口径，但回测无 STOP_LOSS）：

```tsx
<td className={`px-2 py-1.5 ${/BUY/i.test(String(t.action ?? t.side ?? '')) ? 'text-bull' : 'text-bear'}`}>
  {/BUY/i.test(String(t.action ?? t.side ?? '')) ? '买入' : '卖出'}
</td>
```

- 价格列目标代码（与 QuantSim.tsx:768 同款 3 位小数）：

```tsx
<td className="px-2 py-1.5 text-right num">{fmtNum(t.price, 3)}</td>
```

- 验证命令：`cd frontend && pnpm build`（tsc -b && vite build）必须通过。
- `pnpm lint` 仓库级预置失败（eslint 未安装/无配置），跳过不算缺陷。

---

### Task 1: TradeTable 方向列归一化 + 价格列小数位

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx:820`（方向列）
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx:821`（价格列）

**Interfaces:**
- Consumes: `fmtNum`（已在 QuantBacktest.tsx:14 从 `../metrics` 导入，签名 `fmtNum(v, digits?)`）；`t.action`/`t.side`（rqalpha 成交记录字段，取值 `BUY`/`SELL`/`SIDE.BUY`/`SIDE.SELL`/`buy`/`sell`）；`t.price`。
- Produces: 无新接口（纯展示改动，后续无任务依赖）。

- [ ] **Step 1: 修改方向列**

定位 `QuantBacktest.tsx:820`，当前代码：

```tsx
              <td className={`px-2 py-1.5 ${(t.action ?? t.side) === 'BUY' || (t.action ?? t.side) === 'buy' ? 'text-bull' : 'text-bear'}`}>{t.action ?? t.side ?? ''}</td>
```

改为：

```tsx
              <td className={`px-2 py-1.5 ${/BUY/i.test(String(t.action ?? t.side ?? '')) ? 'text-bull' : 'text-bear'}`}>
                {/BUY/i.test(String(t.action ?? t.side ?? '')) ? '买入' : '卖出'}
              </td>
```

- [ ] **Step 2: 修改价格列**

定位 `QuantBacktest.tsx:821`，当前代码：

```tsx
              <td className="px-2 py-1.5 text-right">{t.price ?? ''}</td>
```

改为：

```tsx
              <td className="px-2 py-1.5 text-right num">{fmtNum(t.price, 3)}</td>
```

- [ ] **Step 3: 构建验证**

Run: `cd frontend && pnpm build`
Expected: `tsc -b && vite build` 均成功（退出码 0），无 TS 错误。

- [ ] **Step 4: 自查改动范围**

确认 `git diff --stat` 仅包含 `frontend/src/quant/pages/QuantBacktest.tsx`（1 file，~2 处改动）；方向/价格两行与 Global Constraints 目标代码逐字符一致；`fmtNum` 已有导入未重复添加；无其它列被改动。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(backtest): 成交方向列归一化显示买入/卖出 + 价格 3 位小数对齐模拟盘"
```