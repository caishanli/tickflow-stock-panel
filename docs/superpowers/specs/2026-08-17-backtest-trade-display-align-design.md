# 回测成交方向/价格显示对齐模拟盘 — 设计

日期：2026-08-17

## 背景

量化回测（QuantBacktest.tsx）成交记录表的「方向」「价格」两列与量化模拟盘（QuantSim.tsx）显示口径不一致：

- **方向列**：回测直接输出原始值 `t.action ?? t.side`，而库中 action 既可能是 `BUY`/`SELL`，也可能是 rqalpha 引擎的 `SIDE.BUY`/`SIDE.SELL`（以及小写 `buy`/`sell`），导致界面出现 "SIDE.BUY" 等裸值。模拟盘方向列显示中文「买入 / 卖出」（QuantSim.tsx:765-767，`BUY` 用 `text-bull` 红、其余 `text-bear` 绿）。
- **价格列**：回测直接输出原始 `t.price`，模拟盘用 `fmtNum(t.price, 3)`（QuantSim.tsx:768，3 位小数）。

用户要求：回测成交方向显示与模拟盘一致（中文 买入/卖出），成交价格保留与模拟盘一致的小数位（3 位）。

## 范围

- 仅前端 `frontend/src/quant/pages/QuantBacktest.tsx` 的 `TradeTable` 组件（:800-830）。
- 无后端、无接口、无数据改动。

## 改动

### 1. 方向列（QuantBacktest.tsx:820）

当前：

```tsx
<td className={`px-2 py-1.5 ${(t.action ?? t.side) === 'BUY' || (t.action ?? t.side) === 'buy' ? 'text-bull' : 'text-bear'}`}>{t.action ?? t.side ?? ''}</td>
```

改为（用 `BUY` 大小写不敏感正则归一化 `BUY`/`buy`/`SIDE.BUY`/`SIDE.SELL` 等，`SELL` 类一律「卖出」；回测无 STOP_LOSS 成交，不需要「止损」分支）：

```tsx
<td className={`px-2 py-1.5 ${/BUY/i.test(String(t.action ?? t.side ?? '')) ? 'text-bull' : 'text-bear'}`}>
  {/BUY/i.test(String(t.action ?? t.side ?? '')) ? '买入' : '卖出'}
</td>
```

### 2. 价格列（QuantBacktest.tsx:821）

当前：

```tsx
<td className="px-2 py-1.5 text-right">{t.price ?? ''}</td>
```

改为（与模拟盘 QuantSim.tsx:768 同款 3 位小数；`fmtNum` 已在 QuantBacktest.tsx:14 从 `../metrics` 导入）：

```tsx
<td className="px-2 py-1.5 text-right num">{fmtNum(t.price, 3)}</td>
```

## 验证

- `cd frontend && pnpm build`（tsc -b && vite build）通过。
- `pnpm lint` 仓库级预置失败（eslint 未安装/无配置），与本改动无关，跳过。
- 后端无改动，无需后端测试。

## 非目标

- 不改 `BacktestResult.tsx`（经典回测页）的成交显示。
- 不改成交表其它列（时间/标的/数量/手续费）。
- 不改模拟盘任何代码。