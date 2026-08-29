# Sim Metrics Cards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add win rate, Sharpe ratio, and max drawdown metric cards to the sim account detail page.

**Architecture:** Pure frontend change — compute three metrics from existing `trades` and `equity` data in `SimDetail` via a new `computeSimMetrics` function in `metrics.ts`, then render 3 additional cards in the existing metrics grid.

**Tech Stack:** React 18, TypeScript, Tailwind CSS

## Global Constraints

- Frontend only, no backend changes
- Use existing local `fmtNum`/`fmtPct`/`tone` functions already in `QuantSim.tsx` (not the ones from `metrics.ts` which have different formatting)
- Follow existing card styling pattern: `rounded-card border border-border bg-surface px-3 py-2`
- Grid: `grid-cols-2 sm:grid-cols-5` → `sm:grid-cols-8`

---

### Task 1: Add computeSimMetrics to metrics.ts

**Files:**
- Modify: `frontend/src/quant/metrics.ts`

**Interfaces:**
- Produces: `computeSimMetrics(trades: any[], equity: any[]): SimMetrics`

- [ ] **Step 1: Add SimMetrics interface and computeSimMetrics function**

Append to `frontend/src/quant/metrics.ts` after the existing `tone` function:

```ts
// ---- 模拟盘指标计算 ----

export interface SimMetrics {
  winRate: number | null
  winCount: number
  totalCount: number
  sharpe: number | null
  maxDrawdown: number | null
}

/** 从模拟盘成交 + 净值序列计算胜率、夏普、最大回撤 */
export function computeSimMetrics(trades: any[], equity: any[]): SimMetrics {
  const wr = computeWinRate(trades)
  return {
    winRate: wr.rate,
    winCount: wr.win,
    totalCount: wr.total,
    sharpe: computeSharpe(equity),
    maxDrawdown: computeMaxDrawdown(equity),
  }
}

/** 已平仓胜率（FIFO 配对，按 code 分组） */
function computeWinRate(trades: any[]): { rate: number | null; win: number; total: number } {
  // 按时间排序，分 code 做 FIFO 配对
  const sorted = [...trades].sort((a, b) => String(a.ts).localeCompare(String(b.ts)))
  const lots: Record<string, { price: number; amount: number }[]> = {}
  let win = 0
  let total = 0

  for (const t of sorted) {
    const action = String(t.action ?? '').toUpperCase()
    const code = String(t.code ?? '')
    const amt = Number(t.amount) || 0
    const price = Number(t.price) || 0

    if (action === 'BUY') {
      ;(lots[code] ??= []).push({ price, amount: amt })
    } else if (action === 'SELL' || action === 'STOP_LOSS') {
      // FIFO 配对卖出
      const q = lots[code] ?? []
      let remaining = amt
      let pnlSum = 0
      let costSum = 0
      while (remaining > 0 && q.length > 0) {
        const lot = q[0]
        const take = Math.min(remaining, lot.amount)
        pnlSum += (price - lot.price) * take
        costSum += lot.price * take
        lot.amount -= take
        remaining -= take
        if (lot.amount <= 0) q.shift()
      }
      if (costSum > 0) {
        total++
        if (pnlSum > 0) win++
      }
    }
  }

  if (total === 0) return { rate: null, win: 0, total: 0 }
  return { rate: win / total, win, total }
}

/** 日频夏普比率（无风险利率 0，年化 sqrt(252)） */
function computeSharpe(equity: any[]): number | null {
  // 按天取最后一个净值点
  const dayLast = new Map<string, number>()
  for (const e of equity) {
    const day = String(e.dt ?? '').slice(0, 10)
    if (!day) continue
    const nv = Number(e.net_value ?? e.value ?? 0)
    if (nv > 0) dayLast.set(day, nv)
  }
  const values = Array.from(dayLast.values())
  if (values.length < 2) return null

  const returns: number[] = []
  for (let i = 1; i < values.length; i++) {
    if (values[i - 1] > 0) returns.push(values[i] / values[i - 1] - 1)
  }
  if (returns.length < 2) return null

  const mean = returns.reduce((s, r) => s + r, 0) / returns.length
  const variance = returns.reduce((s, r) => s + (r - mean) ** 2, 0) / (returns.length - 1)
  const std = Math.sqrt(variance)
  if (std === 0) return null

  return (mean / std) * Math.sqrt(252)
}

/** 全程最大回撤（从净值序列） */
function computeMaxDrawdown(equity: any[]): number | null {
  const dayLast = new Map<string, number>()
  for (const e of equity) {
    const day = String(e.dt ?? '').slice(0, 10)
    if (!day) continue
    const nv = Number(e.net_value ?? e.value ?? 0)
    if (nv > 0) dayLast.set(day, nv)
  }
  const values = Array.from(dayLast.values())
  if (values.length < 2) return null

  let peak = values[0]
  let maxDd = 0
  for (const v of values) {
    if (v > peak) peak = v
    const dd = (v - peak) / peak
    if (dd < maxDd) maxDd = dd
  }
  return maxDd === 0 ? null : maxDd
}
```

- [ ] **Step 2: Verify lint**

Run: `cd frontend && pnpm lint`
Expected: no new errors

- [ ] **Step 3: Commit**

```bash
git add frontend/src/quant/metrics.ts
git commit -m "feat: add computeSimMetrics (win rate/sharpe/max drawdown)"
```

---

### Task 2: Add metrics cards to SimDetail in QuantSim.tsx

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx:821-846` (metrics grid)

**Interfaces:**
- Consumes: `computeSimMetrics` from `../metrics` (Task 1)

- [ ] **Step 1: Add import for computeSimMetrics**

In `frontend/src/quant/pages/QuantSim.tsx`, add to the import from `../metrics` (QuantSim.tsx doesn't currently import from metrics.ts, so add a new import line after line 8):

```ts
import * as api from '../api'
import { computeSimMetrics } from '../metrics'  // ← add this line
```

- [ ] **Step 2: Add useMemo to compute simMetrics**

In the `SimDetail` component, after the `alertList` definition (around line 739), add:

```ts
  const simMetrics = useMemo(() => computeSimMetrics(tradeList, eq ?? []), [tradeList, eq])
```

- [ ] **Step 3: Expand grid and append 3 cards**

Change line 821 from `sm:grid-cols-5` to `sm:grid-cols-8`, then append 3 cards after the existing 5th card (收益率, line 845). The new cards go before the closing `</div>` of the grid:

```tsx
      {/* 指标卡 */}
      <div className="grid grid-cols-2 sm:grid-cols-8 gap-2">
        {/* ... existing 5 cards unchanged ... */}
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">胜率</div>
          <div className="text-sm font-medium text-foreground num">
            {fmtPct(simMetrics.winRate)}
            {simMetrics.totalCount > 0 && (
              <span className="text-[10px] text-muted ml-1">
                ({simMetrics.winCount}/{simMetrics.totalCount})
              </span>
            )}
          </div>
        </div>
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">夏普比率</div>
          <div className={`text-sm font-medium num ${tone(simMetrics.sharpe)}`}>
            {fmtNum(simMetrics.sharpe)}
          </div>
        </div>
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">最大回撤</div>
          <div className="text-sm font-medium num text-bear">
            {fmtPct(simMetrics.maxDrawdown)}
          </div>
        </div>
      </div>
```

Note: `tone` is not currently imported in QuantSim.tsx. Add it to the import or use inline className. Since the local `fmtPct` already handles sign display, and `tone` is a small utility, add a local helper or import from metrics. Simplest: use inline `${simMetrics.sharpe == null ? 'text-muted' : simMetrics.sharpe >= 0 ? 'text-bull' : 'text-bear'}` to avoid adding another import.

- [ ] **Step 4: Verify build**

Run: `cd frontend && pnpm build`
Expected: builds successfully (tsc + vite)

- [ ] **Step 5: Verify lint**

Run: `cd frontend && pnpm lint`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add frontend/src/quant/pages/QuantSim.tsx
git commit -m "feat: add win rate/sharpe/max drawdown cards to sim detail"
```

---

### Task 3: Create new branch and merge

**Files:**
- None (git operations only)

- [ ] **Step 1: Create feature branch from custom-main**

```bash
git checkout custom-main
git checkout -b feat/sim-metrics-cards
```

- [ ] **Step 2: Cherry-pick or rebase the two commits**

If commits were made on custom-main, they're already on this branch. If made elsewhere, cherry-pick:

```bash
git log --oneline -3  # verify commits are here
```

- [ ] **Step 3: Final verification**

```bash
cd frontend && pnpm lint && pnpm build
```

Expected: clean lint, successful build

- [ ] **Step 4: Push branch**

```bash
git push -u origin feat/sim-metrics-cards
```
