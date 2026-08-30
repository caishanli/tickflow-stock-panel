# Sim Detail Metrics Cards — Win Rate / Sharpe / Max Drawdown

**Date:** 2026-08-29
**Status:** Approved
**Scope:** Frontend only (no backend changes)

## Goal

Add three performance metric cards (win rate, Sharpe ratio, max drawdown) to the sim account detail page, appended to the existing 5-card metrics row.

## Data Source

All metrics computed in the frontend from existing API data:
- `trades` — from `/sim/accounts/{aid}/trades` (already fetched in `SimDetail`)
- `equity` — from `/sim/accounts/{aid}/equity` (already fetched in `SimDetail`, daily aggregated)

No new backend endpoints needed.

## Metrics Computation

### 1. Win Rate

- Filter trades to SELL and STOP_LOSS actions (closed positions only).
- Group by `code`, FIFO配对 each buy with its corresponding sell(s).
- A round-trip is "winning" if the summed `pnl` for that code's pair > 0.
- Display: `62.5% (10/16)` — percentage + (winning count / total count).
- No closed trades → display `—`.

### 2. Sharpe Ratio

- Extract daily net value sequence from `equity` array (take one point per day, prefer last-of-day).
- Compute daily returns: `r_t = nv_t / nv_{t-1} - 1`.
- Risk-free rate = 0.
- Annualization factor = `sqrt(252)`.
- Formula: `sharpe = (mean(daily_returns) / std(daily_returns)) * sqrt(252)`.
- Fewer than 2 data points → display `—`.
- Display: signed number, 2 decimals (e.g., `1.35`, `-0.42`).

### 3. Max Drawdown

- Use the full equity history (not range-filtered).
- Compute running peak, then drawdown at each point: `dd_t = (nv_t - peak) / peak`.
- Max drawdown = minimum of all `dd_t` values (most negative).
- Display: negative percentage, e.g., `-12.34%`.
- No equity data → display `—`.

## Implementation

### File: `frontend/src/quant/metrics.ts`

Add new function:

```ts
export interface SimMetrics {
  winRate: number | null
  winCount: number
  totalCount: number
  sharpe: number | null
  maxDrawdown: number | null
}

export function computeSimMetrics(trades: any[], equity: any[]): SimMetrics
```

Helper functions (private):
- `computeWinRate(trades)` — FIFO round-trip pairing, returns `{ rate, win, total }`.
- `computeSharpe(equity)` — daily returns → Sharpe.
- `computeMaxDrawdown(equity)` — peak-to-trough max drawdown.

### File: `frontend/src/quant/pages/QuantSim.tsx`

In `SimDetail` component:

1. Import `computeSimMetrics` from `../metrics`.
2. Add `useMemo` to compute metrics from `tradeList` and `eq` (equity data).
3. Append 3 cards to the existing metrics grid (`grid-cols-2 sm:grid-cols-5` → `sm:grid-cols-8`).

Card layout (matching existing style):

```tsx
{/* Win Rate */}
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

{/* Sharpe */}
<div className="rounded-card border border-border bg-surface px-3 py-2">
  <div className="text-[10px] text-muted">夏普比率</div>
  <div className={`text-sm font-medium num ${tone(simMetrics.sharpe)}`}>
    {fmtNum(simMetrics.sharpe)}
  </div>
</div>

{/* Max Drawdown */}
<div className="rounded-card border border-border bg-surface px-3 py-2">
  <div className="text-[10px] text-muted">最大回撤</div>
  <div className="text-sm font-medium num text-bear">
    {fmtPct(simMetrics.maxDrawdown)}
  </div>
</div>
```

## Verification

1. `cd frontend && pnpm lint` — no lint errors.
2. `cd frontend && pnpm build` — builds successfully (tsc + vite).
3. Visual check: open sim detail page, verify 8 cards display correctly on desktop and mobile.
