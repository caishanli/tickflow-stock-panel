// 统一两条回测引擎的指标 key（rqalpha: annualized；jqengine: annual_return）
export interface Metrics {
  total_return: number | null
  annualized: number | null
  sharpe: number | null
  max_drawdown: number | null
  win_rate: number | null
  profit_loss_ratio: number | null
  trade_count: number | null
}

export function pickMetrics(raw: any): Metrics {
  if (!raw) return { total_return: null, annualized: null, sharpe: null, max_drawdown: null, win_rate: null, profit_loss_ratio: null, trade_count: null }
  let m: any = raw
  if (typeof raw === 'string') {
    try { m = JSON.parse(raw) } catch { m = {} }
  }
  return {
    total_return: num(m.total_return),
    annualized: num(m.annualized ?? m.annual_return),
    sharpe: num(m.sharpe),
    max_drawdown: num(m.max_drawdown),
    win_rate: num(m.win_rate),
    profit_loss_ratio: num(m.profit_loss_ratio),
    trade_count: num(m.trade_count),
  }
}

function num(v: any): number | null {
  if (v == null || v === '' || (typeof v === 'number' && !isFinite(v))) return null
  const n = Number(v)
  return isNaN(n) ? null : n
}

export function fmtPct(v: number | null): string {
  if (v == null) return '—'
  return `${(v * 100).toFixed(2)}%`
}

export function fmtNum(v: number | null, digits = 2): string {
  if (v == null) return '—'
  return v.toFixed(digits)
}

// 正→红涨(text-bull)，负→绿跌(text-bear)，null→muted
export function tone(v: number | null): string {
  if (v == null) return 'text-muted'
  return v >= 0 ? 'text-bull' : 'text-bear'
}

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
