// 统一两条回测引擎的指标 key（rqalpha: annualized；jqengine: annual_return）
export interface Metrics {
  total_return: number | null
  annualized: number | null
  sharpe: number | null
  max_drawdown: number | null
}

export function pickMetrics(raw: any): Metrics {
  if (!raw) return { total_return: null, annualized: null, sharpe: null, max_drawdown: null }
  let m: any = raw
  if (typeof raw === 'string') {
    try { m = JSON.parse(raw) } catch { m = {} }
  }
  return {
    total_return: num(m.total_return),
    annualized: num(m.annualized ?? m.annual_return),
    sharpe: num(m.sharpe),
    max_drawdown: num(m.max_drawdown),
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
