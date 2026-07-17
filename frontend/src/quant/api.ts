const B = '/api/quant'

async function j(path: string, init?: RequestInit) {
  const r = await fetch(B + path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!r.ok) throw new Error(`quant api ${r.status}: ${await r.text()}`)
  const body = await r.json()
  return body.data
}

export const listStrategies = () => j('/strategies')
export const getStrategy = (id: string) => j(`/strategies/${id}`)
export const saveStrategy = (id: string | null, name: string, code: string) =>
  id ? j(`/strategies/${id}`, { method: 'PUT', body: JSON.stringify({ name, code }) })
     : j('/strategies', { method: 'POST', body: JSON.stringify({ name, code }) })
export const deleteStrategy = (id: string) => j(`/strategies/${id}`, { method: 'DELETE' })
export const exportStrategy = (id: string) => j(`/strategies/${id}/export`)
export const importStrategy = (name: string, code: string) =>
  j('/strategies/import', { method: 'POST', body: JSON.stringify({ name, code }) })

export const listStrategiesWithLatest = () => j('/strategies/with-latest')
export const listBacktests = (strategyId?: string) =>
  j(`/backtest/runs${strategyId ? `?strategy_id=${encodeURIComponent(strategyId)}` : ''}`)
export const runBacktest = (p: any) => j('/backtest/run', { method: 'POST', body: JSON.stringify(p) })
export const getBacktestStatus = (id: string) => j(`/backtest/${id}/status`)
export const getBacktestEquity = (id: string) => j(`/backtest/${id}/equity`)
export const getBacktestTrades = (id: string) => j(`/backtest/${id}/trades`)
export const getBacktestLogs = (id: string) => j(`/backtest/${id}/logs`)
export const getBacktestCsvUrl = (id: string) => `${B}/backtest/${id}/trades.csv`
export const terminateBacktest = (id: string) => j(`/backtest/${id}/terminate`, { method: 'POST' })
export const deleteBacktest = (id: string) => j(`/backtest/${id}`, { method: 'DELETE' })

export const listAccounts = () => j('/sim/accounts')
export const createAccount = (b: any) => j('/sim/accounts', { method: 'POST', body: JSON.stringify(b) })
export const startAccount = (id: string) => j(`/sim/accounts/${id}/start`, { method: 'POST' })
export const pauseAccount = (id: string) => j(`/sim/accounts/${id}/pause`, { method: 'POST' })
export const resetAccount = (id: string) => j(`/sim/accounts/${id}/reset`, { method: 'POST' })
export const getSimStatus = (id: string) => j(`/sim/accounts/${id}/status`)
export const getSimEquity = (id: string) => j(`/sim/accounts/${id}/equity`)
export const getSimTrades = (id: string) => j(`/sim/accounts/${id}/trades`)

export const getDatasource = () => j('/datasource')
export const saveDatasourcePriority = (priority: string[]) =>
  j('/datasource/priority', { method: 'POST', body: JSON.stringify({ priority }) })
export const saveDatasourceToken = (token: string) =>
  j('/datasource/token', { method: 'POST', body: JSON.stringify({ token }) })
export const verifyDatasource = () => j('/datasource/verify', { method: 'POST' })
