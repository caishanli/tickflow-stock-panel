// SSE 客户端：订阅单个回测的实时事件（status/log/equity/trade）。
// 断线由 EventSource 自动重连；刷新页面时先用轮询接口拉全量历史，再开 SSE 收增量。

export interface BacktestStreamHandlers {
  onStatus?: (s: { status: string; metrics?: string | null }) => void
  onLog?: (l: { rowid?: number; ts: string; level: string; message: string }) => void
  onEquity?: (e: { dt: string; value: number; benchmark: number; cash: number; positions_value: number }) => void
  onTrade?: (t: { ts: string; code: string; action: string; price: number; amount: number; pnl: number; pnl_pct: number; commission: number }) => void
}

export function openBacktestStream(runId: string, handlers: BacktestStreamHandlers): EventSource {
  const es = new EventSource(`/api/quant/backtest/${runId}/stream`)
  if (handlers.onStatus) es.addEventListener('status', (e) => handlers.onStatus!(JSON.parse((e as MessageEvent).data)))
  if (handlers.onLog) es.addEventListener('log', (e) => handlers.onLog!(JSON.parse((e as MessageEvent).data)))
  if (handlers.onEquity) es.addEventListener('equity', (e) => handlers.onEquity!(JSON.parse((e as MessageEvent).data)))
  if (handlers.onTrade) es.addEventListener('trade', (e) => handlers.onTrade!(JSON.parse((e as MessageEvent).data)))
  return es
}

export interface SimStreamHandlers {
  onStatus?: (s: { status: string; state: any }) => void
  onLog?: (l: { ts: string; level: string; message: string }) => void
  onEquity?: (e: { dt: string; net_value: number; cash: number; positions_value: number; pnl: number; pnl_pct: number }) => void
  onTrade?: (t: { ts: string; code: string; action: string; price: number; amount: number; pnl: number; pnl_pct: number; commission: number }) => void
}

export function openSimStream(aid: string, handlers: SimStreamHandlers): EventSource {
  const es = new EventSource(`/api/quant/sim/accounts/${aid}/stream`)
  if (handlers.onStatus) es.addEventListener('status', (e) => handlers.onStatus!(JSON.parse((e as MessageEvent).data)))
  if (handlers.onLog) es.addEventListener('log', (e) => handlers.onLog!(JSON.parse((e as MessageEvent).data)))
  if (handlers.onEquity) es.addEventListener('equity', (e) => handlers.onEquity!(JSON.parse((e as MessageEvent).data)))
  if (handlers.onTrade) es.addEventListener('trade', (e) => handlers.onTrade!(JSON.parse((e as MessageEvent).data)))
  return es
}
