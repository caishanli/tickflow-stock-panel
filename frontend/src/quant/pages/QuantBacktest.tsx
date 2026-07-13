import { useState } from 'react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { DatePicker } from '@/components/DatePicker'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import * as api from '../api'
import { StrategyEditorDialog } from './StrategyEditorDialog'
import { BacktestResult } from './BacktestResult'

export function QuantBacktest() {
  const qc = useQueryClient()
  const { data: strategies } = useQuery({ queryKey: ['quant', 'strategies'], queryFn: api.listStrategies })
  const [editor, setEditor] = useState<{ open: boolean; id: string | null; name: string; code: string }>({ open: false, id: null, name: '', code: '' })
  const [form, setForm] = useState({ symbols: '600000.XSHG', start: '', end: '', frequency: 'daily', fee: 0.0003, slippage: 0.001, capital: 100000, strategyId: '', datasourcePriority: 'rqalpha,joinquant' })
  const [runId, setRunId] = useState<string | null>(null)

  const runMut = useMutation({ mutationFn: () => api.runBacktest({
    strategy_id: form.strategyId, symbols: form.symbols.split(',').map(s => s.trim()).filter(Boolean),
    start: form.start, end: form.end, frequency: form.frequency, fee: +form.fee, slippage: +form.slippage, capital: +form.capital,
    datasource_priority: form.datasourcePriority.split(',').map(s => s.trim()).filter(Boolean),
  }), onSuccess: (d: any) => setRunId(d.run_id) })

  const delMut = useMutation({ mutationFn: (id: string) => api.deleteStrategy(id), onSuccess: () => qc.invalidateQueries({ queryKey: ['quant', 'strategies'] }) })

  // 实时轮询（回测 1–2s）
  const { data: status } = useQuery({
    queryKey: ['quant', 'bt', runId, 'status'], queryFn: () => api.getBacktestStatus(runId!),
    enabled: !!runId, refetchInterval: 1500,
  })
  const { data: equity } = useQuery({ queryKey: ['quant', 'bt', runId, 'equity'], queryFn: () => api.getBacktestEquity(runId!), enabled: !!runId, refetchInterval: 1500 })
  const { data: trades } = useQuery({ queryKey: ['quant', 'bt', runId, 'trades'], queryFn: () => api.getBacktestTrades(runId!), enabled: !!runId, refetchInterval: 1500 })
  const { data: logs } = useQuery({ queryKey: ['quant', 'bt', runId, 'logs'], queryFn: () => api.getBacktestLogs(runId!), enabled: !!runId, refetchInterval: 1500 })

  return (
    <div className="flex flex-col h-full">
      <PageHeader title="量化回测" subtitle="RQAlpha · 聚宽式策略" />
      <div className="flex-1 grid grid-cols-[320px_1fr] overflow-hidden">
        {/* 左：策略列表 */}
        <aside className="border-r border-border p-3 space-y-2 overflow-auto">
          <button onClick={() => setEditor({ open: true, id: null, name: '', code: '' })}
            className="w-full h-9 rounded-lg bg-accent text-white text-xs">新建策略</button>
          {(strategies ?? []).map((s: any) => (
            <div key={s.id} className="flex items-center justify-between rounded-card border border-border bg-surface px-3 h-10 text-xs">
              <span className="text-foreground truncate">{s.name}</span>
              <div className="flex gap-2 shrink-0">
                <button onClick={() => setForm(f => ({ ...f, strategyId: s.id }))} className="text-accent">选</button>
                <button onClick={() => api.getStrategy(s.id).then(d => setEditor({ open: true, id: s.id, name: d.name, code: d.code }))} className="text-muted">编辑</button>
                <button onClick={() => delMut.mutate(s.id)} className="text-bear">删</button>
              </div>
            </div>
          ))}
        </aside>
        {/* 右：运行表单 + 结果 */}
        <section className="p-4 space-y-4 overflow-auto">
          <div className="grid grid-cols-2 gap-3 max-w-2xl">
            <input value={form.symbols} onChange={e => setForm({ ...form, symbols: e.target.value })} placeholder="标的池(逗号分隔)" className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
            <select value={form.frequency} onChange={e => setForm({ ...form, frequency: e.target.value })} className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground">
              <option value="daily">daily</option>
              <option value="1m">1m</option>
            </select>
            <DatePicker value={form.start} onChange={d => setForm({ ...form, start: d })} />
            <DatePicker value={form.end} onChange={d => setForm({ ...form, end: d })} />
            <input type="number" value={form.fee} step="0.0001" onChange={e => setForm({ ...form, fee: +e.target.value })} placeholder="手续费" className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
            <input type="number" value={form.slippage} step="0.0001" onChange={e => setForm({ ...form, slippage: +e.target.value })} placeholder="滑点" className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
            <input type="number" value={form.capital} onChange={e => setForm({ ...form, capital: +e.target.value })} placeholder="本金" className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
            <input value={form.datasourcePriority} onChange={e => setForm({ ...form, datasourcePriority: e.target.value })} placeholder="数据源优先级(逗号分隔)" className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
            <button onClick={() => runMut.mutate()} disabled={!form.strategyId || !form.start || !form.end || runMut.isPending}
              className="h-9 rounded-lg bg-accent text-white text-xs disabled:opacity-50">{runMut.isPending ? '运行中…' : '运行回测'}</button>
          </div>
          {runId && status ? (
            <BacktestResult status={status} equity={equity} trades={trades} logs={logs} />
          ) : (
            <EmptyState title="尚未运行" hint="选择或新建聚宽策略后点击运行" />
          )}
        </section>
      </div>
      <StrategyEditorDialog open={editor.open} initial={{ id: editor.id, name: editor.name, code: editor.code }}
        onClose={() => setEditor({ ...editor, open: false })}
        onSave={(name, code) => { api.saveStrategy(editor.id, name, code).then(() => { setEditor({ ...editor, open: false }); qc.invalidateQueries({ queryKey: ['quant', 'strategies'] }) }) }} />
    </div>
  )
}
