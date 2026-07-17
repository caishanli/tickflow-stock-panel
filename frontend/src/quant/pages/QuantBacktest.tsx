import { useEffect, useState } from 'react'
import { PageHeader } from '@/components/PageHeader'
import { Modal } from '@/components/Modal'
import { DatePicker } from '@/components/DatePicker'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus, ArrowLeft, Play, Square, Download, Trash2, FileCode2,
} from 'lucide-react'
import * as api from '../api'
import { openBacktestStream } from '../stream'
import { CodeEditor } from '../components/CodeEditor'
import { EquityChart } from '../components/EquityChart'
import { pickMetrics, fmtPct, fmtNum, tone } from '../metrics'

const INPUT_CLS =
  'h-9 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground ' +
  'placeholder:text-muted focus:outline-none focus:border-accent/50 transition-colors'

const SECTION_TITLE = 'text-[11px] font-medium uppercase tracking-wide text-muted flex items-center gap-1.5'

function statusTone(s: string | undefined) {
  if (s === 'done') return 'text-bull'
  if (s === 'failed') return 'text-bear'
  if (s === 'running' || s === 'queued') return 'text-accent'
  return 'text-muted'
}

interface FormState {
  name: string
  symbols: string
  start: string
  end: string
  frequency: string
  fee: number
  slippage: number
  capital: number
}

const DEFAULT_FORM: FormState = {
  name: '', symbols: '600000.XSHG', start: '', end: '', frequency: 'daily',
  fee: 0.0003, slippage: 0.001, capital: 100000,
}

export function QuantBacktest() {
  const [view, setView] = useState<'list' | 'new' | 'edit'>('list')
  const [selRun, setSelRun] = useState<string | null>(null)
  const [editorKey, setEditorKey] = useState(0)

  const openNew = () => { setSelRun(null); setEditorKey(k => k + 1); setView('new') }
  const openEdit = (id: string) => { setSelRun(id); setEditorKey(k => k + 1); setView('edit') }

  return (
    <div className="flex flex-col h-full">
      {view === 'list' ? (
        <BacktestList onNew={openNew} onOpen={openEdit} />
      ) : (
        <BacktestEditor key={editorKey} mode={view} sourceRunId={selRun} onBack={() => setView('list')} />
      )}
    </div>
  )
}

function BacktestList({ onNew, onOpen }: { onNew: () => void; onOpen: (id: string) => void }) {
  const { data: runs } = useQuery({ queryKey: ['quant', 'bt', 'runs'], queryFn: () => api.listBacktests() })
  const list = (runs?.data ?? []) as any[]

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="量化回测"
        subtitle="RQAlpha · 聚宽式策略 · 实时 SSE"
        right={
          <button onClick={onNew}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-btn bg-accent text-base text-xs font-medium hover:bg-accent/90 transition-colors">
            <Plus className="h-4 w-4" />新建
          </button>
        }
      />
      <div className="flex-1 p-4 overflow-auto">
        <div className="rounded-card border border-border bg-surface overflow-hidden">
          <table className="w-full text-xs">
            <thead className="text-muted bg-elevated/40">
              <tr className="text-left">
                <th className="px-3 py-2 font-normal w-12">序号</th>
                <th className="px-3 py-2 font-normal">名称</th>
                <th className="px-3 py-2 font-normal">回测周期</th>
                <th className="px-3 py-2 font-normal text-right">收益率</th>
                <th className="px-3 py-2 font-normal text-right">最大回撤</th>
                <th className="px-3 py-2 font-normal text-right">夏普比率</th>
              </tr>
            </thead>
            <tbody className="text-foreground">
              {list.length === 0 && (
                <tr><td colSpan={6} className="px-3 py-10 text-center text-muted">暂无回测记录</td></tr>
              )}
              {list.map((r, i) => {
                const p = (() => { try { return JSON.parse(r.params_json || '{}') } catch { return {} } })()
                const m = pickMetrics(r.metrics_json)
                const period = `${p.start ?? ''} ~ ${p.end ?? ''}`
                return (
                  <tr key={r.id} onClick={() => onOpen(r.id)}
                    className="border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
                    <td className="px-3 py-2 text-muted num">{i + 1}</td>
                    <td className="px-3 py-2">
                      <div className="font-medium">{r.name || r.id}</div>
                      <div className="text-[10px] text-muted truncate max-w-[200px]">{p.symbols?.join(', ')}</div>
                    </td>
                    <td className="px-3 py-2 text-muted num">{period}</td>
                    <td className={`px-3 py-2 text-right num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.max_drawdown ? -m.max_drawdown : null)}`}>{m.max_drawdown == null ? '—' : fmtPct(-m.max_drawdown)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.sharpe)}`}>{fmtNum(m.sharpe)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function BacktestEditor({ mode, sourceRunId, onBack }: {
  mode: 'new' | 'edit'
  sourceRunId: string | null
  onBack: () => void
}) {
  const qc = useQueryClient()
  const { data: source } = useQuery({
    queryKey: ['quant', 'bt', 'src', sourceRunId],
    queryFn: () => api.getBacktestStatus(sourceRunId as string),
    enabled: mode === 'edit' && !!sourceRunId,
  })

  const [form, setForm] = useState<FormState>(DEFAULT_FORM)
  const [code, setCode] = useState<string>('')
  const [liveRunId, setLiveRunId] = useState<string | null>(null)
  const [liveOn, setLiveOn] = useState(true)
  const [tab, setTab] = useState<'log' | 'trade'>('log')
  const [confirmDel, setConfirmDel] = useState(false)

  useEffect(() => {
    if (mode === 'edit' && source?.data) {
      const r = source.data
      const p = (() => { try { return JSON.parse(r.params_json || '{}') } catch { return {} } })()
      setForm({
        name: r.name || '',
        symbols: (p.symbols || []).join(', '),
        start: p.start || '', end: p.end || '',
        frequency: p.frequency || 'daily',
        fee: p.fee ?? 0.0003, slippage: p.slippage ?? 0.001,
        capital: p.capital ?? 100000,
      })
      setCode(p.strategy_code || '')
    }
  }, [mode, source])

  const runMut = useMutation({
    mutationFn: () => api.runBacktest({
      name: form.name,
      strategy_code: code,
      symbols: form.symbols.split(',').map(s => s.trim()).filter(Boolean),
      start: form.start, end: form.end, frequency: form.frequency,
      fee: +form.fee, slippage: +form.slippage, capital: +form.capital,
    }),
    onSuccess: (d: any) => { setLiveRunId(d.run_id); qc.invalidateQueries({ queryKey: ['quant', 'bt', 'runs'] }) },
  })

  const runId = liveRunId
  const { data: status } = useQuery({ queryKey: ['quant', 'bt', runId, 'status'], queryFn: () => api.getBacktestStatus(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: equity } = useQuery({ queryKey: ['quant', 'bt', runId, 'equity'], queryFn: () => api.getBacktestEquity(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: trades } = useQuery({ queryKey: ['quant', 'bt', runId, 'trades'], queryFn: () => api.getBacktestTrades(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: logs } = useQuery({ queryKey: ['quant', 'bt', runId, 'logs'], queryFn: () => api.getBacktestLogs(runId as string), enabled: !!runId, refetchInterval: 2000 })

  useEffect(() => {
    if (!runId || !liveOn) return
    const es = openBacktestStream(runId, {
      onEquity: () => qc.invalidateQueries({ queryKey: ['quant', 'bt', runId, 'equity'] }),
      onTrade: () => qc.invalidateQueries({ queryKey: ['quant', 'bt', runId, 'trades'] }),
      onLog: () => qc.invalidateQueries({ queryKey: ['quant', 'bt', runId, 'logs'] }),
      onStatus: () => qc.invalidateQueries({ queryKey: ['quant', 'bt', runId, 'status'] }),
    })
    return () => { es.close() }
  }, [runId, liveOn, qc])

  const lastStatus = status?.data ? (status.data.state ?? status.data.status) : undefined
  const metrics = pickMetrics(status?.data?.metrics_json)
  const equityData: any[] = Array.isArray(equity?.data) ? equity.data : []

  return (
    <div className="flex flex-col h-full">
      <header className="px-4 py-3 border-b border-border flex items-center gap-3 flex-wrap">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border bg-base text-secondary hover:text-foreground transition-colors">
          <ArrowLeft className="h-4 w-4" />列表
        </button>
        <input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })}
          placeholder="策略名称" className={`${INPUT_CLS} w-48`} />
        <button onClick={() => setLiveOn(v => !v)}
          className={`inline-flex items-center gap-1.5 h-9 px-3 rounded-btn border text-xs transition-colors ${liveOn ? 'border-accent/40 text-accent bg-accent/10' : 'border-border text-muted bg-base'}`}>
          {liveOn ? <Play className="h-3.5 w-3.5" /> : <Square className="h-3.5 w-3.5" />}编译运行
        </button>
        <div className="flex items-center gap-2 text-xs text-muted">
          <span>周期</span>
          <DatePicker value={form.start} onChange={d => setForm({ ...form, start: d })} placeholder="开始" buttonClassName="w-32 justify-between" />
          <span>~</span>
          <DatePicker value={form.end} onChange={d => setForm({ ...form, end: d })} placeholder="结束" buttonClassName="w-32 justify-between" />
        </div>
        <div className="flex items-center gap-2 text-xs text-muted">
          <span>初始金额</span>
          <input type="number" value={form.capital} onChange={e => setForm({ ...form, capital: +e.target.value })}
            className={`${INPUT_CLS} w-28`} />
        </div>
        <button onClick={() => runMut.mutate()} disabled={!code || !form.start || !form.end || !form.name || runMut.isPending}
          className="ml-auto inline-flex items-center gap-1.5 h-9 px-4 rounded-btn bg-accent text-base text-xs font-medium disabled:opacity-50 hover:bg-accent/90 transition-colors">
          <Play className="h-4 w-4" />{runMut.isPending ? '提交中…' : '开始回测'}
        </button>
        {lastStatus && (
          <span className={`text-xs font-medium px-2 py-0.5 rounded-btn bg-base border border-border ${statusTone(lastStatus)}`}>{lastStatus}</span>
        )}
      </header>

      <div className="flex-1 grid grid-cols-1 lg:grid-cols-2 overflow-hidden">
        <div className="border-r border-border p-4 overflow-auto">
          <div className={`${SECTION_TITLE} mb-2`}><FileCode2 className="h-3.5 w-3.5" />策略代码 (Python)</div>
          <div className="rounded-card border border-border overflow-hidden">
            <CodeEditor value={code} onChange={setCode} />
          </div>
        </div>

        <div className="p-4 overflow-auto space-y-4">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            <MetricCard label="收益率" value={fmtPct(metrics.total_return)} tone={tone(metrics.total_return)} />
            <MetricCard label="年化" value={fmtPct(metrics.annualized)} tone={tone(metrics.annualized)} />
            <MetricCard label="夏普" value={fmtNum(metrics.sharpe)} tone={tone(metrics.sharpe)} />
            <MetricCard label="最大回撤" value={metrics.max_drawdown == null ? '—' : fmtPct(-metrics.max_drawdown)} tone={tone(metrics.max_drawdown ? -metrics.max_drawdown : null)} />
          </div>

          <div className="rounded-card border border-border bg-surface">
            <div className="px-4 pt-3 text-xs text-foreground font-medium">收益曲线（策略 / 基准）</div>
            {runId && equityData.length > 0 ? (
              <EquityChart equity={equityData} />
            ) : (
              <div className="h-[260px] grid place-items-center text-xs text-muted">
                {runId ? '暂无净值数据' : '运行回测后展示实时曲线'}
              </div>
            )}
          </div>

          <div className="rounded-card border border-border bg-surface overflow-hidden">
            <div className="flex items-center gap-1 px-2 pt-2">
              <TabBtn active={tab === 'log'} onClick={() => setTab('log')}>日志</TabBtn>
              <TabBtn active={tab === 'trade'} onClick={() => setTab('trade')}>交易记录</TabBtn>
              <div className="ml-auto flex items-center gap-3 pr-2">
                {runId && <a href={api.getBacktestCsvUrl(runId)} className="inline-flex items-center gap-1 text-xs text-accent hover:underline"><Download className="h-3.5 w-3.5" />CSV</a>}
                {runId && <button onClick={() => setConfirmDel(true)} className="inline-flex items-center gap-1 text-xs text-bear hover:underline"><Trash2 className="h-3.5 w-3.5" />删除</button>}
              </div>
            </div>
            <div className="p-3">
              {tab === 'log' ? (
                <LogList logs={Array.isArray(logs?.data) ? logs.data : []} />
              ) : (
                <TradeTable trades={Array.isArray(trades?.data) ? trades.data : []} />
              )}
            </div>
          </div>
        </div>
      </div>

      {confirmDel && runId && (
        <Modal onClose={() => setConfirmDel(false)} ariaLabel="确认删除回测">
          <div className="p-5 space-y-4">
            <h3 className="text-sm font-medium text-foreground">删除回测记录</h3>
            <p className="text-xs text-muted">确定删除 <span className="font-mono">{runId}</span> 及其全部数据？此操作不可恢复。</p>
            <div className="flex justify-end gap-2">
              <button onClick={() => setConfirmDel(false)} className="px-3 py-1.5 rounded-btn bg-elevated text-secondary text-xs hover:text-foreground transition-colors">取消</button>
              <button onClick={() => { api.deleteBacktest(runId).then(() => { qc.invalidateQueries({ queryKey: ['quant', 'bt', 'runs'] }); setConfirmDel(false); setLiveRunId(null) }) }}
                className="px-3 py-1.5 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors">删除</button>
            </div>
          </div>
        </Modal>
      )}
    </div>
  )
}

function MetricCard({ label, value, tone }: { label: string; value: string; tone: string }) {
  return (
    <div className="rounded-card border border-border bg-surface px-3 py-2">
      <div className="text-[10px] text-muted">{label}</div>
      <div className={`text-sm font-medium num ${tone}`}>{value}</div>
    </div>
  )
}

function TabBtn({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button onClick={onClick}
      className={`px-3 py-1.5 rounded-btn text-xs transition-colors ${active ? 'bg-elevated text-foreground' : 'text-muted hover:text-foreground'}`}>
      {children}
    </button>
  )
}

function LogList({ logs }: { logs: any[] }) {
  if (logs.length === 0) return <div className="text-xs text-muted">暂无日志</div>
  return (
    <div className="max-h-64 overflow-auto space-y-0.5 text-[11px] text-muted font-mono">
      {logs.map((l, i) => (
        <div key={i}>{typeof l === 'string' ? l : `${l.level ?? ''} ${l.message ?? JSON.stringify(l)}`}</div>
      ))}
    </div>
  )
}

function TradeTable({ trades }: { trades: any[] }) {
  if (trades.length === 0) return <div className="text-xs text-muted">暂无成交</div>
  return (
    <div className="overflow-auto max-h-64">
      <table className="w-full text-xs">
        <thead className="text-muted sticky top-0 bg-surface">
          <tr className="text-left">
            <th className="px-2 py-1.5 font-normal">时间</th>
            <th className="px-2 py-1.5 font-normal">标的</th>
            <th className="px-2 py-1.5 font-normal">方向</th>
            <th className="px-2 py-1.5 font-normal text-right">价格</th>
            <th className="px-2 py-1.5 font-normal text-right">数量</th>
          </tr>
        </thead>
        <tbody className="text-foreground">
          {trades.map((t, i) => (
            <tr key={i} className="border-t border-border/60">
              <td className="px-2 py-1.5 text-muted">{String(t.ts ?? t.datetime ?? '')}</td>
              <td className="px-2 py-1.5">{t.code ?? t.symbol ?? ''}</td>
              <td className={`px-2 py-1.5 ${(t.action ?? t.side) === 'BUY' || (t.action ?? t.side) === 'buy' ? 'text-bull' : 'text-bear'}`}>{t.action ?? t.side ?? ''}</td>
              <td className="px-2 py-1.5 text-right">{t.price ?? ''}</td>
              <td className="px-2 py-1.5 text-right">{t.amount ?? t.qty ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
