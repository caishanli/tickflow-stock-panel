import { useEffect, useRef, useState, type ReactNode } from 'react'
import { PageHeader } from '@/components/PageHeader'
import { Modal } from '@/components/Modal'
import { toast } from '@/components/Toast'
import { DatePicker } from '@/components/DatePicker'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus, ArrowLeft, Play, Download, Trash2, FileCode2, Activity, Settings2, History, Save,
} from 'lucide-react'
import * as api from '../api'
import { openBacktestStream } from '../stream'
import { CodeEditor } from '../components/CodeEditor'
import { EquityChart } from '../components/EquityChart'
import { pickMetrics, fmtPct, fmtNum, tone } from '../metrics'

const INPUT_CLS =
  'h-9 w-full rounded-btn bg-base border border-border px-2.5 text-xs text-foreground ' +
  'placeholder:text-muted focus:outline-none focus:border-accent/50 transition-colors'

const SECTION_TITLE = 'text-[11px] font-medium uppercase tracking-wide text-muted flex items-center gap-1.5'

function statusTone(s: string | undefined) {
  if (s === 'done') return 'text-bull'
  if (s === 'failed') return 'text-bear'
  if (s === 'running' || s === 'queued') return 'text-accent'
  return 'text-muted'
}

interface FormState {
  start: string
  end: string
  frequency: string
  fee: number
  slippage: number
  capital: number
}

const DEFAULT_FORM: FormState = {
  start: '', end: '', frequency: 'daily',
  fee: 0.0003, slippage: 0.001, capital: 100000,
}

export function QuantBacktest() {
  const [view, setView] = useState<'list' | 'editor'>('list')
  const [selStrategy, setSelStrategy] = useState<string | null>(null)

  const openNew = async () => {
    const d: any = await api.saveStrategy(null, '未命名策略', '# 新策略\n')
    setSelStrategy(d.id); setView('editor')
  }
  const openStrategy = (id: string) => { setSelStrategy(id); setView('editor') }

  return (
    <div className="flex flex-col h-full">
      {view === 'list' ? (
        <StrategyList onNew={openNew} onOpen={openStrategy} />
      ) : (
        <StrategyEditor key={selStrategy} strategyId={selStrategy!} onBack={() => setView('list')} />
      )}
    </div>
  )
}

function StrategyList({ onNew, onOpen }: { onNew: () => void; onOpen: (id: string) => void }) {
  const { data } = useQuery({ queryKey: ['quant', 'strategies', 'latest'], queryFn: () => api.listStrategiesWithLatest() })
  const list = (data?.data ?? []) as any[]

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="量化回测"
        subtitle="策略 · RQAlpha · 聚宽式 · 实时 SSE"
        right={
          <button onClick={onNew}
            className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-accent text-base text-xs font-medium hover:bg-accent/90 transition-colors">
            <Plus className="h-4 w-4" />新建
          </button>
        }
      />
      <div className="flex-1 p-4 overflow-auto">
        <div className="rounded-card border border-border bg-surface overflow-hidden">
          <table className="w-full text-xs">
            <thead className="text-muted bg-elevated/40">
              <tr className="text-left">
                <th className="px-3 py-2 font-normal">策略名称</th>
                <th className="px-3 py-2 font-normal">最新回测周期</th>
                <th className="px-3 py-2 font-normal text-right">收益率</th>
                <th className="px-3 py-2 font-normal text-right">最大回撤</th>
                <th className="px-3 py-2 font-normal text-right">夏普比率</th>
                <th className="px-3 py-2 font-normal text-right">回测次数</th>
              </tr>
            </thead>
            <tbody className="text-foreground">
              {list.length === 0 && (
                <tr><td colSpan={6} className="px-3 py-10 text-center text-muted">暂无策略，点击右上角新建</td></tr>
              )}
              {list.map((s) => {
                const m = pickMetrics(s.latest?.metrics_json)
                const period = s.latest ? `${s.latest.start ?? ''} ~ ${s.latest.end ?? ''}` : '—'
                return (
                  <tr key={s.id} onClick={() => onOpen(s.id)}
                    className="border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
                    <td className="px-3 py-2 font-medium">{s.name}</td>
                    <td className="px-3 py-2 text-muted num">{period}</td>
                    <td className={`px-3 py-2 text-right num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.max_drawdown ? -m.max_drawdown : null)}`}>{m.max_drawdown == null ? '—' : fmtPct(-m.max_drawdown)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.sharpe)}`}>{fmtNum(m.sharpe)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(null)}`}>{s.run_count}</td>
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

function StrategyEditor({ strategyId, onBack }: { strategyId: string; onBack: () => void }) {
  const qc = useQueryClient()
  const { data: strategy } = useQuery({ queryKey: ['quant', 'strategy', strategyId], queryFn: () => api.getStrategy(strategyId) })

  const [code, setCode] = useState<string>('')
  const [name, setName] = useState<string>('')
  const [form, setForm] = useState<FormState>(DEFAULT_FORM)
  const [liveRunId, setLiveRunId] = useState<string | null>(null)
  const [selRunId, setSelRunId] = useState<string | null>(null)
  const [liveOn, setLiveOn] = useState(true)
  const [logTab, setLogTab] = useState<'log' | 'error' | 'trade'>('log')
  const [advOpen, setAdvOpen] = useState(false)
  const [histOpen, setHistOpen] = useState(false)
  const [confirmDel, setConfirmDel] = useState(false)
  const [leftPct, setLeftPct] = useState(50)
  const bodyRef = useRef<HTMLDivElement>(null)
  const dragging = useRef(false)

  const startDrag = () => {
    dragging.current = true
    const onMove = (e: MouseEvent) => {
      if (!dragging.current || !bodyRef.current) return
      const r = bodyRef.current.getBoundingClientRect()
      const pct = ((e.clientX - r.left) / r.width) * 100
      setLeftPct(Math.min(75, Math.max(25, pct)))
    }
    const onUp = () => {
      dragging.current = false
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'
  }

  const seeded = useRef(false)
  useEffect(() => {
    if (strategy?.data && !seeded.current) {
      seeded.current = true
      setName(strategy.data.name || '')
      setCode(strategy.data.code || '')
    }
  }, [strategy])

  const saveStrategy = async () => {
    try {
      await api.saveStrategy(strategyId, name, code)
      qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] })
      toast('策略已保存', 'success', 'top')
    } catch (e: any) {
      toast(e?.message ? `保存失败: ${e.message}` : '保存失败', 'error')
    }
  }

  const runMut = useMutation({
    mutationFn: (short: boolean) => {
      saveStrategy()
      const end = form.end
      const start = short && end ? shiftDays(end, -7) : form.start
      return api.runBacktest({
        name, strategy_id: strategyId, strategy_code: code,
        symbols: extractUniverse(code),
        start, end, frequency: form.frequency,
        fee: +form.fee, slippage: +form.slippage, capital: +form.capital,
      })
    },
    onSuccess: (d: any) => { setLiveRunId(d.run_id); setSelRunId(d.run_id); qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] }) },
  })

  const runId = selRunId ?? liveRunId
  const { data: status } = useQuery({ queryKey: ['quant', 'bt', runId, 'status'], queryFn: () => api.getBacktestStatus(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: equity } = useQuery({ queryKey: ['quant', 'bt', runId, 'equity'], queryFn: () => api.getBacktestEquity(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: trades } = useQuery({ queryKey: ['quant', 'bt', runId, 'trades'], queryFn: () => api.getBacktestTrades(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: logs } = useQuery({ queryKey: ['quant', 'bt', runId, 'logs'], queryFn: () => api.getBacktestLogs(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: runs } = useQuery({ queryKey: ['quant', 'bt', 'runs', strategyId], queryFn: () => api.listBacktests(strategyId), refetchInterval: 3000 })

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
  const runList: any[] = Array.isArray(runs?.data) ? runs.data : []
  const excess = computeExcess(equityData)
  const errLogs = filterErrorLogs(Array.isArray(logs?.data) ? logs.data : [])
  const hasError = errLogs.length > 0

  return (
    <div className="flex flex-col h-full">
      <header className="px-4 py-3 border-b border-border flex items-center gap-2 flex-wrap">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border bg-base text-secondary hover:text-foreground transition-colors">
          <ArrowLeft className="h-4 w-4" />列表
        </button>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="策略名称"
          className="h-9 w-44 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground focus:outline-none focus:border-accent/50" />
        <button onClick={saveStrategy} disabled={!name.trim()}
          className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn border border-border text-xs text-secondary hover:text-foreground transition-colors disabled:opacity-50">
          <Save className="h-3.5 w-3.5" />保存策略
        </button>
        <DatePicker value={form.start} onChange={d => setForm({ ...form, start: d })} label="开始" placeholder="选择日期" buttonClassName="h-9 justify-between w-44" />
        <DatePicker value={form.end} onChange={d => setForm({ ...form, end: d })} label="结束" placeholder="选择日期" buttonClassName="h-9 justify-between w-44" />
        <input type="number" value={form.capital} onChange={e => setForm({ ...form, capital: +e.target.value })} placeholder="初始金额"
          className="h-9 w-28 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground focus:outline-none focus:border-accent/50" />
        <select value={form.frequency} onChange={e => setForm({ ...form, frequency: e.target.value })}
          className="h-9 w-24 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground focus:outline-none focus:border-accent/50">
          <option value="daily">daily</option>
          <option value="1m">1m</option>
        </select>
        <div className="relative">
          <button onClick={() => { setAdvOpen(o => !o); setHistOpen(false) }} title="高级参数"
            className={`inline-flex items-center gap-1 h-9 px-2.5 rounded-btn border border-border text-xs ${advOpen ? 'text-accent' : 'text-secondary hover:text-foreground'} transition-colors`}>
            <Settings2 className="h-3.5 w-3.5" />高级
          </button>
          {advOpen && (
            <div className="absolute z-20 right-0 top-11 w-64 rounded-card border border-border bg-surface p-3 space-y-2 shadow-xl">
              <div className="text-[10px] uppercase tracking-wide text-muted">手续费</div>
              <input type="number" step="0.0001" value={form.fee} onChange={e => setForm({ ...form, fee: +e.target.value })} className={INPUT_CLS} />
              <div className="text-[10px] uppercase tracking-wide text-muted">滑点</div>
              <input type="number" step="0.0001" value={form.slippage} onChange={e => setForm({ ...form, slippage: +e.target.value })} className={INPUT_CLS} />
              <button onClick={() => { saveStrategy(); setAdvOpen(false) }} className="w-full h-9 rounded-btn bg-elevated text-secondary text-xs hover:text-foreground transition-colors">保存策略</button>
            </div>
          )}
        </div>
        <button onClick={() => runMut.mutate(true)} disabled={!code || runMut.isPending}
          className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn border border-border text-xs text-secondary hover:text-foreground transition-colors disabled:opacity-50">
          <Play className="h-3.5 w-3.5" />编译运行
        </button>
        <button onClick={() => runMut.mutate(false)} disabled={!code || !form.start || !form.end || runMut.isPending}
          className="inline-flex items-center gap-1.5 h-9 px-4 rounded-btn bg-accent text-base text-xs font-medium disabled:opacity-50 hover:bg-accent/90 transition-colors">
          <Play className="h-4 w-4" />{runMut.isPending ? '提交中…' : '开始回测'}
        </button>
        {lastStatus && (
          <span className={`text-xs font-medium px-2 py-0.5 rounded-btn bg-base border border-border ${statusTone(lastStatus)}`}>{lastStatus}</span>
        )}
        {liveRunId && (
          <button onClick={() => setLiveOn(!liveOn)} title="实时刷新开关"
            className={`inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border text-xs transition-colors ${liveOn ? 'text-accent' : 'text-muted hover:text-foreground'}`}>
            <Activity className="h-3.5 w-3.5" />{liveOn ? '实时' : '暂停'}
          </button>
        )}
        <div className="relative ml-auto">
          <button onClick={() => { setHistOpen(o => !o); setAdvOpen(false) }} className="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border text-xs text-secondary hover:text-foreground transition-colors">
            <History className="h-3.5 w-3.5" />历史{runList.length > 0 ? `(${runList.length})` : ''}
          </button>
          {histOpen && (
            <div className="absolute z-20 right-0 top-11 w-72 rounded-card border border-border bg-surface p-2 shadow-xl max-h-80 overflow-auto">
              {runList.length === 0 && <div className="px-2 py-4 text-xs text-muted text-center">暂无回测</div>}
              {runList.map((r) => {
                const p = (() => { try { return JSON.parse(r.params_json || '{}') } catch { return {} } })()
                const m = pickMetrics(r.metrics_json)
                const active = (selRunId ?? liveRunId) === r.id
                return (
                  <button key={r.id} onClick={() => { setSelRunId(r.id); setHistOpen(false) }}
                    className={`w-full text-left px-2 py-1.5 rounded-btn text-xs flex items-center justify-between gap-2 ${active ? 'bg-elevated text-foreground' : 'text-secondary hover:text-foreground hover:bg-elevated/60'}`}>
                    <span className="num">{`${p.start ?? ''} ~ ${p.end ?? ''}`}</span>
                    <span className={`num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</span>
                  </button>
                )
              })}
              {selRunId && (
                <button onClick={() => { setSelRunId(null); setHistOpen(false) }} className="w-full mt-1 px-2 py-1.5 rounded-btn text-xs text-muted hover:text-foreground border-t border-border">回到当前回测</button>
              )}
            </div>
          )}
        </div>
      </header>

      <div ref={bodyRef} className="flex-1 min-h-0 flex overflow-hidden">
        <div className="p-3 flex flex-col overflow-hidden" style={{ width: `${leftPct}%` }}>
          <div className={`${SECTION_TITLE} mb-2`}><FileCode2 className="h-3.5 w-3.5" />策略代码 (Python)</div>
          <div className="relative flex-1 min-h-0 rounded-card border border-border overflow-hidden">
            <div className="absolute inset-0">
              <CodeEditor value={code} onChange={setCode} height="100%" />
            </div>
          </div>
        </div>

        <div
          onMouseDown={startDrag}
          className="w-1 shrink-0 cursor-col-resize bg-border hover:bg-accent/60 transition-colors"
          title="拖动调整宽度"
        />

        <div className="flex-1 min-w-0 border-l border-border flex flex-col overflow-hidden">
          <div className="p-3 grid grid-cols-4 gap-2 shrink-0">
            <MetricCard label="收益率" value={fmtPct(metrics.total_return)} tone={tone(metrics.total_return)} />
            <MetricCard label="年化" value={fmtPct(metrics.annualized)} tone={tone(metrics.annualized)} />
            <MetricCard label="夏普" value={fmtNum(metrics.sharpe)} tone={tone(metrics.sharpe)} />
            <MetricCard label="最大回撤" value={metrics.max_drawdown == null ? '—' : fmtPct(-metrics.max_drawdown)} tone={tone(metrics.max_drawdown ? -metrics.max_drawdown : null)} />
            <MetricCard label="超额收益" value={excess == null ? '—' : fmtPct(excess)} tone={tone(excess)} />
            <MetricCard label="胜率" value={fmtPct(metrics.win_rate)} tone={tone(metrics.win_rate)} />
            <MetricCard label="盈亏比" value={fmtNum(metrics.profit_loss_ratio)} tone={tone(metrics.profit_loss_ratio)} />
            <MetricCard label="交易次数" value={metrics.trade_count == null ? (runId ? String(Array.isArray(trades?.data) ? trades.data.length : 0) : '—') : String(metrics.trade_count)} tone="text-foreground" />
          </div>

          <div className="flex-1 min-h-[220px] mx-3 rounded-card border border-border bg-surface">
            <div className="px-4 pt-3 text-xs text-foreground font-medium">收益曲线（策略 / 基准）</div>
            {runId && equityData.length > 0 ? (
              <EquityChart equity={equityData} />
            ) : (
              <div className="h-[260px] grid place-items-center text-xs text-muted">{runId ? '暂无净值数据' : '运行回测后展示实时曲线'}</div>
            )}
          </div>

          <div className="h-[220px] mt-3 mx-3 mb-3 rounded-card border border-border bg-surface flex flex-col overflow-hidden">
            <div className="flex items-center gap-1 px-2 pt-2 shrink-0">
              <TabBtn active={logTab === 'log'} onClick={() => setLogTab('log')}>日志</TabBtn>
              <TabBtn active={logTab === 'error'} onClick={() => setLogTab('error')}>
                错误{hasError && <span className="ml-1 inline-block w-1.5 h-1.5 rounded-full bg-bear align-middle" />}
              </TabBtn>
              <TabBtn active={logTab === 'trade'} onClick={() => setLogTab('trade')}>交易记录</TabBtn>
              <div className="ml-auto flex items-center gap-3 pr-2">
                {runId && <a href={api.getBacktestCsvUrl(runId)} className="inline-flex items-center gap-1 text-xs text-accent hover:underline"><Download className="h-3.5 w-3.5" />CSV</a>}
                {runId && <button onClick={() => setConfirmDel(true)} className="inline-flex items-center gap-1 text-xs text-bear hover:underline"><Trash2 className="h-3.5 w-3.5" />删除</button>}
              </div>
            </div>
            <div className="flex-1 overflow-auto p-3">
              {logTab === 'log' && <LogList logs={Array.isArray(logs?.data) ? logs.data : []} />}
              {logTab === 'error' && <LogList logs={errLogs} />}
              {logTab === 'trade' && <TradeTable trades={Array.isArray(trades?.data) ? trades.data : []} />}
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
              <button onClick={() => { api.deleteBacktest(runId).then(() => { qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] }); qc.invalidateQueries({ queryKey: ['quant', 'bt', 'runs', strategyId] }); setConfirmDel(false); setLiveRunId(null); setSelRunId(null) }) }}
                className="px-3 py-1.5 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors">删除</button>
            </div>
          </div>
        </Modal>
      )}
    </div>
  )
}

function computeExcess(equity: any[]): number | null {
  if (!equity || equity.length === 0) return null
  const first = equity[0], last = equity[equity.length - 1]
  const fv = Number(first.value), lb = Number(last.value)
  const fb = Number(first.benchmark), lbb = Number(last.benchmark)
  if (!(fv > 0) || !(fb > 0)) return null
  return (lb / fv) - (lbb / fb)
}

function isErrorLog(l: any): boolean {
  if (l && typeof l === 'object' && l.level) {
    const lv = String(l.level).toUpperCase()
    if (lv === 'ERROR' || lv === 'CRITICAL' || lv === 'EXCEPTION') return true
  }
  const s = typeof l === 'string' ? l : (l && typeof l.message === 'string' ? l.message : '')
  return /error|exception|traceback|错误/i.test(s)
}

function filterErrorLogs(logs: any[]): any[] {
  return logs.filter(isErrorLog)
}

// 从策略代码里提取标的池（context.universe = [...] / set_universe([...])），
// 用于回填后端 symbols 参数以正确预加载日线数据。无显式标的池时返回空数组。
function extractUniverse(code: string): string[] {
  if (!code) return []
  const out = new Set<string>()
  const patterns = [
    /context\.universe\s*=\s*\[([^\]]*)\]/g,
    /set_universe\s*\(\s*\[([^\]]*)\]\s*\)/g,
    /g\.security\s*=\s*\[([^\]]*)\]/g,
  ]
  for (const re of patterns) {
    let m: RegExpExecArray | null
    while ((m = re.exec(code))) {
      m[1].split(',').map(s => s.trim().replace(/['"]/g, '')).filter(Boolean)
        .forEach(s => out.add(s))
    }
  }
  return [...out]
}

function shiftDays(dateStr: string, days: number): string {
  const d = new Date(dateStr)
  d.setDate(d.getDate() + days)
  return d.toISOString().slice(0, 10)
}

function MetricCard({ label, value, tone }: { label: string; value: string; tone: string }) {
  return (
    <div className="rounded-card border border-border bg-surface px-3 py-2">
      <div className="text-[10px] text-muted">{label}</div>
      <div className={`text-sm font-medium num ${tone}`}>{value}</div>
    </div>
  )
}

function TabBtn({ active, onClick, children }: { active: boolean; onClick: () => void; children: ReactNode }) {
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
