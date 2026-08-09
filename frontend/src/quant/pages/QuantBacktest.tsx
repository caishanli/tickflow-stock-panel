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
import { useBacktestLogs } from '../useBacktestLogs'

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

// 回测运行时间（created_at）格式化为 MM-DD HH:mm
function fmtDateTime(s?: string | null): string {
  if (!s) return ''
  const t = s.replace('T', ' ').slice(0, 16)
  // 取 月-日 时:分
  const m = t.match(/(\d{4})-(\d{2})-(\d{2})[ ](\d{2}):(\d{2})/)
  if (m) return `${m[2]}-${m[3]} ${m[4]}:${m[5]}`
  return t
}

interface FormState {
  start: string
  end: string
  frequency: string
  fee: number
  slippage: number
  capital: number
}

// 默认值对齐独立脚本 scripts/run_jq_rqalpha.py（fee/slippage 均为 0.0001）。
// 注意：rqalpha 的 slippage 是每笔比例，10× 的默认值(0.001)会让高频换仓策略
// 的成交从 127→81 笔、收益由 +51% 翻转为负，故此处必须与离线回测口径一致。
const DEFAULT_FORM: FormState = {
  start: '', end: '', frequency: 'daily',
  fee: 0.0001, slippage: 0.0001, capital: 100000,
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
  const qc = useQueryClient()
  const { data } = useQuery({ queryKey: ['quant', 'strategies', 'latest'], queryFn: () => api.listStrategiesWithLatest() })
  const list = (data ?? []) as any[]
  const [page, setPage] = useState(1)
  const [delIds, setDelIds] = useState<string[] | null>(null)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const PAGE_SIZE = 15

  const totalPages = Math.max(1, Math.ceil(list.length / PAGE_SIZE))
  const safePage = Math.min(page, totalPages)
  const pageItems = list.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE)
  const pageIds = pageItems.map(s => s.id)
  const allPageSelected = pageIds.length > 0 && pageIds.every(id => selected.has(id))

  const toggle = (id: string) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }
  const togglePage = () => {
    setSelected(prev => {
      const next = new Set(prev)
      if (allPageSelected) pageIds.forEach(id => next.delete(id))
      else pageIds.forEach(id => next.add(id))
      return next
    })
  }

  const delMut = useMutation({
    mutationFn: () => Promise.all((delIds as string[]).map(id => api.deleteStrategy(id))),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] })
      setDelIds(null)
      setSelected(new Set())
    },
  })

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="量化回测"
        subtitle="策略 · RQAlpha · 聚宽式 · 实时 SSE"
        right={
          <div className="flex items-center gap-2">
            {selected.size > 0 && (
              <button onClick={() => setDelIds([...selected])}
                className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors">
                <Trash2 className="h-3.5 w-3.5" />删除选中({selected.size})
              </button>
            )}
            <button onClick={onNew}
              className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-accent text-base text-xs font-medium hover:bg-accent/90 transition-colors">
              <Plus className="h-4 w-4" />新建
            </button>
          </div>
        }
      />
      <div className="flex-1 p-4 overflow-auto">
        <div className="rounded-card border border-border bg-surface overflow-hidden">
          <table className="w-full text-xs">
            <thead className="text-muted bg-elevated/40">
              <tr className="text-left">
                <th className="px-3 py-2 font-normal w-10 text-center">#</th>
                <th className="px-3 py-2 w-8 text-center">
                  <input type="checkbox" checked={allPageSelected} onChange={togglePage}
                    className="accent-accent cursor-pointer align-middle" />
                </th>
                <th className="px-3 py-2 font-normal">策略名称</th>
                <th className="px-3 py-2 font-normal">最新回测周期</th>
                <th className="px-3 py-2 font-normal text-right">收益率</th>
                <th className="px-3 py-2 font-normal text-right">最大回撤</th>
                <th className="px-3 py-2 font-normal text-right">夏普比率</th>
                <th className="px-3 py-2 font-normal text-right">回测次数</th>
                <th className="px-3 py-2 font-normal text-right">操作</th>
              </tr>
            </thead>
            <tbody className="text-foreground">
              {pageItems.length === 0 && (
                <tr><td colSpan={9} className="px-3 py-10 text-center text-muted">暂无策略，点击右上角新建</td></tr>
              )}
              {pageItems.map((s, i) => {
                const m = pickMetrics(s.latest?.metrics_json)
                const period = s.latest ? `${s.latest.start ?? ''} ~ ${s.latest.end ?? ''}` : '—'
                const idx = (safePage - 1) * PAGE_SIZE + i + 1
                const checked = selected.has(s.id)
                return (
                  <tr key={s.id} onClick={() => onOpen(s.id)}
                    className={`border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors ${checked ? 'bg-accent/5' : ''}`}>
                    <td className="px-3 py-2 text-center text-muted num">{idx}</td>
                    <td className="px-3 py-2 text-center" onClick={e => e.stopPropagation()}>
                      <input type="checkbox" checked={checked} onChange={() => toggle(s.id)}
                        className="accent-accent cursor-pointer align-middle" />
                    </td>
                    <td className="px-3 py-2 font-medium">{s.name}</td>
                    <td className="px-3 py-2 text-muted num">{period}</td>
                    <td className={`px-3 py-2 text-right num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.max_drawdown ? -m.max_drawdown : null)}`}>{m.max_drawdown == null ? '—' : fmtPct(-m.max_drawdown)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.sharpe)}`}>{fmtNum(m.sharpe)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(null)}`}>{s.run_count}</td>
                    <td className="px-3 py-2 text-right" onClick={e => e.stopPropagation()}>
                      <button onClick={() => setDelIds([s.id])}
                        className="inline-flex items-center gap-1 text-bear hover:underline text-xs">
                        <Trash2 className="h-3.5 w-3.5" />删除
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        {totalPages > 1 && (
          <div className="flex items-center justify-between mt-3 text-xs text-muted">
            <span>共 {list.length} 条 · 第 {safePage}/{totalPages} 页</span>
            <div className="flex items-center gap-1">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={safePage <= 1}
                className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors">上一页</button>
              <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={safePage >= totalPages}
                className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors">下一页</button>
            </div>
          </div>
        )}
      </div>

      {delIds && (
        <Modal onClose={() => setDelIds(null)} ariaLabel="确认删除策略">
          <div className="p-5 space-y-4">
            <h3 className="text-sm font-medium text-foreground">删除策略</h3>
            <p className="text-xs text-muted">确定删除选中的 {delIds.length} 个策略及其全部回测记录？此操作不可恢复。</p>
            <div className="flex justify-end gap-2">
              <button onClick={() => setDelIds(null)} className="px-3 py-1.5 rounded-btn bg-elevated text-secondary text-xs hover:text-foreground transition-colors">取消</button>
              <button onClick={() => delMut.mutate()} disabled={delMut.isPending}
                className="px-3 py-1.5 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors disabled:opacity-50">删除</button>
            </div>
          </div>
        </Modal>
      )}
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
  const [selMode, setSelMode] = useState(false)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [confirmBatchDel, setConfirmBatchDel] = useState(false)
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
    if (strategy && !seeded.current) {
      seeded.current = true
      setName(strategy.name || '')
      setCode(strategy.code || '')
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
    mutationFn: async (short: boolean) => {
      // 子进程优先读"已保存"的策略代码（run_quant_backtest.py 里 strategy_id
      // 对应的库内代码优先于 payload.strategy_code）：先落库再提交，避免跑到旧代码
      await saveStrategy()
      const end = form.end.trim()
      // 用户显式填了开始日期就尊重它；只有「编译运行」且开始日期为空时，
      // 才退化为 end-7 的快速区间（不再覆盖用户已设的 start，避免静默改期）。
      const start = form.start.trim() || (short && end ? shiftDays(end, -7) : '')
      const payload: any = {
        name, strategy_id: strategyId, strategy_code: code,
        symbols: extractUniverse(code),
        frequency: form.frequency,
        fee: +form.fee, slippage: +form.slippage, capital: +form.capital,
      }
      if (start) payload.start = start
      if (end) payload.end = end
      return api.runBacktest(payload)
    },
    onSuccess: (d: any) => { setLiveRunId(d.run_id); setSelRunId(d.run_id); qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] }) },
    onError: (e: any) => toast(e?.message ? `回测提交失败: ${e.message}` : '回测提交失败', 'error'),
  })

  const runId = selRunId ?? liveRunId
  // SSE 负责运行期增量推送（见下方 openBacktestStream），这里只做挂载/切换
  // run_id 时的首拉（历史快照），不再定时轮询，避免与 SSE 重复请求。
  const { data: status } = useQuery({ queryKey: ['quant', 'bt', runId, 'status'], queryFn: () => api.getBacktestStatus(runId as string), enabled: !!runId })
  const { data: equity } = useQuery({ queryKey: ['quant', 'bt', runId, 'equity'], queryFn: () => api.getBacktestEquity(runId as string), enabled: !!runId })
  const { data: trades } = useQuery({ queryKey: ['quant', 'bt', runId, 'trades'], queryFn: () => api.getBacktestTrades(runId as string), enabled: !!runId })
  // 日志增量加载：先拉尾部，向上滚动加载更早，运行期 SSE 新日志从底部追加
  const { logs, hasMore, total, loadingMore, loadEarlier, appendLog } =
    useBacktestLogs(runId as string | null)
  // 运行列表不做定时轮询：发起回测时已 invalidate 刷新；运行结束（终态）时由
  // 下方 SSE onStatus 补刷一次，拿到最终状态。空闲时零请求。
  const { data: runs } = useQuery({
    queryKey: ['quant', 'bt', 'runs', strategyId],
    queryFn: () => api.listBacktests(strategyId),
  })

  // SSE 增量直接追加到缓存，避免每次事件都整表重新拉取（否则运行中日志
  // 洪流会触发数百次 /logs /equity 轮询）。首拉历史由上面的 useQuery 完成。
  const appendTo = (key: any[], row: any, sig?: (r: any) => string) => {
    qc.setQueryData(key, (prev: any[]) => {
      const arr = Array.isArray(prev) ? prev : []
      if (sig && arr.some((r) => sig(r) === sig(row))) return arr
      return [...arr, row]
    })
  }

  const lastStatus = status ? (status.state ?? status.status) : undefined
  // 回测进行中（排队/运行中）禁用两个运行按钮，防止重复发起/并发覆盖
  const isRunning = lastStatus === 'queued' || lastStatus === 'running'

  useEffect(() => {
    if (!runId || !liveOn) return
    if (lastStatus === 'done' || lastStatus === 'failed') {
      // 终态不开 SSE（服务端终态推完即关流，EventSource 会把正常关流当错误
      // 无限自动重连）；改为整表收尾拉一次，同时补齐 SSE 建连间隙可能漏掉的行。
      qc.invalidateQueries({ queryKey: ['quant', 'bt', runId] })
      return
    }
    const es = openBacktestStream(runId, {
      onEquity: (e) => appendTo(['quant', 'bt', runId, 'equity'], e, (r) => String(r.dt)),
      onTrade: (t) => appendTo(['quant', 'bt', runId, 'trades'], t,
        (r) => `${r.ts}|${r.code}|${r.action}|${r.price}`),
      onLog: (l) => appendLog(l),
      onStatus: (s) => {
        qc.setQueryData(['quant', 'bt', runId, 'status'], (prev: any) => ({
          ...(prev || {}), status: s.status, metrics_json: s.metrics ?? (prev || {}).metrics_json,
        }))
        // 终态（完成/失败/取消）时补刷一次运行列表，拿到最终状态与指标
        if (s.status === 'done' || s.status === 'failed' || s.status === 'cancelled') {
          qc.invalidateQueries({ queryKey: ['quant', 'bt', 'runs', strategyId] })
        }
      },
    })
    return () => { es.close() }
  }, [runId, liveOn, qc, lastStatus])

  const metrics = pickMetrics(status?.metrics_json)
  const equityData: any[] = Array.isArray(equity) ? equity : []
  const runList: any[] = Array.isArray(runs) ? runs : []

  // 日期框初始化（只填一次，且不清空用户已输入值）：
  // 1. 有回测记录 → 用最近一次回测的周期（与列表页"最新回测周期"同口径，
  //    runList 按 created_at 倒序，[0] 即最新一次）；
  // 2. 无回测记录（新建策略）→ 默认最近一个月。
  const dateSeeded = useRef(false)
  useEffect(() => {
    if (dateSeeded.current || runs === undefined) return
    dateSeeded.current = true
    let start = '', end = ''
    if (runList.length > 0) {
      try {
        const p = JSON.parse(runList[0].params_json || '{}')
        start = p.start || ''
        end = p.end || ''
      } catch { /* params_json 损坏时静默跳过 */ }
    }
    if (!start && !end) {
      end = new Date().toISOString().slice(0, 10)
      start = shiftDays(end, -30)
    }
    setForm(f => ({ ...f, start: f.start || start, end: f.end || end }))
  }, [runs, runList])

  // 从列表进入编辑器时自动选中最近一次回测，直接展示收益/曲线/日志，
  // 不用手动点"历史"。仅首次执行一次，不覆盖用户之后的手动选择/新提交的运行。
  const runAutoSel = useRef(false)
  useEffect(() => {
    if (runAutoSel.current || runList.length === 0) return
    runAutoSel.current = true
    if (selRunId == null && liveRunId == null) setSelRunId(runList[0].id)
  }, [runList, selRunId, liveRunId])

  const excess = computeExcess(equityData)
  const errLogs = filterErrorLogs(Array.isArray(logs) ? logs : [])
  const hasError = errLogs.length > 0
  // 运行期 metrics 要结束时才产出：用最新净值行实时估算收益率（初值=初始资金），
  // 让用户在回测过程中就能看到实时收益，而不是干等到 done。
  const lastEq = equityData.length > 0 ? equityData[equityData.length - 1] : null
  const liveTotalReturn = metrics.total_return == null && lastEq && +form.capital > 0
    ? Number(lastEq.value) / +form.capital - 1
    : null
  const shownReturn = metrics.total_return ?? liveTotalReturn

  const editorBoxRef = useRef<HTMLDivElement>(null)
  const logScrollRef = useRef<HTMLDivElement>(null)
  const [editorH, setEditorH] = useState(320)
  useEffect(() => {
    const el = editorBoxRef.current
    if (!el) return
    const ro = new ResizeObserver(() => { setEditorH(el.clientHeight) })
    ro.observe(el)
    setEditorH(el.clientHeight)
    return () => ro.disconnect()
  }, [])

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
        <button onClick={() => runMut.mutate(true)} disabled={!code || runMut.isPending || isRunning}
          className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn border border-border text-xs text-secondary hover:text-foreground transition-colors disabled:opacity-50">
          <Play className="h-3.5 w-3.5" />编译运行
        </button>
        <button onClick={() => runMut.mutate(false)} disabled={!code || !form.start || !form.end || runMut.isPending || isRunning}
          className="inline-flex items-center gap-1.5 h-9 px-4 rounded-btn bg-accent text-base text-xs font-medium disabled:opacity-50 hover:bg-accent/90 transition-colors">
          <Play className="h-4 w-4" />{runMut.isPending ? '提交中…' : isRunning ? '运行中…' : '开始回测'}
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
              <div className="flex items-center justify-between px-1 pb-2 mb-1 border-b border-border">
                <span className="text-xs text-muted">
                  {selMode ? `已选 ${selected.size} / ${runList.length}` : `共 ${runList.length} 条`}
                </span>
                <button onClick={() => { setSelMode(s => !s); setSelected(new Set()) }}
                  className={`px-2 py-0.5 rounded-btn text-xs border border-border transition-colors ${selMode ? 'text-accent border-accent/50' : 'text-muted hover:text-foreground'}`}>
                  {selMode ? '取消多选' : '多选'}
                </button>
              </div>
              {runList.length === 0 && <div className="px-2 py-4 text-xs text-muted text-center">暂无回测</div>}
              {runList.map((r) => {
                const p = (() => { try { return JSON.parse(r.params_json || '{}') } catch { return {} } })()
                const m = pickMetrics(r.metrics_json)
                const active = (selRunId ?? liveRunId) === r.id
                const checked = selected.has(r.id)
                if (selMode) {
                  return (
                    <button key={r.id}
                      onClick={() => setSelected(prev => {
                        const n = new Set(prev); n.has(r.id) ? n.delete(r.id) : n.add(r.id); return n
                      })}
                      className={`w-full text-left px-2 py-1.5 rounded-btn text-xs flex items-center gap-2 ${checked ? 'bg-elevated/70 text-foreground' : 'text-secondary hover:bg-elevated/60'}`}>
                      <span className={`inline-flex w-3.5 h-3.5 items-center justify-center rounded border ${checked ? 'bg-accent border-accent text-base' : 'border-border'}`}>
                        {checked && '✓'}
                      </span>
                      <span className="num flex-1 flex flex-col leading-tight min-w-0">
                        <span className="flex items-center gap-1.5">
                          <span className="truncate">{`${p.start ?? ''} ~ ${p.end ?? ''}`}</span>
                          {active && <span className="shrink-0 px-1 rounded bg-accent/20 text-accent text-[10px] font-medium">当前</span>}
                        </span>
                        <span className="text-[10px] text-muted">{fmtDateTime(r.created_at)}</span>
                      </span>
                      <span className={`num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</span>
                    </button>
                  )
                }
                return (
                  <button key={r.id} onClick={() => { setSelRunId(r.id); setHistOpen(false) }}
                    className={`w-full text-left px-2 py-1.5 rounded-btn text-xs flex items-center justify-between gap-2 ${active ? 'bg-elevated text-foreground' : 'text-secondary hover:text-foreground hover:bg-elevated/60'}`}>
                    <span className="flex flex-col leading-tight min-w-0">
                      <span className="num flex items-center gap-1.5">
                        {`${p.start ?? ''} ~ ${p.end ?? ''}`}
                        {active && <span className="px-1 rounded bg-accent/20 text-accent text-[10px] font-medium">当前</span>}
                      </span>
                      <span className="num text-[10px] text-muted">{fmtDateTime(r.created_at)}</span>
                    </span>
                    <span className={`num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</span>
                  </button>
                )
              })}
              {selMode && runList.length > 0 && (
                <div className="flex items-center gap-2 mt-2 pt-2 border-t border-border">
                  <button onClick={() => setSelected(new Set(runList.map(r => r.id)))}
                    className="px-2 py-1 rounded-btn text-xs text-muted hover:text-foreground">全选</button>
                  <button onClick={() => setSelected(new Set())}
                    className="px-2 py-1 rounded-btn text-xs text-muted hover:text-foreground">清空</button>
                  <button disabled={selected.size === 0}
                    onClick={() => setConfirmBatchDel(true)}
                    className="ml-auto inline-flex items-center gap-1 px-2.5 py-1 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 disabled:opacity-40 transition-colors">
                    <Trash2 className="h-3.5 w-3.5" />删除选中({selected.size})
                  </button>
                </div>
              )}
              {!selMode && selRunId && (
                <button onClick={() => { setSelRunId(null); setHistOpen(false) }} className="w-full mt-1 px-2 py-1.5 rounded-btn text-xs text-muted hover:text-foreground border-t border-border">回到当前回测</button>
              )}
            </div>
          )}
        </div>
      </header>

      <div ref={bodyRef} className="flex-1 min-h-0 flex overflow-hidden">
        <div className="p-3 flex flex-col overflow-hidden" style={{ width: `${leftPct}%` }}>
          <div className={`${SECTION_TITLE} mb-2`}><FileCode2 className="h-3.5 w-3.5" />策略代码 (Python)</div>
          <div ref={editorBoxRef} className="relative flex-1 min-h-0 rounded-card border border-border overflow-hidden">
            <CodeEditor value={code} onChange={setCode} height={`${editorH}px`} />
          </div>
        </div>

        <div
          onMouseDown={startDrag}
          className="w-1 shrink-0 cursor-col-resize bg-border hover:bg-accent/60 transition-colors"
          title="拖动调整宽度"
        />

        <div className="flex-1 min-w-0 border-l border-border flex flex-col overflow-hidden">
          <div className="p-3 grid grid-cols-4 gap-2 shrink-0">
            <MetricCard label="收益率" value={fmtPct(shownReturn)} tone={tone(shownReturn)} />
            <MetricCard label="年化" value={fmtPct(metrics.annualized)} tone={tone(metrics.annualized)} />
            <MetricCard label="夏普" value={fmtNum(metrics.sharpe)} tone={tone(metrics.sharpe)} />
            <MetricCard label="最大回撤" value={metrics.max_drawdown == null ? '—' : fmtPct(-metrics.max_drawdown)} tone={tone(metrics.max_drawdown ? -metrics.max_drawdown : null)} />
            <MetricCard label="超额收益" value={excess == null ? '—' : fmtPct(excess)} tone={tone(excess)} />
            <MetricCard label="胜率" value={fmtPct(metrics.win_rate)} tone={tone(metrics.win_rate)} />
            <MetricCard label="盈亏比" value={fmtNum(metrics.profit_loss_ratio)} tone={tone(metrics.profit_loss_ratio)} />
            <MetricCard label="交易次数" value={metrics.trade_count == null ? (runId ? String(Array.isArray(trades) ? trades.length : 0) : '—') : String(metrics.trade_count)} tone="text-foreground" />
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
            <div
              ref={logScrollRef}
              onScroll={(e) => {
                if (e.currentTarget.scrollTop < 40 && hasMore && !loadingMore) loadEarlier()
              }}
              className="flex-1 overflow-auto p-3"
            >
              {logTab === 'log' && (
                <LazyLogList
                  logs={logs}
                  loadingMore={loadingMore}
                  hasMore={hasMore}
                  loaded={logs.length}
                  total={total}
                />
              )}
              {logTab === 'error' && <LogList logs={errLogs} />}
              {logTab === 'trade' && <TradeTable trades={Array.isArray(trades) ? trades : []} />}
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

      {confirmBatchDel && (
        <Modal onClose={() => setConfirmBatchDel(false)} ariaLabel="确认批量删除回测">
          <div className="p-5 space-y-4">
            <h3 className="text-sm font-medium text-foreground">批量删除回测</h3>
            <p className="text-xs text-muted">确定删除选中的 <span className="font-mono">{selected.size}</span> 条回测及其全部数据（日志/净值/成交）？此操作不可恢复。</p>
            <div className="flex justify-end gap-2">
              <button onClick={() => setConfirmBatchDel(false)} className="px-3 py-1.5 rounded-btn bg-elevated text-secondary text-xs hover:text-foreground transition-colors">取消</button>
              <button onClick={() => {
                const ids = Array.from(selected)
                api.batchDeleteBacktests(ids).then(() => {
                  qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] })
                  qc.invalidateQueries({ queryKey: ['quant', 'bt', 'runs', strategyId] })
                  setConfirmBatchDel(false); setSelMode(false); setSelected(new Set())
                  // 若当前正在查看的 run 被删，回到当前回测
                  if ((selRunId ?? liveRunId) && ids.includes(selRunId ?? liveRunId as string)) {
                    setSelRunId(null); setLiveRunId(null)
                  }
                })
              }}
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
  const explicit = [
    /context\.universe\s*=\s*\[([\s\S]*?)\]/g,
    /set_universe\s*\(\s*\[([\s\S]*?)\]\s*\)/g,
    /g\.security\s*=\s*\[([\s\S]*?)\]/g,
  ]
  for (const re of explicit) {
    let m: RegExpExecArray | null
    while ((m = re.exec(code))) {
      m[1].split(',').map(s => s.trim().replace(/['"]/g, '')).filter(Boolean)
        .forEach(s => out.add(s))
    }
  }
  // 兜底：策略常用自定义股票池变量（如 g.global_etf_pool = [...]），
  // 直接抓取代码中所有形如 123456.XSHG / 123456.XSHE 的标的代码
  for (const m of code.matchAll(/['"](\d{6}\.(?:XSHG|XSHE))['"]/g)) {
    out.add(m[1])
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
    <div className="space-y-0.5 text-[11px] text-muted font-mono">
      {logs.map((l, i) => (
        <div key={i}>
          {typeof l === 'string' ? l : `${l.ts ?? ''} [${l.level ?? 'info'}] ${l.message ?? JSON.stringify(l)}`}
        </div>
      ))}
    </div>
  )
}

function LazyLogList({ logs, loadingMore, hasMore, loaded, total }: {
  logs: any[]; loadingMore: boolean; hasMore: boolean; loaded: number; total: number | null
}) {
  return (
    <div>
      <div className="sticky top-[-12px] -mt-3 mb-1 flex items-center gap-2 text-[10px] text-muted">
        {loadingMore ? '加载更早日志…' : hasMore ? '向上滚动加载更早日志' : '已加载全部日志'}
        {total != null && <span className="ml-auto">已加载 {loaded} / {total}</span>}
      </div>
      <LogList logs={logs} />
    </div>
  )
}

function TradeTable({ trades }: { trades: any[] }) {
  if (trades.length === 0) return <div className="text-xs text-muted">暂无成交</div>
  return (
    <div className="overflow-auto">
      <table className="w-full text-xs">
        <thead className="text-muted sticky top-0 bg-surface">
          <tr className="text-left">
            <th className="px-2 py-1.5 font-normal">时间</th>
            <th className="px-2 py-1.5 font-normal">标的</th>
            <th className="px-2 py-1.5 font-normal">方向</th>
            <th className="px-2 py-1.5 font-normal text-right">价格</th>
            <th className="px-2 py-1.5 font-normal text-right">数量</th>
            <th className="px-2 py-1.5 font-normal text-right">手续费</th>
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
              <td className="px-2 py-1.5 text-right num">{fmtNum(t.commission, 2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
