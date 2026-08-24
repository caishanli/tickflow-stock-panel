import { useEffect, useMemo, useState } from 'react'
import ReactECharts from 'echarts-for-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { toast } from '@/components/Toast'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, Plus, Play, Square, RotateCcw, Bell, Trash2 } from 'lucide-react'
import * as api from '../api'
import { openSimStream } from '../stream'
import { AccountDialog, type AccountForm } from './AccountDialog'
import { DingtalkConfigDialog } from './DingtalkConfigDialog'
import { QuantTradeDialog } from '@/components/QuantTradeDialog'
import type { IntradayMarker } from '@/components/EChartsIntraday'

/** 读取 CSS 设计令牌变量，echarts 无法直接消费 var()，需解析为实际颜色 */
function cssVar(name: string, fallback: string) {
  if (typeof window === 'undefined') return fallback
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  return v || fallback
}

/** 从 api 抛错（`quant api 400: {"detail":"..."}`）解析 detail 用于 toast */
function errDetail(e: any) {
  const m = String(e?.message ?? e).match(/\{"detail":"(.*)"\}/)
  return m ? m[1] : String(e?.message ?? e)
}

function statusTone(s: string | undefined) {
  if (s === 'running') return 'text-accent'
  if (s === 'failed') return 'text-bear'
  if (s === 'paused') return 'text-warning'
  return 'text-muted'
}

const STATUS_LABEL: Record<string, string> = {
  created: '未启动', running: '运行中', paused: '已暂停', failed: '失败',
}

const FREQ_LABEL: Record<string, string> = { minute: '分钟级', daily: '日频' }

function fmtNum(v: any, digits = 2) {
  return typeof v === 'number' && Number.isFinite(v) ? v.toFixed(digits) : '—'
}

function fmtPct(v: any) {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—'
  return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(2)}%`
}

function fmtStopLoss(v: any) {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—'
  return `${(v * 100).toFixed(2)}%`
}

const localDay = (d = new Date()) =>
  `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`

function fmtPriceTs(ts: unknown, today?: string): string | null {
  if (!ts) return null
  const m = String(ts).match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/)
  if (!m) return null
  const isToday = `${m[1]}-${m[2]}-${m[3]}` === (today ?? localDay())
  return isToday ? `${m[4]}:${m[5]}` : `${m[2]}-${m[3]} ${m[4]}:${m[5]}`
}

/** 紧凑两行时间（表格固定列用）: 首行日期、次行时分；跨年首行补两位年份，title 留完整时间 */
function splitTs(v: unknown): { d: string; t: string; full: string } {
  const full = String(v ?? '')
  const m = full.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})/)
  if (!m) return { d: full.slice(0, 10), t: '', full }
  const [, y, mo, da, hh, mm] = m
  const d = y === String(new Date().getFullYear()) ? `${mo}-${da}` : `${y.slice(2)}-${mo}-${da}`
  return { d, t: `${hh}:${mm}`, full }
}

/** 当天日期（本地时区 YYYY-MM-DD）；跨天自动更新，驱动现价时间 HH:MM / MM-DD HH:MM 格式翻转 */
function useToday(): string {
  const [today, setToday] = useState(() => localDay())
  useEffect(() => {
    const id = setInterval(() => {
      const d = localDay()
      setToday(prev => (prev === d ? prev : d))
    }, 30_000)
    return () => clearInterval(id)
  }, [])
  return today
}

/** 从 "YYYY-MM-DD HH:MM:SS"（可省略秒）提取日期 + HH:MM，供分时标记定位 */
function parseTradeTime(ts: unknown): { date: string; time: string } | null {
  const s = String(ts ?? '')
  const m = s.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2})/)
  return m ? { date: m[1], time: m[2] } : null
}

function toMarkerAction(action: unknown): IntradayMarker['action'] {
  const a = String(action).toUpperCase()
  if (a === 'BUY') return 'BUY'
  if (a === 'STOP_LOSS') return 'STOP_LOSS'
  return 'SELL'
}

/** 该标的全部成交 → 分时标记 (弹窗按选中日期过滤渲染) */
function buildSymbolMarkers(trades: any[], sym: string): IntradayMarker[] {
  if (!sym) return []
  const out: IntradayMarker[] = []
  for (const t of trades) {
    if ((t.code ?? '') !== sym || typeof t.price !== 'number') continue
    const parsed = parseTradeTime(t.ts)
    if (!parsed) continue
    out.push({ date: parsed.date, time: parsed.time, price: t.price, action: toMarkerAction(t.action) })
  }
  return out
}

const RANGES: { label: string; days: number | null }[] = [
  { label: '全部', days: null },
  { label: '一星期', days: 7 },
  { label: '1个月', days: 30 },
  { label: '3个月', days: 90 },
  { label: '6个月', days: 180 },
  { label: '1年', days: 365 },
]

/** 交易日历查找表：tradeDays 外的日期（如实时会话跨日新增）按工作日索引兜底插补 */
function buildDayLookup(trades: any[], tradeDays: string[]): Map<string, number> {
  const idx = new Map<string, number>()
  tradeDays.filter(Boolean).sort().forEach((d: string, i: number) => idx.set(d, i))
  let next = idx.size
  const isWeekday = (d: string) => {
    const t = new Date(`${d}T00:00:00`)
    const w = t.getDay()
    return !Number.isNaN(t.getTime()) && w >= 1 && w <= 5
  }
  for (const t of trades) {
    const d = String(t?.ts ?? '').slice(0, 10)
    if (d && !idx.has(d) && isWeekday(d)) idx.set(d, next++)
  }
  return idx
}

/** 分标的 FIFO 配对，返回每行（按 ts 排序后的下标）持仓交易日数 */
function computeHoldDays(trades: any[], tradeDays: string[]): Map<number, { hold: number | null; open: boolean }> {
  const days = buildDayLookup(trades, tradeDays)
  const out = new Map<number, { hold: number | null; open: boolean }>()
  const acc = new Map<number, { sum: number; qty: number }>() // key: 买入行 sorted 下标
  const lots: Record<string, { buyOrder: number; buyDay: number; amount: number }[]> = {}
  const sorted = [...trades].sort((a, b) => String(a.ts).localeCompare(String(b.ts)))
  for (let oi = 0; oi < sorted.length; oi++) {
    const t = sorted[oi]
    const d = String(t.ts ?? '').slice(0, 10)
    const day = days.get(d) ?? -1
    const amt = Number(t.amount) || 0
    if (String(t.action).toUpperCase() === 'BUY') {
      (lots[String(t.code ?? '')] ??= []).push({ buyOrder: oi, buyDay: day, amount: amt })
      // 持有中：按已持有的交易日数计（截至最新交易日 = 查找表最大下标）
      const now = days.size - 1
      out.set(oi, { hold: day >= 0 ? Math.max(0, now - day) : null, open: true })
    } else {
      const q = lots[String(t.code ?? '')] ?? []
      let remaining = amt
      let sum = 0, qty = 0
      while (remaining > 0 && q.length > 0) {
        const lot = q[0]
        const take = Math.min(remaining, lot.amount)
        if (day >= 0 && lot.buyDay >= 0) {
          const diff = day - lot.buyDay
          sum += diff * take
          qty += take
          const a = acc.get(lot.buyOrder) ?? { sum: 0, qty: 0 }
          a.sum += diff * take
          a.qty += take
          acc.set(lot.buyOrder, a)
        }
        lot.amount -= take
        remaining -= take
        if (lot.amount <= 0) q.shift()
      }
      out.set(oi, { hold: qty > 0 ? Math.round(sum / qty) : null, open: false })
    }
  }
  for (const [oi, a] of acc) {
    if (a.qty > 0) out.set(oi, { hold: Math.round(a.sum / a.qty), open: false })
  }
  return out
}

export function QuantSim() {
  const qc = useQueryClient()
  const [view, setView] = useState<'list' | 'detail'>('list')
  const [sel, setSel] = useState<string | null>(null)
  const [dialog, setDialog] = useState(false)

  const { data: accounts } = useQuery({
    queryKey: ['quant', 'sim', 'accounts'], queryFn: api.listAccounts,
    refetchInterval: view === 'list' ? 5000 : false,
    // 列表页做监控台用：后台标签页/窗口失焦也要继续拉取，回到页面立即刷新，
    // 否则全局 refetchOnWindowFocus=false + 默认 refetchIntervalInBackground=false
    // 会让净值/收益率长期停留在旧值
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
  })
  const { data: strategies } = useQuery({ queryKey: ['quant', 'strategies'], queryFn: api.listStrategies })
  const strategyName = useMemo(() => {
    const m = new Map<string, string>()
    for (const s of strategies ?? []) m.set(s.id, s.name || s.id)
    return (id: string) => m.get(id) || id || '—'
  }, [strategies])

  // 只重取账户列表 + 状态：日志/成交/净值由 SSE 增量推送与断线自愈补拉维护，
  // 全量重取会在启动瞬间拿到空快照并覆盖 SSE 已推送的行（重取响应晚到即冲空表格）
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] })
    if (sel) qc.invalidateQueries({ queryKey: ['quant', 'sim', sel, 'status'] })
  }
  const startMut = useMutation({
    mutationFn: () => api.startAccount(sel!),
    onSuccess: invalidate,
    onError: (e: any) => toast(errDetail(e), 'error'),
  })
  const pauseMut = useMutation({ mutationFn: () => api.pauseAccount(sel!), onSuccess: invalidate })
  const resetMut = useMutation({ mutationFn: () => api.resetAccount(sel!), onSuccess: invalidate })
  const deleteMut = useMutation({
    mutationFn: (id: string) => api.deleteAccount(id),
    onSuccess: () => { setView('list'); setSel(null); invalidate() },
  })
  const createMut = useMutation({
    mutationFn: (b: AccountForm) => api.createAccount(b),
    onSuccess: () => { setDialog(false); invalidate() },
  })

  const openDetail = (id: string) => { setSel(id); setView('detail') }

  return (
    <div className="flex flex-col h-full">
      <PageHeader title="量化模拟盘" subtitle="策略驱动的实时模拟交易" />
      {view === 'list' ? (
        <SimList
          accounts={accounts ?? []}
          strategyName={strategyName}
          onNew={() => setDialog(true)}
          onOpen={openDetail}
          onDelete={(id) => { if (window.confirm('确定删除该模拟账户？此操作不可恢复。')) deleteMut.mutate(id) }}
          onDeleteMany={(ids) => {
            if (!window.confirm(`确定删除选中的 ${ids.length} 个模拟账户？此操作不可恢复。`)) return
            Promise.all(ids.map(id => api.deleteAccount(id)))
              .then(() => invalidate())
              .catch((e: any) => toast(errDetail(e), 'error'))
          }}
        />
      ) : (
        <SimDetail
          aid={sel!}
          strategyName={strategyName}
          onBack={() => setView('list')}
          startMut={startMut}
          pauseMut={pauseMut}
          resetMut={resetMut}
          deleteMut={deleteMut}
        />
      )}
      <AccountDialog open={dialog} onClose={() => setDialog(false)}
        onSave={(b) => createMut.mutate(b)} saving={createMut.isPending} />
    </div>
  )
}

// ---------------------------------------------------------------------------
// 列表视图
// ---------------------------------------------------------------------------

function SimList({ accounts, strategyName, onNew, onOpen, onDelete, onDeleteMany }: {
  accounts: any[]
  strategyName: (id: string) => string
  onNew: () => void
  onOpen: (id: string) => void
  onDelete: (id: string) => void
  onDeleteMany: (ids: string[]) => void
}) {
  const [selectMode, setSelectMode] = useState(false)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const ids = accounts.map(a => a.id)
  const allSelected = ids.length > 0 && ids.every(id => selected.has(id))
  const toggle = (id: string) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }
  const toggleAll = () => {
    setSelected(prev => {
      const next = new Set(prev)
      if (allSelected) ids.forEach(id => next.delete(id))
      else ids.forEach(id => next.add(id))
      return next
    })
  }
  const switchSelectMode = () => {
    setSelectMode(v => {
      if (v) setSelected(new Set())
      return !v
    })
  }
  return (
    <div className="flex-1 overflow-auto p-4 space-y-3">
      <div className="flex items-center gap-2">
        <button onClick={onNew}
          className="inline-flex items-center gap-1.5 px-3 h-9 rounded-lg bg-accent text-white text-xs">
          <Plus size={14} />新建模拟
        </button>
        {accounts.length > 0 && (
          <button onClick={switchSelectMode}
            className={`inline-flex items-center px-3 h-9 rounded-lg border text-xs transition-colors ${selectMode ? 'border-accent/50 text-accent bg-accent/10' : 'border-border text-secondary hover:text-foreground'}`}>
            {selectMode ? '退出管理' : '批量管理'}
          </button>
        )}
        {selected.size > 0 && (
          <button onClick={() => onDeleteMany([...selected])}
            className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors">
            <Trash2 className="h-3.5 w-3.5" />删除选中({selected.size})
          </button>
        )}
      </div>
      {accounts.length === 0 ? (
        <EmptyState title="暂无模拟账户" hint="点击左上角「新建模拟」创建一个" />
      ) : (
        <>
        <div className="hidden md:block rounded-card border border-border bg-surface overflow-hidden">
            <table className="w-full text-xs">
            <thead className="text-muted border-b border-border">
              <tr className="text-left">
                <th className="px-3 py-2.5 font-normal w-10">#</th>
                {selectMode && (
                  <th className="px-3 py-2.5 w-8 text-center">
                    <input type="checkbox" checked={allSelected} onChange={toggleAll}
                      className="accent-accent cursor-pointer align-middle" />
                  </th>
                )}
                <th className="px-3 py-2.5 font-normal">编号</th>
                <th className="px-3 py-2.5 font-normal">名称</th>
                <th className="px-3 py-2.5 font-normal">策略</th>
                <th className="px-3 py-2.5 font-normal">开始日期</th>
                <th className="px-3 py-2.5 font-normal">频率</th>
                <th className="px-3 py-2.5 font-normal">状态</th>
                <th className="px-3 py-2.5 font-normal text-right">净值</th>
                <th className="px-3 py-2.5 font-normal text-right">当日收益</th>
                <th className="px-3 py-2.5 font-normal text-right">收益率</th>
                <th className="px-3 py-2.5 font-normal text-right w-16"></th>
              </tr>
            </thead>
            <tbody className="text-foreground">
              {accounts.map((a: any, i: number) => {
                const ret = typeof a.net_value === 'number' && a.capital
                  ? a.net_value / a.capital - 1 : null
                return (
                  <tr key={a.id}
                    className="border-t border-border/60 hover:bg-elevated/60">
                    <td className="px-3 py-2.5 text-muted">{i + 1}</td>
                    {selectMode && (
                      <td className="px-3 py-2.5 text-center">
                        <input type="checkbox" checked={selected.has(a.id)} onChange={() => toggle(a.id)}
                          className="accent-accent cursor-pointer align-middle" />
                      </td>
                    )}
                    <td className="px-3 py-2.5 text-muted font-mono" onClick={(e) => {
                      const target = e.target as HTMLElement
                      if (target.closest('.copy-id')) return
                      onOpen(a.id)
                    }}>
                      <span className="copy-id inline-flex items-center gap-1 cursor-pointer hover:text-accent transition-colors"
                        onClick={(e) => {
                          e.stopPropagation()
                          const ta = document.createElement('textarea')
                          ta.value = a.id
                          ta.style.position = 'fixed'
                          ta.style.left = '-9999px'
                          document.body.appendChild(ta)
                          ta.select()
                          try { document.execCommand('copy'); toast('已复制', 'success', 'top') }
                          catch { toast('复制失败', 'error') }
                          document.body.removeChild(ta)
                        }}>
                        {a.id}
                      </span>
                    </td>
                    <td className="px-3 py-2.5 cursor-pointer" onClick={() => onOpen(a.id)}>{a.name}</td>
                    <td className="px-3 py-2.5 text-muted cursor-pointer" onClick={() => onOpen(a.id)}>{strategyName(a.strategy_id)}</td>
                    <td className="px-3 py-2.5 text-muted cursor-pointer" onClick={() => onOpen(a.id)}>{a.start_date || '—'}</td>
                    <td className="px-3 py-2.5 text-muted cursor-pointer" onClick={() => onOpen(a.id)}>{FREQ_LABEL[a.frequency] ?? a.frequency ?? '分钟级'}</td>
                    <td className={`px-3 py-2.5 cursor-pointer ${statusTone(a.status)}`} onClick={() => onOpen(a.id)}>
                      {STATUS_LABEL[a.status] ?? a.status}
                    </td>
                    <td className="px-3 py-2.5 text-right num cursor-pointer" onClick={() => onOpen(a.id)}>{fmtNum(a.net_value)}</td>
                    <td className={`px-3 py-2.5 text-right num cursor-pointer ${a.day_pnl == null ? 'text-muted' : a.day_pnl >= 0 ? 'text-bull' : 'text-bear'}`} onClick={() => onOpen(a.id)}>
                      {a.day_pnl != null ? fmtNum(a.day_pnl) : '—'}
                      {a.day_pnl_pct != null && <div className="text-[10px] opacity-80">{fmtPct(a.day_pnl_pct)}</div>}
                    </td>
                    <td className={`px-3 py-2.5 text-right num cursor-pointer ${ret == null ? '' : ret >= 0 ? 'text-bull' : 'text-bear'}`} onClick={() => onOpen(a.id)}>
                      {fmtPct(ret)}
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      <button onClick={(e) => { e.stopPropagation(); if (window.confirm(`确定删除「${a.name}」？`)) onDelete(a.id) }}
                        className="p-1 rounded hover:bg-bear/10 text-muted hover:text-bear transition-colors"
                        title="删除">
                        <Trash2 size={13} />
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
            </table>
        </div>

        {/* 移动端卡片布局：名称+编号在上，净值/收益率横向排列 */}
        <div className="md:hidden space-y-2">
          {accounts.map((a: any) => {
            const ret = typeof a.net_value === 'number' && a.capital
              ? a.net_value / a.capital - 1 : null
            return (
              <div key={a.id} onClick={() => onOpen(a.id)}
                className="rounded-card border border-border bg-surface p-3 cursor-pointer transition-colors hover:bg-elevated/60">
                <div className="flex items-center gap-2 min-w-0">
                  {selectMode && (
                    <input type="checkbox" checked={selected.has(a.id)} onChange={() => toggle(a.id)}
                      onClick={e => e.stopPropagation()}
                      className="accent-accent cursor-pointer align-middle shrink-0" />
                  )}
                  <div className="flex items-baseline gap-1.5 flex-1 min-w-0">
                    <span className="min-w-0 truncate text-sm font-medium text-foreground">{a.name}</span>
                    <span className="copy-id shrink-0 font-mono text-[11px] text-muted cursor-pointer hover:text-accent transition-colors"
                      onClick={(e) => {
                        e.stopPropagation()
                        const ta = document.createElement('textarea')
                        ta.value = a.id
                        ta.style.position = 'fixed'
                        ta.style.left = '-9999px'
                        document.body.appendChild(ta)
                        ta.select()
                        try { document.execCommand('copy'); toast('已复制', 'success', 'top') }
                        catch { toast('复制失败', 'error') }
                        document.body.removeChild(ta)
                      }}>
                      {a.id}
                    </span>
                  </div>
                  <button onClick={(e) => { e.stopPropagation(); if (window.confirm(`确定删除「${a.name}」？`)) onDelete(a.id) }}
                    className="ml-2 p-1 rounded hover:bg-bear/10 text-muted hover:text-bear transition-colors shrink-0"
                    title="删除">
                    <Trash2 size={13} />
                  </button>
                </div>
                <div className="mt-1 text-[11px] text-muted flex flex-wrap gap-x-2 gap-y-0.5">
                  <span>{strategyName(a.strategy_id)}</span>
                  <span>{FREQ_LABEL[a.frequency] ?? a.frequency ?? '分钟级'}</span>
                  {a.start_date && <span>{a.start_date}</span>}
                  <span className={statusTone(a.status)}>{STATUS_LABEL[a.status] ?? a.status}</span>
                </div>
                 <div className="mt-2 grid grid-cols-2 gap-2">
                  <div className="flex items-baseline gap-1.5">
                    <span className="text-[10px] text-muted shrink-0">净值</span>
                    <span className="text-xs num font-medium">{fmtNum(a.net_value)}</span>
                  </div>
                  <div className="flex items-baseline gap-1.5">
                    <span className="text-[10px] text-muted shrink-0">收益率</span>
                    <span className={`text-xs num font-medium ${ret == null ? '' : ret >= 0 ? 'text-bull' : 'text-bear'}`}>{fmtPct(ret)}</span>
                  </div>
                  <div className="col-start-2 flex items-baseline gap-1.5">
                    <span className="text-[10px] text-muted shrink-0">当日</span>
                    <span className={`text-xs num font-medium ${a.day_pnl == null ? 'text-muted' : a.day_pnl >= 0 ? 'text-bull' : 'text-bear'}`}>{a.day_pnl != null ? fmtNum(a.day_pnl) : '—'}</span>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
        </>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// 详情视图
// ---------------------------------------------------------------------------

function SimDetail({ aid, strategyName, onBack, startMut, pauseMut, resetMut, deleteMut }: {
  aid: string
  strategyName: (id: string) => string
  onBack: () => void
  startMut: any
  pauseMut: any
  resetMut: any
  deleteMut: any
}) {
  const qc = useQueryClient()
  const [tab, setTab] = useState<'trades' | 'stoploss' | 'logs' | 'alerts'>('trades')
  const [preview, setPreview] = useState<{ symbol: string; name: string; date?: string; markers: IntradayMarker[] } | null>(null)
  const [showDingtalkCfg, setShowDingtalkCfg] = useState(false)
  const [rangeDays, setRangeDays] = useState<number | null>(null)
  // 止损日志：首拉由 status 的 stop_loss 初始化，盘中新止损由 onTrade(STOP_LOSS) 实时追加
  const [stoplossRows, setStoplossRows] = useState<any[]>([])
  const today = useToday()
  // 首拉全量历史；运行期增量由 SSE 推送（见下方 openSimStream），不再定时轮询。
  // 兜底：SSE 断线/后台标签页挂起后数据可能长期缺失，10s 定时重取 + 窗口聚焦重取自愈
  const { data: st } = useQuery({
    queryKey: ['quant', 'sim', aid, 'status'], queryFn: () => api.getSimStatus(aid),
    refetchInterval: 10_000, refetchIntervalInBackground: true, refetchOnWindowFocus: true,
  })
  const { data: eq } = useQuery({
    queryKey: ['quant', 'sim', aid, 'equity'], queryFn: () => api.getSimEquity(aid),
    refetchInterval: 10_000, refetchIntervalInBackground: true, refetchOnWindowFocus: true,
  })
  const { data: tr } = useQuery({
    queryKey: ['quant', 'sim', aid, 'trades'], queryFn: () => api.getSimTrades(aid),
    refetchInterval: 10_000, refetchIntervalInBackground: true, refetchOnWindowFocus: true,
  })
  const { data: logs } = useQuery({
    queryKey: ['quant', 'sim', aid, 'logs'], queryFn: () => api.getSimLogs(aid),
    refetchInterval: 10_000, refetchIntervalInBackground: true, refetchOnWindowFocus: true,
  })
  const { data: nameSource } = useQuery({
    queryKey: ['quant', 'sim', 'name-source'], queryFn: () => api.getSimNameSource(),
  })
  const toggleNameSource = useMutation({
    mutationFn: (src: 'jq' | 'tdx') => api.setSimNameSource(src),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['quant', 'sim', 'name-source'] }) },
  })

  const appendTo = (key: any[], row: any, sig?: (r: any) => string) => {
    qc.setQueryData(key, (prev: any[]) => {
      const arr = Array.isArray(prev) ? prev : []
      if (sig && arr.some((r) => sig(r) === sig(row))) return arr
      return [...arr, row]
    })
  }

  useEffect(() => {
    if (!aid) return
    let hadError = false
    const es = openSimStream(aid, {
      onStatus: (s) => qc.setQueryData(['quant', 'sim', aid, 'status'], (prev: any) => ({
        ...(prev || {}), account: { ...(prev?.account || {}), status: s.status }, state: s.state,
      })),
      onEquity: (e) => appendTo(['quant', 'sim', aid, 'equity'], e, (r) => String(r.dt)),
      onTrade: (t) => {
        appendTo(['quant', 'sim', aid, 'trades'], t,
          (r) => `${r.ts}|${r.code}|${r.action}|${r.price}`)
        if (String(t.action) === 'STOP_LOSS') {
          setStoplossRows((prev) => {
            const sig = `${t.ts}|${t.code}|${t.price}`
            if (prev.some((r) => `${r.ts}|${r.code}|${r.price}` === sig)) return prev
            return [...prev, t]
          })
        }
      },
      onLog: (l) => appendTo(['quant', 'sim', aid, 'logs'], l,
        (r) => `${r.ts}|${r.level}|${r.message}`),
    })
    // SSE 断线重连后按最新 rowid 续推，断线期间写入的日志/成交/净值不会被推送，
    // 必须补拉全量自愈，否则缺失段永久保留（直到手动刷新）
    es.addEventListener('error', () => { hadError = true })
    es.addEventListener('open', () => {
      if (hadError) {
        hadError = false
        qc.invalidateQueries({ queryKey: ['quant', 'sim', aid, 'logs'] })
        qc.invalidateQueries({ queryKey: ['quant', 'sim', aid, 'trades'] })
        qc.invalidateQueries({ queryKey: ['quant', 'sim', aid, 'equity'] })
      }
    })
    return () => { es.close() }
  }, [aid, qc])

  const acct = st?.account ?? {}
  const state = st?.state ?? {}
  const positions: Record<string, any> = state?.positions ?? {}
  const posEntries = Object.entries(positions)
  const positionsValue = posEntries.reduce(
    (s, [, p]) => s + (Number(p.amount) || 0) * (Number(p.price) || 0), 0)
  // 按天聚合：每天取最后一个点的净值，日线级别展示（供曲线与指标卡共用）；
  // 同时保留每天第一个点，作为窗口/全程的起点基准
  const { daily, firstOfDay } = useMemo(() => {
    const raw: any[] = Array.isArray(eq) ? eq : []
    const dayFirst = new Map<string, any>()
    const dayLast = new Map<string, any>()
    for (const d of raw) {
      const day = String(d.dt ?? '').slice(0, 10)
      if (!day) continue
      if (!dayFirst.has(day)) dayFirst.set(day, d)
      dayLast.set(day, d)
    }
    return { daily: Array.from(dayLast.values()), firstOfDay: dayFirst }
  }, [eq])
  // 窗口切分：日历天过滤，不足 2 点回退全部；窗口首日改用当天第一个点，
  // 否则首日盘中收益（如开户当天 100000→100551）会被窗口基准吞掉
  const windowed = useMemo(() => {
    let w: any[]
    if (rangeDays == null) {
      w = [...daily]
    } else {
      const t = new Date()
      const cutoff = new Date(t.getFullYear(), t.getMonth(), t.getDate() - rangeDays)
      const m = String(cutoff.getMonth() + 1).padStart(2, '0')
      const d = String(cutoff.getDate()).padStart(2, '0')
      const cutoffStr = `${cutoff.getFullYear()}-${m}-${d}`
      const filtered = daily.filter((x) => String(x.dt ?? '').slice(0, 10) >= cutoffStr)
      w = filtered.length >= 2 ? filtered : [...daily]
    }
    if (w.length > 0) {
      const first = firstOfDay.get(String(w[0].dt ?? '').slice(0, 10))
      if (first && String(first.dt) !== String(w[0].dt)) w = [first, ...w]
    }
    return w
  }, [daily, firstOfDay, rangeDays])
  // 收益基准：账户初始资金（缺失时兜底首日净值）；两者皆无（如重置后无状态）→ null
  const baseNV = useMemo(
    () => Number(st?.state?.start_cash ?? st?.start_cash) ||
      (daily.length > 0 ? Number(daily[0].net_value) : null),
    [st, daily],
  )
  const winFirst = windowed.length > 0 ? windowed[0] : null
  const winLast = windowed.length > 0 ? windowed[windowed.length - 1] : null
  // 总收益率与区间收益率
  const totalRet = baseNV ? (typeof state?.net_value === 'number' && Number.isFinite(state.net_value)
    ? state.net_value / baseNV - 1 : null) : null
  const winRet = (rangeDays != null && winFirst != null && winLast != null && Number(winFirst.net_value) > 0)
    ? Number(winLast.net_value) / Number(winFirst.net_value) - 1
    : null
  const winPnl = (rangeDays != null && winFirst != null && winLast != null)
    ? Number(winLast.net_value) - Number(winFirst.net_value)
    : null
  const displayRet = winRet != null ? winRet : totalRet
  const displayPnl = winPnl != null ? winPnl : state?.pnl

  const curve = useMemo(() => {
    const accent = '#3b82f6'
    const benchColor = '#f59e0b'
    const data: any[] = windowed
    // 策略收益率(%)：相对初始资金累计（首日即反映当天盈亏）
    const stratPct = data.map((d) => Number((((Number(d.net_value ?? 0) / (baseNV ?? 1)) - 1) * 100).toFixed(2)))
    const benchPct = data.map((d) => Number(d.benchmark_pct ?? 0))
    // 直接显示实际累计收益：策略相对初始资金，基准用后端原始累计值（不归一到 0）
    const stratWin = stratPct
    const benchWin = benchPct
    // 当日涨跌幅(%)：从累计收益率反推，(1+r_n)/(1+r_{n-1})-1
    const stratDaily = stratPct.map((v, i) =>
      i === 0 ? 0 : Number((((1 + v / 100) / (1 + stratPct[i - 1] / 100) - 1) * 100).toFixed(2)))
    const benchDaily = benchPct.map((v, i) =>
      i === 0 ? 0 : Number((((1 + v / 100) / (1 + benchPct[i - 1] / 100) - 1) * 100).toFixed(2)))
    const xLabels = data.map((d) => String(d.dt ?? '').slice(0, 10))
    return {
      animation: false,
      grid: { left: 64, right: 16, top: 30, bottom: 46 },
      legend: {
        data: ['策略收益(累计)', '沪深300(累计)'],
        textStyle: { color: cssVar('--muted', '#94a3b8'), fontSize: 11 },
        top: 4, right: 8,
      },
      tooltip: {
        trigger: 'axis',
        backgroundColor: cssVar('--surface', '#1e293b'),
        borderColor: cssVar('--border', '#334155'),
        textStyle: { color: cssVar('--foreground', '#e2e8f0'), fontSize: 12 },
        formatter: (params: any[]) => {
          if (!params || params.length === 0) return ''
          const idx = params[0].dataIndex
          const day = xLabels[idx] ?? ''
          const sCum = stratWin[idx] ?? 0
          const bCum = benchWin[idx] ?? 0
          const sDay = stratDaily[idx] ?? 0
          const bDay = benchDaily[idx] ?? 0
          const fmt = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
          const color = (v: number) => v >= 0 ? '#ef4444' : '#22c55e'
          return `<div style="font-size:11px;margin-bottom:4px;opacity:0.7">${day}</div>` +
            `<div style="display:grid;grid-template-columns:auto auto auto;gap:2px 12px;font-size:12px">` +
            `<span style="color:${accent}">策略</span>` +
            `<span style="color:${color(sCum)}">${fmt(sCum)}</span>` +
            `<span style="color:${color(sDay)};opacity:0.6">${fmt(sDay)}</span>` +
            `<span style="color:${benchColor}">沪深300</span>` +
            `<span style="color:${color(bCum)}">${fmt(bCum)}</span>` +
            `<span style="color:${color(bDay)};opacity:0.6">${fmt(bDay)}</span>` +
            `</div>` +
            `<div style="font-size:10px;margin-top:4px;opacity:0.4">累计 / 当日</div>`
        },
      },
      xAxis: {
        type: 'category',
        data: xLabels,
        axisLabel: { color: cssVar('--muted', '#94a3b8'), fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: cssVar('--border', '#334155') } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        scale: true,
        axisLabel: { color: cssVar('--muted', '#94a3b8'), fontSize: 10, formatter: '{value}%' },
        splitLine: { lineStyle: { color: cssVar('--border', '#334155') } },
      },
      dataZoom: [
        { type: 'inside' },
        { type: 'slider', height: 14, bottom: 6, borderColor: cssVar('--border', '#334155'), textStyle: { color: cssVar('--muted', '#94a3b8'), fontSize: 10 } },
      ],
      series: [
        {
          name: '策略收益(累计)',
          type: 'line',
          data: stratWin,
          symbol: 'none',
          lineStyle: { color: accent, width: 2 },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [{ offset: 0, color: accent + '26' }, { offset: 1, color: accent + '03' }],
            },
          },
        },
        {
          name: '沪深300(累计)',
          type: 'line',
          data: benchWin,
          symbol: 'none',
          lineStyle: { color: benchColor, width: 1.5, type: 'dashed' },
        },
      ],
    } as any
  }, [windowed, baseNV])

  const tradeList: any[] = Array.isArray(tr) ? tr : []

  const tradeDays: string[] = Array.isArray(st?.trade_days) ? st.trade_days : []
  // 成交记录持仓时长：按 ts 排序下标重算，SSE 追加卖出后买入行自动回填
  const sortedTrades = useMemo(() => [...tradeList].sort((a: any, b: any) => String(a.ts).localeCompare(String(b.ts))), [tradeList])
  const holdMap = useMemo(() => computeHoldDays(sortedTrades, tradeDays), [sortedTrades, tradeDays])
  const holdOf = (origIdx: number) => holdMap.get(origIdx)
  const stopLossList: any[] = Array.isArray(st?.stop_loss) ? st.stop_loss : []
  const logList: any[] = Array.isArray(logs) ? logs : []
  const alertList: any[] = logList.filter((l: any) => l.level === 'warn' || l.level === 'error')

  // 止损日志初始化：status 的 stop_loss 是历史全量，与 onTrade 增量去重合并
  useEffect(() => {
    if (stopLossList.length === 0) return
    setStoplossRows((prev) => {
      const seen = new Set(prev.map((r) => `${r.ts}|${r.code}|${r.price}`))
      const missing = stopLossList.filter((r) => !seen.has(`${r.ts}|${r.code}|${r.price}`))
      if (missing.length === 0) return prev
      return [...prev, ...missing]
    })
  }, [stopLossList])

  return (
    <div className="flex-1 overflow-auto p-4 space-y-4">
      {/* 顶栏：返回 + 账户信息 + 控制 */}
      <div className="flex items-center gap-3 flex-wrap rounded-card border border-border bg-surface px-4 py-2.5">
        <button onClick={onBack}
          className="inline-flex items-center gap-1 px-2.5 h-9 rounded-lg bg-elevated text-foreground text-xs">
          <ArrowLeft size={14} />返回列表
        </button>
        <span className="text-sm font-medium text-foreground">{acct.name ?? '—'}</span>
        <span className="text-xs text-muted font-mono cursor-pointer hover:text-accent transition-colors"
          title="点击复制账户ID"
          onClick={() => {
            const ta = document.createElement('textarea')
            ta.value = aid
            ta.style.position = 'fixed'
            ta.style.left = '-9999px'
            document.body.appendChild(ta)
            ta.select()
            try { document.execCommand('copy'); toast('账户ID已复制', 'success', 'top') }
            catch { toast('复制失败', 'error') }
            document.body.removeChild(ta)
          }}>{aid}</span>
        <span className={`text-xs ${statusTone(acct.status)}`}>
          {STATUS_LABEL[acct.status] ?? acct.status ?? '—'}
        </span>
        <span className="text-xs text-muted">
          初始资金 {fmtNum(acct.capital)} · 止损 {fmtStopLoss(acct.stop_loss)} · 策略 {strategyName(acct.strategy_id)} · {FREQ_LABEL[acct.frequency] ?? '分钟级'}
          {acct.start_date ? ` · 自 ${acct.start_date}` : ''}
        </span>
        <button onClick={() => toggleNameSource.mutate(nameSource?.source === 'jq' ? 'tdx' : 'jq')}
          className="inline-flex items-center gap-1 px-2.5 h-9 rounded-lg bg-elevated text-foreground text-xs">
           标的名称源：{nameSource?.source === 'tdx' ? '通达信' : '聚宽'}
        </button>
        <div className="ml-auto flex gap-2">
          <button onClick={() => setShowDingtalkCfg(true)}
            className={`inline-flex items-center gap-1 px-3 h-9 rounded-lg text-xs ${acct?.dingtalk_enabled ? 'bg-accent text-white' : 'bg-elevated text-foreground'}`}>
            <Bell size={13} />钉钉{acct?.dingtalk_enabled ? '已开启' : '推送'}
          </button>
          <button onClick={() => api.toggleDingtalk(aid, !acct?.dingtalk_enabled).then(() => qc.invalidateQueries({ queryKey: ['quant', 'sim'] }))}
            className="inline-flex items-center gap-1 px-2 h-9 rounded-lg bg-elevated text-foreground text-xs">
            {acct?.dingtalk_enabled ? '关闭' : '开启'}
          </button>
          <button
            onClick={() => (acct.status === 'running' ? pauseMut.mutate() : startMut.mutate())}
            disabled={startMut.isPending || pauseMut.isPending}
            className="inline-flex items-center gap-1 px-3 h-9 rounded-lg bg-accent text-white text-xs disabled:opacity-50">
            {acct.status === 'running' ? <><Square size={13} />暂停</> : <><Play size={13} />启动</>}
          </button>
          <button onClick={() => {
            if (!window.confirm('确定重置该模拟账户？将清空当前持仓与状态，重新开始。')) return
            // 乐观清空：本地止损行与交易/日志/净值缓存（重置后明细由 SSE 增量重建）
            setStoplossRows([])
            qc.setQueryData(['quant', 'sim', aid, 'trades'], [])
            qc.setQueryData(['quant', 'sim', aid, 'logs'], [])
            qc.setQueryData(['quant', 'sim', aid, 'equity'], [])
            resetMut.mutate()
          }} disabled={resetMut.isPending}
            className="inline-flex items-center gap-1 px-3 h-9 rounded-lg bg-elevated text-foreground text-xs disabled:opacity-50">
            <RotateCcw size={13} />重置
          </button>
          <button onClick={() => { if (window.confirm('确定删除该模拟账户？此操作不可恢复。')) deleteMut.mutate(aid) }}
            disabled={deleteMut.isPending}
            className="inline-flex items-center gap-1 px-3 h-9 rounded-lg bg-bear/10 text-bear text-xs disabled:opacity-50 hover:bg-bear/20">
            <Trash2 size={13} />删除
          </button>
        </div>
      </div>

      {/* 指标卡 */}
      <div className="grid grid-cols-2 sm:grid-cols-5 gap-2">
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">净值</div>
          <div className="text-sm font-medium text-foreground num">{fmtNum(state?.net_value)}</div>
        </div>
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">现金</div>
          <div className="text-sm font-medium text-foreground num">{fmtNum(state?.cash)}</div>
        </div>
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">持仓市值</div>
          <div className="text-sm font-medium text-foreground num">{fmtNum(positionsValue)}</div>
        </div>
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">盈亏</div>
          <div className={`text-sm font-medium num ${typeof displayPnl === 'number' && displayPnl < 0 ? 'text-bear' : 'text-bull'}`}>
            {fmtNum(displayPnl)}
          </div>
        </div>
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">收益率</div>
          <div className={`text-sm font-medium num ${displayRet == null ? '' : displayRet >= 0 ? 'text-bull' : 'text-bear'}`}>
            {fmtPct(displayRet)}
          </div>
        </div>
      </div>

      {/* 净值曲线 */}
      <div className="rounded-card border border-border bg-surface">
        <div className="px-4 pt-3 flex items-center justify-between">
          <span className="text-xs text-foreground font-medium">净值曲线</span>
          <div className="flex gap-1 pr-2">
            {RANGES.map((r) => (
              <button key={r.label} onClick={() => setRangeDays(r.days)}
                className={`px-2.5 h-6 rounded-btn text-[11px] ${rangeDays === r.days ? 'bg-accent text-white' : 'text-muted hover:text-foreground'}`}>
                {r.label}
              </button>
            ))}
          </div>
        </div>
        {Array.isArray(eq) && eq.length > 0 ? (
          <ReactECharts option={curve} style={{ height: 300 }} notMerge />
        ) : (
          <div className="h-[300px] grid place-items-center text-xs text-muted">
            暂无净值数据（启动后开始累计）
          </div>
        )}
      </div>

      {/* 持仓 */}
      <div className="rounded-card border border-border bg-surface overflow-hidden">
        <div className="px-4 pt-3 pb-2 text-xs text-foreground font-medium">持仓 ({posEntries.length})</div>
        {posEntries.length > 0 ? (
          <div className="overflow-auto max-h-60">
            <table className="w-full text-xs">
                <thead className="text-muted">
                  <tr className="text-left">
                    <th className="sticky top-0 z-20 bg-surface max-md:left-0 w-20 min-w-[5rem] leading-tight px-3 py-1.5 font-normal">买入时间</th>
                    <th className="sticky top-0 z-20 bg-surface max-md:left-20 px-3 py-1.5 font-normal max-md:border-r max-md:border-border">名称</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">代码</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">数量</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">成本</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">现价</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">市值</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">盈亏</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">收益率</th>
                  </tr>
                </thead>
                <tbody className="text-foreground">
                  {posEntries.map(([sym, p]: any) => {
                    const value = (Number(p.amount) || 0) * (Number(p.price) || 0)
                    const zeroPos = Number(p.amount) === 0
                    const pnlPct = zeroPos
                      ? (typeof p.realized_pnl_pct === 'number' ? p.realized_pnl_pct : null)
                      : (Number(p.avg_cost) > 0 ? Number(p.price) / Number(p.avg_cost) - 1 : null)
                    const pnlAmt = zeroPos
                      ? (typeof p.realized_pnl === 'number' ? p.realized_pnl : null)
                      : (pnlPct != null ? pnlPct * Number(p.amount) * Number(p.avg_cost) : null)
                    const tsLabel = fmtPriceTs(p.price_ts, today)
                    const entryTs = splitTs(p.entry_ts)
                    return (
                      <tr key={sym}
                        onClick={() => {
                          setPreview({
                            symbol: sym,
                            name: p.name ?? '',
                            date: today,
                            markers: buildSymbolMarkers(sortedTrades, sym),
                          })
                        }}
                        className="group border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
                        <td className="max-md:sticky max-md:left-0 max-md:z-10 max-md:bg-surface w-20 min-w-[5rem] leading-tight max-md:group-hover:bg-elevated px-3 py-1.5" title={entryTs.full}>
                          <div className="num">{p.entry_ts ? entryTs.d : '—'}</div>
                          {p.entry_ts && entryTs.t && <div className="num">{entryTs.t}</div>}
                        </td>
                        <td className="max-md:sticky max-md:left-20 max-md:z-10 max-md:bg-surface max-md:group-hover:bg-elevated px-3 py-1.5 max-md:border-r max-md:border-border">{p.name ?? ''}</td>
                        <td className="px-3 py-1.5 text-muted">{sym}</td>
                        <td className="px-3 py-1.5 num">{p.amount}</td>
                        <td className="px-3 py-1.5 num">{fmtNum(p.avg_cost, 3)}</td>
                        <td className="px-3 py-1.5 num">
                          {fmtNum(p.price, 3)}
                          {tsLabel && (
                            <span className="ml-1 text-[10px] text-muted font-normal">
                              ({tsLabel})
                            </span>
                          )}
                        </td>
                        <td className="px-3 py-1.5 num">{fmtNum(value)}</td>
                        <td className={`px-3 py-1.5 num ${pnlAmt == null ? '' : pnlAmt >= 0 ? 'text-bull' : 'text-bear'}`}>
                          {pnlAmt == null ? '—' : fmtNum(pnlAmt)}
                        </td>
                        <td className={`px-3 py-1.5 num ${pnlPct == null ? '' : pnlPct >= 0 ? 'text-bull' : 'text-bear'}`}>
                          {fmtPct(pnlPct)}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
            </table>
          </div>
        ) : (
          <div className="px-4 pb-4 text-xs text-muted">暂无持仓</div>
        )}
      </div>

      {/* Tab：成交记录 / 止损日志 / 运行日志 */}
      <div className="rounded-card border border-border bg-surface overflow-hidden">
        <div className="flex gap-1 px-3 pt-3 pb-2 border-b border-border/60">
          {([['trades', `成交记录 (${tradeList.length})`],
             ['stoploss', `止损日志 (${stoplossRows.length})`],
             ['logs', `运行日志 (${logList.length})`],
             ['alerts', `异常 (${alertList.length})`]] as const).map(([k, label]) => (
            <button key={k} onClick={() => setTab(k)}
              className={`px-3 h-8 rounded-btn text-xs ${tab === k ? 'bg-accent text-white' : 'text-muted hover:text-foreground'}`}>
              {label}
            </button>
          ))}
        </div>
        {tab === 'trades' && (
          tradeList.length > 0 ? (
            <div className="overflow-auto max-h-64">
              <table className="w-full text-xs">
                <thead className="text-muted">
                  <tr className="text-left">
                    <th className="sticky top-0 z-20 bg-surface max-md:left-0 w-20 min-w-[5rem] leading-tight px-3 py-1.5 font-normal">时间</th>
                    <th className="sticky top-0 z-20 bg-surface max-md:left-20 px-3 py-1.5 font-normal max-md:border-r max-md:border-border">名称</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">代码</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">持仓时长</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">方向</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">价格</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">数量</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">金额</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">手续费</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">盈亏</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">收益率</th>
                  </tr>
                </thead>
                <tbody className="text-foreground">
                  {[...sortedTrades].reverse().map((t: any, i: number) => {
                    const h = holdOf(sortedTrades.length - 1 - i)
                    return (
                    <tr key={i}
                      onClick={() => {
                        setPreview({
                          symbol: t.code ?? '',
                          name: t.name ?? '',
                          date: String(t.ts ?? '').slice(0, 10),
                          markers: buildSymbolMarkers(sortedTrades, t.code ?? ''),
                        })
                      }}
                      className="group border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
                      <td className="max-md:sticky max-md:left-0 max-md:z-10 max-md:bg-surface w-20 min-w-[5rem] leading-tight max-md:group-hover:bg-elevated px-3 py-1.5" title={splitTs(t.ts).full}>
                        <div className="num">{splitTs(t.ts).d}</div>
                        {splitTs(t.ts).t && <div className="num">{splitTs(t.ts).t}</div>}
                      </td>
                      <td className="max-md:sticky max-md:left-20 max-md:z-10 max-md:bg-surface max-md:group-hover:bg-elevated px-3 py-1.5 max-md:border-r max-md:border-border">{t.name ?? ''}</td>
                      <td className="px-3 py-1.5 text-muted">{t.code ?? ''}</td>
                      {(() => {
                        const holdDays = t.action === 'BUY'
                          ? (h?.open
                              ? (h?.hold == null ? null : h.hold === 0 ? '<1天' : `${h.hold}个交易日`)
                              : null)
                          : h?.hold == null ? null : h.hold === 0 ? '<1天' : `${h.hold}个交易日`
                        const holding = t.action === 'BUY' && !!h?.open
                        if (holdDays == null && !holding) {
                          return <td className="px-3 py-1.5"><span className="text-muted">—</span></td>
                        }
                        return (
                          <td className="px-3 py-1.5">
                            {holdDays}
                            {holding && (
                              <span className="text-muted">{holdDays ? '（持仓中）' : '持仓中'}</span>
                            )}
                          </td>
                        )
                      })()}
                      <td className={`px-3 py-1.5 ${t.action === 'BUY' ? 'text-bull' : 'text-bear'}`}>
                        {t.action === 'BUY' ? '买入' : t.action === 'STOP_LOSS' ? '止损' : '卖出'}
                      </td>
                      <td className="px-3 py-1.5 num">{fmtNum(t.price, 3)}</td>
                      <td className="px-3 py-1.5 num">{t.amount}</td>
                      <td className="px-3 py-1.5 num">{fmtNum(Number(t.price) * Number(t.amount))}</td>
                      <td className="px-3 py-1.5 num">{fmtNum(t.commission, 2)}</td>
                      <td className={`px-3 py-1.5 num ${typeof t.pnl === 'number' && t.pnl !== 0 ? (t.pnl >= 0 ? 'text-bull' : 'text-bear') : 'text-muted'}`}>
                        {typeof t.pnl === 'number' && t.pnl !== 0 ? fmtNum(t.pnl) : '—'}
                      </td>
                      <td className={`px-3 py-1.5 num ${t.action !== 'BUY' && typeof t.pnl_pct === 'number' ? (t.pnl_pct >= 0 ? 'text-bull' : 'text-bear') : 'text-muted'}`}>
                        {t.action === 'BUY' ? '—' : fmtPct(t.pnl_pct)}
                      </td>
                    </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          ) : <div className="px-4 py-4 text-xs text-muted">暂无成交</div>
        )}
        {tab === 'stoploss' && (
          stoplossRows.length > 0 ? (
            <div className="overflow-auto max-h-64">
              <table className="w-full text-xs">
                <thead className="text-muted">
                  <tr className="text-left">
                    <th className="sticky top-0 z-20 bg-surface max-md:left-0 w-20 min-w-[5rem] leading-tight px-3 py-1.5 font-normal">时间</th>
                    <th className="sticky top-0 z-20 bg-surface max-md:left-20 px-3 py-1.5 font-normal max-md:border-r max-md:border-border">名称</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">代码</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">方向</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">价格</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">数量</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">手续费</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">盈亏</th>
                    <th className="sticky top-0 z-20 bg-surface px-3 py-1.5 font-normal">收益率</th>
                  </tr>
                </thead>
                <tbody className="text-foreground">
                  {[...stoplossRows].reverse().map((t: any, i: number) => (
                    <tr key={i} className="group border-t border-border/60 hover:bg-elevated/60 transition-colors">
                      <td className="max-md:sticky max-md:left-0 max-md:z-10 max-md:bg-surface w-20 min-w-[5rem] leading-tight max-md:group-hover:bg-elevated px-3 py-1.5" title={splitTs(t.ts).full}>
                        <div className="num">{splitTs(t.ts).d}</div>
                        {splitTs(t.ts).t && <div className="num">{splitTs(t.ts).t}</div>}
                      </td>
                      <td className="max-md:sticky max-md:left-20 max-md:z-10 max-md:bg-surface max-md:group-hover:bg-elevated px-3 py-1.5 max-md:border-r max-md:border-border">{t.name ?? ''}</td>
                      <td className="px-3 py-1.5 text-muted">{t.code ?? ''}</td>
                      <td className="px-3 py-1.5 text-bear">止损</td>
                      <td className="px-3 py-1.5 num">{fmtNum(t.price, 3)}</td>
                      <td className="px-3 py-1.5 num">{t.amount}</td>
                      <td className="px-3 py-1.5 num">{fmtNum(t.commission, 2)}</td>
                      <td className={`px-3 py-1.5 num ${typeof t.pnl === 'number' && t.pnl !== 0 ? 'text-bear' : 'text-muted'}`}>
                        {typeof t.pnl === 'number' && t.pnl !== 0 ? fmtNum(t.pnl) : '—'}
                      </td>
                      <td className={`px-3 py-1.5 num ${typeof t.pnl_pct === 'number' ? 'text-bear' : 'text-muted'}`}>
                        {fmtPct(t.pnl_pct)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : <div className="px-4 py-4 text-xs text-muted">暂无止损</div>
        )}
        {tab === 'logs' && (
          <div className="max-h-64 overflow-auto p-3 space-y-0.5 text-[11px] text-muted font-mono">
            {logList.length > 0 ? [...logList].reverse().map((l: any, i: number) => (
              <div key={i} className={l.level === 'error' ? 'text-bear' : l.level === 'warn' ? 'text-warning' : ''}>
                {`${l.ts ?? ''} [${l.level ?? 'info'}] ${l.message ?? ''}`}
              </div>
            )) : <div className="text-muted">暂无日志</div>}
          </div>
        )}
        {tab === 'alerts' && (
          <div className="max-h-64 overflow-auto p-3 space-y-0.5 text-[11px] text-muted font-mono">
            {alertList.length > 0 ? [...alertList].reverse().map((l: any, i: number) => (
              <div key={i} className={l.level === 'error' ? 'text-bear' : 'text-warning'}>
                {`${l.ts ?? ''} [${l.level ?? 'warn'}] ${l.message ?? ''}`}
              </div>
            )) : <div className="text-muted">暂无异常</div>}
          </div>
        )}
      </div>
      {preview && (
        <QuantTradeDialog
          symbol={preview.symbol}
          name={preview.name}
          initialView="minute"
          initialDate={preview.date}
          intradayMarkers={preview.markers}
          onClose={() => setPreview(null)}
        />
      )}
      {showDingtalkCfg && <DingtalkConfigDialog onClose={() => setShowDingtalkCfg(false)} />}
    </div>
  )
}
