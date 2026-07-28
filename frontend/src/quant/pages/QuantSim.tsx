import { useEffect, useMemo, useState } from 'react'
import ReactECharts from 'echarts-for-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, Plus, Play, Square, RotateCcw, Bell, Trash2 } from 'lucide-react'
import * as api from '../api'
import { openSimStream } from '../stream'
import { AccountDialog, type AccountForm } from './AccountDialog'
import { DingtalkConfigDialog } from './DingtalkConfigDialog'

/** 读取 CSS 设计令牌变量，echarts 无法直接消费 var()，需解析为实际颜色 */
function cssVar(name: string, fallback: string) {
  if (typeof window === 'undefined') return fallback
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  return v || fallback
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

export function QuantSim() {
  const qc = useQueryClient()
  const [view, setView] = useState<'list' | 'detail'>('list')
  const [sel, setSel] = useState<string | null>(null)
  const [dialog, setDialog] = useState(false)

  const { data: accounts } = useQuery({
    queryKey: ['quant', 'sim', 'accounts'], queryFn: api.listAccounts,
    refetchInterval: view === 'list' ? 5000 : false,
  })
  const { data: strategies } = useQuery({ queryKey: ['quant', 'strategies'], queryFn: api.listStrategies })
  const strategyName = useMemo(() => {
    const m = new Map<string, string>()
    for (const s of strategies ?? []) m.set(s.id, s.name || s.id)
    return (id: string) => m.get(id) || id || '—'
  }, [strategies])

  const invalidate = () => qc.invalidateQueries({ queryKey: ['quant', 'sim'] })
  const startMut = useMutation({ mutationFn: () => api.startAccount(sel!), onSuccess: invalidate })
  const pauseMut = useMutation({ mutationFn: () => api.pauseAccount(sel!), onSuccess: invalidate })
  const resetMut = useMutation({ mutationFn: () => api.resetAccount(sel!), onSuccess: invalidate })
  const deleteMut = useMutation({
    mutationFn: () => api.deleteAccount(sel!),
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

function SimList({ accounts, strategyName, onNew, onOpen }: {
  accounts: any[]
  strategyName: (id: string) => string
  onNew: () => void
  onOpen: (id: string) => void
}) {
  return (
    <div className="flex-1 overflow-auto p-4 space-y-3">
      <div className="flex items-center">
        <button onClick={onNew}
          className="inline-flex items-center gap-1.5 px-3 h-9 rounded-lg bg-accent text-white text-xs">
          <Plus size={14} />新建模拟
        </button>
      </div>
      {accounts.length === 0 ? (
        <EmptyState title="暂无模拟账户" hint="点击左上角「新建模拟」创建一个" />
      ) : (
        <div className="rounded-card border border-border bg-surface overflow-hidden">
          <table className="w-full text-xs">
            <thead className="text-muted border-b border-border">
              <tr className="text-left">
                <th className="px-3 py-2.5 font-normal w-10">#</th>
                <th className="px-3 py-2.5 font-normal">编号</th>
                <th className="px-3 py-2.5 font-normal">名称</th>
                <th className="px-3 py-2.5 font-normal">策略</th>
                <th className="px-3 py-2.5 font-normal">开始日期</th>
                <th className="px-3 py-2.5 font-normal">频率</th>
                <th className="px-3 py-2.5 font-normal">状态</th>
                <th className="px-3 py-2.5 font-normal text-right">净值</th>
                <th className="px-3 py-2.5 font-normal text-right">收益率</th>
              </tr>
            </thead>
            <tbody className="text-foreground">
              {accounts.map((a: any, i: number) => {
                const ret = typeof a.net_value === 'number' && a.capital
                  ? a.net_value / a.capital - 1 : null
                return (
                  <tr key={a.id} onClick={() => onOpen(a.id)}
                    className="border-t border-border/60 hover:bg-elevated/60 cursor-pointer">
                    <td className="px-3 py-2.5 text-muted">{i + 1}</td>
                    <td className="px-3 py-2.5 text-muted font-mono">{a.id}</td>
                    <td className="px-3 py-2.5">{a.name}</td>
                    <td className="px-3 py-2.5 text-muted">{strategyName(a.strategy_id)}</td>
                    <td className="px-3 py-2.5 text-muted">{a.start_date || '—'}</td>
                    <td className="px-3 py-2.5 text-muted">{FREQ_LABEL[a.frequency] ?? a.frequency ?? '分钟级'}</td>
                    <td className={`px-3 py-2.5 ${statusTone(a.status)}`}>
                      {STATUS_LABEL[a.status] ?? a.status}
                    </td>
                    <td className="px-3 py-2.5 text-right num">{fmtNum(a.net_value)}</td>
                    <td className={`px-3 py-2.5 text-right num ${ret == null ? '' : ret >= 0 ? 'text-bull' : 'text-bear'}`}>
                      {fmtPct(ret)}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
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
  const [tab, setTab] = useState<'trades' | 'stoploss' | 'logs'>('trades')
  const [showDingtalkCfg, setShowDingtalkCfg] = useState(false)
  // 首拉全量历史；运行期增量由 SSE 推送（见下方 openSimStream），不再定时轮询。
  const { data: st } = useQuery({
    queryKey: ['quant', 'sim', aid, 'status'], queryFn: () => api.getSimStatus(aid),
  })
  const { data: eq } = useQuery({
    queryKey: ['quant', 'sim', aid, 'equity'], queryFn: () => api.getSimEquity(aid),
  })
  const { data: tr } = useQuery({
    queryKey: ['quant', 'sim', aid, 'trades'], queryFn: () => api.getSimTrades(aid),
  })
  const { data: logs } = useQuery({
    queryKey: ['quant', 'sim', aid, 'logs'], queryFn: () => api.getSimLogs(aid),
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
    const es = openSimStream(aid, {
      onStatus: (s) => qc.setQueryData(['quant', 'sim', aid, 'status'], (prev: any) => ({
        ...(prev || {}), account: { ...(prev?.account || {}), status: s.status }, state: s.state,
      })),
      onEquity: (e) => appendTo(['quant', 'sim', aid, 'equity'], e, (r) => String(r.dt)),
      onTrade: (t) => appendTo(['quant', 'sim', aid, 'trades'], t,
        (r) => `${r.ts}|${r.code}|${r.action}|${r.price}`),
      onLog: (l) => appendTo(['quant', 'sim', aid, 'logs'], l,
        (r) => `${r.ts}|${r.level}|${r.message}`),
    })
    return () => { es.close() }
  }, [aid, qc])

  const acct = st?.account ?? {}
  const state = st?.state ?? {}
  const positions: Record<string, any> = state?.positions ?? {}
  const posEntries = Object.entries(positions)
  const positionsValue = posEntries.reduce(
    (s, [, p]) => s + (Number(p.amount) || 0) * (Number(p.price) || 0), 0)
  const ret = typeof state?.net_value === 'number' && state?.start_cash
    ? state.net_value / state.start_cash - 1 : null

  const curve = useMemo(() => {
    const accent = '#3b82f6'
    const benchColor = '#f59e0b'
    const raw: any[] = Array.isArray(eq) ? eq : []
    // 按天聚合：每天取最后一个点的净值，日线级别展示
    const dayMap = new Map<string, any>()
    for (const d of raw) {
      const day = String(d.dt ?? '').slice(0, 10)
      if (day) dayMap.set(day, d)
    }
    const data = Array.from(dayMap.values())
    // 策略收益率(%)：以初始资金为基准（首日即反映当天盈亏）
    const baseNV = Number(st?.state?.start_cash ?? st?.start_cash ?? (data.length > 0 ? data[0].net_value : 0)) || 1
    const stratPct = data.map((d) => Number((((Number(d.net_value ?? 0) / baseNV) - 1) * 100).toFixed(2)))
    const benchPct = data.map((d) => Number(d.benchmark_pct ?? 0))
    // 当日涨跌幅(%)：从累计收益率反推，(1+r_n)/(1+r_{n-1})-1
    const stratDaily = stratPct.map((v, i) =>
      i === 0 ? 0 : Number((((1 + v / 100) / (1 + stratPct[i - 1] / 100) - 1) * 100).toFixed(2)))
    const benchDaily = benchPct.map((v, i) =>
      i === 0 ? 0 : Number((((1 + v / 100) / (1 + benchPct[i - 1] / 100) - 1) * 100).toFixed(2)))
    const xLabels = data.map((d) => String(d.dt ?? '').slice(0, 10))
    return {
      animation: false,
      grid: { left: 64, right: 16, top: 30, bottom: 32 },
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
          const sCum = stratPct[idx] ?? 0
          const bCum = benchPct[idx] ?? 0
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
          data: stratPct,
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
          data: benchPct,
          symbol: 'none',
          lineStyle: { color: benchColor, width: 1.5, type: 'dashed' },
        },
      ],
    } as any
  }, [eq, st])

  const tradeList: any[] = Array.isArray(tr) ? tr : []
  const stopLossList: any[] = Array.isArray(st?.stop_loss) ? st.stop_loss : []
  const logList: any[] = Array.isArray(logs) ? logs : []

  return (
    <div className="flex-1 overflow-auto p-4 space-y-4">
      {/* 顶栏：返回 + 账户信息 + 控制 */}
      <div className="flex items-center gap-3 flex-wrap rounded-card border border-border bg-surface px-4 py-2.5">
        <button onClick={onBack}
          className="inline-flex items-center gap-1 px-2.5 h-9 rounded-lg bg-elevated text-foreground text-xs">
          <ArrowLeft size={14} />返回列表
        </button>
        <span className="text-sm font-medium text-foreground">{acct.name ?? '—'}</span>
        <span className="text-xs text-muted font-mono">{aid}</span>
        <span className={`text-xs ${statusTone(acct.status)}`}>
          {STATUS_LABEL[acct.status] ?? acct.status ?? '—'}
        </span>
        <span className="text-xs text-muted">
          策略 {strategyName(acct.strategy_id)} · {FREQ_LABEL[acct.frequency] ?? '分钟级'}
          {acct.start_date ? ` · 自 ${acct.start_date}` : ''}
        </span>
        <div className="ml-auto flex gap-2">
          <button onClick={() => setShowDingtalkCfg(true)}
            className={`inline-flex items-center gap-1 px-3 h-9 rounded-lg text-xs ${acct?.dingtalk_enabled ? 'bg-accent text-white' : 'bg-elevated text-foreground'}`}>
            <Bell size={13} />钉钉{acct?.dingtalk_enabled ? '已开启' : '推送'}
          </button>
          <button onClick={() => api.toggleDingtalk(aid, !acct?.dingtalk_enabled).then(() => qc.invalidateQueries({ queryKey: ['quant', 'sim'] }))}
            className="inline-flex items-center gap-1 px-2 h-9 rounded-lg bg-elevated text-foreground text-xs">
            {acct?.dingtalk_enabled ? '关闭' : '开启'}
          </button>
          <button onClick={() => startMut.mutate()} disabled={startMut.isPending || acct.status === 'running'}
            className="inline-flex items-center gap-1 px-3 h-9 rounded-lg bg-accent text-white text-xs disabled:opacity-50">
            <Play size={13} />启动
          </button>
          <button onClick={() => pauseMut.mutate()} disabled={pauseMut.isPending || acct.status !== 'running'}
            className="inline-flex items-center gap-1 px-3 h-9 rounded-lg bg-elevated text-foreground text-xs disabled:opacity-50">
            <Square size={13} />暂停
          </button>
          <button onClick={() => resetMut.mutate()} disabled={resetMut.isPending}
            className="inline-flex items-center gap-1 px-3 h-9 rounded-lg bg-elevated text-foreground text-xs disabled:opacity-50">
            <RotateCcw size={13} />重置
          </button>
          <button onClick={() => { if (window.confirm('确定删除该模拟账户？此操作不可恢复。')) deleteMut.mutate() }}
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
          <div className={`text-sm font-medium num ${typeof state?.pnl === 'number' && state.pnl < 0 ? 'text-bear' : 'text-bull'}`}>
            {fmtNum(state?.pnl)}
          </div>
        </div>
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">收益率</div>
          <div className={`text-sm font-medium num ${ret == null ? '' : ret >= 0 ? 'text-bull' : 'text-bear'}`}>
            {fmtPct(ret)}
          </div>
        </div>
      </div>

      {/* 净值曲线 */}
      <div className="rounded-card border border-border bg-surface">
        <div className="px-4 pt-3 text-xs text-foreground font-medium">净值曲线</div>
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
              <thead className="text-muted sticky top-0 bg-surface">
                <tr className="text-left">
                  <th className="px-3 py-1.5 font-normal">标的</th>
                  <th className="px-3 py-1.5 font-normal text-right">数量</th>
                  <th className="px-3 py-1.5 font-normal text-right">成本</th>
                  <th className="px-3 py-1.5 font-normal text-right">现价</th>
                  <th className="px-3 py-1.5 font-normal text-right">市值</th>
                  <th className="px-3 py-1.5 font-normal text-right">盈亏</th>
                </tr>
              </thead>
              <tbody className="text-foreground">
                {posEntries.map(([sym, p]: any) => {
                  const value = (Number(p.amount) || 0) * (Number(p.price) || 0)
                  const pnlPct = Number(p.avg_cost) > 0 ? Number(p.price) / Number(p.avg_cost) - 1 : null
                  return (
                    <tr key={sym} className="border-t border-border/60">
                      <td className="px-3 py-1.5">{sym}</td>
                      <td className="px-3 py-1.5 text-right num">{p.amount}</td>
                      <td className="px-3 py-1.5 text-right num">{fmtNum(p.avg_cost, 3)}</td>
                      <td className="px-3 py-1.5 text-right num">{fmtNum(p.price, 3)}</td>
                      <td className="px-3 py-1.5 text-right num">{fmtNum(value)}</td>
                      <td className={`px-3 py-1.5 text-right num ${pnlPct == null ? '' : pnlPct >= 0 ? 'text-bull' : 'text-bear'}`}>
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
             ['stoploss', `止损日志 (${stopLossList.length})`],
             ['logs', `运行日志 (${logList.length})`]] as const).map(([k, label]) => (
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
                <thead className="text-muted sticky top-0 bg-surface">
                  <tr className="text-left">
                    <th className="px-3 py-1.5 font-normal">时间</th>
                    <th className="px-3 py-1.5 font-normal">标的</th>
                    <th className="px-3 py-1.5 font-normal">方向</th>
                    <th className="px-3 py-1.5 font-normal text-right">价格</th>
                    <th className="px-3 py-1.5 font-normal text-right">数量</th>
                    <th className="px-3 py-1.5 font-normal text-right">盈亏</th>
                  </tr>
                </thead>
                <tbody className="text-foreground">
                  {[...tradeList].reverse().map((t: any, i: number) => (
                    <tr key={i} className="border-t border-border/60">
                      <td className="px-3 py-1.5 text-muted">{String(t.ts ?? '')}</td>
                      <td className="px-3 py-1.5">{t.code ?? ''}</td>
                      <td className={`px-3 py-1.5 ${t.action === 'BUY' ? 'text-bull' : 'text-bear'}`}>
                        {t.action === 'BUY' ? '买入' : '卖出'}
                      </td>
                      <td className="px-3 py-1.5 text-right num">{fmtNum(t.price, 3)}</td>
                      <td className="px-3 py-1.5 text-right num">{t.amount}</td>
                      <td className={`px-3 py-1.5 text-right num ${typeof t.pnl === 'number' && t.pnl !== 0 ? (t.pnl >= 0 ? 'text-bull' : 'text-bear') : 'text-muted'}`}>
                        {typeof t.pnl === 'number' && t.pnl !== 0 ? fmtNum(t.pnl) : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : <div className="px-4 py-4 text-xs text-muted">暂无成交</div>
        )}
        {tab === 'stoploss' && (
          <div className="max-h-64 overflow-auto p-3 space-y-0.5 text-[11px] text-muted font-mono">
            {stopLossList.length > 0 ? stopLossList.map((l: any, i: number) => (
              <div key={i}>
                {`${l.ts ?? ''} ${l.code ?? ''} ${l.action ?? ''} @ ${l.price ?? ''} (${typeof l.pnl_pct === 'number' ? (l.pnl_pct * 100).toFixed(2) + '%' : ''})`}
              </div>
            )) : <div className="text-muted">暂无触发</div>}
          </div>
        )}
        {tab === 'logs' && (
          <div className="max-h-64 overflow-auto p-3 space-y-0.5 text-[11px] text-muted font-mono">
            {logList.length > 0 ? [...logList].reverse().map((l: any, i: number) => (
              <div key={i} className={l.level === 'error' ? 'text-bear' : l.level === 'warn' ? 'text-warning' : ''}>
                {`[${l.level ?? 'info'}] ${l.ts ?? ''} ${l.message ?? ''}`}
              </div>
            )) : <div className="text-muted">暂无日志</div>}
          </div>
        )}
      </div>
      {showDingtalkCfg && <DingtalkConfigDialog onClose={() => setShowDingtalkCfg(false)} />}
    </div>
  )
}
