import { useState, useEffect, useRef, useCallback } from 'react'
import { keepPreviousData, useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { HardDrive, RefreshCw, Wrench, Server, Activity, Inbox, Clock, CheckCircle2, Loader2 } from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { Skeleton } from '@/components/data/Skeleton'
import { DatePicker } from '@/components/DatePicker'
import { toast } from '@/components/Toast'
import { api, type LocalMarketStatsRow, type StockdataLogRow, type StockdataStatus } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

const PAGE_SIZE_OPTIONS = [10, 20, 50, 100]

const TASK_LABELS: Record<string, string> = {
  backfill: '启动回源 backfill',
  sync: '收盘/手动同步 sync',
  check_day: '单日检验 check_day',
  check_full: '全量检验 check_full',
  full_scan: '00:00 全量巡检',
}

const DATASET_LABELS: Record<string, string> = {
  kline_etf_minute: 'ETF分钟线',
  kline_daily: '股市日线',
  kline_etf_daily: 'ETF日线',
  kline_index_daily: '指数日线',
  kline_minute: '股市分钟线',
  adj_factor_etf: 'ETF前复权因子',
  etf_nav: 'ETF单位净值',
}

type CountKey = Exclude<keyof LocalMarketStatsRow, 'date'>

const COLUMNS: { key: CountKey; label: string }[] = [
  { key: 'stock_daily', label: '股市日线' },
  { key: 'stock_minute', label: '股市分钟线' },
  { key: 'etf_daily', label: 'ETF日线' },
  { key: 'etf_minute', label: 'ETF分钟线' },
  { key: 'index_daily', label: '指数日线' },
  { key: 'index_minute', label: '指数分钟线' },
]

function fmtCount(n: number): string {
  return n.toLocaleString('zh-CN')
}

function fmtTime(t?: string | null): string {
  if (!t) return '—'
  return t.replace('T', ' ').slice(0, 19)
}

function extractMissing(st: StockdataStatus): { label: string; note: string }[] {
  const out: { label: string; note: string }[] = []
  const sources = [
    st.backfill_result,
    st.check_full_result,
    st.full_scan_result,
  ]
  for (const src of sources) {
    const missing = src?.missing
    if (missing && typeof missing === 'object') {
      for (const [key, val] of Object.entries(missing as Record<string, unknown>)) {
        if (typeof val !== 'object' || val === null) continue
        const v = val as Record<string, unknown>
        const isMissing = Boolean(v.missing) || Boolean(v.empty)
        if (isMissing) {
          out.push({ label: DATASET_LABELS[key] ?? key, note: v.empty ? '空分区' : '有缺失/缺口' })
        }
      }
    }
  }
  return out
}

function StockdataStatusPanel() {
  const { data, isLoading, isError, refetch, isFetching } = useQuery({
    queryKey: ['stockdata-status'],
    queryFn: () => api.stockdataStatus(),
    refetchInterval: 5000,
  })

  const active = data?.active_tasks ?? []
  const missing = data ? extractMissing(data) : []

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs text-muted">
          <Server className="h-3.5 w-3.5" />
          <span>服务时间 {fmtTime(data?.ts)}</span>
          <span>·</span>
          <span>启动于 {fmtTime(data?.process_started)}</span>
        </div>
        <button
          onClick={() => refetch()}
          disabled={isFetching}
          className="px-2.5 py-1 rounded-btn border border-border bg-elevated text-secondary hover:text-foreground disabled:opacity-40 transition-colors inline-flex items-center gap-1.5 text-xs"
        >
          <RefreshCw className={`h-3 w-3 ${isFetching ? 'animate-spin' : ''}`} />
          刷新
        </button>
      </div>

      {isLoading ? (
        <div className="space-y-2">
          {Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-10 w-full" />)}
        </div>
      ) : isError ? (
        <EmptyState icon={Server} title="服务不可达" hint="无法获取 stockdata 服务状态，请检查服务是否存活。" />
      ) : (
        <>
          <div className="rounded-card border border-border bg-surface overflow-hidden">
            <div className="flex items-center gap-2 px-3 py-2 border-b border-border/60 text-xs font-medium text-foreground">
              <Activity className="h-3.5 w-3.5 text-accent" />
              当前正在执行
            </div>
            <div className="p-3">
              {active.length === 0 ? (
                <div className="flex items-center gap-2 text-xs text-muted">
                  <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                  空闲，无后台任务
                </div>
              ) : (
                <div className="flex flex-wrap gap-2">
                  {active.map(t => (
                    <span key={t} className="inline-flex items-center gap-1.5 rounded-full border border-accent/40 bg-accent/10 px-2.5 py-1 text-xs text-accent">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      {TASK_LABELS[t] ?? t}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>

          <div className="rounded-card border border-border bg-surface overflow-hidden">
            <div className="flex items-center gap-2 px-3 py-2 border-b border-border/60 text-xs font-medium text-foreground">
              <Inbox className="h-3.5 w-3.5 text-amber-500" />
              待办 / 数据缺口
            </div>
            <div className="p-3">
              {missing.length === 0 ? (
                <div className="flex items-center gap-2 text-xs text-muted">
                  <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                  最近巡检未报告缺口
                </div>
              ) : (
                <ul className="space-y-1.5">
                  {missing.map((m, i) => (
                    <li key={i} className="flex items-center justify-between text-xs">
                      <span className="text-foreground">{m.label}</span>
                      <span className="text-amber-500">{m.note}</span>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>

          <div className="rounded-card border border-border bg-surface overflow-hidden">
            <div className="flex items-center gap-2 px-3 py-2 border-b border-border/60 text-xs font-medium text-foreground">
              <Clock className="h-3.5 w-3.5 text-muted" />
              最近任务记录
            </div>
            <div className="divide-y divide-border/60">
              <TaskRow label="启动回源 backfill" time={data?.last_backfill} note={statusNote(data, 'backfill_result')} />
              <TaskRow label="收盘/手动同步 sync" time={data?.last_sync} note={statusNote(data, 'sync_result')} />
              <TaskRow label="单日检验 check_day" time={data?.last_check_day} note={statusNote(data, 'check_day_result')} />
              <TaskRow label="全量检验 check_full" time={data?.last_check_full} />
              <TaskRow label="00:00 全量巡检" time={data?.last_full_scan} note={data?.full_scan_date} />
            </div>
          </div>
        </>
      )}
    </div>
  )
}

function TaskRow({ label, time, note }: { label: string; time?: string | null; note?: unknown }) {
  const noteStr = typeof note === 'string' ? note : note === undefined || note === null ? undefined : String(note)
  return (
    <div className="flex items-center justify-between px-3 py-2 text-xs">
      <span className="text-secondary">{label}</span>
      <span className="text-muted">{fmtTime(time)}{noteStr ? ` · ${noteStr}` : ''}</span>
    </div>
  )
}

function statusNote(st: StockdataStatus | undefined, field: string): string | undefined {
  if (!st) return undefined
  const src = st[field as keyof StockdataStatus]
  if (!src || typeof src !== 'object') return undefined
  const rec = src as Record<string, unknown>
  if (field === 'backfill_result') {
    const errs = rec.errors
    return Array.isArray(errs) && errs.length ? `errors=${errs.length}` : undefined
  }
  if (field === 'sync_result') {
    const rows = rec.stock_minute_rows
    return rows === undefined ? undefined : `stock_minute_rows=${rows}`
  }
  if (field === 'check_day_result') {
    return rec.day ? String(rec.day) : undefined
  }
  return undefined
}

export function LocalData() {
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [refreshNonce, setRefreshNonce] = useState(0)
  const [refreshingRow, setRefreshingRow] = useState<string | null>(null)
  const qc = useQueryClient()

  const start = startDate || undefined
  const end = endDate || undefined

  const { data, isLoading, isFetching, isError } = useQuery({
    queryKey: QK.localMarketStats(page, pageSize, start, end, refreshNonce),
    queryFn: () => api.localMarketStats(page, pageSize, start, end, refreshNonce > 0),
    placeholderData: keepPreviousData,
  })

  // 刷新请求完成后复位 refreshNonce: 后续 refetch(窗口聚焦/invalidate/重挂载)不再带 refresh=true
  useEffect(() => {
    if (refreshNonce > 0 && !isFetching) setRefreshNonce(0)
  }, [refreshNonce, isFetching])

  const refreshStats = () => {
    qc.invalidateQueries({ queryKey: ['local-market-stats'] })
  }

  const checkDayMut = useMutation({
    mutationFn: (date: string) => api.checkDay(date),
    onSuccess: (_data, date) => {
      toast(`已触发 ${date} 检验补齐`, 'success', 'top')
      setTimeout(refreshStats, 3000)
    },
  })

  const checkFullMut = useMutation({
    mutationFn: () => api.checkFull(),
    onSuccess: () => {
      toast('已触发全量检验补齐', 'success', 'top')
      setTimeout(refreshStats, 3000)
    },
  })

  const refreshRowMut = useMutation({
    mutationFn: (_date: string) => api.localMarketStats(page, pageSize, start, end, true),
    onSuccess: (data, date) => {
      qc.setQueryData(
        QK.localMarketStats(page, pageSize, start, end, refreshNonce),
        (old: typeof data | undefined) => {
          if (!old) return old
          const newRow = data.rows.find(r => r.date === date)
          if (!newRow) return old
          return { ...old, rows: old.rows.map(r => (r.date === date ? newRow : r)) }
        },
      )
    },
    onSettled: () => setRefreshingRow(null),
  })

  const [bottomTab, setBottomTab] = useState<'log' | 'status'>('log')
  const [logLines, setLogLines] = useState<StockdataLogRow[]>([])
  const [logOffset, setLogOffset] = useState(0)
  const [logLoadingMore, setLogLoadingMore] = useState(false)
  const logScrollRef = useRef<HTMLDivElement>(null)
  const LOG_LIMIT = 100

  const loadLogPage = useCallback(async (offset: number) => {
    try {
      const res = await api.stockdataLog(offset, LOG_LIMIT)
      setLogLines(prev => {
        const seen = new Set(prev.map(r => r.line))
        const fresh = res.rows.filter(r => !seen.has(r.line))
        if (fresh.length === 0) return prev
        return offset === 0 ? [...fresh, ...prev] : [...prev, ...fresh]
      })
      return res
    } catch {
      toast('加载日志失败', 'error')
      return null
    }
  }, [])

  const logVisible = bottomTab === 'log'
  useEffect(() => {
    if (!logVisible) return
    setLogLines([])
    setLogOffset(0)
    loadLogPage(0)
    const t = setInterval(() => loadLogPage(0), 5000)
    return () => clearInterval(t)
  }, [logVisible, loadLogPage])

  // 滚动到底加载更早日志
  const onLogScroll = useCallback(() => {
    const el = logScrollRef.current
    if (!el || logLoadingMore) return
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 40) {
      setLogLoadingMore(true)
      const nextOffset = logOffset + LOG_LIMIT
      loadLogPage(nextOffset).then(res => {
        if (res && res.rows.length > 0) setLogOffset(nextOffset)
        setLogLoadingMore(false)
      })
    }
  }, [logLoadingMore, logOffset, loadLogPage])

  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / pageSize))
  const safePage = Math.min(page, totalPages)
  const rows = data?.rows ?? []

  // 数据变少导致当前页越界时回落，避免卡在空页
  useEffect(() => {
    if (safePage < page) setPage(safePage)
  }, [safePage, page])

  const onFilterChange = () => {
    setPage(1)
    setRefreshNonce(0)
  }

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="本地股市数据"
        subtitle={total > 0 ? `本地 Parquet 各日期去重标的数 · 共 ${total} 天` : '本地 Parquet 各日期去重标的数'}
      />
      <div className="flex-1 p-4 overflow-auto space-y-3">
        {!isLoading && !isError && total > 0 && (
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <DatePicker value={startDate} onChange={v => { setStartDate(v); onFilterChange() }} placeholder="起始日期" />
              <span className="text-muted text-xs">~</span>
              <DatePicker value={endDate} onChange={v => { setEndDate(v); onFilterChange() }} placeholder="结束日期" />
              <select
                value={pageSize}
                onChange={e => { setPageSize(Number(e.target.value)); onFilterChange() }}
                className="h-7 rounded-btn border border-border bg-elevated px-2 text-xs text-foreground"
              >
                {PAGE_SIZE_OPTIONS.map(n => (
                  <option key={n} value={n}>{n} 条/页</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-1.5">
              <button
                onClick={() => setRefreshNonce(n => n + 1)}
                disabled={isFetching}
                className="px-3 py-1.5 rounded-btn border border-border bg-elevated text-secondary hover:text-foreground disabled:opacity-40 transition-colors flex items-center gap-1.5"
                title="刷新当前页统计"
              >
                <RefreshCw className={`h-3 w-3 ${isFetching ? 'animate-spin' : ''}`} />
                {isFetching ? '刷新中...' : '刷新'}
              </button>
              <button
                onClick={() => checkFullMut.mutate()}
                disabled={checkFullMut.isPending}
                className="px-3 py-1.5 rounded-btn border border-border bg-elevated text-secondary hover:text-foreground disabled:opacity-40 transition-colors flex items-center gap-1.5"
              >
                <Wrench className="h-3 w-3" />
                {checkFullMut.isPending ? '校验中...' : '全量检验补齐'}
              </button>
            </div>
          </div>
        )}
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 8 }).map((_, i) => (
              <Skeleton key={i} className="h-9 w-full" />
            ))}
          </div>
        ) : isError ? (
          <EmptyState title="加载失败" hint="无法获取本地数据统计，请稍后重试或检查后端服务。" />
        ) : total === 0 ? (
          <EmptyState
            icon={HardDrive}
            title="暂无本地数据"
            hint={startDate || endDate ? '当前日期范围内无数据。' : '本地尚无任何行情数据，数据同步完成后会在此展示各日期的标的覆盖情况。'}
          />
        ) : (
          <>
            <div className="rounded-card border border-border bg-surface overflow-hidden relative">
              {isFetching && !isLoading && (
                <div className="absolute inset-0 bg-elevated/30 flex items-center justify-center z-10">
                  <div className="text-xs text-muted flex items-center gap-2">
                    <RefreshCw className="h-4 w-4 animate-spin" />
                    刷新中...
                  </div>
                </div>
              )}
              <table className="w-full text-xs">
                <thead className="text-muted bg-elevated/40">
                  <tr className="text-left">
                    <th className="px-3 py-2 font-normal">日期</th>
                    {COLUMNS.map(c => (
                      <th key={c.key} className="px-3 py-2 font-normal text-right">{c.label}</th>
                    ))}
                    <th className="px-3 py-2 font-normal text-right">操作</th>
                  </tr>
                </thead>
                <tbody className="text-foreground">
                  {rows.map(row => (
                    <tr key={row.date} className="border-t border-border/60 hover:bg-elevated/60 transition-colors">
                      <td className="px-3 py-2 font-mono num">{row.date}</td>
                      {COLUMNS.map(c => (
                        <td key={c.key} className="px-3 py-2 text-right num text-muted">
                          {fmtCount(row[c.key])}
                        </td>
                      ))}
                      <td className="px-3 py-2 text-right whitespace-nowrap">
                        <button
                          onClick={() => { setRefreshingRow(row.date); refreshRowMut.mutate(row.date) }}
                          disabled={refreshRowMut.isPending}
                          className="px-2 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors inline-flex items-center gap-1"
                        >
                          {refreshingRow === row.date
                            ? <RefreshCw className="h-3 w-3 animate-spin" />
                            : <RefreshCw className="h-3 w-3" />}
                          刷新
                        </button>
                        <button
                          onClick={() => checkDayMut.mutate(row.date)}
                          disabled={checkDayMut.isPending}
                          className="px-2 py-1 ml-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                        >
                          检验
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between mt-3 text-xs text-muted">
              <span>共 {total} 天 · 第 {safePage}/{totalPages} 页</span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => { setPage(p => Math.max(1, p - 1)); setRefreshNonce(0) }}
                  disabled={safePage <= 1}
                  className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                >
                  上一页
                </button>
                <button
                  onClick={() => { setPage(p => Math.min(totalPages, p + 1)); setRefreshNonce(0) }}
                  disabled={safePage >= totalPages}
                  className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                >
                  下一页
                </button>
              </div>
            </div>

            <div className="rounded-card border border-border bg-surface overflow-hidden mt-3">
              <div className="flex items-center border-b border-border/60">
                {(['log', 'status'] as const).map(tab => {
                  const active = bottomTab === tab
                  return (
                    <button
                      key={tab}
                      onClick={() => setBottomTab(tab)}
                      className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium transition-colors border-b-2 ${
                        active ? 'text-accent border-accent' : 'text-secondary border-transparent hover:text-foreground'
                      }`}
                    >
                      {tab === 'log' ? <Activity className="h-3.5 w-3.5" /> : <Server className="h-3.5 w-3.5" />}
                      {tab === 'log' ? '日志' : '服务状态'}
                    </button>
                  )
                })}
              </div>
              {bottomTab === 'log' ? (
                <div
                  ref={logScrollRef}
                  onScroll={onLogScroll}
                  className="h-[30vh] overflow-y-auto p-2 font-mono text-[11px] leading-relaxed text-muted"
                >
                  {logLines.length === 0 ? (
                    <div className="text-center py-6 text-muted/60">暂无日志</div>
                  ) : (
                    logLines.map(r => (
                      <div key={r.line} className="whitespace-pre-wrap break-all">
                        {r.text}
                      </div>
                    ))
                  )}
                  {logLoadingMore && <div className="text-center py-2 text-muted/50">加载更早日志...</div>}
                </div>
              ) : (
                <div className="h-[30vh] overflow-y-auto p-3">
                  <StockdataStatusPanel />
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}
