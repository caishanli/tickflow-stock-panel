import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AnimatePresence, motion } from 'framer-motion'
import { RefreshCw, X } from 'lucide-react'
import { api } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { StockInfoBar } from '@/components/StockInfoBar'
import { StockDailyKChart, getDefaultRange, type StockDailyKChartResult } from '@/components/StockDailyKChart'
import { StockIntradayChart } from '@/components/StockIntradayChart'
import { StockFiveDayChart } from '@/components/StockFiveDayChart'
import { DatePicker } from '@/components/DatePicker'
import { RuleEditor } from '@/components/monitor/RuleEditor'
import { useCapabilities, usePreferences, useQuoteStatus } from '@/lib/useSharedQueries'
import { useFinancialMetrics } from '@/lib/useFinancials'
import { setFocusSymbol, clearFocusSymbol } from '@/lib/useQuoteStream'
import { loadInfoFields, saveInfoFields, buildInfoExtColumnsParam, type ColumnConfig } from '@/lib/stock-info-fields'
import type { ChartMarker, ChartPriceLine, ChartRange } from '@/components/EChartsCandlestick'
import type { IntradayMarker } from '@/components/EChartsIntraday'

/** 视图模式: 分钟(当天分钟线) / 五日(近5交易日分钟拼接) / 日线 / 周线 */
export type QuantViewMode = 'minute' | 'fiveDay' | 'daily' | 'weekly'

// 预设快捷范围
const PRESETS: { label: string; months: number }[] = [
  { label: '半年', months: 6 },
  { label: '1年', months: 12 },
]

const VIEW_LABEL: Record<QuantViewMode, string> = { minute: '分钟', fiveDay: '五日', daily: '日线', weekly: '周线' }

function boardTag(symbol: string): { label: string; color: string } | null {
  if (/^(300|301)/.test(symbol)) return { label: '创', color: 'text-[#f97316] bg-[#f97316]/12 border-[#f97316]/25' }
  if (/^688/.test(symbol))       return { label: '科', color: 'text-purple-400 bg-purple-400/12 border-purple-400/25' }
  if (/^[48]/.test(symbol))      return { label: '北', color: 'text-cyan-400 bg-cyan-400/12 border-cyan-400/25' }
  return null
}

interface Props {
  symbol: string | null
  name?: string
  onClose: () => void
  /** 初始视图 (默认日线; 模拟盘传 minute) */
  initialView?: QuantViewMode
  /** 分钟视图初始选中日期 (成交行 = 交易当日) */
  initialDate?: string
  /** 外部日期范围 (回测按持仓区间传入) */
  dateRange?: { start: string; end: string }
  /** 日线视图标记 (回测成交标记) */
  markers?: ChartMarker[]
  /** 日线视图区间 (回测持仓区间) */
  ranges?: ChartRange[]
  /** 日线视图价格线 (回测买入价/卖出价) */
  priceLines?: ChartPriceLine[]
  /** 分钟视图买卖标记 (date 感知, 仅分钟视图渲染) */
  intradayMarkers?: IntradayMarker[]
  /** 日线视图涨跌停标记 (默认显示; 回测传 false) */
  showLimitMarkers?: boolean
  /** 日线视图标记开关按钮 (默认显示; 回测传 false) */
  showMarkerToggle?: boolean
}

export function QuantTradeDialog({
  symbol, name, onClose, initialView = 'daily', initialDate,
  dateRange: externalDateRange, markers, ranges, priceLines,
  intradayMarkers, showLimitMarkers = true, showMarkerToggle = true,
}: Props) {
  const qc = useQueryClient()
  const [view, setView] = useState<QuantViewMode>(initialView)
  const [selectedDate, setSelectedDate] = useState<string | null>(null)
  const [dateRange, setDateRange] = useState<{ start: string; end: string }>(externalDateRange ?? getDefaultRange())
  const [showMonitorEditor, setShowMonitorEditor] = useState(false)
  const [dailyResult, setDailyResult] = useState<StockDailyKChartResult | null>(null)
  const [fields, setFields] = useState<ColumnConfig[]>(loadInfoFields)
  const extColumns = useMemo(() => buildInfoExtColumnsParam(fields), [fields])

  // ESC 关闭
  useEffect(() => {
    if (!symbol) return
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [symbol, onClose])

  // 焦点股票注册: SSE 实时 invalidate 当前股票日K
  useEffect(() => {
    if (!symbol) return
    setFocusSymbol(symbol)
    return () => clearFocusSymbol()
  }, [symbol])

  // symbol 切换(弹窗不卸载复用)时重置视图/日期
  const prevSymbol = useRef<string | null>(symbol)
  const appliedKeyRef = useRef('')
  useEffect(() => {
    if (prevSymbol.current === symbol) return
    prevSymbol.current = symbol
    setView(initialView)
    setSelectedDate(null)
    setDailyResult(null)
    appliedKeyRef.current = ''
  }, [symbol, initialView])

  // 外部 dateRange 变化时同步 (回测切换成交时窗口跟随持仓区间)
  useEffect(() => {
    setDateRange(externalDateRange ?? getDefaultRange())
  }, [externalDateRange])

  // 加自选
  const watchlist = useQuery({ queryKey: QK.watchlist, queryFn: api.watchlistList, enabled: !!symbol })
  const inWatchlist = (watchlist.data?.symbols ?? []).some((s: any) => s.symbol === symbol)
  const toggleWatchlist = useMutation({
    mutationFn: () => inWatchlist ? api.watchlistRemove(symbol!) : api.watchlistAdd(symbol!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: QK.watchlist })
      qc.invalidateQueries({ queryKey: ['watchlist-enriched'] })
    },
  })

  // 信息条指标配置 + 财务指标
  const handleFieldsChange = useCallback((next: ColumnConfig[]) => {
    setFields(next)
    saveInfoFields(next)
  }, [])
  const { data: caps } = useCapabilities()
  const hasFinancialCap = !!caps?.capabilities?.['financial']
  const hasFinanceField = useMemo(
    () => fields.some(f => f.visible && f.source.type === 'builtin'
      && ['eps', 'bps', 'roe', 'pe_ttm', 'pb', 'gross_margin', 'net_margin', 'debt_ratio', 'revenue_yoy', 'net_income_yoy'].includes(f.source.key)),
    [fields],
  )
  const financials = useFinancialMetrics(hasFinanceField && hasFinancialCap && symbol ? symbol : undefined)

  // 分钟视图轮询偏好 (与自选股弹窗一致)
  const { data: prefs } = usePreferences()
  const { data: quoteStatus } = useQuoteStatus()
  const realtimeRunning = quoteStatus?.running ?? false
  const intradayRefreshOn = prefs?.minute_intraday_refresh ?? false
  const intradayRefetchMs = (intradayRefreshOn && realtimeRunning)
    ? (prefs?.minute_intraday_refresh_interval ?? 6) * 1000
    : undefined

  const rawRows: any[] = dailyResult?.rawRows ?? []

  // 手机屏(≤md)压缩图表高度
  const [narrow, setNarrow] = useState(() => typeof window !== 'undefined' && window.matchMedia('(max-width: 768px)').matches)
  useEffect(() => {
    const mq = window.matchMedia('(max-width: 768px)')
    const onChange = () => setNarrow(mq.matches)
    mq.addEventListener('change', onChange)
    return () => mq.removeEventListener('change', onChange)
  }, [])

  // 分钟视图日期定位(单一来源防竞态): 日K rows 就绪后按 (symbol|date) 应用一次——同标的
  // 换日期点击也会重新定位; 无 date(持仓无入场时间)回退最新交易日。
  // 不可拆成「应用initialDate」+「空则选最新」两个 effect: 同一轮渲染里后者读到旧闭包的
  // selectedDate=null 会覆盖前者的选择(历史 bug: 点哪天都停在最新日)。
  useEffect(() => {
    if (view !== 'minute' || rawRows.length === 0) return
    const latest = String(rawRows[rawRows.length - 1].date).slice(0, 10)
    const key = `${symbol}|${initialDate ?? ''}`
    if (appliedKeyRef.current === key) {
      if (!selectedDate) setSelectedDate(latest)
      return
    }
    appliedKeyRef.current = key
    if (!initialDate) {
      setSelectedDate(latest)
      return
    }
    const target = rawRows.find((r: any) => String(r.date).slice(0, 10) === initialDate)
    setSelectedDate(target ? initialDate : latest)
  }, [symbol, initialDate, view, selectedDate, rawRows])

  // 五日线: 日K尾部 5 个交易日 + 其首日前一日收盘(作百分比基准)
  const fiveDates = useMemo(() => rawRows.slice(-5).map((r: any) => String(r.date).slice(0, 10)), [rawRows])
  const fivePrevClose = useMemo(() => {
    const i0 = fiveDates.length > 0 ? rawRows.findIndex((r: any) => String(r.date).slice(0, 10) === fiveDates[0]) : -1
    return i0 > 0 ? Number(rawRows[i0 - 1].close) : undefined
  }, [fiveDates, rawRows])

  // 分钟视图昨收 = 选中日的前一交易日收盘
  const selectedIdx = selectedDate
    ? rawRows.findIndex((r: any) => String(r.date).slice(0, 10) === selectedDate)
    : -1
  const prevClose = selectedIdx > 0
    ? Number(rawRows[selectedIdx - 1].close)
    : rawRows.length >= 2
      ? Number(rawRows[rawRows.length - 2].close)
      : undefined

  const handleRefresh = () => {
    if (!symbol) return
    qc.invalidateQueries({ queryKey: ['kline', symbol!] })
    if (view === 'minute') {
      qc.invalidateQueries({ queryKey: ['kline-minute', symbol!] })
    }
  }

  return (
    <AnimatePresence>
      {symbol && (
        <div className="fixed inset-0 z-50 flex items-center justify-center">
          {/* 遮罩 */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="absolute inset-0 bg-black/60 backdrop-blur-sm"
            onClick={onClose}
          />

          {/* 弹窗主体 */}
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: 12 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.97, y: 8 }}
            transition={{ duration: 0.2, ease: [0.16, 1, 0.3, 1] }}
            className="relative w-[92vw] max-w-[1100px] max-h-[95vh] max-md:w-full max-md:h-[94dvh] max-md:max-h-none max-md:rounded-none rounded-card border border-border bg-base shadow-2xl overflow-hidden flex flex-col"
          >
            {/* 顶栏 (手机端: 收窄内边距, 隐藏预设/日期范围控件, 保留刷新+关闭) */}
            <div className="flex items-center justify-between px-5 max-md:px-3 py-3 border-b border-border shrink-0 gap-2">
              <div className="flex items-center gap-2 min-w-0">
                {(() => {
                  const board = symbol ? boardTag(symbol) : null
                  return board ? (
                    <span className={`inline-flex items-center justify-center w-[18px] h-[18px] shrink-0 rounded text-[9px] font-bold leading-none border ${board.color}`}>
                      {board.label}
                    </span>
                  ) : null
                })()}
                <span className="font-mono text-sm font-medium text-foreground shrink-0">{symbol}</span>
                {name && <span className="text-xs text-muted truncate">{name}</span>}
              </div>

              <div className="flex items-center gap-1.5 shrink-0">
                <span className="flex items-center gap-1.5 max-md:hidden">
                {PRESETS.map(p => {
                  const now = new Date()
                  const s = new Date(now)
                  s.setMonth(s.getMonth() - p.months)
                  const expected = s.toISOString().slice(0, 10)
                  const isActive = dateRange.start === expected
                  return (
                    <button
                      key={p.label}
                      onClick={() => {
                        const end = new Date().toISOString().slice(0, 10)
                        const ns = new Date()
                        ns.setMonth(ns.getMonth() - p.months)
                        setDateRange({ start: ns.toISOString().slice(0, 10), end })
                      }}
                      className={`h-6 px-1.5 rounded text-[11px] transition-colors cursor-pointer
                        ${isActive
                          ? 'bg-accent/20 text-accent font-medium border border-accent/30'
                          : 'text-muted hover:text-foreground hover:bg-elevated border border-transparent'
                        }`}
                    >
                      {p.label}
                    </button>
                  )
                })}
                <DatePicker
                  value={dateRange.start}
                  onChange={(v) => setDateRange(prev => ({ ...prev, start: v }))}
                  max={dateRange.end}
                />
                <span className="text-muted/40 text-[10px]">~</span>
                <DatePicker
                  value={dateRange.end}
                  onChange={(v) => setDateRange(prev => ({ ...prev, end: v }))}
                  min={dateRange.start}
                />
                </span>

                <span className="text-muted/20 mx-0.5 max-md:hidden">|</span>

                <button
                  onClick={handleRefresh}
                  className="p-1 rounded-btn text-secondary hover:text-foreground hover:bg-elevated transition-colors"
                  title="刷新"
                >
                  <RefreshCw className="h-3.5 w-3.5" />
                </button>

                <button
                  onClick={onClose}
                  className="p-1 rounded-btn text-secondary hover:text-foreground hover:bg-elevated transition-colors"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            </div>

            {/* 内容 */}
            <div className="flex-1 overflow-auto p-4 max-md:p-2">
              <StockInfoBar
                symbol={symbol}
                name={dailyResult?.name}
                stockInfo={dailyResult?.stockInfo}
                rows={rawRows}
                fields={fields}
                onFieldsChange={handleFieldsChange}
                financialMetrics={financials.data?.data?.[0]}
                onMonitor={() => setShowMonitorEditor(true)}
                inWatchlist={inWatchlist}
                onToggleWatchlist={() => toggleWatchlist.mutate()}
              />

              {/* 视图切换器 */}
              <div className="flex items-center gap-1 mt-2 mb-1">
                {(Object.keys(VIEW_LABEL) as QuantViewMode[]).map(m => (
                  <button
                    key={m}
                    onClick={() => setView(m)}
                    className={`px-3 h-7 rounded-btn text-xs cursor-pointer transition-colors ${
                      view === m ? 'bg-accent text-white' : 'bg-elevated text-muted hover:text-foreground'
                    }`}
                  >
                    {VIEW_LABEL[m]}
                  </button>
                ))}
              </div>

              <StockDailyKChart
                dataSource="stockdata"
                symbol={symbol}
                height={420}
                className={view === 'minute' || view === 'fiveDay' ? 'hidden' : undefined}
                dateRange={dateRange}
                period={view === 'weekly' ? 'weekly' : 'daily'}
                markers={view === 'daily' ? markers : undefined}
                ranges={view === 'daily' ? ranges : undefined}
                priceLines={view === 'daily' ? priceLines : undefined}
                showLimitMarkers={view === 'daily' ? showLimitMarkers : false}
                showMarkerToggle={view === 'daily' ? showMarkerToggle : false}
                showIndicatorControls={view !== 'weekly'}
                onDateClick={(d) => setSelectedDate(d)}
                onDataChange={setDailyResult}
                visibleBars={view === 'weekly' ? 120 : 60}
                extColumns={extColumns}
              />
              {view === 'minute' && selectedDate && (
                <StockIntradayChart
                  dataSource="stockdata"
                  symbol={symbol}
                  date={selectedDate}
                  height={narrow ? 300 : 420}
                  prevClose={prevClose}
                  markers={intradayMarkers}
                  refetchIntervalMs={intradayRefetchMs}
                  allowLimitMode={false}
                />
              )}
              {view === 'minute' && !selectedDate && (
                <div className="h-[420px] grid place-items-center text-xs text-muted">
                  加载中…
                </div>
              )}
              {view === 'fiveDay' && fiveDates.length > 0 && (
                <StockFiveDayChart
                  symbol={symbol}
                  dates={fiveDates}
                  prevClose={fivePrevClose}
                  height={narrow ? 300 : 420}
                  markers={intradayMarkers}
                />
              )}
            </div>

            {/* 加监控编辑器弹层 */}
            <AnimatePresence>
              {showMonitorEditor && symbol && (
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  className="absolute inset-0 z-20 flex items-start justify-center overflow-auto bg-black/40 p-4"
                  onClick={() => setShowMonitorEditor(false)}
                >
                  <div className="mt-8 w-full max-w-2xl" onClick={e => e.stopPropagation()}>
                    <RuleEditor
                      rule={null}
                      simple
                      preset={{
                        scope: 'symbols',
                        symbols: [symbol],
                        type: 'signal',
                        logic: 'or',
                      }}
                      onClose={() => setShowMonitorEditor(false)}
                      onSaved={() => setShowMonitorEditor(false)}
                    />
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        </div>
      )}
    </AnimatePresence>
  )
}
