# 量化专用交易弹窗（模拟盘/回测共用，单图+分钟/日线/周线）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新建 `QuantTradeDialog`（单图 + 分钟/日线/周线切换）供量化模拟盘与量化回测使用；回滚共享弹窗的改动；回测 `TradeKlineModal` 替换为 `QuantTradeDialog`。

**Architecture:** 纯前端。新弹窗复制自选股弹窗壳 + 信息条/加自选/加监控逻辑，图表区为视图切换器 + 单图；周线为 `StockDailyKChart` 前端聚合日K（新增可选 `period` prop）；`StockPanel`/`StockPreviewDialog` 恢复 44a5ae5 之前状态；保留 `EChartsIntraday.markers`/`StockIntradayChart.markers`（分钟视图 B/S 标记已实现）。

**Tech Stack:** React 18 + TS + Vite + ECharts 5 + @tanstack/react-query。无前端测试框架。

## Global Constraints

- 后端零改动；不新增依赖。
- **不修改其它弹窗逻辑**：`StockPanel`/`StockPreviewDialog` 恢复 44a5ae5 之前状态（用 `git checkout 44a5ae5^ -- <file>` 取原版）。
- 保留：`StockIntradayChart.markers`、`EChartsIntraday.markers`（含 fail-closed date 契约 `if (m.date !== chartDate) continue`）。
- 视图切换器按钮文案：分钟 / 日线 / 周线；默认视图 `'daily'`，模拟盘传 `initialView="minute"`。
- 周K聚合：ISO 周（周一为一周首），日期标签为周首日，**本地日期手拼 `YYYY-MM-DD`（禁止 `toISOString().slice(0,10)`，时区会偏一天）**。
- 分钟视图无数据沿用现有提示；标记时间 `HH:MM` 直接定位 242 点全天轴（无时区换算）。
- 验证门禁 = `cd frontend && pnpm build`（tsc -b + vite build）。`pnpm lint` 不可用（无 eslint 配置，预存在问题，不算缺陷）。前端无测试框架，本计划不写单测（验证靠 tsc + 构建）。
- 每次任务结束提交一个 commit；不提交 dist、data、node_modules。

---

### Task 1: 共享弹窗回滚（StockPanel / StockPreviewDialog）

**Files:**
- Restore: `frontend/src/components/StockPanel.tsx`
- Restore: `frontend/src/components/StockPreviewDialog.tsx`

**Interfaces:**
- Consumes: 无（这两个文件恢复原状；`StockIntradayChart.markers` 与 `EChartsIntraday.markers` 保留不动）。
- Produces: 无。

- [ ] **Step 1: 恢复原版文件**

Run（从仓库根目录）：

```bash
git checkout 44a5ae5^ -- frontend/src/components/StockPanel.tsx frontend/src/components/StockPreviewDialog.tsx
git status --short
```

Expected: 两个文件显示为 Modified（内容回到 44a5ae5 之前），`EChartsIntraday.tsx`/`StockIntradayChart.tsx` 无改动。

- [ ] **Step 2: 确认回滚干净**

Run: `git diff --stat`
Expected: 仅这 2 个文件；`git diff frontend/src/components/StockPanel.tsx` 中不再有 `initialDate`/`intradayMarkers`/`initialApplied`；`StockPreviewDialog.tsx` 不再有 `initialIntraday`/`intradayMarkers`/分时重置 effect（恢复为只有 `useState(false)` 的分时开关 + 头部「分时」按钮）。

- [ ] **Step 3: 验证构建**

Run: `cd frontend && pnpm build`
Expected: 通过，无类型错误（QuantSim 此时仍引用 `initialIntraday`/`intradayMarkers` props 会报错——见 Step 4 说明）。

注意：Task 1 提交后到 Task 4 完成前，`QuantSim.tsx` 对已删除 props 的引用会让 `pnpm build` 失败。**本任务验证以「tsc 仅报 QuantSim.tsx 相关 props 缺失错误、无其它错误」为准**；若 `pnpm build` 失败仅因 QuantSim.tsx 引用已删 props，视为通过。

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/StockPanel.tsx frontend/src/components/StockPreviewDialog.tsx
git commit -m "refactor(stock): 回滚共享弹窗改动 — StockPanel/StockPreviewDialog 恢复原状"
```

---

### Task 2: StockDailyKChart 周线聚合（period prop）

**Files:**
- Modify: `frontend/src/components/StockDailyKChart.tsx`

**Interfaces:**
- Produces: `export function aggregateWeekly(rows: KlineRow[]): KlineRow[]`；`StockDailyKChart` 新增可选 prop `period?: 'daily' | 'weekly'`（默认 `'daily'`）。

- [ ] **Step 1: 新增 `aggregateWeekly` 导出函数**

放在 `toOHLC` 函数之后：

```ts
/** 日K行按 ISO 周(周一为首)聚合为周K: 开=周首开/高=周内最高/低=周内最低/收=周末收(不足一周按实际)/量额求和, 周均线重算, 副图指标置 null */
export function aggregateWeekly(rows: KlineRow[]): KlineRow[] {
  const weeks = new Map<string, KlineRow>()
  for (const r of rows) {
    const date = typeof r.date === 'string' ? r.date.slice(0, 10) : String(r.date)
    const d = new Date(`${date}T00:00:00`)
    if (Number.isNaN(d.getTime())) continue
    const dow = (d.getDay() + 6) % 7 // Mon=0
    const start = new Date(d)
    start.setDate(d.getDate() - dow)
    // 本地日期手拼 (toISOString 会因时区偏一天)
    const key = `${start.getFullYear()}-${String(start.getMonth() + 1).padStart(2, '0')}-${String(start.getDate()).padStart(2, '0')}`
    const cur = weeks.get(key)
    if (!cur) {
      weeks.set(key, {
        ...r, date: key,
        open: r.open, high: r.high, low: r.low, close: r.close,
        volume: r.volume ?? 0, amount: r.amount ?? 0,
      })
    } else {
      cur.high = Math.max(Number(cur.high), Number(r.high))
      cur.low = Math.min(Number(cur.low), Number(r.low))
      cur.close = r.close
      cur.volume = Number(cur.volume ?? 0) + Number(r.volume ?? 0)
      cur.amount = Number(cur.amount ?? 0) + Number(r.amount ?? 0)
    }
  }
  const sorted = Array.from(weeks.values()).sort((a, b) => String(a.date).localeCompare(String(b.date)))
  const closes = sorted.map((r) => Number(r.close))
  const ma = (n: number, i: number) =>
    i + 1 < n ? null : closes.slice(i + 1 - n, i + 1).reduce((s, v) => s + v, 0) / n
  return sorted.map((r, i) => ({
    ...r,
    ma5: ma(5, i), ma10: ma(10, i), ma20: ma(20, i), ma60: ma(60, i),
    macd_dif: null, macd_dea: null, macd_hist: null,
    rsi_6: null, rsi_14: null, rsi_24: null,
    kdj_k: null, kdj_d: null, kdj_j: null,
    boll_upper: null, boll_lower: null,
  }))
}
```

- [ ] **Step 2: Props 增加 `period` 并接入渲染**

`Props` interface（约第 56 行 `extColumns?: string` 之后）加：

```ts
  /** 聚合周期: daily=日K(默认) / weekly=按ISO周聚合 */
  period?: 'daily' | 'weekly'
```

组件签名解构加 `period = 'daily'`。

`const rows = useMemo(() => toOHLC(kline.data?.rows ?? []), [kline.data?.rows])`（约第 151 行）替换为：

```ts
  const dailyRows = kline.data?.rows ?? []
  const displayRows = useMemo(
    () => period === 'weekly' ? aggregateWeekly(dailyRows) : dailyRows,
    [period, dailyRows],
  )
  const rows = useMemo(() => toOHLC(displayRows), [displayRows])
```

`const limitMarkers = useMemo(() => buildLimitUpMarkers(kline.data?.rows ?? []), [kline.data?.rows])`（约第 152 行）替换为（涨跌停标记永远基于日K，周K日期为周首不匹配）：

```ts
  const limitMarkers = useMemo(() => buildLimitUpMarkers(dailyRows), [dailyRows])
```

`onDataChange` 的 effect（`{ rows, rawRows: kline.data?.rows ?? [], ... }`）保持不变（rawRows 仍为日K行，供信息条/昨收/选中日期使用）。

JSX 中 `showIndicatorControls && rows.length > 0`（约第 172 行）替换为：

```tsx
      {showIndicatorControls && period !== 'weekly' && rows.length > 0 && (
```

（周线隐藏指标控制按钮，避免 macd/rsi/kdj/boll 为 null 时副图渲染异常。）

- [ ] **Step 3: 验证构建**

Run: `cd frontend && pnpm build`
Expected: 通过，无类型错误。

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/StockDailyKChart.tsx
git commit -m "feat(kline): 日K周K前端聚合 — StockDailyKChart 支持 period=weekly"
```

---

### Task 3: QuantTradeDialog 新组件（单图 + 分钟/日线/周线）

**Files:**
- Create: `frontend/src/components/QuantTradeDialog.tsx`

**Interfaces:**
- Consumes: `StockInfoBar`、`StockDailyKChart`（Task 2 的 `period`）、`StockIntradayChart`（`markers` prop）、`DatePicker`、`RuleEditor`、`IntradayMarker`（`EChartsIntraday` 导出）、`ChartMarker`/`ChartPriceLine`/`ChartRange`（`EChartsCandlestick` 导出）。
- Produces: `export type QuantViewMode = 'minute' | 'daily' | 'weekly'`；`QuantTradeDialog`（props 见下）。

- [ ] **Step 1: 创建完整组件**

创建 `frontend/src/components/QuantTradeDialog.tsx`，内容如下（整文件）：

```tsx
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AnimatePresence, motion } from 'framer-motion'
import { RefreshCw, X } from 'lucide-react'
import { api } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { StockInfoBar } from '@/components/StockInfoBar'
import { StockDailyKChart, getDefaultRange, type StockDailyKChartResult } from '@/components/StockDailyKChart'
import { StockIntradayChart } from '@/components/StockIntradayChart'
import { DatePicker } from '@/components/DatePicker'
import { RuleEditor } from '@/components/monitor/RuleEditor'
import { useCapabilities, usePreferences, useQuoteStatus } from '@/lib/useSharedQueries'
import { useFinancialMetrics } from '@/lib/useFinancials'
import { setFocusSymbol, clearFocusSymbol } from '@/lib/useQuoteStream'
import { loadInfoFields, saveInfoFields, buildInfoExtColumnsParam, type ColumnConfig } from '@/lib/stock-info-fields'
import type { ChartMarker, ChartPriceLine, ChartRange } from '@/components/EChartsCandlestick'
import type { IntradayMarker } from '@/components/EChartsIntraday'

/** 视图模式: 分钟(当天分钟线) / 日线 / 周线 */
export type QuantViewMode = 'minute' | 'daily' | 'weekly'

// 预设快捷范围
const PRESETS: { label: string; months: number }[] = [
  { label: '半年', months: 6 },
  { label: '1年', months: 12 },
]

const VIEW_LABEL: Record<QuantViewMode, string> = { minute: '分钟', daily: '日线', weekly: '周线' }

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

  // symbol 切换(弹窗不卸载复用)时重置视图/日期, 重新应用 initialDate
  const prevSymbol = useRef<string | null>(symbol)
  const initialApplied = useRef(false)
  useEffect(() => {
    if (prevSymbol.current === symbol) return
    prevSymbol.current = symbol
    setView(initialView)
    setSelectedDate(null)
    setDailyResult(null)
    initialApplied.current = false
  }, [symbol, initialView])

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
  const financials = useFinancialMetrics(hasFinanceField && hasFinancialCap ? symbol : undefined)

  // 分钟视图轮询偏好 (与自选股弹窗一致)
  const { data: prefs } = usePreferences()
  const { data: quoteStatus } = useQuoteStatus()
  const realtimeRunning = quoteStatus?.running ?? false
  const intradayRefreshOn = prefs?.minute_intraday_refresh ?? false
  const intradayRefetchMs = (intradayRefreshOn && realtimeRunning)
    ? (prefs?.minute_intraday_refresh_interval ?? 6) * 1000
    : undefined

  const rawRows: any[] = dailyResult?.rawRows ?? []

  // initialDate: 日K rows 就绪后优先选中 (仅应用一次, 不在 rows 内回退最新)
  useEffect(() => {
    if (initialDate && !initialApplied.current && rawRows.length > 0) {
      initialApplied.current = true
      const target = rawRows.find((r: any) => String(r.date).slice(0, 10) === initialDate)
      setSelectedDate(target ? initialDate : String(rawRows[rawRows.length - 1].date).slice(0, 10))
    }
  }, [initialDate, rawRows])

  // 分钟视图无选中日期时自动选中最新交易日
  useEffect(() => {
    if (view === 'minute' && !selectedDate && rawRows.length > 0) {
      setSelectedDate(String(rawRows[rawRows.length - 1].date).slice(0, 10))
    }
  }, [view, selectedDate, rawRows])

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
            className="relative w-[92vw] max-w-[1100px] max-h-[95vh] rounded-card border border-border bg-base shadow-2xl overflow-hidden flex flex-col"
          >
            {/* 顶栏 */}
            <div className="flex items-center justify-between px-5 py-3 border-b border-border shrink-0">
              <div className="flex items-center gap-2">
                {(() => {
                  const board = symbol ? boardTag(symbol) : null
                  return board ? (
                    <span className={`inline-flex items-center justify-center w-[18px] h-[18px] rounded text-[9px] font-bold leading-none border ${board.color}`}>
                      {board.label}
                    </span>
                  ) : null
                })()}
                <span className="font-mono text-sm font-medium text-foreground">{symbol}</span>
                {name && <span className="text-xs text-muted">{name}</span>}
              </div>

              <div className="flex items-center gap-1.5">
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

                <span className="text-muted/20 mx-0.5">|</span>

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
            <div className="flex-1 overflow-auto p-4">
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

              {view === 'daily' && (
                <StockDailyKChart
                  symbol={symbol}
                  height={420}
                  dateRange={dateRange}
                  markers={markers}
                  ranges={ranges}
                  priceLines={priceLines}
                  showLimitMarkers={showLimitMarkers}
                  showMarkerToggle={showMarkerToggle}
                  onDateClick={(d) => setSelectedDate(d)}
                  onDataChange={setDailyResult}
                  visibleBars={60}
                  extColumns={extColumns}
                />
              )}
              {view === 'weekly' && (
                <StockDailyKChart
                  symbol={symbol}
                  height={420}
                  dateRange={dateRange}
                  period="weekly"
                  showLimitMarkers={false}
                  showIndicatorControls={false}
                  onDateClick={(d) => setSelectedDate(d)}
                  onDataChange={setDailyResult}
                  visibleBars={120}
                  extColumns={extColumns}
                />
              )}
              {view === 'minute' && selectedDate && (
                <StockIntradayChart
                  symbol={symbol}
                  date={selectedDate}
                  height={420}
                  prevClose={prevClose}
                  markers={intradayMarkers}
                  refetchIntervalMs={intradayRefetchMs}
                />
              )}
              {view === 'minute' && !selectedDate && (
                <div className="h-[420px] grid place-items-center text-xs text-muted">
                  加载中…
                </div>
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
```

- [ ] **Step 2: 验证构建**

Run: `cd frontend && pnpm build`
Expected: 通过，无类型错误。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/QuantTradeDialog.tsx
git commit -m "feat(dialog): 量化专用交易弹窗 QuantTradeDialog — 单图+分钟/日线/周线切换"
```

---

### Task 4: QuantSim 换弹窗 + 回测替换 TradeKlineModal

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx`
- Modify: `frontend/src/pages/backtest/StrategyBacktest.tsx`
- Delete: `frontend/src/pages/backtest/components/TradeKlineModal.tsx`

**Interfaces:**
- Consumes: `QuantTradeDialog` + `QuantViewMode`（Task 3）。

- [ ] **Step 1: QuantSim 换用 QuantTradeDialog**

`frontend/src/quant/pages/QuantSim.tsx` 中把 `import { StockPreviewDialog } from '@/components/StockPreviewDialog'` 替换为：

```ts
import { QuantTradeDialog } from '@/components/QuantTradeDialog'
```

文件末尾的渲染（`{preview && (<StockPreviewDialog .../>)}`）替换为：

```tsx
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
```

其余（`preview` state、`parseTradeTime`/`toMarkerAction`、两表 onClick）保持不变。

- [ ] **Step 2: StrategyBacktest 替换 TradeKlineModal**

`frontend/src/pages/backtest/StrategyBacktest.tsx`：

(a) 删除第 26 行 `import { TradeKlineModal } from './components/TradeKlineModal'`，替换为：

```ts
import { QuantTradeDialog } from '@/components/QuantTradeDialog'
import type { ChartPriceLine, ChartRange } from '@/components/EChartsCandlestick'
```

（若文件已 import 这两个类型则不重复。）

(b) 检查 `fmtPrice` 是否已从 `@/lib/format` 导入（`grep -n "fmtPrice" frontend/src/pages/backtest/StrategyBacktest.tsx`）；未导入则加入 `fmtPrice`（与 TradeKlineModal 原用法一致）。

(c) 文件顶层（工具函数区）加：

```ts
function addDays(date: string, days: number): string {
  const d = new Date(date)
  d.setDate(d.getDate() + days)
  return d.toISOString().slice(0, 10)
}
```

(d) 在 `const [selectedTrade, setSelectedTrade] = useState<StrategyBacktestTrade | null>(null)`（约第 887 行）之后加：

```ts
  const tradeDateRange = useMemo(() => selectedTrade ? {
    start: addDays(String(selectedTrade.entry_date).slice(0, 10), -45),
    end: addDays(String(selectedTrade.exit_date).slice(0, 10), 20),
  } : null, [selectedTrade])

  const tradeRanges = useMemo<ChartRange[]>(() => selectedTrade ? [{
    start: String(selectedTrade.entry_date).slice(0, 10),
    end: String(selectedTrade.exit_date).slice(0, 10),
    label: '持仓区间',
    color: 'rgba(59,130,246,0.07)',
  }] : [], [selectedTrade])

  const tradePriceLines = useMemo<ChartPriceLine[]>(() => {
    if (!selectedTrade) return []
    const start = String(selectedTrade.entry_date).slice(0, 10)
    const end = String(selectedTrade.exit_date).slice(0, 10)
    return [
      {
        value: Number(selectedTrade.entry_price),
        label: `买入价 ${fmtPrice(selectedTrade.entry_price)}`,
        color: '#C74040',
        start,
        end,
      },
      {
        value: Number(selectedTrade.exit_price),
        label: `卖出价 ${fmtPrice(selectedTrade.exit_price)}`,
        color: '#2D9B65',
        start,
        end,
      },
    ]
  }, [selectedTrade])
```

(e) 文件末尾 `<TradeKlineModal trade={selectedTrade} onClose={() => setSelectedTrade(null)} />`（约第 2593 行）替换为：

```tsx
      <QuantTradeDialog
        symbol={selectedTrade?.symbol ?? null}
        name={selectedTrade?.name}
        initialView="daily"
        dateRange={tradeDateRange ?? undefined}
        ranges={tradeRanges}
        priceLines={tradePriceLines}
        showLimitMarkers={false}
        showMarkerToggle={false}
        onClose={() => setSelectedTrade(null)}
      />
```

- [ ] **Step 3: 删除 TradeKlineModal**

```bash
git rm frontend/src/pages/backtest/components/TradeKlineModal.tsx
```

- [ ] **Step 4: 验证构建**

Run: `cd frontend && pnpm build`
Expected: 通过，无类型错误（此步开始不再引用已删 props，全链路类型检查恢复完整）。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/quant/pages/QuantSim.tsx frontend/src/pages/backtest/StrategyBacktest.tsx frontend/src/pages/backtest/components/TradeKlineModal.tsx
git commit -m "feat(quant): 模拟盘/回测改用 QuantTradeDialog, 移除 TradeKlineModal"
```

---

### Task 5: 最终验证与手动冒烟

**Files:** 无改动。

- [ ] **Step 1: 全量类型检查与构建**

Run: `cd frontend && pnpm build`
Expected: 通过。

- [ ] **Step 2: 手动冒烟清单**

启动 `./dev.sh`，打开 http://localhost:3011：

1. **量化模拟盘**：进入有成交的账户详情。
   - 点持仓行 → 弹窗默认**分钟视图**（最新交易日分钟线），无左右分窗；点「日线」/「周线」正常渲染；切回「分钟」显示所选日分钟线。
   - 日线视图点选买入日蜡烛 → 切「分钟」→ 出现红色 B 上三角。
   - 点成交记录 BUY 行 → 交易当日分钟线 + 红色 B；SELL 行 → 绿色 S；STOP_LOSS 行 → 橙色「止损」。
   - 弹窗开着点另一行 → 视图/日期正确重置。
2. **量化回测**：跑出成交后点击某笔成交 → 弹窗日线视图含买入价/卖出价虚线与持仓区间底色，**无**涨跌停板标；切「分钟」/「周线」正常。
3. **自选股回归**：打开弹窗 → 默认仅日K全宽；点头部「分时」→ 左右分窗出现（与改动前一致）；加自选/加监控/日期范围正常。
4. 周线聚合目视：周K 蜡烛高低/收盘与日K 周内走势一致，周均线连续。
