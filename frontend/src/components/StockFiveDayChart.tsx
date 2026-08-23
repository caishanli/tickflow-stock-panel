import { useEffect, useRef } from 'react'
import * as echarts from 'echarts'
import type { ECharts, EChartsOption } from 'echarts'
import { useQuery } from '@tanstack/react-query'
import { api, type MinuteKlineRow } from '@/lib/api'
import { FULL_DAY_TIMES, type IntradayMarker } from '@/components/EChartsIntraday'
import { useChartTheme, type ChartTheme } from '@/lib/theme'

interface Props {
  symbol: string
  /** 最近 N 个交易日(升序), 取自日K rawRows 尾部 */
  dates: string[]
  prevClose?: number
  height?: number
  markers?: IntradayMarker[]
}

const DAY_POINTS = FULL_DAY_TIMES.length

function fmtTime(dt: string): string {
  const m = dt.match(/(\d{2}):(\d{2})/)
  if (!m) return dt.slice(11, 16)
  const h = (parseInt(m[1]) + 8) % 24
  return `${String(h).padStart(2, '0')}:${m[2]}`
}

function isValidPrice(v: number | null | undefined): v is number {
  return typeof v === 'number' && Number.isFinite(v) && v > 0
}

/** 五日分时: 近 N 个交易日分钟线拼接, 买卖点同款「短竖虚线+背景矩形徽标」 */
export function StockFiveDayChart({ symbol, dates, prevClose, height = 420, markers }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<ECharts | null>(null)
  const roRef = useRef<ResizeObserver | null>(null)
  const ct = useChartTheme()

  const days = useQuery({
    queryKey: ['kline-minute-fiveday', symbol, dates.join(',')],
    enabled: !!symbol && dates.length > 0,
    queryFn: async () => {
      const res = await Promise.all(dates.map(d => api.klineMinute(symbol, d, 'stockdata')))
      return res.map(r => r.rows ?? [])
    },
  })

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    let chart = chartRef.current
    if (!chart) {
      chart = echarts.init(el)
      chartRef.current = chart
      roRef.current = new ResizeObserver(() => chart!.resize())
      roRef.current.observe(el)
    }
    const rowsPerDay = days.data
    if (!rowsPerDay || rowsPerDay.length === 0) {
      chart.clear()
      return
    }
    chart.setOption(buildOption(rowsPerDay, dates, prevClose, markers, ct), true)
  }, [days.data, dates, prevClose, markers, ct])

  useEffect(() => {
    return () => {
      roRef.current?.disconnect()
      chartRef.current?.dispose()
      chartRef.current = null
      roRef.current = null
    }
  }, [])

  return (
    <div className="w-full">
      {days.isLoading && <div className="text-xs text-muted py-2">五日分时加载中…</div>}
      <div ref={containerRef} className="w-full" style={{ height: height - (days.isLoading ? 30 : 0), display: days.isLoading ? 'none' : undefined }} />
    </div>
  )
}

function buildOption(daysRows: MinuteKlineRow[][], dates: string[], prevClose: number | undefined, markers: IntradayMarker[] | undefined, ct: ChartTheme): EChartsOption {
  const timeIndexMap = new Map(FULL_DAY_TIMES.map((t, i) => [t, i]))
  const n = daysRows.length
  const total = n * DAY_POINTS

  const closes: (number | null)[] = new Array(total).fill(null)
  const lows: (number | null)[] = new Array(total).fill(null)
  const highs: (number | null)[] = new Array(total).fill(null)
  const vols: any[] = new Array(total).fill(null)

  daysRows.forEach((rows, d) => {
    const base = d * DAY_POINTS
    for (const r of rows) {
      const i = timeIndexMap.get(fmtTime(r.datetime))
      if (i === undefined) continue
      const idx = base + i
      closes[idx] = r.close
      lows[idx] = r.low
      highs[idx] = r.high
      vols[idx] = { value: r.volume, itemStyle: { color: r.close > r.open ? 'rgba(240,68,56,0.6)' : 'rgba(18,183,106,0.6)' } }
    }
  })

  // 全窗口极值 → 自适应范围 + 虚线长度基准
  let lo: number | null = null
  let hi: number | null = null
  for (const arr of [lows, highs]) {
    for (const v of arr) {
      if (!isValidPrice(v)) continue
      if (lo == null || v < lo) lo = v
      if (hi == null || v > hi) hi = v
    }
  }
  const span = lo != null && hi != null && hi > lo ? hi - lo : 0
  const pad = Math.max(span * 0.08, isValidPrice(prevClose) ? prevClose * 0.004 : 0)
  const yMin = lo != null ? lo - pad : undefined
  const yMax = hi != null ? hi + pad : undefined

  // 买卖点徽标 + 短竖虚线
  const markPoints: any[] = []
  const markLines: any[] = []
  for (const m of markers ?? []) {
    const d = dates.indexOf(m.date)
    if (d < 0) continue
    const mi = timeIndexMap.get(m.time)
    if (mi === undefined) continue
    const idx = d * DAY_POINTS + mi
    if (!isValidPrice(closes[idx])) continue
    const stop = m.action === 'STOP_LOSS'
    const buy = m.action === 'BUY'
    const color = stop ? '#F59E0B' : buy ? '#C74040' : '#2D9B65'
    const anchor = stop || !buy ? (highs[idx] ?? m.price) : (lows[idx] ?? m.price)
    const dash = span > 0 ? span * 0.16 : Math.abs(m.price) * 0.004
    const end = buy && !stop ? anchor - dash : anchor + dash
    markLines.push([
      { coord: [idx, anchor] },
      { coord: [idx, end], lineStyle: { color, type: 'dashed', width: 1 } },
    ])
    markPoints.push({
      coord: [idx, end],
      symbol: 'circle', symbolSize: 3,
      itemStyle: { color },
      label: {
        show: true, formatter: stop ? '止损' : buy ? 'B' : 'S',
        position: buy && !stop ? 'bottom' : 'top', distance: 2,
        color: '#FFFFFF', backgroundColor: color,
        padding: [3, 5], borderRadius: 3,
        fontSize: 10, fontWeight: 'bold',
        fontFamily: 'JetBrains Mono, monospace',
      },
      z: 100, zlevel: 10,
    })
  }

  // x 轴标签: 每个交易日起点标 MM-DD; 日界竖分隔线
  const xAxisData: string[] = new Array(total).fill('')
  for (let d = 0; d < n; d++) xAxisData[d * DAY_POINTS] = `${d + 1}`

  const lastValid = [...closes].reverse().find(v => isValidPrice(v)) ?? null
  const up = isValidPrice(prevClose) && isValidPrice(lastValid) ? lastValid >= prevClose : true
  const lineColor = up ? '#C74040' : '#2D9B65'
  const areaColor = up ? 'rgba(199,64,64,0.28)' : 'rgba(34,197,94,0.28)'

  return {
    animation: false,
    backgroundColor: 'transparent',
    tooltip: { show: false },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    dataZoom: [{
      type: 'inside', xAxisIndex: [0], start: 0, end: 100,
      moveOnMouseMove: true, zoomOnMouseWheel: true, filterMode: 'none',
    }],
    grid: [
      { left: 60, right: 55, top: 16, bottom: '26%' },
      { left: 60, right: 55, top: '78%', bottom: 20 },
    ],
    xAxis: [
      {
        type: 'category', data: xAxisData, boundaryGap: false,
        axisLine: { show: false }, axisTick: { show: false },
        axisLabel: {
          color: ct.text, fontSize: 10, fontFamily: 'JetBrains Mono, monospace',
          interval: (idx: number) => idx % DAY_POINTS === 0,
          formatter: (_v: string, idx: number) => {
            const d = Math.floor(idx / DAY_POINTS)
            return idx % DAY_POINTS === 0 && dates[d] ? dates[d].slice(5) : ''
          },
        },
        splitLine: { show: true, interval: (idx: number) => idx > 0 && idx % DAY_POINTS === 0, lineStyle: { color: ct.grid } },
      },
      {
        type: 'category', gridIndex: 1, data: xAxisData, boundaryGap: false,
        axisLine: { show: false }, axisTick: { show: false }, axisLabel: { show: false }, splitLine: { show: false },
      },
    ],
    yAxis: [
      { type: 'value', min: yMin, max: yMax, scale: true, splitLine: { lineStyle: { color: ct.grid } }, axisLabel: { color: ct.text, fontSize: 10, fontFamily: 'JetBrains Mono, monospace', formatter: (v: number) => v.toFixed(2) } },
      { type: 'value', gridIndex: 1, splitLine: { show: false }, axisLabel: { show: false } },
      ...(isValidPrice(prevClose) && yMin != null && yMax != null ? [{
        type: 'value' as const, position: 'right' as const, gridIndex: 0, min: yMin, max: yMax,
        splitLine: { show: false }, axisLabel: {
          color: ct.text, fontSize: 10, fontFamily: 'JetBrains Mono, monospace',
          formatter: (v: number) => {
            const pct = (v - (prevClose as number)) / (prevClose as number) * 100
            if (Math.abs(pct) < 0.01) return '0.00%'
            return (pct > 0 ? '+' : '') + pct.toFixed(2) + '%'
          },
        },
      }] : []),
    ],
    series: [
      {
        name: '价格', type: 'line', data: closes, symbol: 'none', cursor: 'crosshair',
        lineStyle: { width: 1.1, color: lineColor }, connectNulls: false,
        areaStyle: { color: areaColor },
        markPoint: markPoints.length > 0 ? { data: markPoints, animation: false, silent: true } : undefined,
        markLine: markLines.length > 0 ? { symbol: 'none', data: markLines, animation: false, silent: true, zlevel: 10 } : undefined,
      },
      { name: '成交量', type: 'bar', xAxisIndex: 1, yAxisIndex: 1, data: vols, cursor: 'crosshair' },
    ],
  }
}
