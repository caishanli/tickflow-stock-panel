import { useEffect, useMemo, useRef, useState } from 'react'
import * as echarts from 'echarts'
import type { ECharts, EChartsOption } from 'echarts'
import type { MinuteKlineRow, PriceLimitInfo } from '@/lib/api'
import { useChartTheme, type ChartTheme } from '@/lib/theme'

type YMode = 'adaptive' | 'limit'

// 序列颜色 (双主题通用); 画布轴/网格/十字线等主题相关色走 ChartTheme
const THEME = {
  line: '#3B82F6',
  areaFill: 'rgba(59,130,246,0.40)',
  avgLine: '#F59E0B',
  volUp: 'rgba(240,68,56,0.6)',
  volDown: 'rgba(18,183,106,0.6)',
}

export interface IntradayMarker {
  /** 标记所属交易日 YYYY-MM-DD，仅在分时显示该日时渲染 */
  date: string
  /** 交易时刻 HH:MM（本地时间，直接定位全天 242 点时间轴） */
  time: string
  /** 成交价（持仓行用成本价） */
  price: number
  action: 'BUY' | 'SELL' | 'STOP_LOSS'
}

interface Props {
  data: MinuteKlineRow[]
  height?: number
  prevClose?: number
  date?: string
  priceLimit?: PriceLimitInfo
  onPriceHover?: (price: number | null) => void
  showLimitLines?: boolean
  showAvgLine?: boolean
  markers?: IntradayMarker[]
  /** 允许「涨跌停」±10% 纵轴模式 (默认 true; 量化弹窗传 false 锁定自适应放大) */
  allowLimitMode?: boolean
}

function fmtTime(dt: string): string {
  const match = dt.match(/(\d{2}):(\d{2})/)
  if (!match) return dt.slice(11, 16)
  const h = (parseInt(match[1]) + 8) % 24
  return `${String(h).padStart(2, '0')}:${match[2]}`
}

/** 探测分钟数据 volume 单位: 股(×1) / 手(×100)。
 *  stockdata 与本地 mootdx parquet 为股; TickFlow SDK vol 疑似手。
 *  依据 amount ≈ volume(股)×price: median(amount/volume) ≈ 100×median(close) 判为手。 */
function detectVolumeMultiplier(data: MinuteKlineRow[]): number {
  const ratios: number[] = []
  const closes: number[] = []
  for (const d of data) {
    if (!(d.volume > 0) || !(d.close > 0)) continue
    ratios.push(d.amount / d.volume)
    closes.push(d.close)
    if (ratios.length >= 60) break
  }
  if (ratios.length === 0) return 1
  const med = (arr: number[]) => {
    const s = [...arr].sort((a, b) => a - b)
    return s[Math.floor(s.length / 2)]
  }
  const ratio = med(ratios) / med(closes)
  return ratio >= 30 && ratio <= 300 ? 100 : 1
}

function computeAvgPrice(data: MinuteKlineRow[]): number[] {
  // 分时均线 = 累计成交额 / 累计成交量(单位自适应: 股×1 / 手×100)
  const mult = detectVolumeMultiplier(data)
  const result: number[] = []
  let sumAmt = 0
  let sumVol = 0
  for (const d of data) {
    sumAmt += d.amount
    sumVol += d.volume * mult
    result.push(sumVol > 0 ? sumAmt / sumVol : d.close)
  }
  return result
}

function fmtAmt(v: number): string {
  if (v >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(2)}亿`
  if (v >= 10_000) return `${(v / 10_000).toFixed(0)}万`
  return v.toFixed(0)
}

/** 将买卖标记映射到全日时间轴: 仅保留与 chartDate 相同的标记, 且该分钟有真实成交。
 *  样式: 从该分钟 high/low 引一条短竖虚线, 末端挂背景色矩形徽标 B/S/止损。zlevel 置顶。 */
function buildMarkerPoints(
  markers: IntradayMarker[] | undefined,
  chartDate: string | undefined,
  closes: (number | null)[],
  lows: (number | null)[],
  highs: (number | null)[],
  timeIndexMap: Map<string, number>,
): { points: any[]; lines: any[] } {
  const points: any[] = []
  const lines: any[] = []
  if (!markers || markers.length === 0) return { points, lines }
  // 用当日有效高低极值估虚线长度(约为振幅的 1/6)
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
  for (const m of markers) {
    if (m.date !== chartDate) continue
    const idx = timeIndexMap.get(m.time)
    if (idx === undefined || !isValidPrice(closes[idx])) continue
    const stop = m.action === 'STOP_LOSS'
    const buy = m.action === 'BUY'
    const color = stop ? '#F59E0B' : buy ? '#C74040' : '#2D9B65'
    const text = stop ? '止损' : buy ? 'B' : 'S'
    const anchor = stop || !buy ? (highs[idx] ?? m.price) : (lows[idx] ?? m.price)
    const dash = span > 0 ? span * 0.16 : Math.abs(m.price) * 0.004
    // 短竖虚线: 买入向下、卖出/止损向上 (两点线段 = 数组包两个点对象)
    const end = buy && !stop ? anchor - dash : anchor + dash
    lines.push([
      { coord: [idx, anchor] },
      { coord: [idx, end], lineStyle: { color, type: 'dashed', width: 1 } },
    ])
    // 背景色矩形徽标挂在虚线末端
    points.push({
      coord: [idx, end],
      symbol: 'circle', symbolSize: 3,
      itemStyle: { color },
      label: {
        show: true, formatter: text,
        position: buy && !stop ? 'bottom' : 'top', distance: 2,
        color: '#FFFFFF', backgroundColor: color,
        padding: [3, 5], borderRadius: 3,
        fontSize: 10, fontWeight: 'bold',
        fontFamily: 'JetBrains Mono, monospace',
      },
      z: 100, zlevel: 10,
    })
  }
  return { points, lines }
}

function isValidPrice(v: number | null | undefined): v is number {
  return typeof v === 'number' && Number.isFinite(v) && v > 0
}

/** 生成全天分时时间刻度 9:30 ~ 11:30, 13:00 ~ 15:00, 每分钟一个点 (共242个) */
function generateFullDayTimes(): string[] {
  const times: string[] = []
  // 上午 9:30 ~ 11:30 (121 分钟)
  for (let h = 9; h <= 11; h++) {
    const startM = h === 9 ? 30 : 0
    const endM = h === 11 ? 30 : 59
    for (let m = startM; m <= endM; m++) {
      times.push(`${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`)
    }
  }
  // 下午 13:00 ~ 15:00 (121 分钟)
  for (let h = 13; h <= 15; h++) {
    const endM = h === 15 ? 0 : 59
    for (let m = 0; m <= endM; m++) {
      times.push(`${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`)
    }
  }
  return times
}

const FULL_DAY_TIMES = generateFullDayTimes()

/** 计算实际涨跌停价 (四舍五入到2位小数) 和实际涨跌停幅度 */
function getLimitPrices(prevClose: number, priceLimit?: PriceLimitInfo): {
  limitUp: number      // 涨停价 (四舍五入)
  limitDown: number    // 跌停价 (四舍五入)
  upPct: number        // 实际涨停幅度 (如 9.97)
  downPct: number      // 实际跌停幅度 (如 -9.97)
} {
  const pct = priceLimit && Number.isFinite(priceLimit.rate) ? priceLimit.rate : 0.10
  const rawUp = prevClose * (1 + pct)
  const rawDown = prevClose * (1 - pct)
  // A股涨跌停价四舍五入到分 (2位小数)
  const limitUp = isValidPrice(priceLimit?.limit_up)
    ? priceLimit.limit_up
    : Math.round(rawUp * 100) / 100
  const limitDown = isValidPrice(priceLimit?.limit_down)
    ? priceLimit.limit_down
    : Math.round(rawDown * 100) / 100
  const upPct = (limitUp - prevClose) / prevClose * 100
  const downPct = (limitDown - prevClose) / prevClose * 100
  return { limitUp, limitDown, upPct, downPct }
}

function buildOption(data: MinuteKlineRow[], prevClose: number | undefined, avgPrices: number[], lineColor: string, areaColor: string, yMode: YMode, ct: ChartTheme, priceLimit?: PriceLimitInfo, showLimitLines = true, showAvgLine = true, chartDate?: string, markers?: IntradayMarker[]): EChartsOption {
  // 将数据映射到全天时间轴上的正确位置
  const timeIndexMap = new Map(FULL_DAY_TIMES.map((t, i) => [t, i]))
  const closes = new Array(FULL_DAY_TIMES.length).fill(null) as (number | null)[]
  const highs = new Array(FULL_DAY_TIMES.length).fill(null) as (number | null)[]
  const lows = new Array(FULL_DAY_TIMES.length).fill(null) as (number | null)[]
  const avgData = new Array(FULL_DAY_TIMES.length).fill(null) as (number | null)[]
  const volumes = new Array(FULL_DAY_TIMES.length).fill(null) as (any | null)[]

  const volNeutral = 'rgba(161,161,170,0.5)'
  for (let i = 0; i < data.length; i++) {
    const timeKey = fmtTime(data[i].datetime)
    const idx = timeIndexMap.get(timeKey)
    if (idx !== undefined) {
      closes[idx] = data[i].close
      highs[idx] = data[i].high
      lows[idx] = data[i].low
      avgData[idx] = avgPrices[i]
      volumes[idx] = {
        value: data[i].volume,
        itemStyle: {
          color: data[i].close > data[i].open ? THEME.volUp : data[i].close < data[i].open ? THEME.volDown : volNeutral,
        },
      }
    }
  }

  const { points: markerPoints, lines: markerLines } = buildMarkerPoints(markers, chartDate, closes, lows, highs, timeIndexMap)

  const areaStyle: any = {
    color: {
      type: 'linear',
      x: 0, y: 0, x2: 0, y2: 1,
      colorStops: [
        { offset: 0, color: areaColor },
        { offset: 1, color: 'rgba(0,0,0,0)' },
      ],
    },
  }

  const markLineData: any[] = []
  if (prevClose != null) {
    markLineData.push({
      yAxis: prevClose,
      lineStyle: { color: ct.crosshair, type: 'dashed', width: 1 },
      label: { show: false },
      symbol: 'none',
    })
  }

  let yMin: number | undefined
  let yMax: number | undefined
  let yInterval: number | undefined
  let maxDiff = 0
  if (isValidPrice(prevClose) && data.length > 0) {
    // 均价线恒在 [minLow, maxHigh] 内, 不参与范围计算(免疫均价单位错误, 范围贴合实际波动)
    const priceArrays = [closes, highs, lows]
    for (const arr of priceArrays) {
      for (const v of arr) {
        if (!isValidPrice(v)) continue
        const diff = Math.abs(v - prevClose)
        if (diff > maxDiff) maxDiff = diff
      }
    }

    if (showLimitLines && yMode === 'limit') {
      const { limitUp, limitDown } = getLimitPrices(prevClose, priceLimit)
      const limitDiffUp = limitUp - prevClose
      const limitDiffDown = prevClose - limitDown
      const limitDiff = Math.max(limitDiffUp, limitDiffDown)
      // 涨跌停模式: Y 轴按实际涨跌停价
      maxDiff = limitDiff
      yMin = prevClose - maxDiff
      yMax = prevClose + maxDiff
      yInterval = maxDiff
      // 加 markLine 标注涨停价和跌停价 (仅虚线, 不显示文字)
      markLineData.push(
        {
          yAxis: limitUp,
          lineStyle: { color: 'rgba(199,64,64,0.4)', type: 'dashed', width: 1 },
          label: { show: false },
          symbol: 'none',
        },
        {
          yAxis: limitDown,
          lineStyle: { color: 'rgba(45,155,101,0.4)', type: 'dashed', width: 1 },
          label: { show: false },
          symbol: 'none',
        },
      )
    } else {
      // 自适应模式: Y 轴贴合当日实际高低点, 上下各留 15% 边距——单边行情(如 +1%~+3%)不强制显示昨收对侧区域
      let lo: number | null = null
      let hi: number | null = null
      for (const v of lows) if (isValidPrice(v) && (lo == null || v < lo)) lo = v
      for (const v of highs) if (isValidPrice(v) && (hi == null || v > hi)) hi = v
      if (lo != null && hi != null && isValidPrice(prevClose)) {
        const span = hi - lo
        // 至少保证可视范围(防零波动过度放大); 指数地板更紧, 否则低波动指数会被压成横线
        const pad = Math.max(span * 0.15, showLimitLines ? prevClose * 0.004 : prevClose * 0.001)
        yMin = lo - pad
        yMax = hi + pad
        yInterval = (hi - lo) / 2 + pad
      }
    }
  }

  // x 轴标签: 9:30, 10:30, 11:30/13:00, 14:00, 15:00
  // 11:30(idx 120) 和 13:00(idx 121) 相邻会重叠, 合并为一个标签
  const xAxisLabelMap: Record<number, string> = {
    0: '9:30',
    60: '10:30',
    120: '11:30/13:00',
    181: '14:00',
    241: '15:00',
  }
  const xAxisLabelFormatter = (_value: string, idx: number) => {
    return xAxisLabelMap[idx] ?? ''
  }

  return {
    animation: false,
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'transparent',
      borderWidth: 0,
      textStyle: { fontSize: 0 },
      formatter: () => '',
      axisPointer: {
        type: 'cross',
        label: {
          show: true,
          backgroundColor: ct.tooltipBg,
          borderColor: ct.tooltipBorder,
          borderWidth: 1,
          padding: [2, 5],
          color: ct.tooltipText,
          fontSize: 10,
          fontFamily: 'JetBrains Mono, monospace',
        },
        crossStyle: { color: ct.crosshair, type: 'dashed', width: 1 },
        lineStyle: { color: ct.crosshair, type: 'dashed', width: 1 },
      },
    },
    axisPointer: {
      link: [{ xAxisIndex: 'all' }],
    },
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: [0, 1],
        start: 0,
        end: 100,
        moveOnMouseMove: true,
        zoomOnMouseWheel: true,
        filterMode: 'none',
      },
    ],
    grid: [
      { left: 60, right: 55, top: 24, bottom: '28%' },
      { left: 60, right: 55, top: '74%', bottom: 20 },
    ],
    xAxis: [
      {
        type: 'category',
        data: FULL_DAY_TIMES,
        boundaryGap: false,
        axisPointer: {
          show: true,
          lineStyle: { color: ct.crosshair, type: 'dashed', width: 1 },
          label: {
            show: true,
            backgroundColor: ct.tooltipBg,
            borderColor: ct.tooltipBorder,
            borderWidth: 1,
            padding: [2, 4],
            color: ct.tooltipText,
            fontSize: 10,
            fontFamily: 'JetBrains Mono, monospace',
            formatter: (params: any) => {
              return params.value ?? ''
            },
          },
        },
        axisLine: { show: false },
        axisLabel: {
          color: ct.text,
          fontSize: 10,
          fontFamily: 'JetBrains Mono, monospace',
          formatter: xAxisLabelFormatter,
          interval: 0,
        },
        axisTick: { show: false },
        splitLine: {
          show: true,
          lineStyle: { color: ct.grid },
        },
      },
      {
        type: 'category',
        gridIndex: 1,
        data: FULL_DAY_TIMES,
        boundaryGap: false,
        axisLine: { show: false },
        axisLabel: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
      },
    ],
    yAxis: [
      {
        type: 'value',
        min: yMin,
        max: yMax,
        interval: yInterval,
        splitArea: { show: false },
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { lineStyle: { color: ct.grid } },
        axisPointer: {
          label: {
            formatter: (params: any) => {
              const v = params.value
              return typeof v === 'number' ? v.toFixed(2) : ''
            },
          },
        },
        axisLabel: {
          color: ct.text,
          fontSize: 10,
          fontFamily: 'JetBrains Mono, monospace',
          formatter: (v: number) => v.toFixed(2),
        },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
      },
      ...(isValidPrice(prevClose) && yMin != null && yMax != null ? [{
        type: 'value' as const,
        position: 'right' as const,
        gridIndex: 0,
        min: yMin,
        max: yMax,
        interval: yInterval,
        splitArea: { show: false },
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisPointer: {
          label: {
            formatter: (params: any) => {
              const v = params.value
              if (typeof v !== 'number') return ''
              const pct = (v - prevClose) / prevClose * 100
              if (Math.abs(pct) < 0.01) return '0.00%'
              return (pct > 0 ? '+' : '') + pct.toFixed(2) + '%'
            },
          },
        },
        axisLabel: {
          color: ct.text,
          fontSize: 10,
          fontFamily: 'JetBrains Mono, monospace',
          formatter: (v: number) => {
            const pct = (v - prevClose) / prevClose * 100
            if (Math.abs(pct) < 0.01) return '0.00%'
            return (pct > 0 ? '+' : '') + pct.toFixed(2) + '%'
          },
        },
      }] : []),
    ],
    series: [
      {
        name: '价格',
        type: 'line',
        data: closes,
        smooth: false,
        symbol: 'none',
        cursor: 'crosshair',
        lineStyle: { width: 1.2, color: lineColor },
        areaStyle,
        connectNulls: true,
        markLine: (markLineData.length + markerLines.length) > 0 ? { symbol: 'none', data: [...markerLines, ...markLineData], animation: false, silent: true, zlevel: 10 } : undefined,
        markPoint: markerPoints.length > 0 ? { data: markerPoints, animation: false, silent: true } : undefined,
      },
      ...(showAvgLine ? [{
        name: '均价',
        type: 'line' as const,
        data: avgData,
        smooth: false,
        symbol: 'none',
        cursor: 'crosshair',
        lineStyle: { width: 1, color: THEME.avgLine },
        connectNulls: true,
      }] : []),
      {
        name: '成交量',
        type: 'bar',
        data: volumes,
        xAxisIndex: 1,
        yAxisIndex: 1,
        cursor: 'crosshair',
      },
    ],
  }
}

export function EChartsIntraday({ data, height = 320, prevClose, date, priceLimit, onPriceHover, showLimitLines = true, showAvgLine = true, markers, allowLimitMode = true }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<ECharts | null>(null)
  const roRef = useRef<ResizeObserver | null>(null)
  const moRef = useRef<MutationObserver | null>(null)
  const dataRef = useRef(data)
  dataRef.current = data
  const onPriceHoverRef = useRef(onPriceHover)
  onPriceHoverRef.current = onPriceHover
  // 全日索引 → 数据数组索引 的映射 (ref 避免重建 chart)
  const fullDayToDataIdx = useRef<Map<number, number>>(new Map())

  const [infoIdx, setInfoIdx] = useState(data.length - 1)
  const [yMode, setYMode] = useState<YMode>('adaptive')
  // allowLimitMode=false (量化弹窗) 时锁定自适应: ±10% 涨跌停模式会压平低波动标的分时线
  const effectiveYMode: YMode = allowLimitMode ? yMode : 'adaptive'
  const ct = useChartTheme()
  const avgPrices = useMemo(() => computeAvgPrice(data), [data])

  // 分时线颜色：基于最新价 vs 昨收
  const lastClose = data.length > 0 ? data[data.length - 1].close : null
  const lineIsUp = lastClose != null && prevClose != null ? lastClose > prevClose : true
  const lineIsFlat = lastClose != null && prevClose != null ? lastClose === prevClose : false
  const lineColor = lineIsFlat ? '#A1A1AA' : lineIsUp ? '#C74040' : '#2D9B65'
  const areaFill = lineIsFlat ? 'rgba(180,180,190,0.40)' : lineIsUp ? 'rgba(199,64,64,0.40)' : 'rgba(34,197,94,0.40)'

  useEffect(() => {
    setInfoIdx(data.length - 1)
  }, [data.length])

  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    let chart = chartRef.current
    if (!chart) {
      chart = echarts.init(el, undefined, { renderer: 'canvas' })
      chartRef.current = chart
      // 强制 canvas 使用十字光标，覆盖 ECharts 默认的 pointer
      const forceCursor = () => {
        const canvases = el.querySelectorAll('canvas')
        canvases.forEach(c => { c.style.setProperty('cursor', 'crosshair', 'important') })
      }
      forceCursor()
      // MutationObserver: ECharts 内部可能重建/修改 canvas 属性，持续强制 cursor
      const mo = new MutationObserver(forceCursor)
      mo.observe(el, { childList: true, subtree: true, attributes: true, attributeFilter: ['style', 'class'] })
      moRef.current = mo
      roRef.current = new ResizeObserver(() => {
        chart!.resize()
        forceCursor()
      })
      roRef.current.observe(el)

      chart.on('updateAxisPointer', (event: any) => {
        const axesInfo = event.axesInfo
        if (!axesInfo) return
        for (const info of Object.values(axesInfo)) {
          const val = (info as any)?.value
          if (val == null) continue
          const fullDayIdx = typeof val === 'number' ? val : -1
          if (fullDayIdx >= 0) {
            const dataIdx = fullDayToDataIdx.current.get(fullDayIdx) ?? -1
            setInfoIdx(dataIdx)
            const d = dataRef.current
            if (dataIdx >= 0 && dataIdx < d.length) {
              onPriceHoverRef.current?.(d[dataIdx].close)
            }
            return
          }
        }
      })

      chart.on('globalout', () => {
        onPriceHoverRef.current?.(null)
      })
    }

    if (data.length > 0) {
      // 构建全日索引 → 数据索引 的映射
      const timeIndexMap = new Map(FULL_DAY_TIMES.map((t, i) => [t, i]))
      const mapping = new Map<number, number>()
      for (let i = 0; i < data.length; i++) {
        const timeKey = fmtTime(data[i].datetime)
        const fullDayIdx = timeIndexMap.get(timeKey)
        if (fullDayIdx !== undefined) {
          mapping.set(fullDayIdx, i)
        }
      }
      fullDayToDataIdx.current = mapping

      // replaceMerge series: 保留 dataZoom 内部状态(用户缩放位置), 且规避 notMerge 全量重建
      // 反复拆装 InsideZoom 触发的 echarts "_ec_inner" 崩溃
      chart.setOption(buildOption(data, prevClose, avgPrices, lineColor, areaFill, effectiveYMode, ct, priceLimit, showLimitLines, showAvgLine, date, markers), { replaceMerge: ['series'] })
    } else {
      chart.clear()
    }
  }, [data, prevClose, height, lineColor, areaFill, effectiveYMode, ct, priceLimit, showLimitLines, showAvgLine, date, markers])

  useEffect(() => {
    return () => {
      chartRef.current?.off('updateAxisPointer')
      chartRef.current?.off('globalout')
      moRef.current?.disconnect()
      roRef.current?.disconnect()
      chartRef.current?.dispose()
      chartRef.current = null
      moRef.current = null
      roRef.current = null
    }
  }, [])

  const d = infoIdx >= 0 && infoIdx < data.length ? data[infoIdx] : null
  const avg = d != null ? avgPrices[infoIdx] : null
  const chg = d && prevClose != null ? d.close - prevClose : null
  const isUp = chg != null ? chg > 0 : true
  const isFlat = chg != null ? chg === 0 : false
  const priceClr = isFlat ? '#A1A1AA' : isUp ? '#C74040' : '#2D9B65'

  return (
    <div className="w-full">
      {/* 按钮行: 切换式按钮组, 居右 (量化弹窗 allowLimitMode=false 隐藏涨跌停, 锁定自适应) */}
      {showLimitLines && allowLimitMode && <div className="flex items-center justify-end px-1 pb-0.5">
        <div className="inline-flex items-center rounded bg-elevated overflow-hidden">
          <button
            onClick={() => setYMode('adaptive')}
            className={`px-2.5 py-0.5 text-[10px] font-mono cursor-pointer transition-colors ${
              effectiveYMode === 'adaptive'
                ? 'bg-accent/20 text-accent'
                : 'text-muted hover:text-secondary'
            }`}
          >
            自适应
          </button>
          <div className="w-px h-3 bg-border/40" />
          <button
            onClick={() => setYMode('limit')}
            className={`px-2.5 py-0.5 text-[10px] font-mono cursor-pointer transition-colors ${
              effectiveYMode === 'limit'
                ? 'bg-accent/20 text-accent'
                : 'text-muted hover:text-secondary'
            }`}
          >
            涨跌停
          </button>
        </div>
      </div>}
      <div style={{ backgroundColor: ct.infoBarBg }}>
        {/* 第一行: 日期 + OHLC */}
        <div className="flex items-center gap-x-2 px-2 font-mono text-[11px] select-none flex-wrap" style={{ height: 20 }}>
          {!d && <span className="text-muted">—</span>}
          {d && (
            <>
              {date && <span className="text-muted">{date}</span>}
              <span className="text-muted">开</span>
              <span style={{ color: priceClr }}>{d.open.toFixed(2)}</span>
              <span className="text-muted">高</span>
              <span style={{ color: priceClr }}>{d.high.toFixed(2)}</span>
              <span className="text-muted">低</span>
              <span style={{ color: priceClr }}>{d.low.toFixed(2)}</span>
              <span className="text-muted">收</span>
              <span style={{ color: priceClr }} className="font-semibold">{d.close.toFixed(2)}</span>
            </>
          )}
        </div>
        {/* 第二行: 价格+均价+量+额 */}
        <div className="flex items-center gap-x-4 px-2 font-mono text-[11px] select-none" style={{ height: 20 }}>
          {d && (
            <>
              <span className="flex items-center gap-x-1">
                <span style={{ display: 'inline-block', width: 14, height: 2, background: priceClr }} />
                <span style={{ color: priceClr }}>{d.close.toFixed(2)}</span>
              </span>
              {showAvgLine && <span className="flex items-center gap-x-1">
                <span style={{ display: 'inline-block', width: 14, height: 2, background: THEME.avgLine }} />
                <span style={{ color: THEME.avgLine }}>{avg?.toFixed(2)}</span>
              </span>}
              <span className="text-muted">量</span>
              <span className="text-secondary">{d.volume.toFixed(0)}</span>
              <span className="text-muted">额</span>
              <span className="text-secondary">{fmtAmt(d.amount)}</span>
            </>
          )}
        </div>
      </div>
      <div ref={containerRef} className="w-full" style={{ height: height - 42, cursor: 'crosshair' }} />
    </div>
  )
}
