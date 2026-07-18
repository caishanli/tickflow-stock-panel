import { useMemo } from 'react'
import ReactECharts from 'echarts-for-react'

function cssVar(name: string, fallback: string) {
  if (typeof window === 'undefined') return fallback
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  return v || fallback
}

export function EquityChart({ equity }: { equity: any[] }) {
  const option = useMemo(() => {
    const accent = cssVar('--accent', '#3b82f6')
    const muted = cssVar('--muted', '#94a3b8')
    const dates = equity.map((d) => String(d.dt ?? d.date ?? '').slice(0, 10))
    const strat = equity.map((d) => Number(d.value ?? 0))
    const bench = equity.map((d) => Number(d.benchmark ?? 0))
    const norm = (arr: number[]) => {
      const f = arr[0]
      return f ? arr.map((v) => v / f) : arr
    }
    const s = norm(strat)
    const b = norm(bench)
    return {
      animation: false,
      grid: { left: 56, right: 16, top: 28, bottom: 32 },
      legend: { data: ['策略', '基准'], textStyle: { color: muted, fontSize: 10 }, right: 8, top: 4 },
      tooltip: {
        trigger: 'axis',
        backgroundColor: cssVar('--surface', '#1e293b'),
        borderColor: cssVar('--border', '#334155'),
        textStyle: { color: cssVar('--foreground', '#e2e8f0'), fontSize: 12 },
      },
      xAxis: {
        type: 'category', data: dates,
        axisLabel: { color: muted, fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: cssVar('--border', '#334155') } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value', scale: true,
        axisLabel: { color: muted, fontSize: 10 },
        splitLine: { lineStyle: { color: cssVar('--border', '#334155') } },
      },
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 14, bottom: 6, borderColor: cssVar('--border', '#334155'), textStyle: { color: muted, fontSize: 10 } }],
      series: [
        { name: '策略', type: 'line', data: s, symbol: 'none', lineStyle: { color: accent, width: 2 },
          areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: accent + '26' }, { offset: 1, color: accent + '03' }] } } },
        { name: '基准', type: 'line', data: b, symbol: 'none', lineStyle: { color: muted, width: 1.5, type: 'dashed' } },
      ],
    } as any
  }, [equity])

  return <ReactECharts option={option} style={{ height: 260 }} notMerge />
}
