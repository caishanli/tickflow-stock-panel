import { useMemo } from 'react'
import ReactECharts from 'echarts-for-react'
import { useChartTheme } from '@/lib/theme'

export function EquityChart({ equity }: { equity: any[] }) {
  const ct = useChartTheme()
  const option = useMemo(() => {
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
      legend: { data: ['策略', '基准'], textStyle: { color: ct.text, fontSize: 10 }, right: 8, top: 4 },
      tooltip: {
        trigger: 'axis',
        backgroundColor: ct.tooltipBg,
        borderColor: ct.tooltipBorder,
        textStyle: { color: ct.tooltipText, fontSize: 12 },
      },
      xAxis: {
        type: 'category', data: dates,
        axisLabel: { color: ct.text, fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: ct.border } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value', scale: true,
        axisLabel: { color: ct.text, fontSize: 10 },
        splitLine: { lineStyle: { color: ct.grid } },
      },
      dataZoom: [
        { type: 'inside' },
        { type: 'slider', height: 14, bottom: 6, borderColor: ct.border, textStyle: { color: ct.text, fontSize: 10 } },
      ],
      series: [
        {
          name: '策略', type: 'line', data: s, symbol: 'none', lineStyle: { color: '#3b82f6', width: 2 },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(59,130,246,0.15)' },
                { offset: 1, color: 'rgba(59,130,246,0.01)' },
              ],
            } as any,
          },
        },
        {
          name: '基准', type: 'line', data: b, symbol: 'none',
          lineStyle: { color: 'rgba(148,163,184,0.45)', width: 1.5, type: 'dashed' },
        },
      ],
    } as any
  }, [equity, ct])

  return <ReactECharts option={option} style={{ height: 260 }} notMerge />
}
