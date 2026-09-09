import { useMemo } from 'react'
import ReactECharts from 'echarts-for-react'
import { useChartTheme } from '@/lib/theme'
import { findMaxDrawdown } from '../metrics'

export function EquityChart({ equity }: { equity: any[] }) {
  const ct = useChartTheme()
  const option = useMemo(() => {
    const dates = equity.map((d) => String(d.dt ?? d.date ?? '').slice(0, 10))
    const strat = equity.map((d) => Number(d.value ?? 0))
    const bench = equity.map((d) => Number(d.benchmark ?? 0))
    // 累计收益率（%）：相对首日净值的涨跌，含基准。首日为 0%。
    const toReturn = (arr: number[]) => {
      const f = arr[0]
      if (!f) return arr.map(() => 0)
      return arr.map((v) => (v / f - 1) * 100)
    }
    const s = toReturn(strat)
    const b = toReturn(bench)
    // 全程最大回撤两点：峰值（起点）与谷值（终点），无回撤时不标记
    const dd = findMaxDrawdown(strat)
    const ddText = dd ? `最大回撤 ${(dd.drawdown * 100).toFixed(2)}%` : ''
    return {
      animation: false,
      grid: { left: 56, right: 16, top: 28, bottom: 32 },
      legend: { data: ['策略', '基准'], textStyle: { color: ct.text, fontSize: 10 }, right: 8, top: 4 },
      tooltip: {
        trigger: 'axis',
        backgroundColor: ct.tooltipBg,
        borderColor: ct.tooltipBorder,
        textStyle: { color: ct.tooltipText, fontSize: 12 },
        valueFormatter: (v: any) => (v == null ? '-' : `${(Number(v)).toFixed(2)}%`),
      },
      xAxis: {
        type: 'category', data: dates,
        axisLabel: { color: ct.text, fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: ct.border } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value', scale: true,
        axisLabel: { color: ct.text, fontSize: 10, formatter: (v: number) => `${v.toFixed(0)}%` },
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
          markLine: {
            silent: true, symbol: 'none',
            lineStyle: { color: ct.border, width: 1, type: 'solid' },
            data: [{ yAxis: 0 }],
            label: { show: false },
          },
          ...(dd ? {
            // 最大回撤两点标记：峰值（起点，蓝）与谷值（终点，绿），用类目序号定位
            markPoint: {
              silent: true,
              symbol: 'pin',
              symbolSize: 38,
              label: { fontSize: 10 },
              data: [
                {
                  xAxis: dd.peakIdx,
                  yAxis: s[dd.peakIdx],
                  name: '最大回撤起点',
                  itemStyle: { color: '#3b82f6' },
                  // 文字放 pin 上方外部：pin 内 38px 塞不下 6 个汉字
                  label: { formatter: '最大回撤起点', position: 'top', distance: 4, color: '#3b82f6' },
                },
                {
                  xAxis: dd.troughIdx,
                  yAxis: s[dd.troughIdx],
                  name: ddText,
                  itemStyle: { color: '#22c55e' },
                  label: { formatter: `${(dd.drawdown * 100).toFixed(2)}%`, position: 'top', distance: 4, color: '#22c55e' },
                },
              ],
            },
          } : {}),
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
