import { useMemo } from 'react'
import ReactECharts from 'echarts-for-react'

function cssVar(name: string, fallback: string) {
  if (typeof window === 'undefined') return fallback
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  // 变量可能未定义或被解析成字面量 "undefined"/空串，此时退回 fallback
  if (!v || v === 'undefined' || v === 'null') return fallback
  return v
}

// 把任意颜色基值转成 echarts 可用的合法 CSS 颜色。
// - Tailwind v4 下 --accent 是 HSL 分量 "217 91% 60%"（无 hsl() 包裹）；
// - fallback 是 #hex；也可能直接是 hsl()/rgb() 字符串。
// 非法/缺失值统一退回安全蓝。
function toColor(base: string, alpha?: number): string {
  let b = (base || '').trim()
  if (!b || b === 'undefined' || b === 'null') b = '#3b82f6'
  const hexAlpha = (hex: string, a: number) => {
    const h = hex.replace('#', '')
    const n = h.length === 3 ? h.split('').map((c) => c + c).join('') : h
    const r = parseInt(n.slice(0, 2), 16)
    const g = parseInt(n.slice(2, 4), 16)
    const bl = parseInt(n.slice(4, 6), 16)
    return `rgba(${r}, ${g}, ${bl}, ${a})`
  }
  // 已是合法颜色
  if (/^#|^rgba?\(|^hsla?\(/i.test(b)) {
    if (alpha === undefined) return b
    if (b.startsWith('#')) return hexAlpha(b, alpha)
    // hsl()/rgb() 已带括号：直接追加 /alpha（现代语法）
    return b.replace(/\)$/, ` / ${alpha})`)
  }
  // HSL 分量 "217 91% 60%"
  if (alpha === undefined) return `hsl(${b})`
  return `hsl(${b} / ${alpha})`
}

export function EquityChart({ equity }: { equity: any[] }) {
  const option = useMemo(() => {
    const accent = cssVar('--accent', '#3b82f6')
    const muted = toColor(cssVar('--muted', '#94a3b8'))
    const surface = toColor(cssVar('--surface', '#1e293b'))
    const border = toColor(cssVar('--border', '#334155'))
    const foreground = toColor(cssVar('--foreground', '#e2e8f0'))
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
        backgroundColor: surface,
        borderColor: border,
        textStyle: { color: foreground, fontSize: 12 },
      },
      xAxis: {
        type: 'category', data: dates,
        axisLabel: { color: muted, fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: border } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value', scale: true,
        axisLabel: { color: muted, fontSize: 10 },
        splitLine: { lineStyle: { color: border } },
      },
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 14, bottom: 6, borderColor: border, textStyle: { color: muted, fontSize: 10 } }],
      series: [
        { name: '策略', type: 'line', data: s, symbol: 'none', lineStyle: { color: toColor(accent), width: 2 },
          areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: toColor(accent, 0.15) }, { offset: 1, color: toColor(accent, 0.01) }] } } },
        { name: '基准', type: 'line', data: b, symbol: 'none', lineStyle: { color: muted, width: 1.5, type: 'dashed' } },
      ],
    } as any
  }, [equity])

  return <ReactECharts option={option} style={{ height: 260 }} notMerge />
}
