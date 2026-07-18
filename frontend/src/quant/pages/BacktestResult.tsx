import { useMemo } from 'react'
import ReactECharts from 'echarts-for-react'

/** 读取 CSS 设计令牌变量，echarts 无法直接消费 var()，需解析为实际颜色 */
function cssVar(name: string, fallback: string) {
  if (typeof window === 'undefined') return fallback
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  if (!v || v === 'undefined' || v === 'null') return fallback
  return v
}

// 把任意颜色基值转成 echarts 可用的合法 CSS 颜色（HSL 分量/hsl()/#hex 均兼容）。
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
  if (/^#|^rgba?\(|^hsla?\(/i.test(b)) {
    if (alpha === undefined) return b
    if (b.startsWith('#')) return hexAlpha(b, alpha)
    return b.replace(/\)$/, ` / ${alpha})`)
  }
  if (alpha === undefined) return `hsl(${b})`
  return `hsl(${b} / ${alpha})`
}

interface Props {
  status: any
  equity: any
  trades: any
  logs: any
}

function statusTone(s: string | undefined) {
  if (s === 'completed' || s === 'success' || s === 'done') return 'text-bull'
  if (s === 'failed' || s === 'error') return 'text-bear'
  if (s === 'running' || s === 'pending') return 'text-accent'
  return 'text-muted'
}

export function BacktestResult({ status, equity, trades, logs }: Props) {
  const curve = useMemo(() => {
    const accent = toColor(cssVar('--accent', '#3b82f6'))
    const muted = toColor(cssVar('--muted', '#94a3b8'))
    const surface = toColor(cssVar('--surface', '#1e293b'))
    const border = toColor(cssVar('--border', '#334155'))
    const foreground = toColor(cssVar('--foreground', '#e2e8f0'))
    const data: any[] = Array.isArray(equity) ? equity : []
    const dates = data.map((d) => String(d.date ?? d.datetime ?? d.time ?? '').slice(0, 10))
    const values = data.map((d) => Number(d.value ?? d.equity ?? d.nav ?? 0))
    const first = values[0]
    const nav = first ? values.map((v) => (v / first) * 1) : values

    return {
      animation: false,
      grid: { left: 56, right: 16, top: 16, bottom: 32 },
      tooltip: {
        trigger: 'axis',
        backgroundColor: surface,
        borderColor: border,
        textStyle: { color: foreground, fontSize: 12 },
      },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: { color: muted, fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: border } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        scale: true,
        axisLabel: { color: muted, fontSize: 10 },
        splitLine: { lineStyle: { color: border } },
      },
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 14, bottom: 6, borderColor: border, textStyle: { color: muted, fontSize: 10 } }],
      series: [
        {
          name: '净值',
          type: 'line',
          data: nav,
          symbol: 'none',
          lineStyle: { color: accent, width: 2 },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: toColor(accent, 0.15) },
                { offset: 1, color: toColor(accent, 0.01) },
              ],
            },
          },
        },
      ],
    } as any
  }, [equity])

  const statusValue: string = status?.state ?? status?.status ?? status?.phase ?? 'running'
  const _rawMetrics = status?.metrics_json ?? status?.metrics ?? status?.stats
  let metrics: any = {}
  if (typeof _rawMetrics === 'string') {
    try { metrics = JSON.parse(_rawMetrics) } catch { metrics = {} }
  } else if (_rawMetrics && typeof _rawMetrics === 'object') {
    metrics = _rawMetrics
  }
  const tradeList: any[] = Array.isArray(trades) ? trades : []
  const logList: any[] = Array.isArray(logs) ? logs : []

  const metricEntries = Object.entries(metrics).filter(([k]) => typeof metrics[k] === 'number' || typeof metrics[k] === 'string')

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 rounded-card border border-border bg-surface px-4 h-11">
        <span className="text-xs text-muted">状态</span>
        <span className={`text-xs font-medium ${statusTone(statusValue)}`}>{statusValue}</span>
        {status?.progress != null && (
          <span className="text-xs text-muted">进度 {Math.round(status.progress * 100)}%</span>
        )}
      </div>

      <div className="rounded-card border border-border bg-surface">
        <div className="px-4 pt-3 text-xs text-foreground font-medium">净值曲线</div>
        {Array.isArray(equity) && equity.length > 0 ? (
          <ReactECharts option={curve} style={{ height: 300 }} notMerge />
        ) : (
          <div className="h-[300px] grid place-items-center text-xs text-muted">暂无净值数据</div>
        )}
      </div>

      {metricEntries.length > 0 && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
          {metricEntries.slice(0, 8).map(([k, v]) => (
            <div key={k} className="rounded-card border border-border bg-surface px-3 py-2">
              <div className="text-[10px] text-muted truncate">{k}</div>
              <div className={`text-sm font-medium ${typeof v === 'number' && v < 0 ? 'text-bear' : 'text-foreground'}`}>{String(v)}</div>
            </div>
          ))}
        </div>
      )}

      <div className="rounded-card border border-border bg-surface overflow-hidden">
        <div className="px-4 pt-3 pb-2 text-xs text-foreground font-medium">成交记录</div>
        {tradeList.length > 0 ? (
          <div className="overflow-auto max-h-60">
            <table className="w-full text-xs">
              <thead className="text-muted sticky top-0 bg-surface">
                <tr className="text-left">
                  <th className="px-3 py-1.5 font-normal">时间</th>
                  <th className="px-3 py-1.5 font-normal">标的</th>
                  <th className="px-3 py-1.5 font-normal">方向</th>
                  <th className="px-3 py-1.5 font-normal text-right">价格</th>
                  <th className="px-3 py-1.5 font-normal text-right">数量</th>
                </tr>
              </thead>
              <tbody className="text-foreground">
                {tradeList.map((t, i) => (
                  <tr key={i} className="border-t border-border/60">
                    <td className="px-3 py-1.5 text-muted">{String(t.datetime ?? t.time ?? t.date ?? '')}</td>
                    <td className="px-3 py-1.5">{t.symbol ?? t.code ?? ''}</td>
                    <td className={`px-3 py-1.5 ${t.side === 'buy' || t.side === 'BUY' ? 'text-bull' : 'text-bear'}`}>{t.side ?? ''}</td>
                    <td className="px-3 py-1.5 text-right">{t.price ?? ''}</td>
                    <td className="px-3 py-1.5 text-right">{t.quantity ?? t.qty ?? ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="px-4 pb-4 text-xs text-muted">暂无成交</div>
        )}
      </div>

      {logList.length > 0 && (
        <div className="rounded-card border border-border bg-base p-3">
          <div className="text-xs text-foreground font-medium mb-2">运行日志</div>
          <div className="max-h-48 overflow-auto space-y-0.5 text-[11px] text-muted font-mono">
            {logList.map((l, i) => (
              <div key={i}>{typeof l === 'string' ? l : JSON.stringify(l)}</div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
