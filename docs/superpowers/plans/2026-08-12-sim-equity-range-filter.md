# 模拟盘净值曲线快捷时间范围筛选 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 量化模拟盘详情页净值曲线卡片增加「全部/一星期/1个月/3个月/6个月/1年」快捷范围按钮，曲线区间归一化从 0% 起，收益率/盈亏卡片随范围切换为区间值。

**Architecture:** 全部为前端单文件改动（`frontend/src/quant/pages/QuantSim.tsx` 的 SimDetail）。新增 `rangeDays` state，把 equity 日线聚合从 curve useMemo 中提出来共享，派生窗口数据供曲线与卡片复用。无后端改动、无新依赖。

**Tech Stack:** React 18 + TS + echarts-for-react（现有）。

## Global Constraints

- 仅允许修改 `frontend/src/quant/pages/QuantSim.tsx`；其余文件（含持仓、成交记录 Tab）逻辑不变。
- 周期→日历天数映射固定：一星期=7、1个月=30、3个月=90、6个月=180、1年=365；「全部」= null。
- 窗口过滤：`dt(YYYY-MM-DD) >= 今天-天数` 的日历字符串比较；过滤后 <2 个点时回退显示全部数据。
- 曲线区间归一化公式：`rel(i) = (1 + cum[i]/100) / (1 + cum[0]/100) - 1`（cum 为相对 start_cash 的累计收益率%）。
- 卡片：选范围时收益率 = 区间相对收益、盈亏 = 窗口末净值-窗口首日净值；净值/现金/持仓市值保持当前实时值。
- 验收：`cd frontend && pnpm build` 通过；无头浏览器实测按钮切换 + 卡片数值 + echarts series data 首点≈0。
- 不提交 git（用户未要求）。

---

### Task 1: 范围按钮 UI 与 state

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx`（SimDetail 内）

**Interfaces:**
- Consumes: 无（SimDetail 现有 props）。
- Produces: `rangeDays: number | null` state；`setRangeDays`。`RANGES` 常量（模块级）。

- [ ] **Step 1: 新增 RANGES 常量（模块级，`fmtPct` 函数之后）**

```tsx
const RANGES: { label: string; days: number | null }[] = [
  { label: '全部', days: null },
  { label: '一星期', days: 7 },
  { label: '1个月', days: 30 },
  { label: '3个月', days: 90 },
  { label: '6个月', days: 180 },
  { label: '1年', days: 365 },
]
```

- [ ] **Step 2: SimDetail 内新增 state**

在 `const [showDingtalkCfg, setShowDingtalkCfg] = useState(false)` 之后加：

```tsx
const [rangeDays, setRangeDays] = useState<number | null>(null)
```

- [ ] **Step 3: 「净值曲线」卡片标题行加按钮组**

现有（约 550-551 行）：

```tsx
      <div className="rounded-card border border-border bg-surface">
        <div className="px-4 pt-3 text-xs text-foreground font-medium">净值曲线</div>
```

改为：

```tsx
      <div className="rounded-card border border-border bg-surface">
        <div className="px-4 pt-3 flex items-center justify-between">
          <span className="text-xs text-foreground font-medium">净值曲线</span>
          <div className="flex gap-1 pr-2">
            {RANGES.map((r) => (
              <button key={r.label} onClick={() => setRangeDays(r.days)}
                className={`px-2.5 h-6 rounded-btn text-[11px] ${rangeDays === r.days ? 'bg-accent text-white' : 'text-muted hover:text-foreground'}`}>
                {r.label}
              </button>
            ))}
          </div>
        </div>
```

- [ ] **Step 4: 验证**

`cd frontend && pnpm build`（tsc）通过；无头浏览器打开模拟盘详情，确认按钮渲染且「全部」高亮、点击切换高亮。

---

### Task 2: 窗口化曲线 + 卡片区间值

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx`（SimDetail 的 curve useMemo 与指标卡区域）

**Interfaces:**
- Consumes: `rangeDays`（Task 1）；`eq`（equity 查询结果）；`st`（status 查询结果，含 `state`/`start_cash`）。
- Produces: 无对外接口（本文件内使用）。

- [ ] **Step 1: 提取日线聚合 + 窗口切分 + 区间卡片值（放在原 `ret` 定义处）**

删除原代码：

```tsx
  const ret = typeof state?.net_value === 'number' && state?.start_cash
    ? state.net_value / state.start_cash - 1 : null
```

替换为：

```tsx
  // 按天聚合：每天取最后一个点的净值，日线级别展示（供曲线与指标卡共用）
  const daily = useMemo(() => {
    const raw: any[] = Array.isArray(eq) ? eq : []
    const dayMap = new Map<string, any>()
    for (const d of raw) {
      const day = String(d.dt ?? '').slice(0, 10)
      if (day) dayMap.set(day, d)
    }
    return Array.from(dayMap.values())
  }, [eq])
  // 窗口切分：日历天过滤，不足 2 点回退全部
  const windowed = useMemo(() => {
    if (rangeDays == null) return daily
    const t = new Date()
    const cutoff = new Date(t.getFullYear(), t.getMonth(), t.getDate() - rangeDays)
    const m = String(cutoff.getMonth() + 1).padStart(2, '0')
    const d = String(cutoff.getDate()).padStart(2, '0')
    const cutoffStr = `${cutoff.getFullYear()}-${m}-${d}`
    const w = daily.filter((x) => String(x.dt ?? '').slice(0, 10) >= cutoffStr)
    return w.length >= 2 ? w : daily
  }, [daily, rangeDays])
  // 收益基准：账户初始资金（缺失时兜底首日净值）
  const baseNV = useMemo(
    () => Number(st?.state?.start_cash ?? st?.start_cash) ||
      (daily.length > 0 ? Number(daily[0].net_value) : 1),
    [st, daily],
  )
  const winFirst = windowed.length > 0 ? windowed[0] : null
  const winLast = windowed.length > 0 ? windowed[windowed.length - 1] : null
  // 总收益率与区间收益率
  const totalRet = baseNV ? (typeof state?.net_value === 'number' && Number.isFinite(state.net_value)
    ? state.net_value / baseNV - 1 : null) : null
  const winRet = (rangeDays != null && winFirst != null && winLast != null && Number(winFirst.net_value) > 0)
    ? Number(winLast.net_value) / Number(winFirst.net_value) - 1
    : null
  const winPnl = (rangeDays != null && winFirst != null && winLast != null)
    ? Number(winLast.net_value) - Number(winFirst.net_value)
    : null
  const displayRet = winRet != null ? winRet : totalRet
  const displayPnl = winPnl != null ? winPnl : state?.pnl
```

- [ ] **Step 2: curve useMemo 改用 windowed 并做区间归一化**

现 curve useMemo（约 339-435 行）整体替换为：

```tsx
  const curve = useMemo(() => {
    const accent = '#3b82f6'
    const benchColor = '#f59e0b'
    const data: any[] = windowed
    // 策略收益率(%)：相对初始资金累计（首日即反映当天盈亏）
    const stratPct = data.map((d) => Number((((Number(d.net_value ?? 0) / baseNV) - 1) * 100).toFixed(2)))
    const benchPct = data.map((d) => Number(d.benchmark_pct ?? 0))
    // 窗口内区间归一化：从窗口首日 0% 起
    const rel = (cum: number[]) => {
      const first = cum[0]
      if (!first) return cum.map(() => 0)
      return cum.map((v) => Number((((1 + v / 100) / (1 + first / 100) - 1) * 100).toFixed(2)))
    }
    const stratWin = rel(stratPct)
    const benchWin = rel(benchPct)
    // 当日涨跌幅(%)：从累计收益率反推，(1+r_n)/(1+r_{n-1})-1
    const stratDaily = stratPct.map((v, i) =>
      i === 0 ? 0 : Number((((1 + v / 100) / (1 + stratPct[i - 1] / 100) - 1) * 100).toFixed(2)))
    const benchDaily = benchPct.map((v, i) =>
      i === 0 ? 0 : Number((((1 + v / 100) / (1 + benchPct[i - 1] / 100) - 1) * 100).toFixed(2)))
    const xLabels = data.map((d) => String(d.dt ?? '').slice(0, 10))
    return {
      animation: false,
      grid: { left: 64, right: 16, top: 30, bottom: 46 },
      legend: {
        data: ['策略收益(累计)', '沪深300(累计)'],
        textStyle: { color: cssVar('--muted', '#94a3b8'), fontSize: 11 },
        top: 4, right: 8,
      },
      tooltip: {
        trigger: 'axis',
        backgroundColor: cssVar('--surface', '#1e293b'),
        borderColor: cssVar('--border', '#334155'),
        textStyle: { color: cssVar('--foreground', '#e2e8f0'), fontSize: 12 },
        formatter: (params: any[]) => {
          if (!params || params.length === 0) return ''
          const idx = params[0].dataIndex
          const day = xLabels[idx] ?? ''
          const sCum = stratWin[idx] ?? 0
          const bCum = benchWin[idx] ?? 0
          const sDay = stratDaily[idx] ?? 0
          const bDay = benchDaily[idx] ?? 0
          const fmt = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
          const color = (v: number) => v >= 0 ? '#ef4444' : '#22c55e'
          return `<div style="font-size:11px;margin-bottom:4px;opacity:0.7">${day}</div>` +
            `<div style="display:grid;grid-template-columns:auto auto auto;gap:2px 12px;font-size:12px">` +
            `<span style="color:${accent}">策略</span>` +
            `<span style="color:${color(sCum)}">${fmt(sCum)}</span>` +
            `<span style="color:${color(sDay)};opacity:0.6">${fmt(sDay)}</span>` +
            `<span style="color:${benchColor}">沪深300</span>` +
            `<span style="color:${color(bCum)}">${fmt(bCum)}</span>` +
            `<span style="color:${color(bDay)};opacity:0.6">${fmt(bDay)}</span>` +
            `</div>` +
            `<div style="font-size:10px;margin-top:4px;opacity:0.4">累计 / 当日</div>`
        },
      },
      xAxis: {
        type: 'category',
        data: xLabels,
        axisLabel: { color: cssVar('--muted', '#94a3b8'), fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: cssVar('--border', '#334155') } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        scale: true,
        axisLabel: { color: cssVar('--muted', '#94a3b8'), fontSize: 10, formatter: '{value}%' },
        splitLine: { lineStyle: { color: cssVar('--border', '#334155') } },
      },
      dataZoom: [
        { type: 'inside' },
        { type: 'slider', height: 14, bottom: 6, borderColor: cssVar('--border', '#334155'), textStyle: { color: cssVar('--muted', '#94a3b8'), fontSize: 10 } },
      ],
      series: [
        {
          name: '策略收益(累计)',
          type: 'line',
          data: stratWin,
          symbol: 'none',
          lineStyle: { color: accent, width: 2 },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [{ offset: 0, color: accent + '26' }, { offset: 1, color: accent + '03' }],
            },
          },
        },
        {
          name: '沪深300(累计)',
          type: 'line',
          data: benchWin,
          symbol: 'none',
          lineStyle: { color: benchColor, width: 1.5, type: 'dashed' },
        },
      ],
    } as any
  }, [windowed, baseNV])
```

- [ ] **Step 3: 指标卡改用 displayRet / displayPnl**

「盈亏」卡（约 535-540 行）改为：

```tsx
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">盈亏</div>
          <div className={`text-sm font-medium num ${typeof displayPnl === 'number' && displayPnl < 0 ? 'text-bear' : 'text-bull'}`}>
            {fmtNum(displayPnl)}
          </div>
        </div>
```

「收益率」卡（约 541-546 行）改为：

```tsx
        <div className="rounded-card border border-border bg-surface px-3 py-2">
          <div className="text-[10px] text-muted">收益率</div>
          <div className={`text-sm font-medium num ${displayRet == null ? '' : displayRet >= 0 ? 'text-bull' : 'text-bear'}`}>
            {fmtPct(displayRet)}
          </div>
        </div>
```

- [ ] **Step 4: 验证**

1. `cd frontend && pnpm build` 通过。
2. 无头浏览器（已装 chromium-headless-shell）登录 `http://localhost:3011`（密码来自 `.env` AUTH_PASSWORD），进 quant-sim → 第一个运行中账户：
   - 默认「全部」：收益率/盈亏与改前一致。
   - 点「1个月」：用 `page.evaluate` 读 `echarts.getInstanceByDom(...).getOption()`，校验 `series[0].data[0] ≈ 0`、`series[1].data[0] ≈ 0`、`xAxis[0].data[0]` 在窗口首日附近；卡片「收益率」文本 ≈ `series[0].data[last]`，两值相差 < 0.01%。
   - 点「一星期」/「1年」不报错；evaluate 确认 xAxis 首个日期 ≥ 截止日。
   - 点回「全部」数值还原。