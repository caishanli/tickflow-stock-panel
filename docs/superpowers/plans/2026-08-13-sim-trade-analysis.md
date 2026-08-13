# 模拟盘持仓/成交行点击弹窗 + 分时买卖标识 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 量化模拟盘详情页持仓/成交记录行点击后，弹出与自选股一致的 `StockPreviewDialog`，分时图（当天分析线）上按交易时刻显示 B/S/止损 标记。

**Architecture:** 纯前端。新增可选 props 沿 `StockPreviewDialog → StockPanel → StockIntradayChart → EChartsIntraday` 透传；`EChartsIntraday` 价格序列加 `markPoint` 渲染买卖标记（date 感知，仅当分时显示该标记所属日期时渲染）。`QuantSim.SimDetail` 维护 preview state，行点击组装标记数据。

**Tech Stack:** React 18 + TS + Vite + ECharts 5 + @tanstack/react-query。无前端测试框架。

## Global Constraints

- 后端零改动；不新增依赖。
- 所有新增 props 均为**可选**，不破坏现有调用方（Watchlist/Screener/Indices 等）。
- 标记时间用本地时间 `HH:MM` 直接定位分时 242 点全天轴（`FULL_DAY_TIMES`），无时区换算。
- 标记只在分时当前 `date` 与标记 `date` 相同时渲染；标记时刻分钟数据 close 为 null（停牌/无成交）时过滤。
- `pnpm lint` 当前**不可用**（无 eslint 配置文件，预存在问题）。验证门禁 = `pnpm build`（`tsc -b && vite build`），在 `frontend/` 目录执行。
- 每次任务结束提交一个 commit；不提交 dist、data、node_modules。

---

### Task 1: EChartsIntraday 买卖标记渲染

**Files:**
- Modify: `frontend/src/components/EChartsIntraday.tsx`

**Interfaces:**
- Produces: `export interface IntradayMarker { date: string; time: string; price: number; action: 'BUY' | 'SELL' | 'STOP_LOSS' }`（本文件导出，后续任务引用）；`EChartsIntraday` 新增 prop `markers?: IntradayMarker[]`。

- [ ] **Step 1: 新增 `IntradayMarker` 类型与 `markers` prop**

在 `THEME` 常量定义（第 16 行附近）之后插入：

```ts
export interface IntradayMarker {
  /** 标记所属交易日 YYYY-MM-DD，仅在分时显示该日时渲染 */
  date: string
  /** 交易时刻 HH:MM（本地时间，直接定位全天 242 点时间轴） */
  time: string
  /** 成交价（持仓行用成本价） */
  price: number
  action: 'BUY' | 'SELL' | 'STOP_LOSS'
}
```

在 `Props` interface（第 18-27 行）末尾加：

```ts
  markers?: IntradayMarker[]
```

在组件签名（第 402 行 `export function EChartsIntraday({...}: Props)`）解构中加 `markers`：

```ts
export function EChartsIntraday({ data, height = 320, prevClose, date, priceLimit, onPriceHover, showLimitLines = true, showAvgLine = true, markers }: Props) {
```

- [ ] **Step 2: 新增 `buildMarkerPoints` 辅助函数**

放在 `fmtAmt`（第 49-53 行）之后：

```ts
/** 将买卖标记映射到全日时间轴: 仅保留与 chartDate 相同的标记, 且该分钟有真实成交 */
function buildMarkerPoints(
  markers: IntradayMarker[] | undefined,
  chartDate: string | undefined,
  closes: (number | null)[],
  timeIndexMap: Map<string, number>,
): any[] {
  if (!markers || markers.length === 0) return []
  const out: any[] = []
  for (const m of markers) {
    if (chartDate && m.date !== chartDate) continue
    const idx = timeIndexMap.get(m.time)
    if (idx === undefined || !isValidPrice(closes[idx])) continue
    const stop = m.action === 'STOP_LOSS'
    const buy = m.action === 'BUY'
    out.push({
      coord: [idx, m.price],
      symbol: stop ? 'circle' : 'triangle',
      symbolSize: stop ? 17 : 12,
      symbolRotate: buy ? 0 : 180,
      itemStyle: { color: stop ? '#F59E0B' : buy ? '#C74040' : '#2D9B65', borderColor: '#FFFFFF', borderWidth: 0.5 },
      label: {
        show: true,
        position: 'inside',
        color: '#FFFFFF',
        fontSize: 7,
        fontWeight: 'bold',
        formatter: stop ? '止损' : buy ? 'B' : 'S',
      },
    })
  }
  return out
}
```

- [ ] **Step 3: `buildOption` 接入标记**

`buildOption` 签名（第 104 行）末尾追加两参：

```ts
function buildOption(data: MinuteKlineRow[], prevClose: number | undefined, avgPrices: number[], lineColor: string, areaColor: string, yMode: YMode, ct: ChartTheme, priceLimit?: PriceLimitInfo, showLimitLines = true, showAvgLine = true, chartDate?: string, markers?: IntradayMarker[]): EChartsOption {
```

在数据映射 for 循环结束后（第 129 行 `}` 之后、`const areaStyle` 之前）插入：

```ts
  const markerPoints = buildMarkerPoints(markers, chartDate, closes, timeIndexMap)
```

价格序列（第 367-379 行对象）在 `markLine` 行之后加 `markPoint`：

```ts
        markLine: markLineData.length > 0 ? { symbol: 'none', data: markLineData, animation: false, silent: true } : undefined,
        markPoint: markerPoints.length > 0 ? { data: markerPoints, animation: false, silent: true } : undefined,
```

- [ ] **Step 4: setOption 调用与 effect 依赖更新**

第 491 行 `chart.setOption(...)` 调用末尾追加两参并把 `date, markers` 加入依赖数组：

```ts
      chart.setOption(buildOption(data, prevClose, avgPrices, lineColor, areaFill, yMode, ct, priceLimit, showLimitLines, showAvgLine, date, markers), true)
```

第 495 行 effect 依赖数组改为：

```ts
  }, [data, prevClose, height, lineColor, areaFill, yMode, ct, priceLimit, showLimitLines, showAvgLine, date, markers])
```

- [ ] **Step 5: 验证**

Run: `cd frontend && pnpm build`
Expected: `tsc -b` 与 `vite build` 均通过，无类型错误。

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/EChartsIntraday.tsx
git commit -m "feat(intraday): 分时图支持买卖标记 markPoint (B/S/止损)"
```

---

### Task 2: 弹窗组件链透传可选 props

**Files:**
- Modify: `frontend/src/components/StockIntradayChart.tsx`
- Modify: `frontend/src/components/StockPanel.tsx`
- Modify: `frontend/src/components/StockPreviewDialog.tsx`

**Interfaces:**
- Consumes: `IntradayMarker`（Task 1 从 `EChartsIntraday` 导出）。
- Produces: `StockPreviewDialog` 新增可选 props `initialDate?: string`、`initialIntraday?: boolean`（默认 false）、`intradayMarkers?: IntradayMarker[]`。

- [ ] **Step 1: StockIntradayChart 透传 `markers`**

`frontend/src/components/StockIntradayChart.tsx`：

```ts
import { EChartsIntraday, type IntradayMarker } from '@/components/EChartsIntraday'
```

（第 6 行 import 改为上式，`EChartsIntraday` 值导入保留、追加类型导入。）

`Props` interface 末尾加：

```ts
  /** 分时图买卖标记 (date 感知) */
  markers?: IntradayMarker[]
```

组件签名解构加 `markers`，`EChartsIntraday` 调用（第 112-119 行）追加：

```ts
        <EChartsIntraday
          data={minuteRows}
          height={height}
          prevClose={prevClose}
          date={date}
          priceLimit={minute.data?.price_limit ?? undefined}
          onPriceHover={onPriceHover}
          markers={markers}
        />
```

- [ ] **Step 2: StockPanel 新增 `intradayMarkers` + `initialDate`**

`frontend/src/components/StockPanel.tsx` 第 8 行 import 后追加：

```ts
import type { IntradayMarker } from '@/components/EChartsIntraday'
```

`Props` interface（第 16-37 行）追加两字段：

```ts
  /** 分时图买卖标记 (date 感知: 仅分时显示该日时渲染) */
  intradayMarkers?: IntradayMarker[]
  /** 初始选中的日期 (rows 就绪后优先选中, 仅应用一次) */
  initialDate?: string
```

组件签名（第 41-57 行）解构加 `intradayMarkers` 与 `initialDate`。

`StockIntradayChart` 调用（第 157-165 行）追加 `markers={intradayMarkers}`。

在「分时开启且无选中日期时自动选中最新日期」effect（第 106-110 行）之后新增：

```ts
  // 初始选中日期: rows 就绪后优先选中 initialDate (在 rows 内则选中, 否则回退最新),
  // 仅应用一次防止覆盖用户手动点选; symbol 变化时重置
  const initialApplied = useRef(false)
  useEffect(() => {
    if (initialDate && !initialApplied.current && rows.length > 0) {
      initialApplied.current = true
      const target = rows.find(r => r.date === initialDate) ?? rows[rows.length - 1]
      setSelectedDate(target.date)
    }
  }, [initialDate, rows])
```

在 symbol 变化重置 effect（第 96-103 行）体内加一行 `initialApplied.current = false`。

- [ ] **Step 3: StockPreviewDialog 新增三可选 props**

`frontend/src/components/StockPreviewDialog.tsx` 第 8 行后追加：

```ts
import type { IntradayMarker } from '@/components/EChartsIntraday'
```

`Props` interface（第 14-26 行）`triggerInfo` 之后追加：

```ts
  /** 初始选中日期 (模拟盘成交行直接定位交易当日) */
  initialDate?: string
  /** 初始分时开关状态 (默认 false = 自选股原行为) */
  initialIntraday?: boolean
  /** 分时图买卖标识 */
  intradayMarkers?: IntradayMarker[]
```

组件签名解构加三 props（`initialIntraday = false` 默认值）：

```ts
export function StockPreviewDialog({ symbol, name, onClose, triggerInfo, initialDate, initialIntraday = false, intradayMarkers }: Props) {
```

在 ESC 关闭 effect（第 64-72 行）附近新增（切股不卸载弹窗，需手动同步分时开关）：

```ts
  // symbol 切换时重置分时开关到 initialIntraday (弹窗跨股复用不卸载)
  useEffect(() => {
    setShowIntraday(initialIntraday)
  }, [symbol]) // eslint-disable-line react-hooks/exhaustive-deps
```

`StockPanel` 调用（第 256-266 行）追加两 props：

```tsx
              <StockPanel
                symbol={symbol}
                height={420}
                showIntraday={showIntraday}
                onSelectDate={() => { if (!showIntraday) setShowIntraday(true) }}
                dateRange={dateRange}
                onMonitor={() => setShowMonitorEditor(true)}
                inWatchlist={inWatchlist}
                onToggleWatchlist={() => toggleWatchlist.mutate()}
                refetchIntervalMs={intradayRefetchMs}
                initialDate={initialDate}
                intradayMarkers={intradayMarkers}
              />
```

- [ ] **Step 4: 验证**

Run: `cd frontend && pnpm build`
Expected: 通过，无类型错误（新增 props 均为可选，现有调用方不受影响）。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/StockIntradayChart.tsx frontend/src/components/StockPanel.tsx frontend/src/components/StockPreviewDialog.tsx
git commit -m "feat(stock): 弹窗组件链透传 initialDate/initialIntraday/intradayMarkers 可选 props"
```

---

### Task 3: QuantSim 行点击弹出分析窗口

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx`

**Interfaces:**
- Consumes: `StockPreviewDialog`（Task 2，新 props `initialDate`/`initialIntraday`/`intradayMarkers`）、`IntradayMarker`（Task 1）。

- [ ] **Step 1: 引入组件与类型 + 工具函数**

`frontend/src/quant/pages/QuantSim.tsx` 第 7 行 import 区追加：

```ts
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import type { IntradayMarker } from '@/components/EChartsIntraday'
```

在文件顶部工具函数区（`fmtStopLoss` 之后）追加：

```ts
/** 从 "YYYY-MM-DD HH:MM:SS"（可省略秒）提取日期 + HH:MM，供分时标记定位 */
function parseTradeTime(ts: unknown): { date: string; time: string } | null {
  const s = String(ts ?? '')
  const m = s.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2})/)
  return m ? { date: m[1], time: m[2] } : null
}

function toMarkerAction(action: unknown): IntradayMarker['action'] {
  const a = String(action).toUpperCase()
  if (a === 'BUY') return 'BUY'
  if (a === 'STOP_LOSS') return 'STOP_LOSS'
  return 'SELL'
}
```

- [ ] **Step 2: SimDetail 增加 preview state**

`SimDetail` 函数内 `const [tab, setTab] = useState<...>('trades')` 附近新增：

```ts
  const [preview, setPreview] = useState<{ symbol: string; name: string; date?: string; markers: IntradayMarker[] } | null>(null)
```

- [ ] **Step 3: 持仓行点击**

持仓表行（第 693 行 `<tr key={sym} className="border-t border-border/60">`）改为：

```tsx
                      <tr key={sym}
                        onClick={() => {
                          const t = parseTradeTime(p.entry_ts)
                          setPreview({
                            symbol: sym,
                            name: p.name ?? '',
                            markers: t && Number(p.avg_cost) > 0
                              ? [{ date: t.date, time: t.time, price: Number(p.avg_cost), action: 'BUY' }]
                              : [],
                          })
                        }}
                        className="border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
```

（不传 `date` → 默认今日分时；标记 date=买入日，切到买入日才显示。）

- [ ] **Step 4: 成交记录行点击**

成交表行（第 753 行 `<tr key={i} className="border-t border-border/60 hover:bg-elevated/60 transition-colors">`）改为：

```tsx
                    <tr key={i}
                      onClick={() => {
                        const parsed = parseTradeTime(t.ts)
                        setPreview({
                          symbol: t.code ?? '',
                          name: t.name ?? '',
                          date: String(t.ts ?? '').slice(0, 10),
                          markers: parsed && typeof t.price === 'number'
                            ? [{ date: parsed.date, time: parsed.time, price: t.price, action: toMarkerAction(t.action) }]
                            : [],
                        })
                      }}
                      className="border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
```

- [ ] **Step 5: 渲染 StockPreviewDialog**

`SimDetail` 返回 JSX 末尾、`{showDingtalkCfg && <DingtalkConfigDialog .../>}` 之前追加：

```tsx
      {preview && (
        <StockPreviewDialog
          symbol={preview.symbol}
          name={preview.name}
          initialDate={preview.date}
          initialIntraday
          intradayMarkers={preview.markers}
          onClose={() => setPreview(null)}
        />
      )}
```

- [ ] **Step 6: 验证**

Run: `cd frontend && pnpm build`
Expected: 通过，无类型错误。

- [ ] **Step 7: Commit**

```bash
git add frontend/src/quant/pages/QuantSim.tsx
git commit -m "feat(sim): 持仓/成交行点击弹出分析窗口 + 分时买卖标识"
```

---

### Task 4: 最终验证与手动冒烟

**Files:** 无改动。

- [ ] **Step 1: 全量类型检查与构建**

Run: `cd frontend && pnpm build`
Expected: 通过。

- [ ] **Step 2: 手动冒烟清单**

启动 `./dev.sh`，打开 http://localhost:3011 量化模拟盘：

1. 进入一个已有成交的分钟级模拟账户详情。
2. 点持仓行：弹窗打开、默认显示今日分时、无标记；日K点选到买入那天 → 分时切换、出现红色 B 上三角。
3. 点成交记录 BUY 行：弹窗自动选中交易日期、分时打开、成交时刻红色 B。
4. 点 SELL 行：绿色 S 下三角；点 STOP_LOSS 行：橙色圆「止损」。
5. 弹窗内切日期（点不同 K 线蜡烛 / 用日期范围）：标记只在对应交易日出现，不串日。
6. 4/1 前的成交行：弹窗显示「暂无分钟数据」提示，无异常报错。
7. 自选股页打开任意股票弹窗：仍默认 K 线、无标记、行为与改动前一致。
8. 止损日志 tab 行点击无反应（不在范围内）。
