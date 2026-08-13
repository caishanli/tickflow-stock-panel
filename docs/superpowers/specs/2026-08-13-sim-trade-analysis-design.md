# 量化模拟盘：持仓/成交行点击弹出分析窗口 + 分时买卖标识 — 设计

日期：2026-08-13，分支：feat/sim-trade-analysis（从 custom-main 切出）

## 目标

量化模拟盘详情页（`QuantSim`）现有的**持仓表**与**成交记录表**行点击无反应。本次增强：

1. 点击持仓行/成交记录行 → 弹出自选股同款分析窗口（`StockPreviewDialog`，含 K 线 + 分时/当天分析线 + 加自选 + 加监控 + 日期范围等全部能力）。
2. 分时图（当天分析线）上增加**买入/卖出标识**：BUY 红色 B、SELL 绿色 S、STOP_LOSS 橙色「止损」。

## 范围

- **纯前端改动**，后端零改动（成交/持仓数据已在前端内存中，`sim_trades.ts` 与持仓 `entry_ts` 均为本地时间字符串 `YYYY-MM-DD HH:MM:SS`）。
- 止损日志 tab 行**不做**点击（与成交记录同源数据，避免范围膨胀）。
- 不做：后端接口、分钟数据回源策略、持仓浮盈标记、账户列表页行点击。

## 交互

- **持仓行点击**：弹窗打开即显示今日分时（`initialIntraday: true`），无买卖标；在日 K 上点选到买入那天时，分时切换为该日并出现红色 B 标（买入时间 `entry_ts`、价格 `avg_cost`）。标记随选中日期变化，不串日。
- **成交记录行点击**：弹窗打开即选中交易日期（`initialDate` = `ts` 前 10 位）、显示该日分时，成交时刻直接标点——BUY 标 B、SELL 标 S、STOP_LOSS 标「止损」。
- 弹窗内分时开关、日期范围、加自选、加监控、刷新等能力与自选股完全一致（复用组件）。

## 组件改动（全部新增可选 props，不破坏现有调用方）

新类型 `IntradayMarker`，定义在 `EChartsIntraday.tsx` 并导出（仿 `ChartMarker` 从 `EChartsCandlestick.tsx` 导出的先例）：

```ts
export interface IntradayMarker {
  date: string          // YYYY-MM-DD，标记所属交易日
  time: string          // HH:MM，交易时刻
  price: number         // 成交价（持仓行用 avg_cost）
  action: 'BUY' | 'SELL' | 'STOP_LOSS'
}
```

- `time` 与分时图 242 点全天轴（`FULL_DAY_TIMES`，本地时区 9:30~11:30、13:00~15:00）直接按 `HH:MM` 索引定位，无时区换算（`ts` 为本地时间）。
- **date 感知**：标记只在分时图当前 `date` 与标记 `date` 相同时渲染——这是持仓行「默认今天、切到买入日才显示买入点」的关键。

### 1. `EChartsIntraday.tsx`

- 新增 prop `markers?: IntradayMarker[]`。
- `buildOption` 中按当前 `date` 过滤标记，`HH:MM` 映射 `FULL_DAY_TIMES` 索引；该时刻分钟数据 close 为 null（停牌/无成交）时**自动过滤**，避免标在空位。
- 渲染方式：价格序列上加 `markPoint`（`coord: [idx, price]`），或独立 scatter 层：
  - BUY：红色（#ef4444）上三角箭头，标签「B」
  - SELL：绿色（#22c55e）下三角箭头，标签「S」
  - STOP_LOSS：橙色（#f59e0b）圆点，标签「止损」

### 2. `StockIntradayChart.tsx`

- 新增 prop `markers?: IntradayMarker[]`，透传 `EChartsIntraday`。

### 3. `StockPanel.tsx`

- 新增 prop `intradayMarkers?: IntradayMarker[]`，透传 `StockIntradayChart`。
- 新增 prop `initialDate?: string`：挂载后 rows 就绪时，若 `initialDate` 在 rows 中则优先选中，否则回退最后一天；用 ref 标记「初始应用已执行」，防止覆盖用户后续手动点选；symbol 切换时重置 ref（复用现有 prevSymbol effect）。
- 现有「分时开启且无选中日期 → 自动选中最新日期」effect 保持兜底，优先级低于 `initialDate`。

### 4. `StockPreviewDialog.tsx`

- 新增可选 props：`initialDate?: string`、`initialIntraday?: boolean`（默认 false，保持现有打开行为）、`intradayMarkers?: IntradayMarker[]`。
- symbol 变化（含首次挂载）时 `setShowIntraday(initialIntraday)`；三者透传 `StockPanel`。

### 5. `frontend/src/quant/pages/QuantSim.tsx`

- 新增 state：`preview: { symbol: string; name: string; date?: string; markers: IntradayMarker[] } | null`。
- 持仓行点击：`setPreview({ symbol: sym, name: p.name, markers: [{ date: entry_ts 前 10 位, time: entry_ts HH:MM, price: avg_cost, action: 'BUY' }] })`；`date` 不传（默认今日）；无 `entry_ts` 的旧持仓仍可点击（今日分时，无标记）。
- 成交记录行点击：`setPreview({ symbol: t.code, name: t.name, date: ts 前 10 位, markers: [{ date: 同 date, time: ts HH:MM, price: t.price, action: t.action 映射 }] })`；action 映射：`BUY → BUY`、`SELL → SELL`、`STOP_LOSS → STOP_LOSS`。
- 渲染：`<StockPreviewDialog symbol={preview.symbol} name={preview.name} initialDate={preview.date} initialIntraday intradayMarkers={preview.markers} onClose={() => setPreview(null)} />`。
- 行样式加 `cursor-pointer` 与 hover 反馈（与列表页/账户列表行一致）。

## 边界情况

- 4/1 前交易日期或无分钟数据日期：分时区显示现有「暂无分钟数据」提示与获取按钮，标记不渲染；不做特殊处理。
- 停牌/非交易时刻：标记时刻 minute close 为 null 时自动过滤。
- 成交行 SSE 增量推送后点击：直接用行内数据，无额外请求。
- 自选股弹窗其它调用方（Watchlist/Screener 等）不传新 props → 行为完全不变。

## 验证

- 前端无测试脚本：`cd frontend && pnpm lint` + `pnpm build`（`tsc -b` 类型检查）通过。
- 手动冒烟：开一个分钟级模拟账户跑出成交 → 点持仓行/成交行验证弹窗日期、分时、B/S/止损标颜色与位置、切日期不串标、4/1 前成交显示空分时。
- 自选股弹窗回归：确认可选 props 未影响原有打开行为（默认仍为 K 线、不选中特定日期、无标记）。
