# 量化专用交易弹窗（模拟盘/回测共用，单图 + 分钟/日线/周线）— 设计

日期：2026-08-13，分支：feat/sim-trade-analysis

## 目标

量化模拟盘详情页的**持仓表**与**成交记录表**行点击后，弹出**量化专用交易弹窗**（`QuantTradeDialog`）：单一图表区 + 分钟/日线/周线视图切换；分钟视图显示当天（或交易当日）分钟线并标注买入/卖出/止损点。量化回测的成交回放弹窗（`TradeKlineModal`）替换为该弹窗。

**不修改**自选股/监控等其它调用方的弹窗（`StockPreviewDialog`/`StockPanel` 恢复原状）。

## 背景与问题

- 第一版实现给共享 `StockPanel`/`StockPreviewDialog` 加了 `initialDate`/`initialIntraday`/`intradayMarkers` 可选 props。模拟盘弹窗强制打开「分时」后触发了 StockPanel 的既有**左右分窗**布局（日K 左 + 分时 右），且右侧因该日无分钟数据显示「暂无分钟数据」。
- 用户反馈：不需要左右分窗；默认应显示**当天的分钟线**，并有**分钟/日线/周线**切换按钮；成交行应显示**交易当日**的分钟线。自选股弹窗默认（分时关）也是单图，风格应统一。

## 方案：复制专用弹窗，共享组件回滚

### 1. 新组件 `frontend/src/components/QuantTradeDialog.tsx`

复制 `StockPreviewDialog` 的弹窗壳（遮罩/动画/顶栏：板块标、标的、名称、半年/1年预设、日期范围、刷新、关闭）+ `StockPanel` 的信息条/财务/加自选/加监控逻辑，但图表区改为：

- **视图切换器（分钟 / 日线 / 周线）+ 单一图表区**（无左右分窗）
  - **分钟**：`StockIntradayChart`（当天分钟线 + B/S/止损标记），无选中日期时自动选中最新交易日；`prevClose` 取日K前一交易日收盘
  - **日线**：`StockDailyKChart`（markers/ranges/priceLines/涨跌停标记在此生效）
  - **周线**：`StockDailyKChart period="weekly"`（前端聚合，见 §3）
- 顶栏日期范围、刷新、加自选、加监控、分时轮询偏好（`minute_intraday_refresh`）与自选股弹窗一致。
- symbol 切换（弹窗不卸载复用）：重置视图到 `initialView`、清空选中日期、重新应用 `initialDate`。

Props（全部可选，除 symbol/onClose）：

```ts
export type QuantViewMode = 'minute' | 'daily' | 'weekly'
interface Props {
  symbol: string | null
  name?: string
  onClose: () => void
  initialView?: QuantViewMode          // 默认 'daily'；模拟盘传 'minute'
  initialDate?: string                 // 分钟视图初始选中日期（成交行=交易当日）
  dateRange?: { start: string; end: string }  // 回测传持仓区间
  markers?: ChartMarker[]              // 日线成交标记（回测）
  ranges?: ChartRange[]                // 日线持仓区间（回测）
  priceLines?: ChartPriceLine[]        // 日线买入/卖出价线（回测）
  intradayMarkers?: IntradayMarker[]   // 分钟视图买卖标记（模拟盘）
  showLimitMarkers?: boolean           // 默认 true；回测传 false（维持原 TradeKlineModal 视觉）
  showMarkerToggle?: boolean           // 默认 true；回测传 false
}
```

### 2. 共享组件回滚（其它弹窗恢复原状）

- `StockPanel.tsx`、`StockPreviewDialog.tsx`：**恢复 44a5ae5 之前的状态**（移除 `initialDate`/`initialIntraday`/`intradayMarkers` props、`initialApplied` effect、分时重置 effect、透传）。
- **保留**：`StockIntradayChart.markers` 与 `EChartsIntraday.markers`（含 fail-closed date 契约）——QuantTradeDialog 分钟视图使用。

### 3. 周线聚合（纯前端，后端零改动）

`StockDailyKChart` 新增可选 prop `period?: 'daily' | 'weekly'`（默认 `'daily'`，其它调用方不传 → 行为不变）：

- `aggregateWeekly(rows)` 导出函数：按 **ISO 周**（周一为一周首）分桶——开=周首日开、高=周内最高、低=周内最低、收=周五收（最后一根不足一周按实际）、量/额求和；周均线 ma5/10/20/60 由周收盘重算；macd/rsi/kdj/boll 置 null。
- 周线视图下隐藏指标控制按钮（`showIndicatorControls && period !== 'weekly'`），避免 null 指标渲染异常；涨跌停标记基于**日K**构建、周线视图由调用方隐藏（`showLimitMarkers={false}`）。
- 周K日期标签用周首日（本地日期手拼 `YYYY-MM-DD`，不用 `toISOString` 避免时区偏移）。
- `onDataChange` 的 `rawRows` 仍为日K行（信息条/昨收/选中日期逻辑不受影响）。

### 4. 接线

- **模拟盘** `QuantSim.tsx`：持仓/成交行点击 → `QuantTradeDialog`，`initialView="minute"`：
  - 持仓行：不传 `initialDate` → 分钟视图自动选最新交易日；买入 B 标（date=买入日）切到买入日才显示
  - 成交记录行：`initialDate` = 交易日期 → 直接显示交易当日分钟线 + B/S/止损标
  - `preview` state、`parseTradeTime`/`toMarkerAction` 复用现有实现
- **回测** `StrategyBacktest.tsx`：删除 `TradeKlineModal` 引用；成交点击 → `QuantTradeDialog`，`initialView="daily"`，传 `dateRange`（entry 前 45 天 ~ exit 后 20 天）、`ranges`（持仓区间）、`priceLines`（买入价/卖出价）、`showLimitMarkers={false}`、`showMarkerToggle={false}`；删除 `TradeKlineModal.tsx`。

## 边界情况

- 分钟视图无数据（4/1 前/停牌/未回源）：沿用现有「暂无分钟数据」+ 获取按钮提示；可切日线/周线。
- 周线最后一根不足一周按实际交易日收。
- 加自选/加监控在回测场景同样可用（与自选股弹窗一致）。
- 自选股/监控/指数等其它弹窗行为与改动前完全一致。

## 验证

- `cd frontend && pnpm build`（tsc + vite）通过（前端无测试框架；pnpm lint 无配置不可用，预存在）。
- 手动冒烟：
  1. 模拟盘持仓行点击 → 弹窗默认分钟视图（最新交易日分钟线），切 日线/周线 正常，切回分钟仍显示所选日
  2. 成交记录 BUY/SELL/STOP_LOSS 行点击 → 交易当日分钟线 + B/S/止损标
  3. 回测成交点击 → 日线视图含买入/卖出价线、持仓区间，无涨跌停板标；切分钟/周线正常
  4. 自选股弹窗回归：打开/分时开关/左右分窗/加自选/加监控行为与改动前完全一致
  5. 弹窗内切日期标记不串日；周线聚合数据目视正确

## 不做（YAGNI）

- 不改共享弹窗布局；不改后端；不做 5/15/30/60 分钟多周期；周线指标仅 MA（macd/rsi/kdj/boll 置 null 且隐藏控制）。
