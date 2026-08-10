# 量化模拟盘成交记录增加每个标的的持仓时长

- 日期：2026-08-10
- 分支：`feature/sim-trade-hold-days`（基于 `custom-main`）

## 背景与目标

模拟盘「成交记录」tab（`frontend/src/quant/pages/QuantSim.tsx`）目前逐行展示买入/卖出
成交，用户无法直观看出每个标的拿了多久。目标：

1. 成交记录表格新增「持仓时长」列，**买入行与卖出行都显示**。
2. 鼠标悬停在表格任意单元格时**整行高亮**。

## 口径定义

- **持仓时长 = 交易日个数**（真实交易日历，剔除周末/节假日）：
  `持仓交易日数 = 交易日索引(卖出日) − 交易日索引(买入日)`。T+1 最小持有 = 1；
  日内买卖（0）显示 `<1天`。
- **买卖配对 = 分标的 FIFO**：按时间正序回放每只标的的成交，买入建批次，卖出按
  先进先出抵消批次。多批次合并卖出时按数量加权；分批卖出时按卖出数量加权回填买入行。
- **卖出行** 时长 = 卖出日 − 匹配买入批次的加权平均交易日指数（四舍五入为整数）。
- **买入行** 时长 = 该批次被平仓时，匹配卖出日 − 买入日（按数量加权，取整）。
  尚未平仓的买入行显示 `持有中`；无匹配数据的行显示 `—`。

## 架构决策：前端计算 + 后端供交易日历

买卖行都需要持仓时长，而买入行的时长要到未来卖出时才可知。SSE 增量只推送新成交，
若后端逐行预计算/落库，买入行无法在匹配的卖出到达时被回填。因此：

- **前端持有全量 `tradeList`**（首拉 `getSimTrades` + SSE 追加），按数据变化重算，
  买入行自动回填，无需额外事件。
- **后端只提供真实交易日历**：`sim_status` 响应 data 新增 `trade_days: list[str]`，
  范围 `[账户 start_date(或最早成交日), 今天]`，来自 `StockDataClient.get_trade_days`；
  网络异常降级为工作日（周一~周五）日历。
- 不改 `sim_trades` schema、不改 SSE 事件结构。

## 改动清单

### 1. 后端：`sim_status` 附带交易日历（`backend/app/quant/api/quant.py`）

- `sim_status`（:298）data 追加 `trade_days`：
  - 起始日 = `account.start_date`（空则取该账户最早成交日；再空取今天）
  - 结束日 = 今天
  - 取数：`from ..datasource.network_client import StockDataClient`，
    `client.get_trade_days(start, end)` 返回 `["YYYY-MM-DD", ...]`
  - 异常兜底：用 `pandas.bdate_range`（工作日）降级，保证接口不挂
- additive，不破坏现有前端字段

### 2. 前端：持仓时长计算 + 展示 + 整行高亮（`frontend/src/quant/pages/QuantSim.tsx`）

- 新增纯函数 `computeHoldDays(trades, tradeDays)`：
  - `tradeDays` 转 `Map<YYYY-MM-DD, index>`；交易日不在表内的行按工作日索引兜底插补
  - 按 `ts` 正序回放：BUY 建批次 `{amount, idx}`；SELL 先进先出抵消，
    记录卖出行总加权时长与每批次被平仓的加权时长
  - 返回 `Map<tradeIndex, {holdTd: number|null, open: boolean}>`
- 成交记录表新增「持仓时长」列（表头 + 单元格）：
  - `holdTd == null`：未平仓买入 → `持有中`；其余 → `—`
  - `holdTd == 0`：`<1天`
  - `holdTd >= 1`：`{n}个交易日`
- 成交记录 `<tbody>` 行 className 追加 `hover:bg-elevated/60`（沿用本文件 :146 已有惯例），
  实现整行高亮
- 持仓时长随 `tr`/`st` 变化（useMemo/useEffect）自动重算，SSE 追加卖出后买入行回填

## 影响面

- 只改量化模拟盘成交记录（后端 `sim_status` + 前端 QuantSim.tsx），不动回测成交表、
  不动 schema、不动文档中其他模拟盘功能
- 交易日历取数失败时降级工作日近似（仅节假日隔离时段有轻微偏差），不影响正确性
- 数据不一致（卖出多于可用批次等）按剩余批次截断，不报错

## 测试

- 后端：新增 `_build_trade_days` 辅助单测（含降级路径）；`sim_status` 返回含
  `trade_days` 字段（现有 test_service/test_api 风格补齐）
- 前端无测试脚本：`pnpm lint` + `pnpm build`（`tsc -b && vite build`）通过
- 手动验收：启动模拟盘跑出成交后，成交记录买卖行显示 `<1天`/`N个交易日`/`持有中`，
  鼠标悬停整行高亮

## 验收

- 成交记录表列头含「持仓时长」
- 买入行（已平仓）显示持仓交易日数，未平仓显示 `持有中`
- 卖出行显示持仓交易日数；日内买卖显示 `<1天`
- 悬停任意单元格整行高亮