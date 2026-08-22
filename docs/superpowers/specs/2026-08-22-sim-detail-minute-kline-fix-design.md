# 模拟盘详情页分时图修复设计：Y轴自适应 + 买卖点标记 + `.SS` 归一化

日期：2026-08-22
分支：`fix/sim-detail-minute-kline-flat`（基于 `custom-main`）

## 问题

量化模拟盘详情页（`QuantSim.tsx`）点击持仓/交易记录表中标的代码，弹出的分钟线「基本都是一条直线」。

根因（已用真实数据验证）：

1. **Y轴被撑到 ±10%**：`frontend/src/components/EChartsIntraday.tsx:56` 的 `computeAvgPrice` 把 volume 按「手」×100 折算股数，但 stockdata 分钟数据 volume 单位是**股**（实测 `amount ≈ volume × close`，本地 `kline_minute`/`kline_etf_minute` parquet 同样为股）。均价线值因此变成真实价的 1/100 → 自适应模式的 maxDiff 被 |avg − prevClose| ≈ 昨收撑爆 → 被钳到 ±10% 涨跌停带 → 价格线压成直线。
2. **买卖标记基本不可见**：标记链路（QuantSim → QuantTradeDialog → StockIntradayChart → EChartsIntraday）已打通，但持仓行点击不传日期 → 弹窗自动选中最新交易日 → 入场日标记因 `m.date !== chartDate` 被过滤；成交行点击只显示被点击那一笔的标记。
3. **ptrade 账户分时空白**：ptrade 域代码为 `.SS` 后缀（如 `518880.SS`），后端 `_to_jq_code` 系列只认 `SH/XSHG` → `.SS` 误判为深市 → 全链路查无数据，接口返回 `source:"none"` 空数据。

用户需求：纵坐标根据实际波动范围自动缩放；弹窗中显示买卖点位标记。

## 方案选型

采用 **方案 A：前端自适应修正 + 标记全量化 + 后端 `.SS` 归一化**（无 API 契约变化，改动集中）。

否决的备选：

- 方案 B（后端统一 volume 单位契约）：TickFlow 回退路径单位在 Free 模式下无法验证，且影响 info-bar 换手率等所有消费方，爆炸半径大。
- 方案 C（前端按 dataSource 显式传 volumeUnit prop）：stockdata 模式内部含 TickFlow 回退分支，单一 prop 无法表达混合来源。

## 设计

### §1 Y轴自适应修复（frontend/src/components/EChartsIntraday.tsx）

1. **maxDiff 剔除均价线**：`buildOption` 中参与范围计算的 `priceArrays` 由 `[closes, highs, lows, avgData]` 改为 `[closes, highs, lows]`。均价线数学上恒在 `[minLow, maxHigh]` 内，不可能合法地撑大范围；剔除后 Y 轴严格贴合真实价格波动，且免疫均价计算错误。
2. **自适应统一边距**：×1.1 padding 从 `!showLimitLines` 分支改为自适应模式统一生效。处理顺序：
   - raw maxDiff = max(|high − prevClose|, |low − prevClose|)（仅有效 bar）
   - 若超出涨跌停带则钳制到 limitDiff（保留上限语义）
   - 否则 ×1.1 留边
   - minDiff 地板兜底（showLimitLines ? 1% : 0.1%，防零波动除零）
   - 昨收居中对称（yMin/yMax = prevClose ∓ maxDiff）惯例保持不变
3. **均价线 volume 单位自探测**：取 volume>0 且 close>0 的有效 bar，计算 `median(amount/volume)` 与 `median(close)` 的比值——比值落在 [30, 300] 判为「手」（累计股数 = Σvolume×100），否则按「股」（Σvolume）；无有效 bar 回退按股。useMemo 缓存随 data 重算。自动兼容 stockdata(股) / 本地 parquet(股) / TickFlow 回退(疑似手) 三种来源。

不受影响：指数页 `showAvgLine={false}` 不走均价；MiniIntraday（自选/选股迷你分时）按下标画线不经过此逻辑。

### §2 买卖点标记全量化（frontend/src/quant/pages/QuantSim.tsx）

- 新增 helper `buildSymbolMarkers(trades, sym)`：从账户全部成交（`sortedTrades`，组件 L711 已有）过滤 `t.code === sym`，映射 `{date, time, price, action: 'BUY'|'SELL'|'STOP_LOSS'}`（复用现有 `parseTradeTime` / `toMarkerAction`）；时间解析失败的条目跳过。同账户内代码格式一致，直接字符串匹配。
- **成交记录行点击**：markers 从「仅本笔」改为该标的全部成交；初始日期仍为交易当日。
- **持仓行点击**：补传 `date = parseTradeTime(p.entry_ts)?.date`（开在入场当日）；markers 传全部成交。entry_ts 缺失时不传 date，沿用弹窗回退最新交易日的现有行为。
- **渲染端零改动**：`buildMarkerPoints` 已按「标记日期 === 图表选中日期」过滤，切日期标记自动跟随；B/S/止损三角样式沿用。

### §3 `.SS` 代码归一化（后端三处）

把 `"SS"` 加入沪市后缀集合：

| 文件 | 函数 | 影响 |
|---|---|---|
| `backend/app/api/kline.py:39` | `_to_jq_code` | 弹窗分钟+日K端点 |
| `backend/app/services/stockdata/sources.py:44` | `_to_jq` | stockdata 服务侧 |
| `backend/app/quant/datasource/network_client.py:40` | 客户端归一化 | StockDataClient |

顺带把 `sources.py:57` 的指数判定后缀集合加 `"SS"`（一致性；`.SS` 的 `000xxx` 沪市指数判定正确）。前端不做符号转换，弹窗头部仍显示原始 `.SS`（与表格一致）。

注：`rqalpha_bridge._to_jq` 已支持 `.SS`（见 `tests/quant/test_ptradecompat.py`），无需改动；`mootdx_service.py:199` 的 mkt 来自服务端响应，属不同上下文，不在本次范围。

### §4 验证与验收

自动化：

- 扩展 `tests/test_kline_stockdata_source.py::test_to_jq_code` 加 `.SS` 用例；为 `sources.py::_to_jq`、`network_client.py` 归一化补断言
- `cd backend && uv run --extra dev pytest tests/test_kline_stockdata_source.py` 及相关 quant 测试
- `uv run --extra dev ruff check app`、`uv run --extra dev mypy app`
- 前端（无测试脚本）：`cd frontend && pnpm lint && pnpm build`

手动验收（`./dev.sh` → 模拟盘详情页）：

1. 点成交记录行：波动占图高比例合理（不再贴成直线），黄色均价线在价格带内而非贴底，当日全部 B/S/止损标记可见
2. 点持仓行：直接打开入场当日，B 点落在买入分钟上
3. 切换日期：标记跟随日期显隐
4. ptrade 账户（`.SS`）：分时/日K 出图不再空白
5. 回归：自选列表迷你分时、指数页分时、个股页分时无样式回归

## 边界（明确不修）

- 货币ETF（如 511880）日内波幅 ~0.01% 属真实数据，修后仍近似直线
- 个股页 default 路径北京 naive 时间戳经 fmtTime +8h 全部落不到时间轴的独立缺陷（另一隐性 bug，另行处理）
- kline.py `date.today()` vs `cn_today()` 的时区分歧（Docker UTC 凌晨误判「今天」，另行处理）

## 影响面

- 前端：`EChartsIntraday.tsx`、`QuantSim.tsx`（两个文件）
- 后端：`api/kline.py`、`services/stockdata/sources.py`、`quant/datasource/network_client.py`（各一行级改动 + 测试）
- 无 API 契约变化、无 schema 变化、无新依赖
