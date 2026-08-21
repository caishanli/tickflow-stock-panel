# 模拟盘持仓现价显示逐股行情时间 — 设计

日期：2026-08-21　分支：`feature/sim-position-price-ts`（自 `custom-main`）

## 目标

量化模拟盘「持仓」表的现价列，在价格后附该价格对应的行情 bar 时间，逐股精确：

- 当日价格 → `HH:MM`（如 `10:31`）
- 非当日（停牌/隔夜旧价）→ `MM-DD HH:MM`
- 无时间信息（历史遗留数据）→ 不显示

## 方案

feed 返回值扩为三元组 `(prices, bar_dt, price_ts)`，新增 `Position.price_ts` 字段随
`positions_json` 持久化。否决的备选：aux 旁路 dict（重启丢失、matcher 路径覆盖不到）。

数据链路（全部已有通道，无 API/DB 变更）：

```
live_feed.refresh / _hist_feed          → price_ts = {code: str(bar_ts)}
runner._strategy_tick / _mark_to_market → pos.price_ts（ptrade 域 key 经 _to_pt 转换）
Position.price_ts (jqengine context.py) → _state_from_portfolio 序列化 "price_ts"
positions_json（无 schema JSON）        → read_sim_state → /status 与 SSE 全量透传
QuantSim.tsx 现价单元格                  → 小字渲染
```

## 后端改动

### 1. `backend/app/quant/simulate/live_feed.py`

`refresh()` 循环内已有每 code 的 `bar = sub.index[-1]`（line ~70），收集为
`price_ts[code] = str(bar)`，返回 `(prices, latest, price_ts)`。docstring 同步更新。

### 2. `backend/app/quant/simulate/runner.py`

- `_hist_feed`（line ~717）：补跑价取自 `now` 时刻 bar，`price_ts = {code: str(now)}`；
  当日实时兜底分支用快照各 code 的 bar 时间；返回三元组。
- `_mark_to_market`（line ~965）：解包三元组，`pos.price = px` 处同步写
  `pos.price_ts`。
- `_strategy_tick`（line ~1008）：解包三元组；ts dict 的 key 与 prices 一样经
  `_to_pt` 转换；line ~1061 设置 `pos.price` 处同步写 `pos.price_ts`。
- 收盘重估（line ~638）：`pos.price_ts = str(close_ts)`。

### 3. `backend/app/quant/jqengine/engine/jq/context.py`

`Position.__init__` 增加 `price_ts=None` 字段。

### 4. 持久化（`runner.py` 内）

- `_state_from_portfolio`（line ~439）：每持仓增加 `"price_ts": getattr(p, "price_ts", None)`。
- `_restore_portfolio`（line ~415）：恢复 `price_ts=sp.get("price_ts")`。
- `positions_json` 为无 schema JSON，老数据缺字段自然兼容，无需 DB 迁移；
  `/sim/accounts/{aid}/status` 与 SSE `status` 事件全量透传 state，API 层零改动。

## 前端改动

`frontend/src/quant/pages/QuantSim.tsx` 现价单元格（line ~727）：价格后追加小字
`<span className="text-muted">`，格式化：与今天同日 → `HH:MM`，否则 → `MM-DD HH:MM`，
字段缺失不渲染。

## 边界情况

- 停牌股：refresh 沿用旧帧，`sub.index[-1]` 为旧日期 → 显示旧时间（正确语义）。
- 重启恢复：`price_ts` 从 positions_json 还原。
- matcher 止损路径改写的 state 价格为瞬时值，tick 末尾统一由 portfolio 回写覆盖，无需单独处理。

## 测试

- `backend/tests/quant/test_live_feed.py`：6 处二元组解包改三元组；新增 price_ts 断言
  （正常 bar、空帧沿用旧帧、全空）。
- runner 侧现有测试回归：`uv run --extra dev pytest tests/quant -q`。
- 前端：`pnpm lint` + `pnpm build`。
