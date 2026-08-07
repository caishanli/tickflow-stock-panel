# 模拟盘实时打标 + 收盘重估 设计

**目标**：修复模拟盘「今日收盘重估」缺失，并在盘中提供实时估值（秒级跟随行情）。

**背景（实测复现，账户 ed6ccd5c）**：今日 13:15 买入 517520 后，`pos.price` 停在买入价 2.031，
实际收盘 2.112，净值低估约 +3,904；快照在 15:00 后停止演进。两个根因：

1. **收盘重估缺失**：`_eod`（runner.py:469）只按持仓「最后收到的 feed 价」落终态。
   若某持仓当天最后一根分钟价没取到（补跑盘中买入 → 买入后才成为持仓、不在预取池 →
   `_hist_feed`/`get_minute_price_at` 后续 bar 返回 None → `pos.price` 停在买入价），
   收盘快照就是错的。且 `_replay_history` 的「今天」分支（runner.py:680）有意不跑 `_eod`，
   收盘后启动/重置账户时也没有收盘重估。

2. **盘中只按分钟 bar 打标**：`_run_strategy_loop` 主循环每 ~60s 才 tick 一次
   （runner.py:882 `time.sleep(max(1, 60 - now.second + TICK_OFFSET))`），
   只有此时 `_strategy_tick` 才刷新 `pos.price`（runner.py:762-764）。两次 tick 之间前端净值不动。

## 方案

### 1. 盘中实时打标（引擎内 mark 步骤）

在 `_run_strategy_loop` 主循环交易时段增加一个亚分钟 mark 步骤：

- 新增常量 `MARK_INTERVAL = 10`（秒），`MARK_SNAPSHOT_TICK = 0.0005`（快照阈值，0.05%）。
- 每轮策略 tick 之后、下一个 60s tick 之前，以 `MARK_INTERVAL` 间隔循环：
  - 若 `in_trading(now)` 且存在持仓，调 `live_feed.refresh(dm, position_codes, now)`（走网络客户端
    `current_snapshot`）取最新价；
  - 对每只持仓更新 `pos.price` 与 `state["positions"][code]["price"]`，重算 `net_value`/`pnl`；
  - 仅当任一持仓价相对上次打标价变动超过 `MARK_SNAPSHOT_TICK` 时，才写 `save_state`
    （SSE `status` 事件 → 前端卡片/持仓表刷新）并落一条 `sim_snapshot`，避免刷屏。
  - mark 步骤**只改估值，不触发策略回调，不跑 matcher**（策略 tick 保持 60s 驱动原样）。

实现位置：`_run_strategy_loop` 的 `while` 主循环内，`_strategy_tick` 后插入子循环。
注意保持「对齐分钟边界」语义——mark 子循环内 sleep 需用更细粒度（`MARK_INTERVAL`），
并在下次 tick 前主动退出（由 `_strategy_tick` 的 bar 去重保证不重复驱动）。

### 2. 收盘打标 `_revalue_at_close`

新增独立函数 `_revalue_at_close(dm, ctx, state, bar_dt)`：

- 对每个持仓，用可靠的**当日收盘价**重赋值 `pos.price`：
  - 首选 `dm.get_minute_price_at(code, 当日 15:00)`（真实 1m 收盘价）；
  - 取不到时回退 `_last_price(provider.get_daily(code, today, today))`。
- 更新 `state["positions"]` 并重算 `net_value`/`pnl`，落 `save_state` + `sim_snapshot`。

挂载点（两处）：

- **`_eod`**（runner.py:469）：每交易日收盘处理时，在 `_persist` 前调用
  `_revalue_at_close(dm, ctx, state, 当日 15:05)` → 终态快照即真实收盘净值。
- **`_replay_history`「今天」分支结束**（runner.py:688 之后）与
  **`_replay_partial_day` 结束**：当 `now` 已过 `SESSION_END_GRACE`（15:02）时，
  在补跑 batch 落库前调用 `_revalue_at_close` → 收盘后启动账户也能先按 close 重估再进实时。

**注意**：`_revalue_at_close` 在补跑期间调用时，写快照必须走 `_persist`（batch 攒批），
不能直接 `db.insert_sim_snapshot`——否则收盘重估快照与补跑快照落库顺序错乱。

## 影响面

- 修改文件：`backend/app/quant/simulate/runner.py`（主循环 mark + `_revalue_at_close` + `_eod`/补跑接线）。
- 新增测试：`backend/tests/quant/test_runner_mark.py`。
- 不动：策略回调 / matcher / 撮合 / SSE 协议（`state` 与快照结构不变，前端零改动）。
- 回测与 fixture 对齐命令不受影响（模拟盘内部改动）。

## 验收

1. `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py -q` 通过。
2. `uv run --extra dev pytest tests/quant/test_runner_strategy.py -q` 通过（回归）。
3. `uv run --extra dev ruff check app` / `uv run --extra dev mypy app` 通过。
