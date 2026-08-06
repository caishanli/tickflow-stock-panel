# 模拟盘补跑提速设计（wufu v5.2 示例）

日期：2026-08-07
分支：`optimize/sim-wufu-replay-speed`

## 背景与问题

模拟盘账户带 `start_date`（如 07-10）第一次启动/重置时，`run_loop → _run_strategy_loop`
进入 `_replay_history`，按历史交易日逐分钟补跑至今天。wufu-v5.2 池约 116 只 ETF，
从 07-10 补跑到当前（约 20 个交易日）当前耗时远超目标（>2 分钟）。

根因：**模拟盘补跑从不设置分钟线固定窗口。**

- 回测路径（`rqalpha_bridge`）在启动前调 `dm.set_minute_window(start, end)`，把整个回测区间钉住，
  池内标的分钟数据经 `preload_minute_for_pool` 走**一次批量** `get_minute_pool` 取回进 `_minute_mem`，
  后续直接内存命中。
- 模拟盘补跑**没有** `set_minute_window`，DM 走默认滑动 **15 天窗口**（`minute_lookback=15d`）。
  每个补跑交易日 `as_of` 前移 → `_minute_cached()` 覆盖校验 miss → 池内全部标的**重新网络回源
  （逐标的 `get_minute_price_at` + 策略自带的 `preload_minute_for_pool`，wufu-v5.2.py:295）**。

结果：07-10→今约 20 个交易日 × ~116 只反复回源，网络往返占补跑墙钟时间的主体。

## 目标与验收

- 目标：wufu v5.2 从 07-10 补跑到当前，账户 start/reset 墙钟 < 2 分钟。
- 正确性：补跑结果必须与现状逐笔一致（交易、净值、快照、日志与当前完全可复现）。

## 方案

最小改动，仅动模拟盘补跑路径（`runner.py` 的 `_replay_history`），复用现有
`set_minute_window` / `preload_minute_for_pool`，**改动回测路径**。

1. 在 `_replay_history` 算出 `days = _trade_days_between(dm, start_date, today)` 之后，
   调 `dm.set_minute_window(start_date, today)`，把整个补跑区间钉为分钟窗口。
   - 语义与 `rqalpha_bridge` 回测前调用一致。
   - `_replay_partial_day`（日内盘中重启补跑）同样在补跑前钉窗口（start=当天，end=今天）。

2. 补跑前对池内标的一次性批量预取：`dm.preload_minute_for_pool(pool_codes, today)`，
   单次 `get_minute_pool` 网络往返取回全部标的全窗口分钟，填进 `_minute_mem`。
   - pool codes = 策略 `_seed_universe` 后 `ctx.universe` 合的标集 + 持仓 + 强指数
     （与 `_strategy_tick` 里 `watch` 口径一致，取自 `ctx.universe` 与持仓）。
   - 此后策略自身 `preload_minute_for_pool` 与逐 bar `get_minute_price_at` 全部内存命中。

## 正确性保证

- 内存中缓存前向分钟不会向当前 bar 泄漏未来数据：`get_minute_price_at` 用
  `df.index.searchsorted(dt, side='right') - 1` 切片取 ≤ dt 的收盘，回测早已依赖同一保证。
- 覆盖校验（`_minute_cached`、H6c）在窗口内连续命中；窗口外（实时段）自然 miss 后
  回退新窗口，自愈，无需手动重置窗口。
- 只改内存缓存生命周期，不改取数口径（真实 mootdx 分钟、前复权、拆分调整逻辑不变），
  故交易/净值逐笔与现状可复现。

## 风险与验证（plan 阶段）

- **内存**：116 只 × 20 交易日分钟放在内存，规模可控（每只约 4800 bar）。
  若个别策略池超大再评估批量分批，本轮不扩。
- **正确性门禁**：以 `tests/fixtures/wufu_v52/sim_260710/live_transaction_list.csv`（17 笔交易）
  与对齐 diff 为准；`diff_jq_vs_local` / `run_quant_sim.py` 对齐命令为验收手段。
- **回归**：`uv run --extra dev pytest tests/quant/test_runner_strategy.py` 全绿。

## 不做

- 不改 backtest 路径（慢源复用给它提速）。
- 不求对接批量并行 / asyncio 预取（`get_minute_pool` 单次往返已等价）。
- 不加本地持久化分钟缓存文件（过度设计，窗口修复已消除冗余 IO）。