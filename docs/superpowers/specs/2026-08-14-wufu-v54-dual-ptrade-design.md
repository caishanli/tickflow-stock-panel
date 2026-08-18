# wufu v5.4 双持仓 PTrade 可运行版 + 引擎适配（wufu-v5.4-dual-adapt.ptrade）

- 日期：2026-08-14
- 分支：`feat/ptrade-engine`
- 目标：
  1. 产出一个**可直接粘贴到真实 PTrade 平台运行**的 wufu v5.4 双持仓策略文件
     `backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py`；
  2. 让**本地引擎能直接跑这个 ptrade 文件**（模拟盘 + 回测），
     性能、收益、成交与 JQ 双持仓策略文件（`wufu-v5.4-dual-adapt.py`）对齐。

## 现状

JQ 策略有两条执行路径，均以 `.XSHG/.XSHE`（rqalpha 原生 order_book_id 格式，也是本地 DataManager 数据键格式）为引擎内部代码域：

- **回测**：rqalpha + `app/quant/jqcompat.py`（`rqalpha_bridge.run_jq_backtest` / `scripts/run_jq_rqalpha.py`）
- **模拟盘**：本地单机引擎 `app/quant/jqengine/engine/jq/`（`simulate/runner.py` 驱动，双持仓模拟盘账户 404a7e64 用此路径），不走 rqalpha

参照物：
- JQ 双持仓策略：`backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.py`（holdings_num=2，`select_cross_asset_dual()` 跨资产双持仓 + 自适应权重）
- 单持仓 PTrade 版：`backend/tests/fixtures/wufu_v54/wufu-v5.4.ptrade.py`（含完整 PTrade 平台适配层）

## 代码域决策（核心）

**ptrade 路径全程 PTrade 代码域**（`.SS`/`.SZ`）：
- 策略侧 API（`get_history` 返回列、`get_positions()` 键、`order` 入参、`_cd()` 快照）
- 引擎内部（`ctx.portfolio.positions` / `trades` / `minute_prices` / `no_buy` / `no_sell` / `g.*` 池子 / `set_universe`）

**仅数据层边界翻译为引擎原生 ID**（`.SS→.XSHG`、`.SZ→.XSHE`）。这是给 rqalpha/DataManager 说它的母语，不依赖 jqcompat，与聚宽策略无关。

- `names.resolve_name` / `matcher._is_etf` / `matcher.step` 均为纯 6 位代码前缀匹配，`.SS/.SZ/.XSHG/.XSHE` 通吃，无需转换。
- 真正需要翻译的只有 ~6 个数据层触点，runner 里加 `conv` 钩子（jq 策略恒等，ptrade 策略翻译）：
  - `_seed_universe`（g.* 池为 PTrade 代码 + 硬编码指数码转策略域）
  - `_strategy_tick`（feed watch 转引擎码、返回 prices 键转回 PTrade 再写 `minute_prices`）
  - `_prev_close_dm`、`_revalue_at_close`、`_hist_feed`、`_replay_history`

## 交付物

| # | 交付物 | 位置 |
|---|---|---|
| 1 | ptrade 双持仓策略文件 | `backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py` |
| 2 | 本地 ptrade 引擎（模拟盘） | `backend/app/quant/ptradeengine/` |
| 3 | rqalpha ptrade 兼容层（回测） | `backend/app/quant/ptradecompat.py` |
| 4 | runner 路由 + 回测入口 | `simulate/runner.py` 改造 + `rqalpha_bridge.run_ptrade_backtest` + `scripts/run_ptrade_rqalpha.py` |
| 5 | 测试 | `backend/tests/quant/test_ptradeengine_*.py` 等 |

## 1. ptrade 双持仓策略文件

底座 = 单持仓 `wufu-v5.4.ptrade.py`（完整 PTrade 适配层保留：`_pt()/_wide/_as_series_values/_cd/_set_last_data/_is_halted/_limit_prices/_positions_map/smart_order_target_value/minute_level_stop_loss/check_defensive_etf_available` 等），增量 = JQ 双持仓 `wufu-v5.4-dual-adapt.py`：

1. **initialize**：`holdings_num=2`；新增 `cross_slot1_floor=0.3` / `cross_slot1_retain_ratio=0.85` / `cross_adaptive=True` / `cross_weight_cap=0.85` / `target_weights=[0.5,0.5]`；`defensive_etf=511880.SS`；初始化日志补双持仓段。
2. **调度**：`run_daily(context, afternoon_routine, '13:10')` / `sell_routine 13:10` / `buy_routine 13:10`（对齐模拟盘口径 42f91131）；`check_weak_period_daily 09:40`；`before_trading_start` / `after_trading_end` / `handle_data`（分钟止损）沿用单持仓 ptrade 骨架。
3. **get_final_ranked_etfs 第 4 步**：整块替换为 `select_cross_asset_dual()`（从 JQ 版移植，`log_buffer` 用 `%` 格式化，`current_holdings = list(_positions_map().keys())`）；`g.ranked_candidates_full = filtered_list` 保留。
4. **execute_buy_trades**：改槽位加权分配——按 `g.target_weights[slot]` 目标市值下单（`_get_available_cash` / `_get_total_value`），最后一笔用剩余现金，含涨停/停牌顺延 fallback。
5. **卖出/止损/防御 ETF/智能下单**：与单持仓 ptrade 版一致。

## 2. 本地 ptrade 引擎（模拟盘路径）

新建 `backend/app/quant/ptradeengine/`，镜像 `jqengine/engine/jq/`：

- **`context.py`**：基于共享 `Context` 增加 `blotter.current_dt`（策略 `_current_dt` 依赖）；`Position`/`Portfolio` 加只增别名（`enable_amount→closeable_amount`、`cost_basis→avg_cost`、`last_sale_price→price`、`portfolio_value→total_value`），与 jq 共用撮合/持久化逻辑。
- **`ptrade_api.py`**：PTrade API 适配层，`_state` 形状与 `jq_api._state` 一致（`minute_prices`/`no_buy`/`no_sell`/`fee_config`/`trades`/`log_sink` + `on_new_day()`）：
  - `get_history(count, '1d'/'1m', field, security_list, fq='pre')` → DataFrame(index=时间, columns=PTrade 代码)，数据走 DataManager（与 jq `get_price` 同源同 fq 口径）
  - `get_trading_day` / `get_trade_days` / `get_position` / `get_positions` / `order` / `set_universe` / `get_stock_name` 等
  - `get_stock_status(HALT)` 由分钟数据可得性推导；`get_market_list` / `get_market_detail` 复用 DataManager etf 名录，失败降级固定池
  - `run_daily(context, func, time)` 注册 `(func, time)`；`handle_data` / `before_trading_start` 包装为 `(ctx)` 签名，把每 bar 快照（`minute_prices` + 日线 OHLC）组装成 `{PTrade 代码: SecurityUnitData}` 喂给策略
- **`ptrade_loader.py`**：编译 ptrade 策略，产出与 jq bundle 同形状的 bundle（hooks + `daily` 任务 + `conv` 代码转换器）。

**runner 改造**：`_load_engine` 按策略 flavor（代码含 `.SS`/`.SZ` → ptrade）路由；~6 个数据触点加 `conv` 钩子（见代码域决策）。matcher/names/`_persist` 零改动。

## 3. rqalpha ptrade 兼容层（回测路径）

新建 `backend/app/quant/ptradecompat.py`，镜像 `jqcompat.py`：

- **`install_ptradecompat(...)`**：`register_api` 注入 PTrade API（`get_history` / `get_trading_day` / `get_trade_days` / `get_position` / `get_positions` / `order` / `run_daily(context,func,time)` / `set_universe` / `get_stock_status` / `get_stock_name` / `get_market_list` / `get_market_detail` / `set_benchmark` / `set_commission` / `set_slippage` / `log`），strategy 侧 PTrade 代码，内部转 rqalpha 原生 order_book_id。
- **数据源复用 `JqDataSource`**：仅给 rqalpha 引擎喂数据，与策略语言无关。
- **钩子适配**：`handle_data(context, bar_dict)` / `before_trading_start(context, data)` 由 bridge 喂给 rqalpha 前重写策略源码，把 BarDict 转成 `{PTrade 代码: SecurityUnitData}`（与 `_set_last_data` 消费格式一致）。
- **`rqalpha_bridge.run_ptrade_backtest(strategy_path, params)`**：镜像 `run_jq_backtest`（dm 预载/offline/区间收敛/安装 ptradecompat/rqalpha run/提取 trades+equity+metrics），universe 提取与 benchmark 做 `.SS/.SZ` 双向映射。
- **`scripts/run_ptrade_rqalpha.py`**：镜像 `run_jq_rqalpha.py`。

## 4. 验证

1. 策略文件 `py_compile` + API 清单核对（无 `jqdata` / `record` / `set_option` / `set_order_cost` / f-string 等非 PTrade 调用）。
2. **回测对齐（核心）**：`run_ptrade_rqalpha.py`（ptrade 双持仓）vs `run_jq_rqalpha.py`（jq dual-adapt），同窗口 04-01~08-11、同现金/费率/滑点——同一 rqalpha 引擎同一数据，仅语言层不同，成交序列 + 逐日净值差 ≤0.05%。
3. 本地引擎对齐：短窗口跑 ptradeengine vs jqengine，成交一致。
4. 单测：`select_cross_asset_dual` 移植一致性（同一输入 jq/ptrade 两个版本选出相同槽位/权重）、代码转换、`get_history` 形状、ptrade 策略文件编译。
5. 新代码过 `ruff` + `mypy`。

## 实施顺序

- **Phase 1**：ptrade 策略文件 + ptradecompat（rqalpha）+ `run_ptrade_backtest` + `run_ptrade_rqalpha.py` + 回测对齐验证。
- **Phase 2**：ptradeengine（本地）+ runner 路由 + 模拟盘对齐。
