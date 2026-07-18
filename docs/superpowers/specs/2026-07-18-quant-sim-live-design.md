# 设计文档：量化模拟盘（策略驱动实时模拟）后端

- 日期：2026-07-18
- 状态：已澄清（brainstorming 结论见 §1）
- 范围：把现有「止损看护型」模拟盘骨架升级为「策略驱动全自动实时模拟盘」。仅后端；前端下一轮再做。
- 依赖：`2026-07-13-quant-rqalpha-design.md`（总体架构）、`CONSTRAINTS.md`（C1/C2/C3 分钟数据约束）

## 1. 已确认的需求决策

| 决策点 | 结论 |
|--------|------|
| 模拟盘形态 | **策略驱动全自动**：账户绑定一个聚宽式策略，实时盘自动跑策略、产生买卖信号并本地撮合 |
| 策略执行频率 | **盘中分钟级**：交易时段每分钟驱动策略（run_daily / run_minute / handle_data） |
| 引擎 | **自研 jqengine 单机引擎**（`app/quant/jqengine/engine/jq/`，聚宽 API 子集），不走 rqalpha |
| 账户约束 | 暂不加止盈/最大持仓/仓位上限，仅保留现有 `capital / stop_loss` |
| 本次范围 | **只后端**（db / engine / runner / service / API），前端 QuantSim 页下一轮再改 |

### 现状盘点（为什么不是从零写）

- 已有：sim 五表（`db.py`）、账户 CRUD + start/pause/reset API、独立进程 `run_quant_sim.py`、止损 `Matcher`、`QuantSim.tsx` 简单页面。
- 缺口：`runner.py` 只对已有持仓做止损巡检，**持仓没有来源**；策略完全不参与实时盘。
- 可复用的关键资产：`jqengine/engine/jq/`（loader 编译策略 + 聚宽 API 实现 + Portfolio 撮合，目前仓库内无驱动方——祖先项目的"回测桥"未移植）；`jqengine/datasource/manager.py` 的 `DataManager`（`_daily_mem`/`_minute_mem`/`fetch`/`get_minute_price_at`，符合 engine API 的 manager 协议）；`mootdx_src.get_minute(code)` 可取当日实时分钟 bar。

## 2. 总体架构

```
POST /sim/accounts {name, capital, stop_loss, strategy_id}
POST /sim/accounts/<aid>/start
   └─ service.account_start()  派生子进程 run_quant_sim.py <aid>（现有机制不变）

run_quant_sim.py（独立进程）
   └─ runner.run_loop(aid)
        ├─ 读账户 → strategies store 取策略源码
        ├─ jq.load_strategy(code, manager=dm, fee, slippage, cash)  → ctx + 注册的 run_daily/run_minute
        ├─ 从 sim_state 恢复持仓/现金（崩溃续跑）
        ├─ init(context)（一次）
        └─ 主循环（交易时段每分钟）：
             1. live_feed.refresh(dm, watch_codes)   # mootdx 实时分钟 → _minute_mem + minute_prices
             2. ctx.current_dt = 最新 bar 时刻
             3. 触发到期 run_daily（open/close/HH:MM/every_bar，每日一次）
             4. 触发 run_minute 与策略 handle_data(context)（若定义）
             5. Matcher.step() 止损巡检（现有逻辑，用新价格）
             6. 落地：sim_trades（新增成交）/ sim_state / sim_equity_snapshots / sim_logs
```

- FastAPI 与子进程仍**只经 quant.db 通信**；pause 文件 + pid 杀进程组语义不变。
- 引擎 API 命名空间内已有 `order/order_value/order_target_percent` 等（`engine/jq/api.py`），策略原样调用，成交进 `_state["trades"]`，由 bridge 逐笔落库。

## 3. 数据模型变更（`quant/db.py`）

- `sim_accounts` **新增 `strategy_id TEXT` 列**（migration：仿现有 pid/name 的 `PRAGMA table_info` + `ALTER TABLE` 模式）。`insert_sim_account` 签名加 `strategy_id`。
- 新表 `sim_logs(account_id, ts, level, message)` + `insert_sim_log()` / `get_sim_logs()`（策略 `log.info` 与 runner 事件落库，供后续前端日志面板）。
- `sim_trades` / `sim_equity_snapshots` / `sim_state` / `sim_stop_loss` 结构不变。
- `sim_state.positions_json` 扩展持仓字段：在现有 `{amount, avg_cost, price, buy_dt, today_amount}` 口径上序列化 engine Position（新增 `today_amount` 用于 T+1，见 §4）。恢复时逐字段读，缺省容忍旧格式。

## 4. 撮合规则（`engine/jq/api.py` 增强，对齐 `simulate/matcher.py` 口径）

现有 engine `order()` 只有 fee+slippage，需补齐 A 股交易规则（与 matcher 注释中声明的口径一致）：

- 佣金双边 `fee_rate`（CONFIG，默认万 3），**不设最低 5 元**（与 matcher 口径一致）；
- 印花税：卖出时非 ETF 收 0.05%（复用 matcher 的 `_is_etf` 前缀判定与 `QUANT_SIM_STAMP_TAX` 环境变量）；
- 滑点双边：买 `price*(1+slippage)`，卖 `price*(1-slippage)`（现有）；
- 整手：买入数量向下取 100 股整数倍（ETF/股票同）；卖出可零股清仓（简化）；
- T+1：`Position` 新增 `today_amount`（当日买入量），卖出可用量 = `amount - today_amount`；`on_new_day()` 清零，由 bridge 每日盘前调用；
- 涨跌停：bridge 侧维护 `no_sell`（跌停禁卖，现有 runner 逻辑）并新增 `no_buy`（涨停禁买，同一 `_prev_close` 助手，对称实现）；引擎 `order()` 接受可选 `no_sell/no_buy` 集合（由 bridge 经 `_state` 注入，默认空，保持引擎可独立测试）。

成交记录增强：`_state["trades"]` 每条补 `side`（buy/sell）与卖出时的 `avg_cost`（卖出前成本），bridge 据此算 `pnl / pnl_pct` 落 `sim_trades`。

**日志 sink**：`LogProxy` 目前只 print。新增 `_state["log_sink"]`（callable `(level, msg)`），bridge 注入为「写 `sim_logs` + print」，策略 `log.info` 实时落库。

## 5. 实时数据馈送（`simulate/live_feed.py`，新文件）

- `refresh(dm, codes, now) -> dict[code, price]`：
  1. 逐 code 调 `dm.sources["mootdx"].get_minute(code)` 取最新分钟帧（含当日盘中 bar）；
  2. 与 `dm._minute_mem[code]` 合并（去重 keep=last），更新 `_minute_cov`，使 engine `get_price(1m)` / `get_minute_price_at` 命中新数据；
  3. 返回各 code 最新 bar 收盘价 → `_state["minute_prices"]` 快照 + `_state["minute_mode"]=True`。
- **落盘**：盘中只在内存合并；`real_<code>` 于当日收盘后（`after_trading_end` 钩子处）统一写 `minute.db`——盘中取的是 mootdx 真实 1m，按 C1 允许且应当落盘，但避免每分钟全帧重写。插值/合成数据本路径不涉及，C2 不触发。
- **降级**：单 code 刷新失败 → 保留内存旧帧（沿用 `_ensure_minute_windowed` 滑窗加载的历史段），价格用最后已知价，告警进 `sim_logs`；mootdx 整体不可用 → 本轮跳过策略触发（不产生信号），止损巡检也顺延（无新价），进程不死。
- 日线：`dm.preload_daily()` 启动时一次；策略 `get_price(daily)` 走 `_daily_mem`，当日未收盘日线自然缺席（等同聚宽实盘语义）。
- watch 集合 = `context.universe ∪ 持仓 ∪ benchmark`，每轮动态取（策略可在定时任务里改 universe）。

## 6. 主循环（`simulate/runner.py` 重写）

`run_loop(account_id, dm=None, feed=None, poll_interval=20, idle_interval=30)`：

1. 读账户（无则退出）→ `store.get_strategy(strategy_id)` 取源码（无策略 → status=failed + 明确报错）。
2. `dm = get_data_manager()`（在线模式：`_offline=False`、`_use_real_minute=True`、确保 tushare token 注入，仿 `run_jq_backtest` 的处理）；`dm.preload_daily()`。
3. `jq.load_strategy(code, dm, fee=CONFIG.fee_rate, slippage=CONFIG.slippage, cash=剩余现金)`；用 sim_state 恢复 `portfolio`（持仓含 `today_amount`/`buy_dt`）。
4. 交易日判定：盘中时段（现有 `in_trading`）+ 当日是否交易日（`dm.fetch("get_daily", "000300.XSHG", ...)` 末根日期==今日，每日缓存一次；取数失败降级为 weekday 判定，与现状一致）。
5. 盘前（每交易日一次）：`on_new_day()`（T+1 清零 + `clear_current_data_cache()` + run_daily 已触发集合重置）→ 策略 `before_trading_start(context)`（若定义）。
6. 分钟循环（对齐分钟边界，每分钟 +5~10s 触发，保证刚收 bar 可读）：
   - `live_feed.refresh` → `minute_prices`、`ctx.current_dt = 最新 bar 时刻`；
   - 依次触发：`run_daily` 到期任务（'open'→09:31 bar、'close'→14:59 bar、'HH:MM'→对应 bar、'every_bar'→每 bar；每任务每日一次，'every_bar' 除外）→ `run_minute` 注册 → 模块级 `handle_data(context)`（若定义）；
   - 每个策略回调单独 try/except，异常写 `sim_logs` 并继续；
   - 跌停判定（现有 `_prev_close` + `LIMIT_DOWN_PCT`）→ `no_sell`；对称算 `no_buy`（涨停，`LIMIT_UP_PCT = +0.098`）；注入 `_state`；
   - `Matcher.step()` 止损巡检（现有签名不变）；
   - 落地（§7）。
7. 收盘后（每交易日一次）：策略 `after_trading_end(context)`（若定义）→ `real_<code>` 落盘 → 写当日最终快照。
8. pause 文件 / 异常 → status=failed（现有语义保留）；暂停退出 → status=paused。

策略回调签名约定：回调接收 `context` 单参（与 engine loader 的命名空间一致；聚宽的 `handle_data(context, data)` 第二参不支持，策略用 `get_current_data()`/`get_price` 取数——样例策略即此风格）。

## 7. 状态与落库

每轮分钟循环结束后：

1. **成交落库**：drain `_state["trades"]` 增量 → `db.insert_sim_trade(aid, ts, code, action, price, amount, pnl, pnl_pct, commission)`（BUY/SELL；sell 时 pnl 用卖出前 avg_cost 计）。
2. **状态落库**：portfolio → `db.upsert_sim_state(...)`（positions_json 含 `amount/avg_cost/price/buy_dt/today_amount`）。
3. **净值快照**：`db.insert_sim_snapshot(aid, dt, net_value, cash, positions_value, pnl, pnl_pct)`（每分钟一行，~240 行/日/账户，量级可接受）。
4. **日志**：策略 `log.*` 经 sink 实时 `insert_sim_log`；runner 关键事件（启动/恢复/触发失败/刷新失败/收盘落盘）同表。

崩溃恢复：重启进程经 sim_state 恢复现金/持仓（含 today_amount），止损日志与成交历史不丢；`started_at` 不重置。

## 8. API 与 service 变更（`api/quant.py` / `service.py`）

- `AccountIn` 新增 `strategy_id: str = ""`；提供时校验策略存在（`store.get_strategy`），不存在返回 400。
- `service.account_create(name, capital, stop_loss, strategy_id="")` 透传落库。
- **策略绑定为可选**：账户有 `strategy_id` → runner 走策略驱动主循环；无 → 保留旧「止损看护」循环（既有账户与手工看护场景不受影响，存量测试不动）。
- 新增 `GET /sim/accounts/{aid}/logs`（读 `sim_logs`，limit 参数，正序返回）。
- `GET /sim/accounts/{aid}/status` 返回中补 `strategy_name`（join strategies 表），其余字段不变。
- start/pause/reset/terminate 语义与进程控制（pid 杀进程组 + pause 文件）不变。

## 9. 不做（YAGNI）

- 不做止盈/仓位约束/多策略账户（Q4 已否）。
- 不做策略代码沙箱（内网自用，与回测一致直接 exec）。
- 不改 rqalpha 回测路径与 vectorbt 回测页；不改 `QuantDataProvider`（live 走 jqengine DataManager）。
- 不做 SSE 推送（前端下一轮再定轮询/SSE）；`sim_logs` 只做轮询读接口。
- 不做涨停买入排队的部分成交；涨停直接拒单（等同跌停禁卖的顺延语义）。

## 10. 测试（`backend/tests/quant/`）

- `test_engine_order_rules.py`（新）：印花税（ETF 免）、整手、T+1（当日买入不可卖/次日可卖）、no_buy/no_sell、trades 增量字段（side/avg_cost）。
- `test_live_feed.py`（新）：stub mootdx 源验证 `_minute_mem` 合并去重、`minute_prices` 快照、单 code 失败降级。
- `test_runner_strategy.py`（新）：stub manager + 假时钟，验证 init 一次、run_daily 时刻触发（open/close/HH:MM 每日一次）、handle_data 每 bar、止损联动、trades/state/快照/日志落库、sim_state 恢复。
- 既有 `test_matcher.py` / `test_fix_sim.py` / `test_run_quant_sim.py` / `test_service.py` 保持绿（run_loop 签名兼容：`provider` 参数保留但新增 `dm`；或同步更新用例，尽量小改）。
- 运行：`cd backend && uv run --extra dev pytest tests/quant/`；`ruff check app`；`mypy app`（遵循 AGENTS.md）。

## 11. 实施顺序

1. `db.py`：strategy_id 迁移 + sim_logs 表 + 读写函数。
2. `engine/jq`：Position `today_amount`、order 交易规则、log_sink、trades 字段增强、`on_new_day`。
3. `simulate/live_feed.py`：mootdx 实时刷新 + 合并 + 快照 + 收盘落盘。
4. `simulate/runner.py`：重写主循环（策略加载/调度/触发/落地/恢复）。
5. `service.py` + `api/quant.py`：strategy_id 绑定 + logs 端点 + 校验。
6. 测试 + 三道检查（pytest/ruff/mypy）。

## 12. 风险

- `engine/jq` 在仓库内长期无驱动方，可能有潜伏 bug → 以单测覆盖撮合与调度热路径；样例策略（五福）手动跑通验收。
- mootdx 盘中实时 bar 的稳定性/限速 → 每分钟每 code 一次调用（watch 集合通常 <30），失败降级不杀进程；服务器自动轮换沿用 `_with_server_retry`。
- 盘中日线缺失导致策略用昨收计算（聚宽实盘同语义），在设计文档与日志中明示。
- 账户 start 在非交易时段：直接进 idle 循环，盘前钩子在下个交易日触发；不补跑当日已过的 run_daily（避免午后启动补打早盘信号）。

## 13. 追加：开始模拟日期（start_date）与历史补跑

账户创建新增可选 `start_date`（YYYY-MM-DD，`sim_accounts.start_date` 列，含旧库迁移；`AccountIn` 校验格式，非法 400）。语义：

- **等于今天或留空**：立即进实时循环（原行为）。
- **早于今天**：先**历史补跑**——从 start_date 起逐交易日（沪深300 指数日线取交易日表）、逐分钟 bar（`_session_minutes` 生成 240 根/日）驱动同一 `_strategy_tick`，价格由 `_hist_feed` 经 `dm.get_minute_price_at` 滑窗取历史分钟（遵守 C1/C2：近 3 月真实 1m、更早 baostock 5m 插值，均在内存不落盘）；每日走 `_pre_market`/`_eod` 完整钩子（T+1 跨日解冻、run_daily 每日触发）。补跑期间成交/快照/日志照常落库（快照 dt 为历史时刻），补跑结束后实时循环从今日 bar 无缝接入（`last_bar` 单调递增天然衔接）。逐日检查 pause 文件，可中断。
- **晚于今天**：进程启动但到日前空转（`today < start_date` 走 idle 分支）。
- **续跑不补跑**：`sim_state` 已有存档（崩溃/暂停重启）时跳过补跑，直接续实时；`reset` 清档后再次 start 会重新补跑（符合预期）。

前端 `AccountDialog` 加「开始模拟日期」`DatePicker`（默认今天=立即开始，附补跑说明文案），随账户创建传 `start_date`。

补跑验收口径：首日 `run_daily('open')` 建仓，次日同任务再平衡因手续费侵蚀产生小额调仓（同时验证 T+1 跨日解冻），净值曲线自 start_date 起连续——见 `test_strategy_loop_replays_history_then_live`。

## 14. 追加：运行频率（frequency）与模拟盘前端

### 14.1 frequency 字段（后端）

`sim_accounts.frequency TEXT DEFAULT 'minute'`（含旧库迁移），`AccountIn` 校验仅收 `minute/daily`：

- **minute（默认）**：现有逐分钟驱动（run_daily 到期触发 + run_minute + handle_data 每 bar）。
- **daily**：每交易日**只驱动一次**（开盘首个有效 bar），该 tick 内 `_fire_session(force_all=True)` 全量触发当日全部 run_daily 任务（忽略设定时刻）+ run_minute + handle_data；当日后续 bar 不再刷新数据源（`aux["daily_done"]` 门控）。历史补跑同样每日只走 09:31 bar，与实时口径一致。

另：`list_sim_accounts` 改为 LEFT JOIN `sim_state`，列表行带最新 `net_value/pnl`（前端列表展示）。

### 14.2 模拟盘前端（`frontend/src/quant/`）

- **入口**：既有菜单「量化模拟盘」→ `/quant-sim`（`router.tsx`/`Layout.tsx` 早已挂载，本次不动）。
- **QuantSim.tsx 重写**为单页双视图：
  - **列表视图**：左上角「新建模拟」按钮；表格列 序号/名称/策略/开始日期/频率/状态/净值/收益率（策略名由 `/strategies` 联表映射；净值/收益率来自 `list_sim_accounts` 的联表字段；5s 轮询）。行点击进详情。
  - **详情视图**：顶栏（返回/名称/状态徽标/策略·频率·起始日期/启动·暂停·重置按钮）；指标卡（净值/现金/持仓市值/盈亏/收益率）；净值曲线（echarts-for-react，token 取色同 `BacktestResult`）；持仓表（数量/成本/现价/市值/盈亏%）；Tab 切换「成交记录 / 止损日志 / 运行日志」（新增 `api.getSimLogs`）。全部 4s 轮询。
- **AccountDialog.tsx**：字段 = 交易名称 / 使用策略（下拉，取自与量化回测同一 `/strategies` 策略库）/ 初始资金 / 运行频率（分钟级·日频）/ 开始模拟日期 / 止损比例；名称与策略必选。
