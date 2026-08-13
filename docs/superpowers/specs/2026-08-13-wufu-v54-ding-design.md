# wufu-v5.4-ding 设计：买卖钉钉通知 + 账户 3% 止损通知

日期：2026-08-13
分支：`feat/wufu-v5.4-ding`

## 目标

克隆 wufu v5.4 为 `wufu-v5.4-ding` 策略，实现：

1. **所有买入、卖出的钉钉通知**（策略层 `log.notify`）。
2. **账户自动 3% 止损**（引擎层 Matcher，已有默认 -3%）的钉钉通知。

即"双通道"：策略自身下单走策略层通知，账户止损（Matcher）走引擎层通知。

## 背景事实（已探明）

- 钉钉通道已存在：`backend/app/quant/notify.py::send_dingtalk`（Markdown+加签，
  fire-and-forget）；`backend/app/quant/simulate/runner.py::_emit_log(account_id, "notify", msg)`
  在 `dingtalk_enabled` + webhook 配置好时异步推送。
- 策略内 `log.notify()` → `LogProxy.notify`（`engine/jq/api.py`）→ log_sink →
  `_emit_log(account_id, "notify", msg)`，链路已通（见 `tests/quant/test_strategy_notify.py`）。
- 补跑/回放期间不推钉钉：`_emit_log` 对 `account_id in _replay_active_ids` 抑制 notify。
- 账户止损由 `backend/app/quant/simulate/matcher.py::Matcher` 实现：`run_loop` 里
  `stop = acct.get("stop_loss") or 0.03`，每持仓跌破成本 -3% 触发止损卖出，落库
  `sim_stop_loss` + `sim_trades`，但当前不发任何 notify。
- wufu v5.4 实跑版为注册表 `data/quant_strategies/42f91131.py`（五福v5.4，ddd911f4 账户绑定，
  含 08-12 A3 退出均线 20→15 优化）。仓库 fixture `backend/tests/fixtures/wufu_v54/wufu-v5.4.py`
  为旧版。**克隆源 = 42f91131.py**（与用户实跑一致）。
- 买卖与止损都在 `smart_order_target_value(security, target_value, context)`（42f91131 约 1263 行）
  里执行，成功时 `log.info("📥 买入 …")` / `log.info("📤 卖出 …")`。

## 方案

### 1. 分支与产物

- 从 `custom-main` 新建分支 `feat/wufu-v5.4-ding`。
- 克隆 `data/quant_strategies/42f91131.py` 为：
  - `backend/tests/fixtures/wufu_v54/wufu-v5.4-ding.py`（入库源码，头部注释标注 v5.4-ding）
  - 注册到策略库 `data/quant_strategies/wufu-v5.4-ding.py`，id=`wufu-v5.4-ding`，名「五福v5.4钉钉版」。

### 2. 策略层通知（克隆文件内）

在 `smart_order_target_value` 下单成功后追加 `log.notify`（保留原 `log.info`）：

- 买入：`📥 买入 {name}({security}) 数量{amount} 价格{price:.3f} 佣金{commission:.2f}`
- 卖出：`📤 卖出 {name}({security}) 数量{amount} 价格{price:.3f} 佣金{commission:.2f} 盈利{pnl:+.2f}({pnl_pct:+.2%}) 持仓{n_days}个交易日`

数据来源：

- `name` = `get_security_name(security)`；`price` = `data[security].last_price`；`amount` = `abs(diff)`。
- 卖出 pnl / pnl_pct：下单**前**取 `cur_pos.avg_cost`（`cur_pos = context.portfolio.positions.get(security)`），
  `pnl = (price - avg_cost) * abs(diff)`，`pnl_pct = price/avg_cost - 1`。
- 持仓交易日数：新增 `g._entry_date = {}`，买入成功时记录 `context.current_dt.date()`，卖出后清除；
  卖出时 `get_trade_days(start_date=g._entry_date[security], end_date=today)` 取长度，
  失败降级为自然日/工作日差值。取不到 entry_date 时显示 `持仓?天`。
- 佣金估算：买入用策略既有 `buy_commission_rate`（0.0001）语义即可，卖出按
  `abs(diff)*price*0.0001` 估算展示（仅展示用，与引擎实际撮合佣金允许有出入）。

### 3. Matcher 层通知（账户 3% 止损）

- `Matcher.__init__(stop_loss, account_id=None, on_stop_loss=None)`：新增可选回调。
- 触发止损卖出时（`matcher.py` 落 `sim_stop_loss`/`sim_trades` 处），调用
  `on_stop_loss({dt, code, name, action, price, amount, pnl, pnl_pct, commission})`。
- `runner.run_loop` 统一接线（watcher 与策略两种模式共用同一 matcher）：
  `Matcher(stop, account_id=account_id, on_stop_loss=…)` → 回调内
  `_emit_log(account_id, "notify", "🚨 【账户止损】…")`，走既有钉钉通道。
- 补跑抑制复用现有 `_replay_active_ids` 逻辑（`_emit_log` 已处理），不额外改。

### 4. 测试

- `tests/quant/test_matcher_dingtalk.py`：
  - `on_stop_loss` 传入时止损触发回调且字段完整（含 name/pnl/pnl_pct/commission）；
  - 未触发止损不回调；
  - 跌停/停牌/T+1 冻结等不卖出场景不回调。
- `tests/quant/test_wufu_ding_strategy.py`：
  - `wufu-v5.4-ding.py` 可 `py_compile`；
  - 文件内买卖路径含 `log.notify`（静态断言关键行）；
  - 复用 `test_strategy_notify.py` 的 log_sink 端到端：`log.notify` → sim_logs 出现 notify 级 →
    `_send_dingtalk_async` 被调用（不联网，mock）。

### 5. 验证命令（backend/ 下）

```bash
uv run --extra dev pytest tests/quant/test_matcher_dingtalk.py \
  tests/quant/test_wufu_ding_strategy.py \
  tests/quant/test_strategy_notify.py tests/quant/test_runner_dingtalk.py -q
uv run --extra dev ruff check app/quant/simulate/runner.py app/quant/simulate/matcher.py
uv run --extra dev mypy app/quant/simulate/runner.py app/quant/simulate/matcher.py
```

（`wufu-v5.4-ding.py` 是策略脚本，不进 ruff/mypy。）

## 非目标

- 不改 ddd911f4 现有账户/现有 42f91131 策略。
- 不动 PTrade 移植版 `wufu-v5.4.ptrade.py`。
- 不改 `_send_dingtalk_async` 的包裹格式（标题/账户/时间头）。
- 不回填历史成交通知（补跑抑制）。
