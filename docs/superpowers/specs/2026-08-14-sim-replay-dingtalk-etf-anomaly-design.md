# 设计：模拟盘补跑钉钉抑制 + 全市场ETF成交额异常自检

日期：2026-08-14
分支：`custom-main`

## 目标

1. **补跑期间不发日常钉钉**：重放历史（如 07-10→08-14）时，当前每天推一条按天汇总钉钉（一次补跑 ~160 条），需抑制。
2. **策略成交额异常自检**：`calculate_global_etf_threshold` 回看 3 天全市场 ETF 总成交额，若某天明显偏低，`log.error` 进"异常"标签 + `log.notify` 推钉钉，并剔除异常天再算流动性阈值。

## 背景事实（已探明）

- 钉钉链路：策略 `log.notify()` → `LogProxy.notify`（`engine/jq/api.py`）→ log_sink → `_emit_log(account_id, "notify", msg)`；实时逐条推，补跑期间（`account_id in _replay_active_ids`）不逐条推，攒到 `_replay_day_notifies`。
- 补跑钉钉唯一来源 = `_emit_eod_notify`（`runner.py:253`）的 `replay_mode` 分支：每个重放日 `_eod` 调用它，构建按天成交表格汇总并 `_dispatch_dingtalk` → 长补跑刷屏。
- "异常"标签 = 模拟盘详情页 `QuantSim.tsx` 的 `alerts` tab：`logList.filter(l => l.level === 'warn' || l.level === 'error')`。策略 `log.error` 进该标签，但**不推钉钉**（`_emit_log` 只对 `notify` 推钉钉）。
- 原始 bug：08-13 ETF 成交额数据未进入模拟盘内存缓存 → 09:31 算出 `1469.57亿 (225只)`，而正常两天 `~4000亿 (1658只)`。只数与金额双双掉到正常 ~1/3，是"数据残缺"的可靠信号（正常只数波动 1622~1658，<2%）。

## 方案

### 1. 补跑抑制日常钉钉（`backend/app/quant/simulate/runner.py`）

`_emit_eod_notify` 的 `replay_mode` 分支：仍构建按天汇总文本、仍 `_replay_day_notifies.clear()`，但**跳过 `_dispatch_dingtalk`**。补跑结束进实时后 `replay_mode=False`，恢复正常逐条/收盘推送。

### 2. 策略成交额异常自检（策略 `data/quant_strategies/wufu-v5.4-ding.py`）

在 `calculate_global_etf_threshold` 的 `daily_totals`/`daily_counts` 已算出、且 `len(daily_totals) >= 3` 之后加自检：

- 对每个交易日 `day`，取**另两天**的「有成交只数」较大者 `max_count` 与「总成交额」较大者 `max_money`。
- 若 `count(day) < max_count * 0.5` 或 `money(day) < max_money * 0.5` → `day` 判为异常天。
- 任一异常天存在时：
  - 对每个异常天：`log.error("🚨【成交额异常】{date} 全市场ETF总成交额 {money:.2f}亿元 ({count}只ETF有成交)，明显低于其他两天，疑似数据回源不完整，已剔除该日计算阈值")` 进"异常"标签；再 `log.notify(同文案)` 推钉钉。
  - 阈值改用**剔除异常天后的正常日均值**（`daily_totals[good].mean() / global_threshold_divisor`）；若剔除后不足 2 个正常日，回落保守阈值 1000万。
  - 日志标注"（已剔除异常日）"。
- 无异常天：走原逻辑（3 日均值）。

### 3. 补跑期间异常仍即时推钉钉（`runner.py`）

`_replay_log_sink` 中 `level == "notify"` 分支：消息以 `🚨【成交额异常】` 开头时，**直接 `_dispatch_dingtalk`**（不攒入 `_replay_day_notifies`），保证补跑期间异常告警即时送达；其余 notify 维持攒批（汇总已不再推，仅留日志）。

## 测试

- `backend/tests/quant/test_runner_strategy.py`（或新增文件）：
  - 补跑期间 `_emit_eod_notify` 不触发 `_dispatch_dingtalk`（monkeypatch 桩验证不调用）；实时 EOD 正常调用。
  - 补跑期间异常 notify（`🚨【成交额异常】` 前缀）立即 `_dispatch_dingtalk`。
- 策略异常检测：把判定逻辑抽成可单测的辅助函数（如 `_anomalous_etf_days(daily_totals, daily_counts) -> list[date]`），单元测试覆盖：正常 3 天不判、只数掉到 50% 以下判、金额掉到 50% 以下判、边界恰好 50% 不判；并验证阈值剔除异常天后的均值与"异常"日志文案。
- 既有 `tests/quant/` 全量回归（3 个已知预存失败除外）。

## 涉及文件

- `backend/app/quant/simulate/runner.py`（2 处：`_emit_eod_notify`、`_replay_log_sink`）
- `data/quant_strategies/wufu-v5.4-ding.py`（策略自检，实跑版）
- `backend/tests/fixtures/wufu_v54/wufu-v5.4-ding.py`（同步 fixture）
- `backend/tests/quant/` 新增/更新测试
