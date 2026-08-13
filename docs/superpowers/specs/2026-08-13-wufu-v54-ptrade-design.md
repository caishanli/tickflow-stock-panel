# wufu v5.4 PTrade 移植版（wufu-v5.4.ptrade）

- 日期：2026-08-13
- 文件：`backend/tests/fixtures/wufu_v54/wufu-v5.4.ptrade.py`
- 目标：把聚宽 v5.4（`backend/tests/fixtures/wufu_v54/wufu-v5.4.py`）移植为可在 PTrade 平台运行的独立策略文件。

## 做法

以已验证的 `backend/tests/fixtures/wufu_v52/wufu-v5.2.ptrade.py` 为底座（平台适配层全部保留），叠加 v5.2→v5.4 的增量逻辑。增量来源为 v5.2 与 v5.4 聚宽源码 diff。

## 平台适配原则（沿用 v5.2 ptrade）

- 代码格式 `.XSHG → .SS` / `.XSHE → .SZ`（`_pt()` 自动转换）
- `get_history(count, '1d', field, security_list, fq='pre')` / `_wide()` / `_as_series_values()` 规整数据
- `get_current_data()[code].lastPrice/.highLimit/.lowLimit/.paused/.volume`
- 持仓 `get_position/get_positions`，现金/总资产双通道兜底
- `run_daily(context, func, time=...)`；晨间 `before_trading_start`、收盘 `after_trading_end`、分钟止损 `handle_data`
- 日志统一 `%` 格式化（无 f-string）；移除 `record()`/`set_option`/`jqdata` 等聚宽独有调用
- 动态池枚举用 `get_market_list/get_market_detail`，失败优雅降级为固定池

## 增量改动清单（v5.2 ptrade → v5.4 ptrade）

1. **initialize**：版本日志改 v5.4；新增 v5.3 配置（A1 `enable_profit_protect/profit_protect_trigger=0.05/profit_protect_stop=1.04/_profit_protected`、A2 `hold_buffer=1.0`、A3 `weak_exit_ma_lookback=15`）与 v5.4 配置（D3 `enable_take_profit=False/take_profit_ratio=0.08/take_profit_pullback=0.03/_peak_price`）；初始化日志补 v5.3/v5.4 两段。
2. **check_a_share_weak_period**：走弱期退出均线改为 `weak_exit_ma_lookback`；`data_lookback = max(进入/退出均线)`；跟踪 `exit_above_count`；退出条件 `exit_above_count >= 3`。
3. **get_final_ranked_etfs**：第四步末尾加入 A2 持仓宽容逻辑（`hold_buffer < 1.0` 时持仓得分≥候选门槛×buffer 则保留）。
4. **minute_level_stop_loss**：加入 D3 高位回落止盈（曾浮盈≥8% 且从峰值回落≥3% 卖出）+ A1 盈利保护止损（曾浮盈≥5% 后止损线升至成本×1.04）；持仓不可卖/空仓时清理 `_profit_protected`/`_peak_price` 状态。

## 验证

本仓库无 PTrade 运行环境，验证方式：

- `python -m py_compile` 语法检查
- 与 v5.2 ptrade 逐项核对 API 使用（无 `jqdata`/`record`/`set_option`/`set_benchmark(jq格式)` 等非 PTrade 调用）
- 逻辑 delta 与 v5.2→v5.4 聚宽 diff 一一对应
