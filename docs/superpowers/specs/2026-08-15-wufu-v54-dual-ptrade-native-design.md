# PTrade 原生化改造设计（wufu-v5.4 双持仓版）

日期：2026-08-15
分支：`feat/ptrade-engine`

## 背景与目标

现状：双持仓 PTrade 策略 `backend/tests/fixtures/dual_v54/wufu-v5.4-dual-adapt.ptrade.py`
（约 2079 行）由 JQ 版移植而来，带有约 535 行「聚宽 → PTrade」转换层
（`_pt`/`_cd`/`_BarUnit`/`_wide`/`_as_series_values`/`_positions_map`/`_pos_*`/
`_safe_log`/`_warn`/`_update_universe`/`_get_total_value`/`_get_available_cash`/
`_current_dt` 等），正文调用这些转换函数 115 次。

目标：
1. 策略尽量直接调用 PTrade 原生库函数（`get_history`/`get_positions`/`get_position`/
   `get_stock_status`/`get_trading_day`/`get_trade_days`/`get_market_list`/
   `get_market_detail`/`order`/`set_universe`/`context.portfolio`/`log`）。
2. 尽量少用 jq→ptrade 转换函数，代码尽量精简。
3. **必须与 JQ 双持仓版完全对齐**（现有对齐门禁：130 笔成交一致、日净值零差）。
   重构只删转换层，不改任何交易逻辑。

## 方案（已确认）

方案 A：原生重写 + 引擎对齐。策略删掉纯 jq 痕迹与格式归一化层，正文直接调原生
PTrade API；引擎 `ptrade_api.py`/`ptradecompat.py` 保证策略用到的函数返回稳定、
与真 PTrade 一致的格式（get_history 恒宽表、get_positions 恒 dict、Position 恒含
amount/enable_amount/cost_basis/last_sale_price、handle_data 的 data 快照恒含
close/price/volume/money）。

范围：只改双持仓版；单持仓底座（`wufu-v5.4.ptrade.py`）不动。

## Section 1 — 引擎对齐点（确认性加固）

引擎两侧（本地 `ptrade_api.py` 与 rqalpha `ptradecompat.py`）已基本满足格式保证，
本次仅确认/补缝：

| 函数 | 需保证的格式（已基本满足） |
|---|---|
| `get_history` | 恒返回宽表 DataFrame：index=datetime, columns=PTrade 码；单标的也返回 1 列 DataFrame（列名=标的码） |
| `get_positions()` | 恒 `{PTrade码: Position}`，仅含 amount>0 |
| `get_position(code)` | 无持仓返回空 Position（amount=0，不抛错） |
| `Position` 字段 | `amount` / `enable_amount` / `cost_basis` / `last_sale_price` |
| `handle_data` 的 `data` | `{PTrade码: obj}`，obj 含 `close/price/volume/money` |
| `get_stock_status`/`get_trading_day`/`get_trade_days`/`get_market_list`/`get_market_detail`/`get_stock_name`/`order`/`set_universe`/`log` | 签名与真 PTrade 一致 |

改动点：
- `get_history` 单标的返回列名对齐：若单标的返回列名不是标的码则补（策略用 `df[code]` 取列）。

## Section 2 — 策略重写

### 整体删除（约 480 行）

- `_pt` → ETF 池直接写 `.SS/.SZ`（`initialize` 里去掉 `[_pt(c) for c in ...]`）。
- `_BarUnit`/`_set_last_data`/`_cd`/`_cd_field` → `handle_data`/`before_trading_start`
  里存模块级 `_BARS`（`data` 原样保留），正文直接 `_BARS[code].close`；
  `_current_price` 改读 `data[code].price/close` + `get_history` 回退。
- `_safe_log`/`_warn`/`_debug` → `log.warn`/`log.debug`。
- `_wide`/`_as_series_values` → `df[code].values` / `df.values`（引擎保证宽表）。
- `_positions_map`/`_pos_amount`/`_pos_avail`/`_pos_cost`/`_pos_price`/`_get_position`
  → `get_positions().items()` + `pos.amount/enable_amount/cost_basis/last_sale_price`。
- `_get_total_value`/`_get_available_cash` → `context.portfolio.portfolio_value` /
  `context.portfolio.cash`。
- `_update_universe` → `set_universe(...)` 直接。
- `_current_dt`/`_today`/`_previous_trading_day`/`_last_n_trade_days` →
  `context.blotter.current_dt` / `get_trading_day(-1)` / `get_trade_days(...)`。

### 保留但精简（原生数据访问，去冗余兜底，约 340 → ~150 行）

- `_is_halted`/`_refresh_halt_status` → 直接 `get_stock_status(chunk,
  query_type='HALT', query_date=...)`，保留按日缓存 + 分块。
- `_limit_prices` → `get_history(1,'1d',field,security_list=code)[code].iloc[-1]`。
- `get_security_name` → `g.etf_names_dict` + `get_stock_name` 直用。
- `_get_today_volumes`/`_get_today_volume`/`_get_money_avg_series`/
  `_get_money_daily_totals` → 分块逻辑保留（性能需求），返回值直接 `df[chunk].sum()` 等。
- `_get_all_fund_codes`/`_ensure_fund_universe` → `get_market_list/get_market_detail`
  直用，去掉 dict/list 双兼容分支。

### 正文不动

`check_a_share_weak_period`/`select_cross_asset_dual`/`execute_buy/sell_trades`/
`smart_order_target_value`/`minute_level_stop_loss` 等交易逻辑一行不改（只替换其
调用的转换函数为原生写法）。

## Section 3 — 验收

1. 对齐门禁（硬指标）：
   `cd backend && uv run --extra dev pytest -m integration tests/quant/test_ptrade_vs_jq_alignment.py`
   — 130 笔成交一致、日净值零差。
2. 单测全绿：
   `uv run --extra dev pytest tests/quant/test_ptrade_strategy_file.py tests/quant/test_ptradecompat.py tests/quant/test_sim_runner_flavor.py`
3. ruff 干净：`uv run --extra dev ruff check app tests`。
4. 回测冒烟：`scripts/run_ptrade_rqalpha.py` 跑 07-10~07-14 无异常。
5. 改造后同步更新 store 里 `dual_v54_ptrade` 策略文件（模拟盘账户下次重启即用新版）。

## 风险

- 唯一对齐风险是 `get_history` 结果切片方式（`_as_series_values` → `df[code].values`）——
  引擎格式已保证一致，且对齐门禁兜底。
- 真 PTrade 个别版本若返回格式不同，需在真机上微调（接受方案 A 的取舍）。
