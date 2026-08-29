# 聚宽股票策略回测支持（财务数据 + 缺失 API）设计

日期：2026-08-24
状态：已确认（用户拍板：mootdx affair 财务回填 + 兼容层扩展）

## 背景

策略 f51e08f9「大小外择时小市值3.0」（聚宽社区克隆）在 UI 回测失败：
`ModuleNotFoundError: No module named 'jqfactor'`（run e6cb7031）。

根因排查结论（详见会话记录）：

1. `jqcompat.py` 只伪造了 `jqdata` 模块；策略还导入 `jqfactor`、
   `jqlib.technical_analysis`、`talib`。
2. 三者的消费函数（`filter_roic`/`boll_filter` 等）经核实**全是死代码**
   （0 次调用），导入空壳即可，无需实现函数体。
3. 修完 import 还会连环缺失运行期 API：
   - `FixedSlippage` / `run_monthly` / `run_weekly` / `order_target_value` /
     `OrderStatus` / `get_index_stocks`
   - `get_fundamentals` + `query` + `valuation`/`income`/`cash_flow`/`indicator`
     （选股主链路依赖）
   - `context.previous_date` / `context.run_params.type`
4. 数据缺口：本地无财务数据（`data/financials/*` 为空目录）、无指数成分
   本地缓存。

## 方案总览

### A. 财务数据：mootdx affair 回填（用户选定）

通达信专业财务文件（gpcw）：每季度一个 zip **覆盖全市场 5551 只 × 585 列**，
实测单季 5.8MB 约 2 秒下载。近 6 个报告期（2025Q1~2026Q2）12 次下载几分钟完成。
对比 baostock 逐只逐季方案（≈12.7 万次查询、11~18 小时）快两个数量级，
且 baostock 缺失的扣非净利润/经营现金流字段 TDX 全有。

落盘：`data/financials/tdx/stat=YYYYMMDD/part.parquet`（长表：code, stat_date,
可用日, 需要的列子集——不落全部 585 列，只留映射所需 ~20 列）。

字段映射（jq → TDX 列名）：

| jq 字段 | TDX 列 | 备注 |
|---|---|---|
| income.net_profit | 五、净利润 | |
| income.np_parent_company_owners | 归属于母公司所有者的净利润 | |
| income.operating_revenue | 营业总收入(万元) | ×1e4 转元 |
| cash_flow.subtotal_operate_cash_inflow | 经营活动现金流入小计 | |
| indicator.adjusted_profit | 扣除非经常性损益后的净利润 | |
| indicator.roe / inc_return | 净资产收益率 | |
| indicator.roa | 净利润 ÷ 资产总计 | 派生 |
| valuation.pb_ratio | close ÷ 每股净资产 | 派生（价格侧实时算） |
| valuation.market_cap | 总股本 × close | 派生（单位亿） |
| valuation.circulating_market_cap | 已上市流通A股 × close | 派生（单位亿） |

交叉验证：平安银行总股本与 baostock 一致（±10 股舍入差）。

**Point-in-time 防前视**：TDX 无公告日(pubDate)，用法定披露窗口保守近似——
该季报仅在「报告期对应的法定披露截止日」之后可见：
Q1→当年4/30；中报→8/31；三季报→10/31；年报→次年4/30。
落盘时预计算 `visible_from` 列，查询时按 `previous_date >= visible_from`
取最新一条。

### B. 服务架构

遵循「量化侧唯一取数入口 = StockDataClient」约束：

- stockdata 服务（scheduler 启动 backfill 阶段 + 手动脚本）负责下载解析
  gpcw zip → 落 parquet；
- 服务新增协议方法 `get_financials(stat_dates=None)` 返回全量财务长表；
- `StockDataClient.get_financials()` 网络获取，bridge/jqcompat 进程内缓存。

### C. jqcompat 兼容层扩展

1. 假模块：`jqfactor` / `jqlib.technical_analysis`（含 `Bollinger_Bands`
   占位抛错）/ `talib` —— 仅保证 import 成功（消费方均为死代码）。
2. 新增 API shim（注册进 rqalpha.api 与假 jqdata 模块，同现有模式）：
   - `FixedSlippage(value)`：滑点类，set_slippage 接收后存状态（rqalpha
     sys_simulation 的 slippage 由 config 下发，桥接层把 FixedSlippage(0)
     归一为 config 值覆盖）。
   - `run_monthly(func, when, time)` / `run_weekly(func, weekday, time)`：
     复用 `_DAILY_AT` 分钟事件机制，回调内部先判断「当日是否满足月内第 N 个
     交易日 / 周几」再执行（聚宽语义：每月第 N 个交易日、每周第 N 个交易日）。
   - `order_target_value(security, value)`：基于 rqalpha `order_target_value`
     桥接（ptrade 侧已有同签名实现可参考）；返回对象带 `.filled/.status`。
   - `OrderStatus` 枚举（至少 `held`）。
   - `get_index_stocks(index_code, date=None)` → StockDataClient，结果按
     回测进程缓存。
   - `context.previous_date`（当前交易日的前一交易日）、
     `context.run_params.type`（恒 `'backtest'`）。
3. `get_fundamentals(q, date=None)` + `query(...)` + 表对象
   `valuation/income/cash_flow/indicator`：
   - ORM 最小实现：支持 `.filter(条件链)`、`.order_by(col.asc()/desc())`、
     `.limit(n)`、`.code.in_(list)`、列间比较（如
     `cash_flow.x/indicator.y>2.0`）、`.between(lo,hi)`；
   - 执行时物化：以 date（默认 previous_date）为锚，逐标的取
     `visible_from<=date` 的最新季报行 + 当日 close，派生估值列后内存过滤/
     排序，返回 pandas DataFrame（聚宽返回口径：一列 code + 所查列）。
   - 无数据的过滤条件行为：数值列缺失（NaN）一律判 False（与聚宽
     「无财报数据被过滤掉」一致）；仅 White_Horse 的
     `cash_flow.subtotal_operate_cash_inflow/adjusted_profit` 比值类条件
     因两列常同时缺失会导致全清空——该比值条件缺数据时放行（设计偏差，
     已向用户披露）。

### D. 宇宙与性能

- 回测宇宙 = 窗口内 000300.XSHG ∪ 399101.XSHE 成分并集 + foreign_ETF 5 只 +
  基准。通过 StockDataClient.get_index_stocks 拉（当前成分近似历史成分，
  设计偏差：成分随时间变动，小市值指数月度调仓影响可控）。
- 频率必须 1m：`run_daily('HH:MM')` 依赖分钟 bar 事件触发（日线级会静默
  丢失全部调度）。股票分钟数据 2026-04-01 起本地已有（kline_minute）。
- 性能门禁：全程 ≤30 分钟（首跑基线，参照 wufu 门禁风格后续收紧）。

## 验收

1. `uv run --extra dev pytest backend/tests/quant/` 新增用例全绿：
   - 披露窗口 point-in-time 单测（构造 stat/pub 边界日期）
   - query ORM filter/order/limit/between 单测
   - run_monthly/run_weekly 触发日判定单测
2. f51e08f9 回测 2026-04-01 ~ 2026-08-24 status=done，产出完整 metrics
   （total_return/annualized/sharpe/max_drawdown/win_rate/trade_count）
   与交易明细，向用户汇报。
3. 既有 wufu 对齐测试不回归（`tests/quant/test_wufu_backtest_perf.py` 等）。

## 明示的设计偏差

- 成分股用当前成员近似历史（无本地历史成分缓存）。
- TDX 财务无 pubDate，法定披露窗口近似。
- White_Horse 现金流/扣非比值条件缺数据时放行。
- `income.np_parent_company_owners` 用归母净利润列直映（TDX 有此列）。
