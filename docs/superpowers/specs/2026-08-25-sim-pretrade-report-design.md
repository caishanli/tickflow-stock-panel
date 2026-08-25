# 模拟盘预买卖报告（d092ad90 克隆 + 11:30/13:01 预报告）设计

- 日期：2026-08-25
- 状态：已确认（用户批准方案 A）
- 分支基线：custom-main

## 背景与目标

模拟盘 `d092ad90`（五福v5.4-钉钉版，策略 `wufu-v5.4-ding.py`）每天 **13:10** 依次执行
「动量排名 → 卖出 → 买入」。用户希望在调仓前提前获知"今天 13:10 大概率卖什么、买什么"，
用于盘中决策参考。

目标：

1. 从 `d092ad90` **克隆**一个新模拟盘账户（镜像当前现金/持仓状态续跑）。
2. 新账户使用策略副本，在每天 **11:30** 与 **13:01** 各发一条**预买卖报告**：
   - 预计买入（目标集 − 当前持仓，按排名顺序）；
   - 若预测到会卖出：按"最可能被卖"排序取**前 3**；
   - 无需卖出时报"持仓均在目标内"。
3. 报告打印到 log（`sim_logs`）并发送钉钉。

非目标：

- 不改动原账户 d092ad90 与原策略 wufu-v5.4-ding.py 的任何行为。
- 不做 runner/引擎层通用预报告钩子（YAGNI，见备选方案）。

## 方案选型

| 方案 | 说明 | 结论 |
|---|---|---|
| A. 策略副本内置预报告 | 复制策略文件加两个 `run_daily` 定时任务 | ✅ 采用 |
| B. runner 层通用钩子 | 账户表加报告时间配置，引擎回调策略约定接口 | 过度设计 |
| C. 主后端 APScheduler 读库计算 | 策略外复制计算逻辑 | 状态易失真 |

采用理由：零引擎改动、复用已验证的 `log.notify → sim_logs + 钉钉` 链路、
天然只对新账户生效、符合本仓库"策略变体=文件副本"的既有惯例（sw2/sw3/d2/d3…）。

## 详细设计

### 1. 策略副本 `wufu-v5.4-ding-report.py`

全量复制 `data/quant_strategies/wufu-v5.4-ding.py`，三处改动：

1. **注册定时任务**（`initialize` 内）：

   ```python
   run_daily(pre_trade_report, time='11:30')  # 午间预报告
   run_daily(pre_trade_report, time='13:01')  # 尾盘预报告（距 13:10 决策 9 分钟）
   ```

2. **排名核心拆分**：把 `get_final_ranked_etfs(context)` 的计算部分抽成
   `_compute_etf_ranking(context) -> tuple[all_metrics, filtered_list]`（不打全量排名日志）。
   原函数改为「调 `_compute_etf_ranking` + 打日志」，行为不变。
   - quiet 版**不写** `g._assessed_codes` / `g.ranked_candidates_full`
     （这两个仍由 13:10 正式管线赋值），避免污染正式流水线的数据缺失保护与买入 fallback 状态。

3. **`pre_trade_report(context)`**：
   - 补跑守卫：`(datetime.now() - context.current_dt).total_seconds() > 300` 即历史补跑，
     直接 return（不浪费全池计算、不产生无用日志；补跑期 notify 本就不推钉钉，但会拖慢补跑）。
   - 池子守卫：`g.merged_etf_pool` 未就绪（晨间流水线未跑）则 log.info 提示后 return。
   - 分钟预热：复用 `dm.preload_minute_for_pool(g.merged_etf_pool, context.current_dt)`
     （同 13:10 流水线，避免逐标的回源）。
   - quiet 计算排名 → 预测目标集 = 排名前 `g.holdings_num`；排名为空时走防御模式
     （`check_defensive_etf_available` → 目标=`g.defensive_etf`），与 `execute_sell_trades` 同口径。
   - **预计卖出** = 持仓中 ∉ 目标集者。排序规则："最可能被卖"优先 =
     - 常规模式：按该持仓在完整过滤排名中的位置，**排名越靠后越先卖**；未参与评估
       （停牌/数据缺失）的单列 ⚠️ 区分（对应 13:10 数据缺失保护不会强卖）。取前 3。
     - 防御模式：所有非防御持仓都是候选，按动量得分升序取前 3。
   - **预计买入** = 目标集 − 当前持仓，按排名顺序列出（不设前 3 限制，通常 ≤3 只）。
   - 组装 markdown 后 `log.notify()` 一条消息（自动落 `sim_logs` + 推钉钉）。

### 2. 报告格式

```
📋 预买卖报告 08-25 13:01（预测 13:10 调仓 | 🟢正常期 | 池520只）
📥 预计买入：513500 标普500ETF → 510300 沪深300ETF
📤 预计卖出（最可能前3）：
1️⃣ 159502 标普生物科技ETF嘉实（排名187/520，动量0.012）
2️⃣ …
3️⃣ …
⚠️ 未参与评估（停牌/数据缺失，13:10 有保护不会强卖）：512880红利ETF
💤 无卖出时：「✅ 持仓全部在目标内，13:10 预计不动」
```

### 3. 账户克隆

`backend/scripts/run_quant_sim.py` 增加 `--clone-from <aid>`：

- 读源账户行：复制 `capital / stop_loss / start_date / frequency`（可被现有 CLI 参数覆盖）、
  `dingtalk_enabled`；新 `id = uuid4().hex[:8]`、新 name（默认 `{源name}-预报告`，可用 `--name` 覆盖）、
  `status='created'`、`strategy_id` 必须显式指定为新策略 id。
- 克隆 `sim_state` 整行（cash / positions_json / net_value / pnl / start_cash /
  stop_loss_log_json / dt）→ 新账户同名状态。启动后 runner 检测存档 dt 早于当前时间 →
  自动补跑今日剩余分钟，无缝续跑。
- 不复制 `sim_trades / sim_logs / sim_equity_snapshots`（新账户历史从克隆日起算）。
- 配合 `--autostart` 创建即拉起。

本次执行的具体命令（实施时）：

```bash
cd backend && uv run python scripts/run_quant_sim.py \
  --create --clone-from d092ad90 --strategy-id wufu-v5.4-ding-report \
  --name "五福v5.4-钉钉版-预报告" --autostart
```

新账户 id 不指定时由脚本自动生成（uuid8）；`--clone-from` 未提供 `--strategy-id` 时报错退出
（克隆必须显式指向新策略，防止误用源策略）。

策略行与文件由实施步骤先行创建（`strategies` 表插入 id=wufu-v5.4-ding-report、
file=wufu-v5.4-ding-report.py，文件落 `data/quant_strategies/`）。

### 4. 边界情况

| 场景 | 处理 |
|---|---|
| 11:30 量比外推 | `get_volume_ratio` 已含午休扣减（`hour>=13` 减 90）；上午以 elapsed=120 分钟外推，口径一致 |
| 11:30 bar 触发 | `_daily_due('11:30', bar_dt)` 在上午收盘 bar（11:30）满足 `time>=HH:MM`，正常触发 |
| 排名整体为空 | 报告标注防御模式；非防御持仓全部列为卖出候选 |
| 持仓停牌/未参与评估 | 单列 ⚠️（13:10 保护逻辑实际不会卖） |
| 历史补跑 | `(now - current_dt) > 5min` 直接跳过 |
| 当日无交易时段（非交易日） | runner 主循环已保证不 tick，无需处理 |
| 13:01 分钟线尚未回源 | preload 走 StockDataClient 按需回源当日分钟，与服务端 lazy 回源语义一致 |

### 5. 测试与验收

- 单测（backend/tests）：导入策略文件（仿 `tests/fixtures/wufu_v54` 模式），
  对报告组装纯函数（输入 ranked_list + positions + targets → 输出文本）断言：
  - 卖出候选排序正确（排名靠后者在前）、截断前 3；
  - 未评估持仓单列；防御模式兜底文案；无卖出文案。
- 实盘验收：克隆账户启动后，当日 13:01 收到钉钉报告；与 13:10 实际成交对比买卖方向一致
  （允许尾盘 9 分钟内价格剧变导致的个别差异，连续观察数日校准）。

## 影响面

- 新增：`data/quant_strategies/wufu-v5.4-ding-report.py`、strategies 表一行、克隆出的 sim 账户。
- 修改：`backend/scripts/run_quant_sim.py`（仅加 `--clone-from` 参数）。
- 不动：runner/jqengine/db schema/API/前端、原策略与原账户。
