# 回源链路统一加固设计（失败重试 + 事件落本地 + 断点审计）

- 日期：2026-08-21
- 状态：已评审通过
- 背景：159667 工业母机ETF 6-10 份额拆分（1:3）在因子表缺失数周，导致 wufu-v5.2
  回测 07-02 候选池翻转（动量 -0.66 vs 聚宽 +4.98）。根因：`sync_adj_factor` 对
  1662 只逐个查 xdxr，任一次 socket 异常 → `_xdxr_rows` 把 None 缓存 → 该标的当轮
  静默跳过、无告警，直到手工重跑才自愈。

## 目标

任一回源链路的单次 socket 失败不再产生静默数据缺口：可发现、当轮自动补、补不上有告警。

## 非目标

- 不做跨轮 pending 文件（00:00 全量巡检 `scan_and_backfill_full` 已兜底跨日缺口）
- 不引入第二数据源（东财基金分红接口等）
- 不抽通用回源框架（四条链路数据形态差异大，只共享原语）
- 不替换既有 00:00 巡检 / 分钟覆盖率自愈（`STOCK_MINUTE_RESUME_COVERAGE`）

## 共享原语（mootdx_service.py 内函数，无框架抽象）

1. **同步内重试**：每轮回源结束，对「查询失败标的 ∪ 审计缺口」用新 `MootdxSource`
   实例（新缓存、换服务器）重试一轮。
2. **断点审计**（因子链路专用）：`_audit_uncovered_breakpoints(daily, factor_df,
   threshold=0.2)` —— 因子调整后序列仍含 >20% 单日跳变即视为缺口。严格大于排除
   20cm ETF 恰好 ±20% 的合法涨跌。只负责发现缺口驱动回源，不合成数据。
3. **返回值约定**：各链路返回 dict 统一增加 `query_failed` 字段（本轮查询失败的
   标的列表）。

## 第 1 层：查询失败不缓存（mootdx_src._xdxr_rows）

- 整轮服务器轮换耗尽 → 返回 None 且**不缓存**；下次调用自动重试。
- 只有成功结果（[] 或事件列表）进进程内缓存。
- 区分语义：`[]`=服务器确认无事件；`None`=本次没查到。

## 第 2 层：原始事件落本地（因子链路核心）

新增 `data/adj_factor_etf/xdxr_events.parquet`（symbol + category/year/month/day +
suogu/fenhong/songzhuangu/peigu/peigujia，稀疏只增）。

`sync_adj_factor` 新流程：

```
加载本地事件表 events_map
逐标的查询 xdxr：
  成功（[] 或事件列表）→ 归一化后覆盖 events_map[sym6]
  失败（None）        → 记入 query_failed，沿用 events_map 旧事件
保存事件表（先于因子表写入）
因子重建 = 纯本地计算（events_map × 日线 close），数学与现状一致：
  cat==11: factor = 1/suogu
  cat==1 : factor = (prev_close - fh + pgj*pg) / (1+sg+pg) / prev_close
写入 all.parquet（既有合并逻辑不变）
```

效果：事件一旦落袋，socket 再抖不影响既有因子；socket 只在"发现新事件"时重要。

## 第 3 层：断点审计兜底

每轮因子同步结束：

```
audit_uncovered = _audit_uncovered_breakpoints(daily, factor_table)
retry_syms = query_failed ∪ audit_uncovered
若非空 → 新 MootdxSource 重试一轮 → 有新事件则重建因子表并复审
仍缺 → logger.WARNING 列出标的（钉钉消费）
```

## 四条链路接入

| 链路 | 查询失败时 | 审计 |
|---|---|---|
| 因子 sync_adj_factor | 沿用本地事件表重建 | 断点审计（新增） |
| 日线 sync_daily | 失败标的当轮重试一轮 | 复用 00:00 巡检 |
| 分钟 sync_etf_minute / sync_stock_minute | 失败标的当轮重试一轮 | 最新分区覆盖率自愈（既有）+ 00:00 巡检 |
| NAV sync_etf_nav | 整表失败即重试一次 | 无需（单表全量） |

## 告警

统一 `logger.warning`（钉钉消费），消息含链路名 + 缺口数量 + 标的样例（≤10 只）。
当轮重试成功不告警；两道网（当轮重试 → 00:00 巡检）都失败才持续告警。

## 错误处理

- 重试轮再失败：保留 `mootdx_sync_failures.csv` 追加记录 + WARNING，不阻塞主流程
- 事件表损坏（读失败）：按空表处理并 WARNING，等价于回到现状行为
- 因子数学保持与现版本完全一致（cat==11 / cat==1 两分支不动）

## 测试

沿用 `tests/quant/test_sync_adj_factor.py` 既有约定（monkeypatch
`_etf_universe`/`DataManager`/`MootdxSource`/`ADJ_FACTOR_PATH`）：

1. `test_raw_events_persisted` —— 事件行落 xdxr_events.parquet
2. `test_query_failure_keeps_local_events` —— 次轮查询全失败，因子从本地事件重建不丢
3. `test_xdxr_failure_not_cached` —— 失败返回 None 不缓存，下次调用重试
4. `test_audit_retries_and_warns` —— 首轮漏事件 → 审计发现 → 重试补回；仍缺告警
5. 日线/分钟/NAV 链路各一个故障注入测试（fake source 先失败后成功，断言
   query_failed 与最终落盘完整）

## 现状处理

工作区已有半成品编辑（辅助函数已加、主循环改一半）：推倒按本设计重做；
已完成并验证的第 1 层（`_xdxr_rows` 失败不缓存）保留。

## 验收

- 全部单测通过 + ruff/mypy 干净
- 手工触发 `sync_adj_factor()` 后，断点审计 79/79 覆盖维持
- wufu-v5.2 回测 260401-260820 对齐数字不低于当前水平（最大日差 ≤4.68%）
