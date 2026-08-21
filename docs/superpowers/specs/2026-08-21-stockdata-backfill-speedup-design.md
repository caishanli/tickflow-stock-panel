# stockdata 回源提速设计（2026-08-21）

## 背景与根因

排查结论见 `debug/stockdata-backfill-slow` 分支调查（日志 + py-spy + 同机实测延迟）：

1. **重启绞杀循环**：guardian 把 stockdata 服务生命周期绑在主后端上，每次重启 SIGKILL
   数小时级回源任务；启动 backfill 发现当日分区覆盖率 <95% 即全量重拉 ~2000 只，
   进度只能 61%→63%→73% 地爬。今天 7 次重启。
2. **全量历史拉取，99% 丢弃**：所有修复路径用 `get_minute(max_bars=40000)` 拉 4/1 至今
   全量（~29 页/只）再按日过滤。实测某轮 range run 拉 7500 万 bar 只留 78 万。
3. **服务器选择失灵**：`_make_client` 按固定列表顺序选第一个 TCP 可达者；实测同种请求
   #1 服务器 1529ms/页 vs #6 53ms/页（30 倍差），且为突发性限速；16 台中 9 台对有效
   标的返回空（云节点），空响应触发全列表轮换且 `_server_idx` 停留坏区间。
4. **完全串行**：单线程单连接逐只拉取；健康延迟下全市场下限也要 ~2.3h。
5. **可观测性黑洞**：进度日志每 25 只才打一条，全部标的逼近 190s 守护上限时出现
   68 分钟零日志。

连带问题：盘中限速机制（08-13 引入）把任务拖长、制造与次日盘中的重叠——是回源慢的
结果而非独立问题。另有数据完整性隐患：分页遇短页即 break，把限速截断误判为历史尽头。

## 目标

- **全面改造**：吞吐、重启恢复、服务器选择、并发、可观测性一次到位。
- **验收标准**：15:35 触发后 ETF 分钟+因子+日线+指数 ≤10min；股票分钟当日分区
  （~5200 只）≤20min；重启后不重拉已落盘标的。
- **并发风险偏好**：盘后多并发；删除盘中限速双参数（短促并发替代持续轰击）；仅保留
  一个轻量保险：交易时段遇到 >500 只的大任务时 worker 减半。
- **重启策略**：保持现有 guardian 架构，断点续传 + 中断感知，不做生命周期解耦。

## 设计

### 1. 组件总览

改动集中在 stockdata 服务进程内：

```
mootdx_src.py      get_minute(since=...) 按需分页、短页自愈、延迟感知选服
backfill_pool.py   (新) BackfillPool: N worker 线程、线程独立 MootdxSource
mootdx_service.py  sync_* 提交逐 symbol 任务到池、删盘中限速、manifest、持锁
```

实时路径（sources.NetworkPuller）不动。两池场景天然错开：实时池只服务盘中按需请求；
回源池的任务盘后 ~20 分钟内跑完。

### 2. 按需分页 + 短页自愈

pytdx `start=N` 从最新往回数，每页 800 bar，A 股 240 bar/交易日。

```python
def _pages_needed(since: date, today: date) -> int:
    n_days = trading_days_between(since, today)   # 复用 _trade_days_in_range
    return ceil(n_days * 240 / 800) + 1           # +1 页余量；本地本来就按日期过滤
```

- `get_minute(code, since=None)`：since 为空 = 全量（仅首次初始化/显式重建）；
  给了 since 只拉覆盖 `[since, today]` 的页。
- 调用方改造：
  - 收盘后当日同步 / resume 补最新分区 → since=目标日（1~2 页 vs 现在 29 页）
  - 残片日/历史缺口修复 → since=min(缺失日)
  - 全市场初始化 → 全量模式（唯一长任务，靠并发+manifest 兜底）
- **短页自愈**：分页中遇 `0 < len(df) < 800` 不再直接 break，补发一页
  `start += len(df)` 探测——有数据即限速截断，继续拉；连续空才是真尽头。

### 3. BackfillPool

```python
class BackfillPool:
    def __init__(self, workers=None)   # BACKFILL_WORKERS env，默认 6
                                       # 交易时段且任务 >500 只 → workers 减半
    def map_symbols(self, fn, symbols, on_batch_done) -> 结果聚合
```

- 每 worker 线程内独立 MootdxSource（socket 非线程安全，同 NetworkPuller._source()
  模式），启动时取 `rank[i % len(good)]` 分散绑定不同服务器。
- 任务粒度 = 单 symbol；每攒满 100 只由主线程统一 flush 分区（写盘单线程，避免
  parquet 并发写同一文件）+ manifest 记账。
- 失败语义沿用现状：超时/空 → 该 worker 重建连接、记 failures.csv、不阻断整批。

### 4. 服务器选择

**建连时排名**（进程级缓存，TTL 30 分钟）：
1. 并发 TCP probe 全部 16 台（复用 probe_servers）；
2. 对可达者各发一次真实数据请求（600519 日线 10 根）测延迟并校验非空；
3. 按延迟排序得 rank[]；对已知有效标的返回空的节点降权沉底。

**运行时切换**（每 worker 连接滚动统计）：
- 最近 8 页平均延迟 >800ms（健康值 ~55ms 的 15 倍）→ 切换到 rank 下一台；
- 连续 3 次空响应 → 同样切换；
- 切换 = 丢弃当前 client 按排名重建，不再盲目 `_server_idx+1` 轮换、不停留坏区间。

**删除项**：`_BACKFILL_THROTTLE_EVERY/SLEEP`、`_BACKFILL_INTRADAY_EVERY/SLEEP` 与
`_throttle_backfill` 全部移除。

### 5. 断点续传 manifest + 中断感知

**manifest**（data/backfill_state.json，原子写 tmp+rename）：

```json
{"stock_minute": {"targets": ["600000.SH", ...], "done": ["600000.SH", ...],
                  "mode": "full|recent", "updated_at": "..."}}
```

- 任务启动写 targets；每批 flush 成功后把该批 symbol 追加进 done 落盘。
- 重启后新任务：todo = targets − done − 最新分区已有 → 精确断点续传。
- 日常短任务本身几分钟；manifest 主要价值在全量初始化（小时级）。

**中断感知**：
- guardian 单实例 PID 锁保证同机无并发写者——分区 mtime <10 min = 上个进程刚被杀在
  中途，日志打「上次回源中断于 X 分区 (N/M 只)，从断点继续」而非误导性「残缺」。
- `<95% 全量补齐` 判定保留（单实例下语义正确），配合 manifest 后补的只是真实缺口。

### 6. 可观测性 + 持锁

- 时间驱动进度日志：sync 循环每 60s 打一条（区间处理数/速率/当前标的/ETA）；池级汇总
  各 worker 当前服务器+延迟。
- 启动 backfill 持 `_sync_lock`，与 15:35 cron、00:00 巡检串行。
- 内容校验去重：backfill_to_now 里 kline_etf_daily 250 分区扫描两遍 → 复用一次结果。

### 7. 测试

单元测试：分页数学（offset 估算/+1 余量）、短页自愈（mock 截断页）、manifest 断点
恢复、延迟排名降权。现有 pytest 套件回归；wufu 回测性能门禁不受影响（量化侧走
StockDataClient，不碰回源路径）。

## 验收对照

| 验收标准 | 实现路径 | 验证方式 |
|---|---|---|
| ETF分钟+因子+日线+指数 ≤10min | 池并发（逐 symbol 单请求） | 手动 trigger 计时 |
| 股票分钟当日 ≤20min | 1 页/只 × 6 worker | 15:35 cron 后查日志耗时 |
| 重启不重拉 | manifest + 最新分区成员 | 杀进程重启对比 todo 数 |
| 数据完整 | 短页自愈 + 本地日期过滤 | 单测 mock 截断场景 |
