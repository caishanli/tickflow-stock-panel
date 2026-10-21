# 2026-10-21 sb_gaokai_sim：09:26 选股超时 + 日志重复 排障记录

账户：`sb_gaokai_sim`（首板高开一进二，strategy `5313ae33`）。当日无持仓、无成交。

## 现象

1. 09:26 选股没在 09:30 前结束：09:26:00 开始，**09:44:05** 才输出【选股结束】。
2. 页面日志里同一天出现**两段一模一样的选股**：
   - 第一段 09:26:00 → 09:44:05（心跳停在 ST 过滤 4813/4853，间隔 30s 级别）
   - 第二段 09:44:06 → 09:56:03（同样的 36 只首板候选、0 只通过、35 只"均价或成交额不符"）
3. 09:26 之后的 stdout 日志（`data/quant_sim/sb_gaokai_sim.log`）被
   `get_daily_* 日线回源失败` / `实时分钟为空` 刷屏。

## 时间线（证据）

| 时间 | 事件 | 证据 |
| --- | --- | --- |
| 09:26:00 | 盘前任务 `run_daily('09:26')` 开始 | sim_logs |
| 09:26:33–09:32:46 | ST 过滤 4853 只，**14~18 只/秒**（10-20 同日为 145 只/秒） | 心跳日志 |
| 09:32:46–09:43:08 | ST 过滤结束 → 基础股票池，**空转 10 分钟**（10-20 仅 28s） | sim_logs |
| 09:33:00 | stockdata 子进程死亡，主后端守护重新拉起 | `data/backend.log.1:77576` |
| 09:34:50–09:36 | 生产 checkout 里并发跑着 pytest（`/tmp/pytest-of-caisl/pytest-194`），测试日志写进 `data/backend.log` | `data/backend.log.1:77577+` |
| 09:44:05 | 第一轮结束（已过 09:30） | sim_logs |
| 09:44:06 | 第二轮开始（同一天重复选股） | sim_logs |
| 09:56:03 | 第二轮结束 | sim_logs |

## 根因

### A. 选股为什么慢 10~20 倍：缺名标的每个付 0.5s 的 ETF 名录回源

`get_security_name()` 的兜底路径：名称整表（`get_stock_names()`，7377 条）里没有的代码，
会 `mgr.fetch("get_etf_list")` 再查 ETF 名录。两个问题：

1. **ETF 名录没有任何缓存**：服务端全表扫描实测 **0.5s/次**（`get_etf_list` × 3 次实测 0.50/0.50/0.50s）。
2. **缺名结果不写回缓存**：同一个退市/长期停牌标的，每次解析都要重付一次。

当日 instruments 快照 5887 只里有 **347 只**不在名称表内（`000003`、`600242`、`600220` 这类退市/异常标的）。
全市场扫描里每只标的会被解析 1~2 次：

- ST 过滤（`current_data[stock]` → `_daily_info` → `_limit_rate_by_name` + `__getitem__` 各一次名称解析）：347 × ~1s ≈ **6 分钟** ✓ 与实测 09:26→09:32 吻合；
- 新股过滤（`get_security_info` 内部 2 次 `get_security_name`）：347 × ~1s ≈ **6~10 分钟** ✓ 与实测 09:32→09:43 吻合。

对照实测：同样 300 只标的（其中 239 只是"无日线/缺名"）冷进程遍历
`get_current_data()`：**修复前 140.4s，修复后 5.9s**（1 次 ETF 名录回源 + 其余全命中）。

补充：今日 univers 比 10-20 多了 248 只（5474 vs 5226），缺名标的占比上升，
把原本"秒级"的兜底放大成十几分钟。

### B. 日志为什么重复：超时结束的盘前任务被重试

`_fire_preopen_daily` 里，策略置 `preopen_auction_missing` 且未成交时会
`aux["fired"].discard(key)`，让下一轮重新触发（本意：竞价快照晚到时重试）。
但第一轮 09:44:05 才结束，**已经过了 09:30 竞价窗口**，重试既拿不到竞价数据，
又把整轮选股原样再跑一遍；随后 09:26 `run_daily` 的 `_daily_due`（`t >= 09:26`）
也会在盘中 tick 认定它"还没跑过"。

### C. 附带发现：长驻模拟盘 DB 连接泄漏

`with get_conn() as c` 只提交/回滚、**不关连接**（sqlite3 的上下文管理器语义）。
`sb_gaokai_sim` 单进程累积 **847 个 quant.db fd**（其他账户 180~1245 个），
重启前 fd 已接近 1024 软限；每次 `get_quant_setting`（`_name_source()` 每条名称解析都会调）都新开连接。

### D. 当日并发 pytest（非直接原因，但污染日志/抢资源）

09:34–09:36 生产 checkout 在跑 pytest：`data/backend.log.1` 里混入
`tests/backtest/test_factor_batch.py` 的 `ValueError: broken factor`、
`/tmp/pytest-of-caisl/pytest-194/...` 等测试日志。stockdata 子进程 09:33 死亡
与 pytest 无直接证据关联（dmesg 不可用），但测试与实盘抢同一台 2 核机、共用
`data/backend.log`，盘中跑全量测试应避免。

## 修复

| 文件 | 改动 |
| --- | --- |
| `backend/app/quant/jqengine/engine/jq/api.py` | ① 新增 `_bulk_etf_list()`：ETF 名录**日级进程缓存**，缺名兜底不再每次回源；② `_bulk_stock_names()` 失败**负缓存 30s**（`_STOCK_NAMES_FAILED_AT` + `_NAMES_FAIL_TTL`），避免名称服务抖动时全市场逐只重拉整表 |
| `backend/app/quant/simulate/runner.py` | 盘前任务重试加"09:30 竞价窗口"条件；窗口外不 discard `fired`，并落一条 warn（`盘前任务 09:26 耗时 Ns，HH:MM:SS 才结束、已过 09:30 竞价窗口，不再重试`） |
| `backend/app/quant/db.py` | `get_conn()` 改为返回 `_ClosingConn`：`with` 退出时 commit/rollback 后 **close**，堵住 fd 泄漏；`list_sim_accounts` 内的二次查询移到新的 `with` 块（原先复用已关闭连接） |

回归测试：

- `tests/quant/test_security_name_cache.py`：+ 失败负缓存（含 TTL 后恢复）、+ ETF 名录只拉一次；
- `tests/quant/test_auction_safety.py`：+ 跑过 09:30 的盘前任务不再重试（并断言盘中 tick 不会二次触发）；
- `tests/quant/test_db.py`：+ `with get_conn()` 退出后连接已关闭、+ 50 次读写后 fd 不增长。

验证：`pytest tests/quant/test_security_name_cache.py tests/quant/test_auction_safety.py tests/quant/test_db.py -q` → 35 passed；
`pytest tests -q --ignore=tests/backtest/test_fix_api.py` → 2337 passed，8 failed 均为存量失败
（`test_fix_api.py` 引用的 `BACKTEST_MAX_SYMBOLS`、`test_fix_engine_cleanup.py` 引用的
`_check_minute_fill_guard` 在当前代码里不存在，与本改动无关）。

## 影响与处置

- 22:19 重启 `sb_gaokai_sim`（无持仓、无成交）以加载修复；新进程 fd=10（原 857），
  启动即完成全市场日线预载，次日 09:26 不必再冷启动逐只回源。
- 重启触发的补跑额外写了一条"收盘处理完成"（22:20:19）与 15:02 时戳的补跑行，
  已在 `sim_logs` / `sim_equity_snapshots` 中删除，当日日志保持一份收盘记录。

## 后续建议

1. 09:25 盘前在 live 模式做一次 `count=1` 的全市场日线批量预载（一行 batch，
   实测服务端 10~30s），比逐只回源更稳；本次重启后的进程已有此热缓存，但重启即丢。
2. 盘中不要在生产 checkout 跑全量 pytest；测试日志不应写进 `data/backend.log`
   （建议 root conftest 把 app logging 指到临时目录）。
3. stockdata 子进程 09:33 死亡原因未定位（无 dmesg 权限）；建议在守护里记录
   子进程 exit code/signal 与当时 RSS/swap，便于下次归因。
