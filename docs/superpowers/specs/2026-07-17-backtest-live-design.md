# 设计文档：量化回测实时写库与 SSE 推送

- 日期：2026-07-17
- 状态：已评审（approved）
- 范围：在现有 `app/quant` 模块基础上，将回测结果从「结束一次性落库」改为「运行期实时落库」，并新增 SSE 通道做增量推送；前端新建独立页面 `QuantBacktest` 用 SSE 实时展示。
- 依赖：`docs/superpowers/specs/2026-07-13-quant-rqalpha-design.md`（总体架构）

## 1. 目标与非目标

### 目标
- 回测运行期间，日志（策略 `log.info`）、收益（每日一行）、成交（每笔）**实时写 `quant.db`**。
- 新增 SSE 端点，按事件类型增量推送 `log / trade / equity / status` 给前端。
- 前端新建独立页面 `frontend/src/pages/quant/QuantBacktest.tsx`（列表 + 新建 + 实时详情三块），用 SSE 接收增量；刷新/断线后用现有轮询接口恢复历史。
- 复用 `frontend/src/quant/api.ts` 既有 `runBacktest/getBacktestStatus/Equity/Trades/Logs/CsvUrl/Terminate/Delete`。

### 非目标
- 不改动现有 vectorbt 回测页与 SSE 因子引擎（`pages/backtest/` + `lib/backtestTask.ts`）。
- 不引入跨进程消息队列（如 redis/rabbitmq）；采用「写库 + SSE 查库增量」方案（A/B 混合）。
- 不做策略代码沙箱（内网自用，直接执行，与现状一致）。

## 2. 关键决策（已与用户确认）

| 决策点 | 结论 |
|--------|------|
| 实时机制 | **SSE**（非轮询；与现有因子回测的 SSE 一致） |
| 前端页面 | **新建独立页面** `QuantBacktest.tsx`（不动现有 `StrategyBacktest`） |
| 代码执行 | **直接执行**用户 Python（内网自用，等同现状） |
| SSE 事件粒度 | **4 类事件**（status/log/equity/trade）+ 断线/刷新重连恢复 |
| 后端落库方案 | **A/B 混合**：rqalpha mod 实时写 `quant.db`（WAL）+ SSE 端点查 DB 增量推送，避开跨进程队列 |

## 3. 数据流（A/B 混合）

```
前端新建页 ──POST /api/quant/backtest/run──> submit_backtest()
   (策略代码+参数)                          生成 8位 run_id
                                            db.upsert_run(status=queued)
                                            Popen(run_quant_backtest.py <run_id>)

run_quant_backtest.py (子进程)
   └─ rqalpha_bridge.run_backtest()
        ├─ LiveStreamMod 钩子:
        │    on_user_log  ──> db.insert_log()        实时（逐行）
        │    after_trading─> db.insert_equity_row()  每日一行
        │    on_trade     ──> db.insert_trade()       每笔
        │    status 变更   ──> db.upsert_run()/update_run()
        └─ 结束 ──> db.update_run(done/failed, metrics)

前端 SSE ──GET /api/quant/backtest/{id}/stream──> 查 DB 增量(按偏移)──> 推 event
前端轮询(兜底/重连) ──/status /equity /trades /logs──> 直接读 DB
```

- `quant.db` 开 **WAL 模式**，子进程写、SSE 主进程读并发安全。
- `run_id` = 8 位随机（`uuid.uuid4().hex[:8]`），前端新建即拿到，用于所有后续查询与 SSE。
- DB 始终是最新真值；SSE 只是增量推送通道，断线后用轮询接口按已有数据重建 UI，无缝继续。

## 4. 后端改造

### 4.1 实时写库 mod（`app/quant/rqalpha_bridge.py` 新增 `LiveStreamMod`）
- 注册 rqalpha 事件钩子：
  - `user_log` / `user_system_log` → `db.insert_log(run_id, ts, level, message)`（策略 `log.info` 逐行进库）。
  - `after_trading` → 取当日组合价值，构造一行 `(run_id, dt, value, benchmark, cash, positions_value)` → `db.insert_equity_row()`（单行增量插入，替代结束时的 `bulk_insert_equity`）。
  - `on_trade` → `db.insert_trade(run_id, ts, code, action, price, amount, pnl, pnl_pct, commission)` 每笔即时。
  - 状态变更（queued→running→done/failed）→ `db.upsert_run` / `update_run`。
- 现有"结束后 `bulk_insert_equity` + 循环 `insert_trade`"逻辑改为：运行时已逐行写入，结束时只补 `update_run(done, metrics)`。保留结束时全量提取写库作为异常兜底（mod 未触发时仍能落库）。

### 4.2 db.py 增量接口
- `init_db` 中 `PRAGMA journal_mode=WAL`（首次建库设）。
- 新增 `insert_equity_row(run_id, dt, value, benchmark, cash, positions_value)`（单行，供每日收盘调）。
- 现有 `bulk_insert_equity` 保留作兜底。
- 新增 `list_runs()`（读 `backtest_runs` 全部/近 N 条，供列表页）。
- 新增增量读取辅助（供 SSE）：`get_logs_after(run_id, offset)`、`get_trades_after(run_id, offset)`、`get_equity_after(run_id, offset)`，按自增 id 偏移返回新增行。
- `backtest_logs` / `backtest_trades` / `backtest_equity` 表需确保有 `AUTOINCREMENT` 主键 `id` 以支持偏移增量。

### 4.3 SSE 端点（`app/quant/api/quant.py` 新增）
- `GET /api/quant/backtest/{run_id}/stream`：返回 `text/event-stream`（用 sse-starlette 或直接 `StreamingResponse`）。
- 每个连接维护"已推偏移"（log 行数 / trade 行数 / equity 行数）。连接内定时（~0.5s）查该 run_id 的增量行，按类型推：
  - `event: status` + `data: {status, metrics?}`
  - `event: log` + `data: {ts, level, message}`
  - `event: trade` + `data: {...}`
  - `event: equity` + `data: {dt, value, benchmark, cash, positions_value}`
- 连接关闭即退出循环。
- 轮询接口（`/status` `/equity` `/trades` `/logs`）**保持不变**，作为 SSE 断线/刷新的兜底与历史恢复。

### 4.4 子进程（`run_quant_backtest.py`）
- 当前 stdout/stderr 丢 DEVNULL；`LiveStreamMod` 直接写 DB（不依赖 stdout），故子进程 stdout 仍可丢弃。
- `run_backtest` 需把 `run_id` 透传给 `LiveStreamMod`（经 `params["run_id"]`）。
- `submit_backtest` 已生成 run_id 并 Popen，无需大改。

## 5. 前端改造（新建独立页面）

### 5.1 页面 `frontend/src/pages/quant/QuantBacktest.tsx`
- **列表区**：`getBacktestRuns()`（新增 api，对应 `GET /backtest/runs`）列出所有 run（run_id、策略、状态、起止、收益、时间），带"新建"按钮。点某行 → 进入详情。
- **新建面板**：策略代码编辑器（复用现有 `CodeEditor`/CodeMirror）+ 参数表单（start/end、initial_cash、benchmark、provider、strategy 名/策略参数）。"运行" → `POST /backtest/run`（body `{code, params}`），拿到 8 位 run_id 后跳详情并自动开 SSE。
- **实时详情区**（选中 run 后）：收益曲线（读 `/equity`，SSE 增量追加）、交易表（读 `/trades`）、日志面板（读 `/logs`，自动滚底）、状态条（queued/running/done/failed + metrics）。

### 5.2 SSE 客户端 `frontend/src/quant/stream.ts`（新建）
- `openBacktestStream(run_id, handlers)`：`EventSource('/api/quant/backtest/{run_id}/stream')`，handlers：`onLog/onTrade/onEquity/onStatus`。
- **断线重连 + 刷新恢复**：`EventSource` 自带断线重连；页面挂载时若已有 run_id，先调 `/equity` `/trades` `/logs` 拉全量历史（兜底），再开 SSE 收增量。复用 `frontend/src/quant/api.ts` 既有封装。

### 5.3 复用与新建
- 复用：`api.ts` 现有封装、现有 `CodeEditor` 组件、`backtestTask.ts` 的 SSE 重连思路。
- 新建：`QuantBacktest.tsx`、`stream.ts`、后端 `GET .../stream` 端点、`LiveStreamMod`、`db.insert_equity_row` / `list_runs` / `*_after` 增量接口、`GET .../runs` 列表端点。
- 路由：`router.tsx` 新增 `/quant-backtest` → `QuantBacktest`（沿用现有 lazy 命名导出模式）。
- 视觉风格：严格遵循 `2026-07-13-quant-rqalpha-design.md` §11 设计令牌与组件复用约束（暗色默认、token 类、复用 `PageHeader`/`Modal`/`EmptyState`/图表组件）。

## 6. 实施顺序
1. db.py：WAL + `insert_equity_row` + `list_runs` + `*_after` 增量接口 + 表加自增 id。
2. rqalpha_bridge.py：`LiveStreamMod` 实时写库；改造 `run_backtest` 结束逻辑。
3. api/quant.py：SSE 端点 `/stream` + 列表端点 `/runs`。
4. 前端：`QuantBacktest.tsx` + `stream.ts` + 路由 + api.ts 增量（runs 列表）。
5. 端到端验证：跑一单回测，确认 SSE 实时推送日志/收益/交易 + 详情页正常。

## 7. 风险
- rqalpha 事件钩子 API（`after_trading` / `on_trade` / `user_log`）需核对版本（6.2.1）的事件名与签名。
- WAL 下 SQLite 多进程写需控制单写者（子进程是唯一写者，SSE 只读），避免写写冲突。
- SSE 长连接与 uvicorn 线程模型：用 `anyio` 后台任务循环查库，注意不阻塞事件循环。
