# 设计文档：基于 RQAlpha 的量化回测与模拟盘

- 日期：2026-07-13
- 状态：待评审
- 范围：在 tickflow-stock-panel 中新增独立的「量化回测」与「量化模拟盘」模块，后端基于 RQAlpha 跑聚宽式 Python 策略，复用 quant-daydayup 的数据源获取代码；前端新增两个菜单与页面。

## 1. 目标与非目标

### 目标
- 新增后端子系统 `backend/app/quant/`，提供 RQAlpha 回测、聚宽式策略管理、实时模拟盘、离线回放。
- 数据源**可配置双源**：TickFlow 本地数据 + quant-daydayup 多源（tushare/mootdx/astock）降级，统一优先级调度。
- 前端新增「量化回测」「量化模拟盘」两个菜单与页面。
- 聚宽式 Python 策略（直接复用 `wufu_etf_rotation.py` 等）可在前端编辑器编写、保存、运行。

### 非目标
- 不改动现有 vectorbt 回测页（信号/因子/策略三模式）与 screener 的 18 内置策略体系。新模块完全独立。
- 不接入实盘券商（xtquant/MiniQMT）。模拟盘仅本地撮合 + 止损。
- 不引入 IBKR 等海外连接器。

## 2. 关键决策（已与用户确认）

| 决策点 | 结论 |
|--------|------|
| 与现有回测关系 | 新增独立模块，保留 vectorbt 回测页 |
| 模拟盘形态 | 同时支持「实时盘」+「离线回放」 |
| 数据源 | 可配置双源（TickFlow 本地 + 多源降级），设置可切换优先级 |
| 策略格式 | 聚宽式 Python（rqalpha 原生支持） |
| 回测进程模型 | **独立 OS 进程**（脚本 `run_quant_backtest.py`，FastAPI 派生子进程并读库） |
| 模拟盘进程模型 | **独立 OS 进程**（脚本 `run_quant_sim.py`，可 FastAPI 派生或 pm2/nohup 守护） |
| 结果存储 | **独立 SQLite 库 `data/quant.db`**（收益/日志/交易记录全落库，与 tickflow 的 DuckDB/Parquet 数据层完全隔离） |
| 前端更新方式 | 短轮询只读接口（回测 1–2s、模拟盘 3–5s），与 quant-daydayup 一致 |
| 实时盘进程模型 | **独立进程**（类 quant-daydayup 的 pm2/nohup 守护），非 APScheduler 内置 job |
| 策略代码编辑器 | **CodeMirror**（@uiw/react-codemirror，轻量） |
| 分钟数据绝对约束 | **C1** 有 1m 用 1m；无 1m 用 5m 插值；本地有就用本地，本地没有就用 mootdx/baostock 获取再存本地（禁止"本地无就放弃/不回源"）。**C2** 插值数据只用于当次计算，**绝不写 `minute.db`**；只有真实获取的 1m/5m 才落盘。**C3** 回测区间不偷偷裁剪起点，缺失段按 C1 回源/插值补齐。详见根目录 `CONSTRAINTS.md`。 |

## 3. 后端架构

新增顶层包 `backend/app/quant/`，与现有 `backtest/`、`strategy/`、`services/` 并列，互不依赖。

```
backend/app/quant/
├── __init__.py
├── datasource/
│   ├── __init__.py
│   ├── base.py            # DataSource 抽象 + DataSourceError（改编自 quant-daydayup）
│   ├── manager.py         # QuantDataProvider：可配置优先级 + 降级 + 缓存
│   ├── tickflow_src.py    # 适配器：读 tickflow 本地 enriched/kline parquet
│   ├── tushare_src.py    # vendored
│   ├── mootdx_src.py     # vendored
│   ├── astock_src.py      # vendored（包裹 astock_skill.py）
│   ├── astock_skill.py   # vendored a-stock-data 函数（无 pip 依赖）
│   ├── baostock_src.py   # vendored（5min 插值中间层）
│   ├── minute_synth.py   # vendored（日线合成分钟）
│   └── cache.py          # vendored Parquet 缓存
├── rqalpha_bridge.py     # 实现 rqalpha AbstractDataSource；落地 bundle + run()
├── strategies/
│   ├── store.py          # 聚宽 .py 策略 CRUD，落 data/quant_strategies/
│   └── samples/         # 内置样例（wufu_etf_rotation.py 等迁移）
├── simulate/
│   ├── __init__.py
│   ├── matcher.py        # 止损巡检（改编自 quant-daydayup）
│   ├── protocol.py       # 账户状态读写（quant DB）
│   ├── runner.py         # 实时盘主循环（独立进程入口逻辑）
│   └── replay.py        # 离线回放（复用 rqalpha_bridge 跑历史）
├── db.py                 # 独立 SQLite 库 (data/quant.db)：回测/模拟盘 收益+日志+交易 全部落库
├── service.py            # 编排：回测提交(派生子进程)、账户管理、读库
└── config.py             # QUANT_DATA_PRIORITY / TUSHARE_TOKEN / FEE / SLIPPAGE / STOP_LOSS
```

> 回测与模拟盘**均为独立 OS 进程**，FastAPI 只做「提交任务 + 读库返回」的薄层，不直接跑 rqalpha / 不内嵌循环。

```
backend/scripts/
├── run_quant_backtest.py  # 回测独立进程：读 run_id 参数 → rqalpha_bridge → 写 quant.db
└── run_quant_sim.py       # 模拟盘独立进程：读 account_id → 主循环 → 写 quant.db
```

### 3.1 数据源 `datasource/`
- 直接 vendored 自 `/home/ubuntu/quant-daydayup/backend/app/datasource/` 的各文件，接口保持一致（`get_daily`/`get_minute`/`get_index_realtime`/`get_etf_list`/`get_stock_list`）。
- 新增 `tickflow_src.py`：实现 `DataSource` 接口，内部通过现有 `app.tickflow.repository.KlineRepository` 与 `app.parquet.scan_enriched_parquet` 读取本地 `kline_daily_enriched`、`kline_minute`、`kline_etf_*` 等。
- `QuantDataProvider`（在 `manager.py` 中）持有一个有序源列表，按配置 `QUANT_DATA_PRIORITY`（如 `tickflow,tushare,mootdx,astock`）依次尝试，单源失败/超时/无数据自动降级；本地 Parquet 缓存避免重复请求。
- `astock_skill.py` 直接 vendor，运行时仅依赖 `requests`。

### 3.2 RQAlpha 桥接 `rqalpha_bridge.py`（仅供回测独立进程调用）
- 实现 rqalpha 的 `AbstractDataSource`/`AbstractCalendar`，内部调用 `QuantDataProvider` 取数。
- 回测运行流程（在 `backend/scripts/run_quant_backtest.py <run_id>` **独立进程**内执行，FastAPI 不直跑）：
  1. 从 quant DB 读 `backtest_runs` 中该 run_id 的参数（标的池、区间、频率 daily/1m、手续费、滑点、本金、数据源优先级）。
  2. 将所需标的的 daily（及 1m，若频率=1m）落地为 rqalpha bundle 到 `data/quant_bundle/<run_id>/`（带缓存，按标的+区间命中复用）。
  3. 用 `rqalpha.run()` 运行用户聚宽式策略源码（经 `str(config)` 注入参数）。
  4. 回收 `portfolio`、成交明细、持仓、基准曲线 → 归一化为指标（总收益、年化、夏普、最大回撤、胜率）+ 净值/回撤序列。
  5. 全量写入 quant DB（`backtest_equity` / `backtest_trades` / `backtest_logs` / `backtest_runs` 指标与 status=done）；失败则写 `backtest_logs` 并置 status=failed。

### 3.3 策略管理 `strategies/store.py`
- 聚宽 `.py` 策略文件落 `data/quant_strategies/`（加入 `.gitignore`），元数据（id/name/updated_at）可落 quant DB `strategies` 表。
- CRUD：列表/读取/保存/删除/导出/导入。内置样例从 quant-daydayup `strategy/` 迁移（如 `wufu_etf_rotation.py`）。

### 3.4 模拟盘 `simulate/`（独立进程）
- **实时盘**（独立进程）：
  - 入口 `backend/scripts/run_quant_sim.py <account_id>`。可由 FastAPI `POST /sim/accounts/<id>/start` **派生子进程**启动（detached subprocess），也可用户用 `nohup`/`pm2` 守护（类 quant-daydayup 的 `pm2 start backend/scripts/run_simulate.py --name sim-1 -- 1`）。无论哪种，都是与 uvicorn **分离的 OS 进程**。
  - 主循环（`runner.py`）：交易时段（9:30-11:30, 13:00-15:00）每分钟取持仓最新价（`QuantDataProvider.get_minute` + 实时接口），调用 `matcher.step()` 做止损巡检。
  - 状态/账本（`db.py` + `protocol.py`）：实时净值、现金、持仓、止损日志、盈亏**全部写 quant DB**（见 §3.7）；同时周期性写 `sim_equity_snapshots` 形成净值曲线。**不再用独立 JSON 文件**——统一进库，便于前端实时读取与历史追溯。
  - 非交易时段休眠；进程崩溃由 quant DB 中已落盘的快照/成交恢复，不丢历史净值。
- **离线回放**（`replay.py`）：选聚宽策略 + 区间，复用 `rqalpha_bridge` 跑历史（与回测同一条独立进程路径），产出与回测一致的分析视图。
- 前端「模拟盘」页经 FastAPI 读 quant DB 展示，并下发 启动/暂停/重置（启动=派生子进程/返回启动命令；暂停/重置=写控制标记或清库表）。

### 3.5 API `api/quant.py`（FastAPI Router, prefix `/api/quant`）
> FastAPI 只做「提交任务 + 读 quant DB 返回」，回测/模拟盘的重活都在独立进程。前端**轮询**这些只读接口实现实时更新（与 quant-daydayup 的轮询方式一致）。

| 分组 | 方法 | 路径 | 说明 |
|------|------|------|------|
| 策略 | GET/POST | `/strategies` | 列表 / 新建 |
| 策略 | GET/PUT/DELETE | `/strategies/<id>` | 读取/保存/删除 |
| 策略 | GET | `/strategies/<id>/export` | 导出 .py |
| 策略 | POST | `/strategies/import` | 导入 .py |
| 回测 | POST | `/backtest/run` | 提交回测：写 `backtest_runs`(status=queued) → 派生子进程 `run_quant_backtest.py`，返回 run_id |
| 回测 | GET | `/backtest/<id>/status` | 状态(running/done/failed) + 指标（读 DB） |
| 回测 | GET | `/backtest/<id>/equity` | 净值 + 基准曲线（读 `backtest_equity`） |
| 回测 | GET | `/backtest/<id>/trades` | 成交明细（读 `backtest_trades`） |
| 回测 | GET | `/backtest/<id>/logs` | 运行日志（读 `backtest_logs`） |
| 回测 | GET | `/backtest/<id>/trades.csv` | 成交导出 |
| 回测 | POST | `/backtest/<id>/terminate` | 终止（杀子进程 + 置 failed） |
| 回测 | DELETE | `/backtest/<id>` | 删除（DB 行 + bundle） |
| 模拟盘 | GET/POST | `/sim/accounts` | 账户列表 / 新建 |
| 模拟盘 | POST | `/sim/accounts/<id>/{start,pause,reset}` | 启动(派生子进程)/暂停/重置 |
| 模拟盘 | GET | `/sim/accounts/<id>/status` | 实时净值/持仓/止损日志（读 DB） |
| 模拟盘 | GET | `/sim/accounts/<id>/equity` | 净值曲线快照（读 `sim_equity_snapshots`） |
| 模拟盘 | GET | `/sim/accounts/<id>/trades` | 成交 + 止损记录（读 DB） |
| 数据源 | GET | `/datasource` | 当前优先级 |
| 数据源 | POST | `/datasource/priority` | 保存优先级 |
| 数据源 | POST | `/datasource/token` | 保存 Tushare Token |
| 数据源 | POST | `/datasource/verify` | 连通性校验 |

- 统一响应包络沿用现有约定（成功 `{data:...}`，失败抛 `HTTPException`）。
- 实时更新靠前端**短轮询**：回测提交后每 ~1–2s 轮询 `/status`+`/equity`+`/logs`；模拟盘页每 ~3–5s 轮询 `/status`+`/equity`+`/trades`（与 quant-daydayup 的轮询模型一致）。如需更实时，后续可换 sse-starlette，但首版以轮询为主，避免过度设计。

### 3.6 独立数据库 `db.py`（核心：与 tickflow 数据层完全隔离）
- 使用 **Python 标准库 `sqlite3`**，库文件 `data/quant.db`（`QUANT_DB_PATH`，加入 `.gitignore`）。
- **不**使用 tickflow 的 DuckDB / Parquet 数据层，也不写入 `data/backtest_results/`——完全独立，便于上游合并时不冲突、且可单独备份/清理。
- 建表（`quant/db.py` 内 `init_db()`，首次访问自动建）：
  - `backtest_runs(id, strategy_id, params_json, status, metrics_json, created_at, finished_at, error)`
  - `backtest_equity(run_id, dt, value, benchmark, cash, positions_value)`
  - `backtest_trades(run_id, ts, code, action, price, amount, pnl, pnl_pct, commission)`
  - `backtest_logs(run_id, ts, level, message)`
  - `strategies(id, name, file, created_at, updated_at)`
  - `sim_accounts(id, name, capital, stop_loss, status, created_at, started_at)`
  - `sim_equity_snapshots(account_id, dt, net_value, cash, positions_value, pnl, pnl_pct)`
  - `sim_trades(account_id, ts, code, action, price, amount, pnl, pnl_pct, commission)`
  - `sim_stop_loss(account_id, ts, code, action, price, pnl_pct)`
- 所有读写经 `db.py` 的统一连接/游标封装；回测/模拟盘独立进程与 FastAPI 都连这同一个 `data/quant.db`（SQLite 支持多进程读写，写操作加事务）。

### 3.7 配置与依赖
- `backend/pyproject.toml` 新增可选 extra（不污染基础安装）：
  ```toml
  quant = [
      "rqalpha>=0.26",
      "mootdx>=0.11.7",
      "tushare>=1.4.29,<2",
      "baostock",
      "requests>=2.31",
  ]
  ```
  `astock_skill.py` 仅依赖 `requests`，直接 vendor。**无需**额外 DB 依赖（sqlite3 为标准库）。
- 运行需 `uv sync --extra quant`（类比现有 `uv sync --extra backtest`）。
- 配置项（`quant/config.py` 读 `.env`）：`QUANT_DATA_PRIORITY`、`TUSHARE_TOKEN`、`QUANT_FEE_RATE`、`QUANT_SLIPPAGE`、`QUANT_DEFAULT_STOP_LOSS`、`QUANT_DB_PATH=data/quant.db`。
- `.env.example` 增加上述变量；`.gitignore` 增加 `data/quant_strategies/`、`data/quant_bundle/`、`data/quant.db`。

## 4. 前端架构

全部新代码落在独立目录 `frontend/src/quant/`（见 §11），不改动现有 `pages/`、`components/`。仅 `App.tsx` 与 `Layout.tsx` 做单行挂载（见 §10）。图表复用现有 `pages/backtest/charts` 组件（ECharts / Lightweight Charts），请求用 Tanstack Query，样式用 Tailwind。

```
frontend/src/quant/
├── pages/
│   ├── QuantBacktest.tsx        # 主页面：策略列表 + 运行 + 结果
│   ├── QuantSim.tsx             # 模拟盘：账户列表 + 实时状态 + 离线回放
│   ├── StrategyEditorDialog.tsx # CodeMirror 编辑聚宽 .py
│   ├── BacktestResult.tsx       # 净值/回撤/成交表/CSV
│   ├── AccountDialog.tsx        # 新建/配置账户
│   └── SimReplay.tsx           # 离线回放视图
├── components/
│   └── CodeEditor.tsx          # 封装 @uiw/react-codemirror（python 高亮）
└── api.ts                       # /api/quant/* 请求封装
```

- **菜单**：在 `Layout.tsx` 增加两项：`/quant-backtest` 标签「量化回测」、`/quant-sim` 标签「量化模拟盘」（放在现有「回测」「监控中心」附近）。
- **量化回测页**：
  - 左侧策略列表（CRUD，点开 `StrategyEditorDialog` 用 CodeMirror 编写聚宽 Python）。
  - 运行表单：标的池（文本/多选）、起止日期、频率（daily / 1m）、手续费、滑点、本金、数据源优先级。
  - 提交后**短轮询**（1–2s）`/status`+`/equity`+`/logs`+`/trades` 实时刷新进度与收益/日志/成交；结果视图：净值曲线 + 基准、回撤、月度收益（ECharts）、成交明细表、CSV 导出按钮。复用 `pages/backtest/charts` 已有图表组件。
- **量化模拟盘页**：
  - 账户列表：新建/启动/暂停/重置（调用 `/sim/accounts` 与 `<id>/{start,pause,reset}`）。
  - 实时面板：**短轮询**（3–5s）`/status`+`/equity`+`/trades`，实时刷新净值、现金、持仓、止损日志。
  - 离线回放：选聚宽策略 + 区间 → 复用量化回测的结果展示组件。
 - **代码编辑器**：新增依赖 `@uiw/react-codemirror` + `@codemirror/lang-python`，封装为 `frontend/src/quant/components/CodeEditor.tsx`（比 Monaco 体积小，契合项目轻量取向）。

## 5. 数据流

1. **回测（独立进程）**：前端提交聚宽策略源码 + 参数 → `POST /api/quant/backtest/run` → FastAPI `service.py` 写 `backtest_runs`(status=queued, params_json) 到 `quant.db` → **派生子进程** `run_quant_backtest.py <run_id>` → 进程内 `rqalpha_bridge` 经 `QuantDataProvider`（TickFlow 本地优先，失败降级多源）取数 → 落地 bundle → `rqalpha.run()` → 指标/净值/成交/日志**全写入 `quant.db`**（status 置 done/failed）→ 前端每 1–2s 轮询 `/status`+`/equity`+`/logs`+`/trades` 实时展示。
2. **实时盘（独立进程）**：前端 `POST /sim/accounts/<id>/start` → FastAPI 派生子进程 `run_quant_sim.py <id>`（或用户 pm2/nohup 守护）→ 进程交易时段每分钟经 `QuantDataProvider.get_minute` 取价 → `matcher` 止损 → **实时净值/持仓/成交/止损日志写入 `quant.db`** 并周期写 `sim_equity_snapshots` → 前端每 3–5s 轮询 `/status`+`/equity`+`/trades` 实时展示。`pause`/`reset` 经 FastAPI 写控制标记/清库表。
3. **离线回放**：前端选策略+区间 → 复用回测独立进程路径（`replay.py` 调 `rqalpha_bridge`）→ 结果同样落 `quant.db` → 同回测结果视图。

> 关键点：FastAPI **不**跑 rqalpha、**不**嵌循环、**不**直接持有结果；重活在独立进程，结果唯一落在 `data/quant.db`。前端只读 API（读库）轮询更新。

## 6. 错误处理与降级
- 单数据源失败：自动降级下一级；全失败返回明确错误（绝不造伪数据，沿用 quant-daydayup 约定）。
- rqalpha 未安装（`uv sync --extra quant` 未执行）：API 检测后返回友好提示，类比现有 `VectorbtUnavailable`。
- 回测子进程崩溃：status 保持 running/failed，前端轮询可见；重跑即可，`quant.db` 旧行可清理。
- 实时盘进程崩溃：由 `quant.db` 中已落盘的 `sim_equity_snapshots`/成交恢复，不丢历史净值；重启子进程续跑。
- 聚宽 API 未实现子集：运行时明确报错（沿用 quant-daydayup 的 jq 兼容层思路，但 rqalpha 原生支持大部分聚宽 API）。

## 7. 测试
- 后端 `backend/tests/quant/`：
  - `test_matcher.py`：止损巡检逻辑（固定价/盈亏触发/费用扣除）。
  - `test_datasource.py`：`QuantDataProvider` 降级（用 stub 源验证优先级与失败切换）；`tickflow_src` 用本地 enriched parquet 跑通。
  - `test_db.py`：`quant.db` 建表 + 读写（runs/equity/trades/logs/sim 快照与成交）。
  - `test_rqalpha_bridge.py`：用内置迷你 CSV bundle（离线、无网络）跑一个简单聚宽策略，校验指标与成交回收，并验证结果落 `quant.db`。
  - 需 token/网络的用例用 `pytest.mark.skipif` 保护（`TUSHARE_TOKEN` 缺失或 `QUANT_OFFLINE` 时跳过）。
- 运行方式：`cd backend && uv run --extra dev --extra quant pytest tests/quant/`（dev extra 提供 pytest，quant extra 提供 rqalpha 等）。
- 前端：无测试脚本（沿用现状），手动验证。

## 8. 实施里程碑（建议顺序）
1. 后端 vendored 数据源 + `QuantDataProvider` + `tickflow_src` 适配器，单测降级。
2. `quant/db.py` 独立 SQLite 库（建表 + 读写封装）。
3. `rqalpha_bridge` + 聚宽策略 store + `run_quant_backtest.py` 回测独立进程 + 回测 API（提交派生子进程 / 读库）。
4. 模拟盘 `matcher`/`protocol`/`runner`/`replay` + 账户 API + `run_quant_sim.py` 独立进程（落 `quant.db`）。
5. `pyproject` quant extra + `.env.example` + `.gitignore`（含 `data/quant.db`）。
6. 前端菜单 + 量化回测页（CodeMirror 编辑器 + 轮询实时刷新结果）。
7. 前端量化模拟盘页（账户 + 实时轮询 + 回放）。
8. 端到端联调（Free 模式 + 本地数据，无需 token 即可验证主链路：提交→子进程跑→DB 落库→前端轮询）。

## 9. 风险与注意
- rqalpha bundle 构建需覆盖 daily 与 1m 两种频率；1m 依赖多源（mootdx 真实分钟 / 日线合成）质量，需明确告知用户分钟回测的数据来源与局限。
- 实时盘为独立进程，FastAPI 与子进程**仅通过 `quant.db` 通信**（不共享内存/文件 JSON）；「暂停」语义需定义为写控制标记由进程读取，而非强杀。
- vendored 代码来自 MIT/Apache-2.0 仓库（a-stock-data、quant-daydayup），保留原始 license 头与出处注释。
- 不改动现有回测与 screener，避免回归。

## 10. 隔离与最小改动原则（重要）

原工程持续更新，用户需要频繁合并上游。因此本模块遵循「**新代码尽量单独成目录，对原文件只做最小必要改动**」：

- **后端**：全部新代码落在独立包 `backend/app/quant/`（含 vendored 的 `datasource/`），不改动 `backtest/`、`strategy/`、`services/`、`tickflow/` 任何现有文件。`tickflow_src.py` 仅**只读 import** 现有 `app.tickflow.repository` / `app.parquet`，不修改它们。
- **前端**：全部新页面/组件/API 封装落在独立目录 `frontend/src/quant/`，不改动现有 `pages/`、`components/`（除下面列出的挂载点）。
- **唯一允许的“挂载点”改动**（均为小增量，易 rebase）：
  1. `backend/pyproject.toml`：新增一个 `quant = [...]` extra 块（依赖声明，纯增量）。
  2. `backend/app/main.py`：`app.include_router(quant_router, prefix="/api/quant")` 一行挂载（增量）。
  3. `frontend/package.json`：新增 `@uiw/react-codemirror` + `@codemirror/lang-python` 两个依赖（增量）。
  4. `frontend/src/router.tsx`：新增 2 个 `lazy` 导入 + 2 个 `children` `<Route>` 指向 `quant/` 页面（增量，沿用现有 named-export 懒加载模式）。
  5. `frontend/src/components/Layout.tsx`：菜单数组新增 2 项（`量化回测` / `量化模拟盘`），单行插入，不改其它菜单结构。
  6. `.env.example`：新增量化相关变量（增量）。
  7. `.gitignore`：新增 `data/quant_*` 目录与 `data/quant.db`（增量）。
  8. `backend/scripts/run_quant_backtest.py` 与 `backend/scripts/run_quant_sim.py`：**全新文件**，不在原目录树内冲突。
 
> 上述均为「追加/新增」式改动，上游更新时冲突概率极低；即使冲突也只发生在单行附近，易于手动解决。所有业务逻辑、API 路由、策略存储、模拟盘进程、结果数据库均不侵入原文件。

## 11. 前端风格一致性（强约束）

新增的「量化回测」「量化模拟盘」页面**必须与现有页面视觉、交互、布局完全一致**。实现时严格复用既有设计语言与组件原语，禁止另起一套样式。

### 11.1 设计令牌（必须全部走 token，禁止硬编码色值）
- 颜色全部用 Tailwind 映射的 CSS 变量类（`frontend/src/index.css` + `tailwind.config.ts`）：
  `bg-base` / `bg-surface` / `bg-elevated` / `border-border` / `text-foreground` / `text-muted` / `text-accent` / `bg-accent` / `text-bull`（涨·红）/ `text-bear`（跌·绿）/ `text-warning` / `text-danger`。
- **暗色为默认**（`html.dark`），亮色由 `:root` 反转。新页面一律用上述令牌类，**不得写 `#hex` / `rgb()` 字面量**，否则切换主题会错位、与全局不一致。
- 涨/跌、盈亏一律用 `text-bull` / `text-bear`；数字用 `.num` / `.tabular`（等宽 + `tabular-nums`），价格格式化复用 `@/lib/format` 的 `fmtPrice` / `fmtPct` / `priceColorClass`。

### 11.2 必须复用的既有组件/约定
- **页面外壳**：每个页面顶部用 `PageHeader`（title + 可选 subtitle + `right` 放操作按钮 + `border-b border-border`），与现有页一致。
- **对话框**：策略编辑、账户配置等弹窗一律用共享 `Modal` 原语（已处理 ESC / 焦点陷阱 / 遮罩点击关闭 / 无障碍），**不要自制 dialog**。
- **卡片容器**：统一 `rounded-card border border-border bg-surface`（与现有卡片一致），层次用 `bg-elevated` 区分。
- **空/加载/错误态**：复用 `EmptyState`；轻提示用 `Toast` / `AlertToast`。
- **日期选择**：复用 `DatePicker`，不要引入新日期库。
- **图表**：净值/回撤/月度收益等复用现有 ECharts 组件（`pages/backtest/charts/` 下的 `StrategyNavChart` / `ReturnDistributionChart` 等）与 `EChartsCandlestick` / `StockDailyKChart`；配色走 token，跟随暗/亮主题。K 线涨跌色用 `--bull` / `--bear`。
- **表格**：复用现有 `stock-table` 的表头/单元格样式（`text-muted` 表头、行 hover、`border-border` 分隔），不另写表格样式。
- **表单控件**：输入框/下拉样式对齐现有（参考 `pages/settings/DataSourceEditor.tsx`）：
  `w-full h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground placeholder:text-muted/30 focus:outline-none focus:ring-2 focus:ring-accent/40 transition-shadow`。
- **动画**：如需要入场/列表动画，用项目已有的 `framer-motion`（`motion` / `AnimatePresence`），风格对齐 `pages/backtest/StrategyBacktest.tsx`。

### 11.3 工程约定（与现有保持一致）
- **路由**：在 `frontend/src/router.tsx` 用现有 `lazy(() => import('./pages/X').then(m => ({ default: m.X })))` 命名导出懒加载模式新增 2 条 `children` 路由（`quant-backtest` / `quant-sim`），保持代码分割、不增大首屏 bundle。
- **数据请求**：统一用 `@tanstack/react-query`（参考 `useQuery` + `QK` query keys from `@/lib/queryKeys`），API 封装放 `frontend/src/quant/api.ts`，函数签名/返回类型对齐 `@/lib/api` 的既有风格。
- **菜单**：在 `Layout.tsx` 菜单数组追加 2 项，图标从 `lucide-react` 取（与现有菜单一致），不新增图标库。
- **代码编辑器（CodeMirror）**：`@uiw/react-codemirror` + `@codemirror/lang-python` 必须做**主题跟随**——根据 `html.dark` 切换 light/dark 编辑器主题，否则会与页面主题冲突。
- 复用既有 lib 工具：`@/lib/cn`（className 合并）、`@/lib/format`、`@/lib/storage`、`@/lib/useSharedQueries`（`useCapabilities` / `useDataStatus`）、`@/lib/board` 等，不要重复造轮子。

## 12. 目录落地总览（新增 vs 改动）

```
新增目录：
   backend/app/quant/            # 全部后端新逻辑（见 §3，含 db.py / rqalpha_bridge / simulate / datasource）
   backend/scripts/run_quant_backtest.py  # 回测独立进程（全新文件）
   backend/scripts/run_quant_sim.py       # 模拟盘独立进程（全新文件）
   frontend/src/quant/          # 全部前端新页面/组件/API（见 §4）
   data/quant_strategies/       # 用户聚宽策略（gitignore）
   data/quant_bundle/           # rqalpha bundle 缓存（gitignore）
   data/quant.db                # 独立结果库 SQLite（gitignore，见 §3.6）

 改动现有文件（均最小增量，见 §10）：
   backend/pyproject.toml       +quant extra
   backend/app/main.py           +1 行 include_router
   frontend/package.json        +2 依赖
    frontend/src/router.tsx        +2 lazy 导入 + 2 Route
    frontend/src/components/Layout.tsx  +2 菜单项
   .env.example                 +量化变量
   .gitignore                   +data/quant_* 与 data/quant.db
```
