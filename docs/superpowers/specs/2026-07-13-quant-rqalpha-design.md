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
| 实时盘进程模型 | **独立进程**（类 quant-daydayup 的 pm2/nohup 守护），非 APScheduler 内置 job |
| 策略代码编辑器 | **CodeMirror**（@uiw/react-codemirror，轻量） |

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
│   ├── protocol.py       # 账户状态读写（data/quant_sim/<id>.json）
│   ├── runner.py         # 实时盘主循环（独立进程入口逻辑）
│   └── replay.py        # 离线回放（复用 rqalpha_bridge 跑历史）
├── db.py                 # 模拟盘成交/止损账本（DuckDB / SQLite）
├── service.py            # 编排：回测提交、账户管理、结果落盘
└── config.py             # QUANT_DATA_PRIORITY / TUSHARE_TOKEN / FEE / SLIPPAGE / STOP_LOSS
```

### 3.1 数据源 `datasource/`
- 直接 vendored 自 `/home/ubuntu/quant-daydayup/backend/app/datasource/` 的各文件，接口保持一致（`get_daily`/`get_minute`/`get_index_realtime`/`get_etf_list`/`get_stock_list`）。
- 新增 `tickflow_src.py`：实现 `DataSource` 接口，内部通过现有 `app.tickflow.repository.KlineRepository` 与 `app.parquet.scan_enriched_parquet` 读取本地 `kline_daily_enriched`、`kline_minute`、`kline_etf_*` 等。
- `QuantDataProvider`（在 `manager.py` 中）持有一个有序源列表，按配置 `QUANT_DATA_PRIORITY`（如 `tickflow,tushare,mootdx,astock`）依次尝试，单源失败/超时/无数据自动降级；本地 Parquet 缓存避免重复请求。
- `astock_skill.py` 直接 vendor，运行时仅依赖 `requests`。

### 3.2 RQAlpha 桥接 `rqalpha_bridge.py`
- 实现 rqalpha 的 `AbstractDataSource`/`AbstractCalendar`，内部调用 `QuantDataProvider` 取数。
- 回测运行流程：
  1. 解析运行参数（标的池、区间、频率 daily/1m、手续费、滑点、本金、数据源优先级）。
  2. 将所需标的的 daily（及 1m，若频率=1m）落地为 rqalpha bundle 到 `data/quant_bundle/<run_id>/`（带缓存，按标的+区间命中复用）。
  3. 用 `rqalpha.run()` 运行用户聚宽式策略源码（经 `str(config)` 注入参数）。
  4. 回收 `portfolio`、成交明细、持仓、基准曲线 → 归一化为指标（总收益、年化、夏普、最大回撤、胜率）+ 净值/回撤序列。
- 结果持久化到 `data/backtest_results/quant_<run_id>.parquet`（沿用现有 backtest_results 目录约定）。

### 3.3 策略管理 `strategies/store.py`
- 聚宽 `.py` 策略文件落 `data/quant_strategies/`（加入 `.gitignore`）。
- CRUD：列表/读取/保存/删除/导出/导入。内置样例从 quant-daydayup `strategy/` 迁移（如 `wufu_etf_rotation.py`）。

### 3.4 模拟盘 `simulate/`
- **实时盘**（独立进程）：
  - 入口 `backend/scripts/run_quant_sim.py <account_id>`，由用户用 `nohup`/`pm2` 守护（类 quant-daydayup 的 `pm2 start backend/scripts/run_simulate.py --name sim-1 -- 1`）。
  - 主循环（`runner.py`）：交易时段（9:30-11:30, 13:00-15:00）每分钟取持仓最新价（`QuantDataProvider.get_minute` + 实时接口），调用 `matcher.step()` 做止损巡检，写状态。
  - 状态读写（`protocol.py`）：账户实时状态 JSON 落 `data/quant_sim/<account_id>.json`（净值/现金/持仓/止损日志/盈亏）。成交与止损事件追加写入 DuckDB/SQLite 账本（`db.py`）。
  - 非交易时段休眠；进程崩溃可由持久化 JSON 恢复，不丢历史净值。
- **离线回放**（`replay.py`）：选聚宽策略 + 区间，复用 `rqalpha_bridge` 跑历史，产出与回测一致的分析视图（净值/回撤/成交）。
- 前端「模拟盘」页通过 FastAPI 读 `data/quant_sim/*.json` + 账本展示，并下发 启动/暂停/重置 指令（启动=生成启动命令/触发独立进程；暂停/重置=写控制标记或清状态）。

### 3.5 API `api/quant.py`（FastAPI Router, prefix `/api/quant`）
| 分组 | 方法 | 路径 | 说明 |
|------|------|------|------|
| 策略 | GET/POST | `/strategies` | 列表 / 新建 |
| 策略 | GET/PUT/DELETE | `/strategies/<id>` | 读取/保存/删除 |
| 策略 | GET | `/strategies/<id>/export` | 导出 .py |
| 策略 | POST | `/strategies/import` | 导入 .py |
| 回测 | POST | `/backtest/run` | 提交回测（策略源码+参数），返回 run_id |
| 回测 | GET | `/backtest/<id>/status` | 状态 + 指标 |
| 回测 | GET | `/backtest/<id>/equity` | 净值 + 基准曲线 |
| 回测 | GET | `/backtest/<id>/trades` | 成交明细 |
| 回测 | GET | `/backtest/<id>/trades.csv` | 成交导出 |
| 回测 | POST | `/backtest/<id>/terminate` | 终止 |
| 回测 | DELETE | `/backtest/<id>` | 删除 |
| 模拟盘 | GET/POST | `/sim/accounts` | 账户列表 / 新建 |
| 模拟盘 | POST | `/sim/accounts/<id>/{start,pause,reset}` | 启动/暂停/重置 |
| 模拟盘 | GET | `/sim/accounts/<id>/status` | 实时状态 + 账本 |
| 数据源 | GET | `/datasource` | 当前优先级 |
| 数据源 | POST | `/datasource/priority` | 保存优先级 |
| 数据源 | POST | `/datasource/token` | 保存 Tushare Token |
| 数据源 | POST | `/datasource/verify` | 连通性校验 |

- 统一响应包络沿用现有约定（成功 `{data:...}`，失败抛 `HTTPException`）。
- 回测运行可同步或后台线程执行（rqalpha 单次回测通常在秒~分钟级）；若需长任务，参考现有 `backtest.py` 的 SSE/队列模式，但首版以「提交→轮询 status」为主，避免过度设计。

### 3.6 配置与依赖
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
  `astock_skill.py` 仅依赖 `requests`，直接 vendor。
- 运行需 `uv sync --extra quant`（类比现有 `uv sync --extra backtest`）。
- 配置项（`quant/config.py` 读 `.env`）：`QUANT_DATA_PRIORITY`、`TUSHARE_TOKEN`、`QUANT_FEE_RATE`、`QUANT_SLIPPAGE`、`QUANT_DEFAULT_STOP_LOSS`、`QUANT_RUNTIME_DIR=data/quant_sim`。
- `.env.example` 增加上述变量；`.gitignore` 增加 `data/quant_strategies/`、`data/quant_bundle/`、`data/quant_sim/`。

## 4. 前端架构

新增路由与菜单，复用现有 `Layout.tsx` 菜单模式与 `pages/backtest/charts` 图表组件（ECharts / Lightweight Charts）、Tanstack Query、Tailwind。

```
frontend/src/
├── pages/
│   ├── quant-backtest/
│   │   ├── QuantBacktest.tsx        # 主页面：策略列表 + 运行 + 结果
│   │   ├── StrategyEditorDialog.tsx  # CodeMirror 编辑聚宽 .py
│   │   └── BacktestResult.tsx        # 净值/回撤/成交表/CSV
│   └── quant-sim/
│       ├── QuantSim.tsx              # 账户列表 + 实时状态
│       ├── AccountDialog.tsx         # 新建/配置账户
│       └── SimReplay.tsx            # 离线回放
├── components/quant/
│   └── CodeEditor.tsx               # 封装 @uiw/react-codemirror（python 高亮）
└── lib/api/quant.ts                 # /api/quant/* 请求封装
```

- **菜单**：在 `Layout.tsx` 增加两项：`/quant-backtest` 标签「量化回测」、`/quant-sim` 标签「量化模拟盘」（放在现有「回测」「监控中心」附近）。
- **量化回测页**：
  - 左侧策略列表（CRUD，点开 `StrategyEditorDialog` 用 CodeMirror 编写聚宽 Python）。
  - 运行表单：标的池（文本/多选）、起止日期、频率（daily / 1m）、手续费、滑点、本金、数据源优先级。
  - 结果视图：净值曲线 + 基准、回撤、月度收益（ECharts）、成交明细表、CSV 导出按钮。复用 `pages/backtest/charts` 已有图表组件。
- **量化模拟盘页**：
  - 账户列表：新建/启动/暂停/重置（调用 `/sim/accounts` 与 `<id>/{start,pause,reset}`）。
  - 实时面板：净值、现金、持仓、止损日志（轮询 `/sim/accounts/<id>/status`）。
  - 离线回放：选聚宽策略 + 区间 → 复用量化回测的结果展示组件。
- **代码编辑器**：新增依赖 `@uiw/react-codemirror` + `@codemirror/lang-python`，封装为 `components/quant/CodeEditor.tsx`（比 Monaco 体积小，契合项目轻量取向）。

## 5. 数据流

1. **回测**：前端提交聚宽策略源码 + 参数 → `POST /api/quant/backtest/run` → `service.py` 写参 → `rqalpha_bridge` 经 `QuantDataProvider`（TickFlow 本地优先，失败降级多源）取数 → 落地 bundle → `rqalpha.run()` → 指标/序列落 `data/backtest_results/quant_<id>.parquet` → 前端轮询 `status`/`equity`/`trades` 展示。
2. **实时盘**：用户 `nohup/pm2` 起 `run_quant_sim.py <id>` → 独立进程每分钟经 `QuantDataProvider.get_minute` 取价 → `matcher` 止损 → 写 `data/quant_sim/<id>.json` + 账本 → 前端轮询 `status` 展示。FastAPI `start/pause/reset` 控制进程生命周期与状态文件。
3. **离线回放**：前端选策略+区间 → `replay.py` 复用 `rqalpha_bridge` 跑历史 → 同回测结果视图。

## 6. 错误处理与降级
- 单数据源失败：自动降级下一级；全失败返回明确错误（绝不造伪数据，沿用 quant-daydayup 约定）。
- rqalpha 未安装（`uv sync --extra quant` 未执行）：API 检测后返回友好提示，类比现有 `VectorbtUnavailable`。
- 实时盘进程崩溃：由 `data/quant_sim/<id>.json` 持久化状态恢复，不丢历史净值。
- 聚宽 API 未实现子集：运行时明确报错（沿用 quant-daydayup 的 jq 兼容层思路，但 rqalpha 原生支持大部分聚宽 API）。

## 7. 测试
- 后端 `backend/tests/quant/`：
  - `test_matcher.py`：止损巡检逻辑（固定价/盈亏触发/费用扣除）。
  - `test_datasource.py`：`QuantDataProvider` 降级（用 stub 源验证优先级与失败切换）；`tickflow_src` 用本地 enriched parquet 跑通。
  - `test_rqalpha_bridge.py`：用内置迷你 CSV bundle（离线、无网络）跑一个简单聚宽策略，校验指标与成交回收。
  - 需 token/网络的用例用 `pytest.mark.skipif` 保护（`TUSHARE_TOKEN` 缺失或 `QUANT_OFFLINE` 时跳过）。
- 运行方式：`cd backend && uv run --extra dev --extra quant pytest tests/quant/`（dev extra 提供 pytest，quant extra 提供 rqalpha 等）。
- 前端：无测试脚本（沿用现状），手动验证。

## 8. 实施里程碑（建议顺序）
1. 后端 vendored 数据源 + `QuantDataProvider` + `tickflow_src` 适配器，单测降级。
2. `rqalpha_bridge` + 聚宽策略 store + 回测 API + 离线回放。
3. 模拟盘 `matcher`/`protocol`/`runner`/`replay` + 账户 API + 独立进程脚本。
4. `pyproject` quant extra + `.env.example` + `.gitignore`。
5. 前端菜单 + 量化回测页（CodeMirror 编辑器 + 结果视图）。
6. 前端量化模拟盘页（账户 + 实时 + 回放）。
7. 端到端联调（Free 模式 + 本地数据，无需 token 即可验证主链路）。

## 9. 风险与注意
- rqalpha bundle 构建需覆盖 daily 与 1m 两种频率；1m 依赖多源（mootdx 真实分钟 / 日线合成）质量，需明确告知用户分钟回测的数据来源与局限。
- 实时盘为独立进程，FastAPI 仅通过文件/账本通信；「暂停」语义需定义为写控制标记由进程读取，而非强杀。
- vendored 代码来自 MIT/Apache-2.0 仓库（a-stock-data、quant-daydayup），保留原始 license 头与出处注释。
- 不改动现有回测与 screener，避免回归。
