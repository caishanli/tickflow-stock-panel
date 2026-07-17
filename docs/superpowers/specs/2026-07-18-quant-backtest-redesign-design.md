# 量化回测页面重构设计 (QuantBacktest Redesign)

日期：2026-07-18

## 背景与目标

当前 `frontend/src/quant/pages/QuantBacktest.tsx` 是一个朴素的三栏 grid（列表 / 新建 / 实时详情），使用 `rounded-lg` + `ring-1 ring-border/40` 等偏离系统设计的样式，且列表缺少用户期望的指标列（序号/名称/周期/收益率/最大回撤/夏普）。

目标：重构为符合系统既有设计语言（`rounded-card` / `rounded-btn` / `bg-surface` / `border-border` / `text-bull` / `text-bear`，参考 `frontend/src/pages/backtest/StrategyBacktest.tsx` 与 `FactorBacktest.tsx`）的页面，采用「列表视图 → 新建/编辑视图」单页状态切换，并按用户确认的布局实现。

## 已确认的需求决策

1. **名称来源**：新增「策略名称」输入框。后端 `backtest_runs` 表新增 `name` 字段并存储。
2. **编辑行为**：点列表项进入编辑页，可改名称/参数/代码；「开始回测」后生成一条**新**回测记录（不覆写原记录）。
3. **右侧下半区**：收益指标卡片常驻显示（顶部一排），下方用 Tab 在「日志 / 交易记录」之间切换。
4. **导航方式**：单页状态切换（不新增路由），保持 `QuantBacktest` 单文件内 `view: 'list' | 'new' | 'edit'` 状态。

## 数据模型变更（后端）

### `backtest_runs` 表新增 `name` 列
- `backend/app/quant/db.py`：
  - `_SCHEMA` 的 `backtest_runs` DDL 增加 `name TEXT`。
  - 提供 migration：对已有库执行 `ALTER TABLE backtest_runs ADD COLUMN name TEXT`（在 `init_db` 中 `executescript` 后检测列是否存在并 ALTER，或用 `PRAGMA table_info` 判断）。
  - `insert_run(run_id, strategy_id, name, params_json, status)` 与 `upsert_run(...)` 签名增加 `name` 参数并写入。
  - `update_run` 增加 `name` 可选参数（编辑后重跑时更新名称，可选；本次以提交时写入为主）。
  - `list_runs` / `get_run` 返回包含 `name`（dict 已含全部列，自动包含）。

### API (`backend/app/quant/api/quant.py`)
- `BacktestIn` 新增 `name: str = ""`。
- `run_backtest`：`params` 已含 `name`（model_dump 自带），`submit_backtest(params)` 内部 `db.insert_run(... name=params.get("name", ""))`。
- `get_run` / `list_runs` 已返回全列，前端直接读 `name`。

### `service.submit_backtest`
- `db.insert_run(run_id, params.get("strategy_id", ""), params.get("name", ""), json.dumps(params), "queued")`。

## 指标字段（前端读取约定）

两条回测引擎产出 metrics_json 的 key 略有差异，前端统一做兼容读取：
- rqalpha 路径：`total_return, annualized, sharpe, max_drawdown`
- jqengine 路径：`total_return, annual_return, max_drawdown, sharpe`

前端指标映射（统一函数 `pickMetrics`）：
- 收益率 = `total_return`
- 年化 = `annualized ?? annual_return`
- 夏普 = `sharpe`
- 最大回撤 = `max_drawdown`

列表列与指标卡片均使用上述映射。收益率/年化/夏普为正显示 `text-bull`（红涨），为负显示 `text-bear`（绿跌）。

## 前端页面结构（单文件 `QuantBacktest.tsx`）

状态：`view: 'list' | 'new' | 'edit'`；`selRun: string | null`（编辑时记录被克隆的源 run id）。

### 视图 1：列表视图 (`BacktestList`)
- 顶部 `PageHeader` 右侧（或列表卡片右上角）放「新建」按钮（`Plus` 图标，`bg-accent`）。
- `rounded-card border border-border bg-surface` 包裹的表格：
  - 列：序号（行号，从 1 起）/ 名称（`name`）/ 回测周期（`params_json.start ~ end`）/ 收益率 / 最大回撤 / 夏普比率。
  - 周期从 `params_json` 解析（含 symbols、frequency 可在名称旁或 tooltip 体现）。
  - 指标值格式化为百分比（收益率/回撤/年化 ×100，保留 2 位；夏普保留 2 位）。
  - 行 hover 高亮（`hover:bg-elevated/60`），点击 → `view='edit'`、`selRun=id` 并加载该 run 的 `params_json`（name/symbols/start/end/frequency/capital/fee/slippage）+ 策略代码（从 `params_json.strategy_code` 或另存字段；当前策略代码存于 `params_json.strategy_code`）。
  - 空状态：`EmptyState`。

### 视图 2/3：新建 / 编辑共享编辑器 (`BacktestEditor`)
- 进入 `new` 时表单为默认值（含空白名称、默认标的 `600000.XSHG`、默认周期空）；进入 `edit` 时表单由 `selRun` 的 `params_json` 填充。
- **顶栏**（`rounded-card border border-border bg-surface` 容器，`flex items-center gap-3 flex-wrap`）：
  - 策略名称输入框（`INPUT_CLS`，placeholder「策略名称」）
  - 「编译运行」开关（`Play`/`Square` 图标 toggle，控制 SSE 实时刷新是否开启；默认开）
  - 回测周期：`DatePicker`(start) + `DatePicker`(end)
  - 初始金额：`input number`（`capital`）
  - 「开始回测」主按钮（`bg-accent`，disabled 当无名称/无代码/无周期/提交中）
  - 左侧「返回列表」按钮（`ArrowLeft`，`view='list'`）
- **主区**：`grid grid-cols-1 lg:grid-cols-2 gap-4`（各占约 50% 宽）：
  - **左（50%）**：`CodeEditor` 策略代码区，包在 `rounded-card border border-border` 内，占满可用高度（`min-h` 或大高度）。
  - **右（50%）**：
    - 上：**收益指标卡片常驻**一排（收益率 / 年化 / 夏普 / 最大回撤 4 个小卡片，`grid grid-cols-2 sm:grid-cols-4`），数据来自当前 run 的 `metrics_json`（实时 SSE 刷新）。
    - 上：**基准 vs 策略收益曲线图** —— 复用 `BacktestResult` 的 ECharts 逻辑，改为双 series：策略 `value`（归一化净值）+ 基准 `benchmark`（归一化）。`benchmark` 为空时仅画策略曲线。曲线高度约 260–300px。
    - 下：**Tab 切换**（`rounded-btn` 标签栏）：「日志」(`BacktestResult` 日志列表) / 「交易记录」(`BacktestResult` 成交表格)。

### 实时 SSE（沿用现有）
- 提交「开始回测」后，`runMut.onSuccess` 设置 `selRun = run_id` 并保持当前视图（edit/new 切为以该 run 为源的实时态），通过 react-query 查询 + `openBacktestStream` 增量刷新 `equity/trades/logs/status`。
- status 徽标沿用 `statusTone`（done/failed/running/queued）。
- 结束后 `invalidate` 列表。

## 复用与令牌
- 组件：`PageHeader`、`EmptyState`、`DatePicker`、`Modal`(删除确认)、`CodeEditor`、`BacktestResult`(曲线/表格/日志逻辑，按需抽取或内联)。
- 设计令牌：`rounded-card` / `rounded-btn` / `bg-surface` / `bg-base` / `border-border` / `text-muted` / `text-foreground` / `text-accent` / `text-bull` / `text-bear` / `INPUT_CLS` 风格。
- 图标（lucide-react）：`Plus, ArrowLeft, Play, Square, History, Activity, ListChecks, Download, Trash2, FileCode2` 等。

## 不做（YAGNI）
- 不新增独立路由（保持单页状态切换）。
- 不修改已跑完回测的不可变结果；编辑=克隆再跑新记录。
- 不做策略保存/管理库（沿用 params_json 内联代码，后续可扩展）。

## 验证
- `npx tsc -b` 通过（前端）。
- 后端：已有 run 记录升级不报错（migration 兼容）；`/backtest/runs` 返回含 `name`；`/backtest/run` 接受 `name` 并落库。
- 前端 `:3011` 手动验证：列表显示名称/周期/三项指标；新建→编辑布局（顶栏+左代码50%+右指标/曲线/Tab）；提交后实时曲线增长、日志/交易实时追加；删除确认弹窗。
