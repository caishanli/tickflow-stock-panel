# 量化回测页面重构 v2（策略实体 + 标签页编辑器）设计

日期：2026-07-18

## 背景与目标

当前 `QuantBacktest.tsx` 以「回测 run」为列表单元（每 run 一行），且编辑页只有单一视图。用户要求改为**以策略(strategy)为聚合单元**：

- 顶层列表改为**策略列表**：每个策略一行，展示该策略**最后一次回测**的收益率/周期/回撤/夏普与回测次数。
- 同一策略可有多个回测区间/初始资金，因此列表显示「最后一次」情况。
- 进入策略编辑器后，右上角用**标签页**切换：**编辑策略 / 回测详情 / 回测列表**。
- 顶栏有「编译运行」按钮（短周期快速验证代码能否跑通）与「开始回测」按钮（按配置区间正式回测）。
- 策略代码编辑框占满左侧空白区域。
- 布局参照聚宽（策略列表 → 策略编辑页，编辑页左代码右参数、上方标签切换详情/列表）。

## 已确认的需求决策

1. **策略分组键**：独立策略实体 `strategy_id`（复用现有 `strategies` 表与 `/strategies` CRUD）。
2. **新建**：点「新建」创建一个策略实体（分配 `strategy_id` + 名称），进入编辑器默认在「编辑策略」标签。
3. **编译运行**：是**按钮**（非标签）。用短周期（默认取配置结束日前约 5 个交易日窗口，或固定如最近一段）快速跑一次，验证策略代码可编译/运行；跑通后再用「开始回测」跑长周期。
4. **编辑器标签**：**编辑策略 / 回测详情 / 回测列表** 三个标签（无独立「编译运行列表」标签）。
5. **顶层列表**：改为策略列表（不再平铺所有 run）。

## 数据模型与后端变更

### 现状
- `backtest_runs` 已有 `strategy_id TEXT`、`name TEXT`、`params_json`、`metrics_json`、`created_at`。
- `runBacktest` 的 `BacktestIn` 已有 `strategy_id: str = ""`（前端当前未传）。
- `strategies` 表（id/name/file/updated_at）+ `/strategies` CRUD 已存在，前端 `api.listStrategies/saveStrategy/getStrategy` 已存在。

### 需要新增/调整
1. **run 落库时写入 `strategy_id`**：`service.submit_backtest(params)` 已把 `params` 整包 `json.dumps` 进 `params_json`；需在 `db.insert_run` 时把 `params.get("strategy_id","")` 写入 `strategy_id` 列（当前 `insert_run` 第 2 参已是 `strategy_id`，但 `submit_backtest` 传的是 `params.get("strategy_id","")` —— 已正确，仅需前端传对值）。
2. **策略列表查询**：新增 `db.list_strategies_with_latest()`（或在 API 层组装）：
   - 取所有 strategy（`strategies` 表）；
   - 对每个 strategy，取 `backtest_runs` 中 `strategy_id = sid` 按 `created_at DESC` 第一条（最新 run）；
   - 返回 `{ id, name, run_count, latest: { period(start~end), metrics_json, status, run_id } }`。
   - SQL 示例：`SELECT * FROM backtest_runs WHERE strategy_id=? ORDER BY created_at DESC LIMIT 1`；`run_count` 用 `SELECT COUNT(*) FROM backtest_runs WHERE strategy_id=?`。
3. **新增 API**：`GET /api/quant/strategies/backtests`（或复用 `/backtest/runs` 前端按 `strategy_id` 分组）。推荐新增 `GET /api/quant/strategies/with-latest` 返回上述聚合，避免前端大量请求。
4. **策略下的回测列表**：前端用现有 `/backtest/runs` 过滤 `strategy_id === sid` 即可（或新增 `GET /api/quant/backtest/runs?strategy_id=...`）。
5. **编译运行短周期**：前端「编译运行」按钮调用 `runBacktest` 时，`start/end` 自动改为短窗口（如 `end = 配置结束日`，`start = end 前 5 个交易日`≈ `end - 7 天`），其余参数同正式回测；后端无需改动。

## 前端页面结构（`QuantBacktest.tsx`）

状态：`pageView: 'list' | 'editor'`；`selStrategy: string | null`；编辑器内 `tab: 'edit' | 'detail' | 'runs'`。

### 顶层：策略列表（`StrategyList`）
- `PageHeader` 右上「新建」按钮 → 调 `saveStrategy(null, '未命名策略', '# 新策略\n')` 得到新 `strategy_id` → `pageView='editor'`、`selStrategy=id`、`tab='edit'`。
- 表格 `rounded-card` 包裹，列：**策略名称** / **最新回测周期** / **收益率** / **最大回撤** / **夏普** / **回测次数**。
  - 周期/指标取自该策略最新 run 的 `params_json` 与 `metrics_json`（经 `pickMetrics`）。
  - 回测次数 = `run_count`。
  - 无回测的策略显示「—」。
  - 行点击 → `pageView='editor'`、`selStrategy=id`、`tab='detail'` 若有最新 run 否则 `'edit'`。

### 编辑器（`StrategyEditor`）
- **顶栏**（flex 一行，`rounded-card`/border 容器，`flex-wrap`）：
  - 「返回列表」按钮（ArrowLeft）
  - 策略名称（输入框，绑定 strategy name；失焦/点保存时 `saveStrategy(id, name, code)`）
  - 「编译运行」按钮（Play 图标）：用短周期跑一次（见后端 #5），跑后切到「回测详情」显示该 run。
  - 「开始回测」按钮（accent 主按钮）：按配置周期跑。
  - 状态徽标（latest run status，沿用 `statusTone`）。
  - 右侧：**标签栏**「编辑策略 / 回测详情 / 回测列表」（`TabBtn` 风格）。
- **主区**（按 `tab` 渲染）：
  - `edit`（编辑策略）：
    - 布局 `grid grid-cols-1 lg:grid-cols-[1fr_20rem]`（**左侧代码占满空白**，`min-h` 撑满；右侧 20rem 参数/操作列）。
    - 左：`CodeEditor`（占满高度，`rounded-card border` 包裹）。
    - 右：参数卡片（回测周期 DatePicker×2、初始金额 input、频率/手续费/滑点等可折叠或精简）、「保存策略」按钮。
    - 代码改动后点「开始回测」/「编译运行」时自动 `saveStrategy` 最新 code+name。
  - `detail`（回测详情）：
    - 顶部：该策略「最新 run」或当前选中 run 的状态 + 指标卡片一排（收益率/年化/夏普/最大回撤，常驻）。
    - 策略/基准双序列曲线（`EquityChart`）。
    - 下方 Tab：**日志 / 交易记录**（沿用 `LogList`/`TradeTable`）。
    - 顶部可选「选择回测」下拉/列表以切换看哪个 run（默认最新）。
    - 删除按钮（Modal 确认，删除该 run）。CSV 导出。
  - `runs`（回测列表）：
    - 该策略下所有 run 的表格（周期/收益率/回撤/夏普/状态/操作），点击某行 → 切到 `detail` 并加载该 run；含「删除」。

### 实时 SSE
- 沿用 `openBacktestStream` + react-query 增量；「编译运行」与「开始回测」提交后都进入实时态（detail 标签自动展示曲线增长、日志/交易实时追加）。
- 编译运行若失败（代码报错），状态变 `failed`，detail 显示错误日志，便于用户改代码重跑。

## 复用与令牌
- 组件：`PageHeader`、`Modal`、`DatePicker`、`CodeEditor`、`EquityChart`、`metrics.ts`(`pickMetrics/fmtPct/fmtNum/tone`)。
- 设计令牌：`rounded-card`/`rounded-btn`/`bg-surface`/`bg-base`/`border-border`/`text-muted`/`text-foreground`/`text-accent`/`text-bull`/`text-bear`/`INPUT_CLS`。
- 图标：`Plus, ArrowLeft, Play, Square, Download, Trash2, FileCode2, ListChecks, Activity` 等。

## 不做（YAGNI）
- 不引入全新的策略管理页（复用 `/strategies` 与现有 CRUD）。
- 不做策略的导入/导出 UI（API 已存在，按需后补）。
- 不新增独立「编译运行列表」标签（编译运行是按钮）。

## 验证
- 后端：`/api/quant/strategies/with-latest` 返回每策略最新 run 指标与次数；run 落 `strategy_id` 正确。
- 前端 `npx tsc -b` 通过。
- 手动：新建策略→写代码→编译运行(短周期)看是否报错→开始回测(长周期)→列表显示该策略最新指标与次数→点进编辑器可切 编辑/详情/列表 三标签，代码框占满左侧。
