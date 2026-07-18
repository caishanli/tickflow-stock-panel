# 量化回测页 - 单屏布局重构

**日期**：2026-07-18
**范围**：`frontend/src/quant/pages/QuantBacktest.tsx`（仅前端，无后端改动）
**目标**：将当前多 tab (编辑/详情/回测列表) 切换页合并为单屏工作台，参数提到顶栏，左编辑器/右详情实时联动。

## 目标 & 非目标

**目标**
- 顶栏一行放常用回测参数（标的池、start、end、初始金额、频率），高级参数（fee/slippage）折叠到 popover。
- 主体单屏 grid：左满高 Python 编辑器（已有 CodeMirror 语法高亮），右三层弹性布局。
- 右侧上：8 项指标卡；中：收益/基准曲线；下：三 tab (日志/错误/交易)。
- 历史回测 run 从顶部 tab 移到 header 抽屉/popover。
- SSE + react-query 保持不变，编译回测/真实回测期间指标、曲线、日志、交易实时刷新。

**非目标**
- 不改后端。error tab 内容前端根据 log level 过滤即可。
- 不做 metric 定义变更（沿用 `pickMetrics` + backend 现有字段）。
- 不拆分多个文件（选 A 方案）。

## 需求要点

- 顶栏字段（左到右）：`← 列表` | 策略名 input | 标的池 | start | end | 初始金额 | 频率 | ⚙高级 popover(fee, slippage, 保存策略) | `编译运行` | `开始回测` | 状态徽章 | 实时开关 | `历史 ▾` popover
- 主体：`grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_28rem]`
  - **左**：`flex flex-col`，`CodeEditor` 高度 `flex-1`（改 CodeEditor 支持 `height="100%"`），去掉 CodeMirror 内置 360px 硬编码
  - **右**：`flex flex-col overflow-hidden`，三段：
    - 顶：`grid grid-cols-4 gap-2` 8 张 `MetricCard`（收益率、年化、夏普、最大回撤、超额收益、胜率、盈亏比、交易次数）
    - 中：`flex-1 min-h-[220px]` 收益曲线（`EquityChart` 现有）
    - 下：`h-[220px] flex flex-col` tab 区（日志/错误/交易），tab 内容 `flex-1 overflow-auto`

## metric 字段映射

| 卡片 | 数据源 | 计算 |
|---|---|---|
| 收益率 | `metrics.total_return` | 直接 fmtPct |
| 年化 | `metrics.annualized` (前端 `pickMetrics` 已提取 annualized/annual_return) | fmtPct |
| 夏普 | `metrics.sharpe` | fmtNum |
| 最大回撤 | `metrics.max_drawdown` | fmtPct(-x) |
| **超额收益** | 前端从 `equity` 尾行计算 | `last.value/first.value - last.benchmark/first.benchmark`，无 benchmark 显示 — |
| **胜率** | `metrics.win_rate` | fmtPct |
| **盈亏比** | `metrics.profit_loss_ratio` | fmtNum |
| **交易次数** | `metrics.trade_count` 或 `trades.length` | 整数 |

需扩展 `frontend/src/quant/metrics.ts` 的 `pickMetrics`，返回增加 `win_rate / profit_loss_ratio / trade_count`（如未存在保持向前兼容返回 `null`）。

## 错误 tab 过滤规则

日志已有 SSE `onLog`。错误 tab = 前端对 `logs.data` 数组用一次过滤：
- `l.level` 若存在，取 `ERROR / CRITICAL`
- 否则字符串包含 `error / exception / traceback / 错误` 的行归入错误 tab
错误 tab 有新条目时在 tab 标签旁显示 `● 红点`。

## 历史 popover 交互

- header 尾部按钮 `历史(3)` 显示当前策略 run 数量；点击弹出下拉表（复用 `Modal` 简化为绝对定位 popover 或复用现有 `Dropdown` 组件；如无则新加内嵌 div，`onBlur` 关闭）
- 点击历史一行：`setSelRunId(id)`，右侧详情立即刷新（`runId` 依赖切换，react-query 自动重取）
- 默认 `selRunId=null`，展示 `liveRunId`；选中历史后可通过 `清除` 按钮回到 live

## 数据流 & 实时刷新

保持现有：
- `runBacktest` mutation 拿到 `run_id`，`setLiveRunId`
- 5 个 react-query：status / equity / trades / logs / runs（各 2s 轮询兜底）
- SSE 主线：`openBacktestStream(runId, {onEquity/onTrade/onLog/onStatus})` 触发 `invalidateQueries`
- 单屏无 tab 切换，因此不再需要「编辑 → 详情」的 `setTab('detail')`；status 徽章直接在 header 常驻

## 布局伪 HTML

```
<div flex flex-col h-full>
  <header px-4 py-3 border-b flex flex-wrap gap-2>
    ← 列表  [策略名]  [标的池]  [start]  [end]  [初始金额]  [频率▾]
    ⚙高级  编译运行  开始回测  status  实时  历史(N)▾
  </header>
  <div flex-1 grid grid-cols-[1fr_28rem] overflow-hidden>
    <div p-3 flex flex-col overflow-hidden>          <!-- 编辑器 -->
      <SECTION_TITLE>策略代码 (Python)</SECTION_TITLE>
      <div flex-1 min-h-0 rounded-card border overflow-hidden>
        <CodeEditor height="100%" />
      </div>
    </div>
    <div border-l flex flex-col overflow-hidden>     <!-- 右三层 -->
      <div p-3 grid grid-cols-4 gap-2>...8 metric cards...</div>
      <div flex-1 min-h-[220px] mx-3 rounded-card border><EquityChart /></div>
      <div h-[220px] mt-3 mx-3 mb-3 rounded-card border flex flex-col>
        <tabs 日志 / 错误● / 交易>
        <div flex-1 overflow-auto>...</div>
      </div>
    </div>
  </div>
</div>
```

## 关键实现改动清单

1. **CodeEditor**：删掉 `height="360px"`，加 `height` 可选 prop，默认 `100%`，容器控制高。
2. **QuantBacktest.tsx / StrategyEditor**：
   - 删除 `tab` state + tab 切换分支
   - 删除 `runs` 顶部 tab；数据 query 仍保留供 header popover 用
   - 新增 header popover（`高级参数` / `历史`）
   - 新增 `MetricCard × 8` 布局 + 超额收益本地计算
   - 新增下方 tab: `日志 / 错误 / 交易`，`errorLogs` = `logs.filter(isError)`
3. **metrics.ts**：`pickMetrics` 返回补充 `win_rate / profit_loss_ratio / trade_count`
4. **删除** `frontend/src/quant/pages/QuantBacktest.tsx` 里 `TabBtn` 顶栏用法可保留（下方 tabs 复用）

## 边界 & 错误处理

- 无 `runId` 时：metric 全部 `—`，曲线区显示「运行回测后展示实时曲线」，tab 内容显示「暂无 xx」
- `equity` 只有 `value` 无 `benchmark`：超额收益显示 `—`
- 高级 popover 关闭时保存到 form state，运行时读取
- CodeMirror `height="100%"` 需要外层容器给定明确高度（`min-h-0` + `flex-1`）

## 测试

前端无测试脚本，人工验证：
- `pnpm lint` 通过、`pnpm build` 通过
- 页面加载：新建策略进入 → 单屏、参数在 header
- 编译运行：曲线/日志/交易实时刷新
- 历史 popover 切换 run：右侧数据切换
- 错误 tab 过滤：写一个抛异常的策略，运行后错误 tab 有内容 + 红点

## 风险

- CodeMirror 100% 高度：需外层 flex 明确高度，否则塌陷。fallback：`calc(100vh - Xpx)`。
- header 换行：字段过多时窄屏可能换到两行，已用 `flex-wrap`，不阻塞。
- 超额收益：需要 `equity` 数据 benchmark 非空；老回测记录可能没有，显示 `—` 而非报错。
