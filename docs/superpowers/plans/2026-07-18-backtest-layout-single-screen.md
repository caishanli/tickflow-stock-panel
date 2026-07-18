# 量化回测页单屏布局重构 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把量化回测的编辑/详情/回测列表三 tab 合并为单屏工作台：header 一行放常用参数 + 高级折叠 + 历史抽屉；左满高 Python 编辑器，右三层（8 指标卡 / 收益基准曲线 / 日志·错误·交易 tab）。

**Architecture:** 仅前端改动，不动后端。改写 `QuantBacktest.tsx` 的 `StrategyEditor`，去掉 tab 切换；`CodeEditor` 支持 `height` prop 占满左栏；`metrics.ts` 的 `pickMetrics` 补 `win_rate/profit_loss_ratio/trade_count`；超额收益前端用 equity 表 benchmark 列末点计算。

**Tech Stack:** React 18 + Vite + TypeScript, Tailwind, CodeMirror (`@uiw/react-codemirror` + `@codemirror/lang-python`), echarts-for-react, TanStack Query, SSE。

## Global Constraints

- 仅改前端；不新增后端接口；不新增 npm 依赖。
- 超额收益前端本地算：`last.value/first.value - last.benchmark/first.benchmark`，benchmark 缺失显示 `—`。
- 错误 tab 前端过滤：`l.level` 为 `ERROR/CRITICAL`，或字符串含 `error/exception/traceback/错误`。
- 不拆分文件（方案 A）。
- 改动后：`pnpm lint` 通过、`pnpm build` 通过。
- 沿用现有 SSE (`openBacktestStream`) 与 5 个 react-query（status/equity/trades/logs/runs）。

---

### Task 1: CodeEditor 支持 height prop

**Files:**
- Modify: `frontend/src/quant/components/CodeEditor.tsx`

**Interfaces:**
- 改 `CodeEditor` 接口：新增可选 `height?: string | number`（默认 `100%`）；把硬编码 `height="360px"` 替换为该 prop。

- [ ] **Step 1: 编辑 CodeEditor，支持 height**

将 `frontend/src/quant/components/CodeEditor.tsx` 的 `CodeEditor` 改为：

```tsx
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { githubDark, githubLight } from '@uiw/codemirror-theme-github'
import { useTheme } from '@/lib/theme'

export function CodeEditor({ value, onChange, readOnly, height = '100%' }: {
  value: string
  onChange?: (v: string) => void
  readOnly?: boolean
  height?: string | number
}) {
  const theme = useTheme()
  const dark = theme === 'dark'
  return (
    <CodeMirror
      value={value}
      height={height}
      theme={dark ? githubDark : githubLight}
      extensions={[python()]}
      readOnly={readOnly}
      onChange={onChange}
      className="rounded-card border border-border overflow-hidden text-xs"
    />
  )
}
```

- [ ] **Step 2: 验证 lint**

Run: `cd frontend && pnpm lint`
Expected: 无错误（仅改了 prop，默认行为不变）。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/quant/components/CodeEditor.tsx
git commit -m "feat(quant): CodeEditor 支持 height prop，便于占满容器"
```

---

### Task 2: pickMetrics 补充胜率/盈亏比/交易次数

**Files:**
- Modify: `frontend/src/quant/metrics.ts`

**Interfaces:**
- `Metrics` 接口新增 `win_rate: number | null`、`profit_loss_ratio: number | null`、`trade_count: number | null`
- `pickMetrics(raw)` 返回这些值（取 `win_rate`/`profit_loss_ratio`/`trade_count`，缺失为 null）

- [ ] **Step 1: 编辑 metrics.ts**

在 `frontend/src/quant/metrics.ts` 中：

接口改为：

```ts
export interface Metrics {
  total_return: number | null
  annualized: number | null
  sharpe: number | null
  max_drawdown: number | null
  win_rate: number | null
  profit_loss_ratio: number | null
  trade_count: number | null
}
```

`pickMetrics` 返回改为：

```ts
  return {
    total_return: num(m.total_return),
    annualized: num(m.annualized ?? m.annual_return),
    sharpe: num(m.sharpe),
    max_drawdown: num(m.max_drawdown),
    win_rate: num(m.win_rate),
    profit_loss_ratio: num(m.profit_loss_ratio),
    trade_count: num(m.trade_count),
  }
```

- [ ] **Step 2: 验证 lint**

Run: `cd frontend && pnpm lint`
Expected: 无错误。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/quant/metrics.ts
git commit -m "feat(quant): pickMetrics 补充胜率/盈亏比/交易次数"
```

---

### Task 3: StrategyEditor 改为单屏布局（header + 左编辑器 + 右三层）

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx`

**Interfaces:**
- 删除 `tab` state 及其三个 tab 分支。
- 删除 `runMut.onSuccess` 里的 `setTab('detail')`。
- 新增 header 内联组件：`AdvParamsPopover`（fee/slippage + 保存策略）、`HistoryPopover`（基于 `runs` 列表选 run）。
- 新增 `excessReturn(equity)` 本地计算函数。
- 新增 `errorLogs(logs)` 过滤函数。
- 右侧新结构：8 张 `MetricCard` + `EquityChart` + 下 tab（日志/错误/交易）。
- `TabBtn` 复用于下方 tab。

- [ ] **Step 1: 替换 StrategyEditor（含新 header、布局、辅助函数）**

将 `frontend/src/quant/pages/QuantBacktest.tsx` 的 `StrategyEditor`、辅助函数替换为以下完整实现（保留文件顶部 import、`INPUT_CLS`、`SECTION_TITLE`、`statusTone`、`DEFAULT_FORM`、`FormState`、`QuantBacktest`、`StrategyList`、`shiftDays` 不变）：

```tsx
function StrategyEditor({ strategyId, onBack }: { strategyId: string; onBack: () => void }) {
  const qc = useQueryClient()
  const { data: strategy } = useQuery({ queryKey: ['quant', 'strategy', strategyId], queryFn: () => api.getStrategy(strategyId) })

  const [code, setCode] = useState<string>('')
  const [name, setName] = useState<string>('')
  const [form, setForm] = useState<FormState>(DEFAULT_FORM)
  const [liveRunId, setLiveRunId] = useState<string | null>(null)
  const [selRunId, setSelRunId] = useState<string | null>(null)
  const [liveOn, setLiveOn] = useState(true)
  const [logTab, setLogTab] = useState<'log' | 'error' | 'trade'>('log')
  const [advOpen, setAdvOpen] = useState(false)
  const [histOpen, setHistOpen] = useState(false)
  const [confirmDel, setConfirmDel] = useState(false)

  useEffect(() => {
    if (strategy?.data) {
      setName(strategy.data.name || '')
      setCode(strategy.data.code || '')
    }
  }, [strategy])

  const saveStrategy = () => api.saveStrategy(strategyId, name, code)

  const runMut = useMutation({
    mutationFn: (short: boolean) => {
      saveStrategy()
      const end = form.end
      const start = short && end ? shiftDays(end, -7) : form.start
      return api.runBacktest({
        name, strategy_id: strategyId, strategy_code: code,
        symbols: form.symbols.split(',').map(s => s.trim()).filter(Boolean),
        start, end, frequency: form.frequency,
        fee: +form.fee, slippage: +form.slippage, capital: +form.capital,
      })
    },
    onSuccess: (d: any) => { setLiveRunId(d.run_id); setSelRunId(d.run_id); qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] }) },
  })

  const runId = selRunId ?? liveRunId
  const { data: status } = useQuery({ queryKey: ['quant', 'bt', runId, 'status'], queryFn: () => api.getBacktestStatus(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: equity } = useQuery({ queryKey: ['quant', 'bt', runId, 'equity'], queryFn: () => api.getBacktestEquity(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: trades } = useQuery({ queryKey: ['quant', 'bt', runId, 'trades'], queryFn: () => api.getBacktestTrades(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: logs } = useQuery({ queryKey: ['quant', 'bt', runId, 'logs'], queryFn: () => api.getBacktestLogs(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: runs } = useQuery({ queryKey: ['quant', 'bt', 'runs', strategyId], queryFn: () => api.listBacktests(strategyId), refetchInterval: 3000 })

  useEffect(() => {
    if (!runId || !liveOn) return
    const es = openBacktestStream(runId, {
      onEquity: () => qc.invalidateQueries({ queryKey: ['quant', 'bt', runId, 'equity'] }),
      onTrade: () => qc.invalidateQueries({ queryKey: ['quant', 'bt', runId, 'trades'] }),
      onLog: () => qc.invalidateQueries({ queryKey: ['quant', 'bt', runId, 'logs'] }),
      onStatus: () => qc.invalidateQueries({ queryKey: ['quant', 'bt', runId, 'status'] }),
    })
    return () => { es.close() }
  }, [runId, liveOn, qc])

  const lastStatus = status?.data ? (status.data.state ?? status.data.status) : undefined
  const metrics = pickMetrics(status?.data?.metrics_json)
  const equityData: any[] = Array.isArray(equity?.data) ? equity.data : []
  const runList: any[] = Array.isArray(runs?.data) ? runs.data : []
  const excess = computeExcess(equityData)
  const errLogs = filterErrorLogs(Array.isArray(logs?.data) ? logs.data : [])
  const hasError = errLogs.length > 0

  return (
    <div className="flex flex-col h-full">
      <header className="px-4 py-3 border-b border-border flex items-center gap-2 flex-wrap">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border bg-base text-secondary hover:text-foreground transition-colors">
          <ArrowLeft className="h-4 w-4" />列表
        </button>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="策略名称"
          className="h-9 w-44 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground focus:outline-none focus:border-accent/50" />
        <input value={form.symbols} onChange={e => setForm({ ...form, symbols: e.target.value })} placeholder="标的池(逗号分隔)"
          className="h-9 flex-1 min-w-[12rem] rounded-btn bg-base border border-border px-2.5 text-xs text-foreground focus:outline-none focus:border-accent/50" />
        <DatePicker value={form.start} onChange={d => setForm({ ...form, start: d })} placeholder="开始" buttonClassName="h-9 justify-between" className="w-36" />
        <DatePicker value={form.end} onChange={d => setForm({ ...form, end: d })} placeholder="结束" buttonClassName="h-9 justify-between" className="w-36" />
        <input type="number" value={form.capital} onChange={e => setForm({ ...form, capital: +e.target.value })} placeholder="初始金额"
          className="h-9 w-28 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground focus:outline-none focus:border-accent/50" />
        <select value={form.frequency} onChange={e => setForm({ ...form, frequency: e.target.value })}
          className="h-9 w-24 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground focus:outline-none focus:border-accent/50">
          <option value="daily">daily</option>
          <option value="1m">1m</option>
        </select>
        <div className="relative">
          <button onClick={() => { setAdvOpen(o => !o); setHistOpen(false) }} title="高级参数"
            className={`inline-flex items-center gap-1 h-9 px-2.5 rounded-btn border border-border text-xs ${advOpen ? 'text-accent' : 'text-secondary hover:text-foreground'} transition-colors`}>
            <Settings2 className="h-3.5 w-3.5" />高级
          </button>
          {advOpen && (
            <div className="absolute z-20 right-0 top-11 w-64 rounded-card border border-border bg-surface p-3 space-y-2 shadow-xl">
              <div className="text-[10px] uppercase tracking-wide text-muted">手续费</div>
              <input type="number" step="0.0001" value={form.fee} onChange={e => setForm({ ...form, fee: +e.target.value })} className={INPUT_CLS} />
              <div className="text-[10px] uppercase tracking-wide text-muted">滑点</div>
              <input type="number" step="0.0001" value={form.slippage} onChange={e => setForm({ ...form, slippage: +e.target.value })} className={INPUT_CLS} />
              <button onClick={() => { saveStrategy(); setAdvOpen(false) }} className="w-full h-9 rounded-btn bg-elevated text-secondary text-xs hover:text-foreground transition-colors">保存策略</button>
            </div>
          )}
        </div>
        <button onClick={() => runMut.mutate(true)} disabled={!code || runMut.isPending}
          className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn border border-border text-xs text-secondary hover:text-foreground transition-colors disabled:opacity-50">
          <Play className="h-3.5 w-3.5" />编译运行
        </button>
        <button onClick={() => runMut.mutate(false)} disabled={!code || !form.start || !form.end || runMut.isPending}
          className="inline-flex items-center gap-1.5 h-9 px-4 rounded-btn bg-accent text-base text-xs font-medium disabled:opacity-50 hover:bg-accent/90 transition-colors">
          <Play className="h-4 w-4" />{runMut.isPending ? '提交中…' : '开始回测'}
        </button>
        {lastStatus && (
          <span className={`text-xs font-medium px-2 py-0.5 rounded-btn bg-base border border-border ${statusTone(lastStatus)}`}>{lastStatus}</span>
        )}
        {liveRunId && (
          <button onClick={() => setLiveOn(!liveOn)} title="实时刷新开关"
            className={`inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border text-xs transition-colors ${liveOn ? 'text-accent' : 'text-muted hover:text-foreground'}`}>
            <Activity className="h-3.5 w-3.5" />{liveOn ? '实时' : '暂停'}
          </button>
        )}
        <div className="relative ml-auto">
          <button onClick={() => { setHistOpen(o => !o); setAdvOpen(false) }} className="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border text-xs text-secondary hover:text-foreground transition-colors">
            <History className="h-3.5 w-3.5" />历史{runList.length > 0 ? `(${runList.length})` : ''}
          </button>
          {histOpen && (
            <div className="absolute z-20 right-0 top-11 w-72 rounded-card border border-border bg-surface p-2 shadow-xl max-h-80 overflow-auto">
              {runList.length === 0 && <div className="px-2 py-4 text-xs text-muted text-center">暂无回测</div>}
              {runList.map((r) => {
                const p = (() => { try { return JSON.parse(r.params_json || '{}') } catch { return {} } })()
                const m = pickMetrics(r.metrics_json)
                const active = (selRunId ?? liveRunId) === r.id
                return (
                  <button key={r.id} onClick={() => { setSelRunId(r.id); setHistOpen(false) }}
                    className={`w-full text-left px-2 py-1.5 rounded-btn text-xs flex items-center justify-between gap-2 ${active ? 'bg-elevated text-foreground' : 'text-secondary hover:text-foreground hover:bg-elevated/60'}`}>
                    <span className="num">{`${p.start ?? ''} ~ ${p.end ?? ''}`}</span>
                    <span className={`num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</span>
                  </button>
                )
              })}
              {selRunId && (
                <button onClick={() => { setSelRunId(null); setHistOpen(false) }} className="w-full mt-1 px-2 py-1.5 rounded-btn text-xs text-muted hover:text-foreground border-t border-border">回到当前回测</button>
              )}
            </div>
          )}
        </div>
      </header>

      <div className="flex-1 min-h-0 grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_28rem] overflow-hidden">
        <div className="p-3 flex flex-col overflow-hidden">
          <div className={`${SECTION_TITLE} mb-2`}><FileCode2 className="h-3.5 w-3.5" />策略代码 (Python)</div>
          <div className="flex-1 min-h-0 rounded-card border border-border overflow-hidden">
            <CodeEditor value={code} onChange={setCode} height="100%" />
          </div>
        </div>

        <div className="border-l border-border flex flex-col overflow-hidden">
          <div className="p-3 grid grid-cols-4 gap-2 shrink-0">
            <MetricCard label="收益率" value={fmtPct(metrics.total_return)} tone={tone(metrics.total_return)} />
            <MetricCard label="年化" value={fmtPct(metrics.annualized)} tone={tone(metrics.annualized)} />
            <MetricCard label="夏普" value={fmtNum(metrics.sharpe)} tone={tone(metrics.sharpe)} />
            <MetricCard label="最大回撤" value={metrics.max_drawdown == null ? '—' : fmtPct(-metrics.max_drawdown)} tone={tone(metrics.max_drawdown ? -metrics.max_drawdown : null)} />
            <MetricCard label="超额收益" value={excess == null ? '—' : fmtPct(excess)} tone={tone(excess)} />
            <MetricCard label="胜率" value={fmtPct(metrics.win_rate)} tone={tone(metrics.win_rate)} />
            <MetricCard label="盈亏比" value={fmtNum(metrics.profit_loss_ratio)} tone={tone(metrics.profit_loss_ratio)} />
            <MetricCard label="交易次数" value={metrics.trade_count == null ? (runId ? String(Array.isArray(trades?.data) ? trades.data.length : 0) : '—') : String(metrics.trade_count)} tone="text-foreground" />
          </div>

          <div className="flex-1 min-h-[220px] mx-3 rounded-card border border-border bg-surface">
            <div className="px-4 pt-3 text-xs text-foreground font-medium">收益曲线（策略 / 基准）</div>
            {runId && equityData.length > 0 ? (
              <EquityChart equity={equityData} />
            ) : (
              <div className="h-[260px] grid place-items-center text-xs text-muted">{runId ? '暂无净值数据' : '运行回测后展示实时曲线'}</div>
            )}
          </div>

          <div className="h-[220px] mt-3 mx-3 mb-3 rounded-card border border-border bg-surface flex flex-col overflow-hidden">
            <div className="flex items-center gap-1 px-2 pt-2 shrink-0">
              <TabBtn active={logTab === 'log'} onClick={() => setLogTab('log')}>日志</TabBtn>
              <TabBtn active={logTab === 'error'} onClick={() => setLogTab('error')}>
                错误{hasError && <span className="ml-1 inline-block w-1.5 h-1.5 rounded-full bg-bear align-middle" />}
              </TabBtn>
              <TabBtn active={logTab === 'trade'} onClick={() => setLogTab('trade')}>交易记录</TabBtn>
              <div className="ml-auto flex items-center gap-3 pr-2">
                {runId && <a href={api.getBacktestCsvUrl(runId)} className="inline-flex items-center gap-1 text-xs text-accent hover:underline"><Download className="h-3.5 w-3.5" />CSV</a>}
                {runId && <button onClick={() => setConfirmDel(true)} className="inline-flex items-center gap-1 text-xs text-bear hover:underline"><Trash2 className="h-3.5 w-3.5" />删除</button>}
              </div>
            </div>
            <div className="flex-1 overflow-auto p-3">
              {logTab === 'log' && <LogList logs={Array.isArray(logs?.data) ? logs.data : []} />}
              {logTab === 'error' && <LogList logs={errLogs} />}
              {logTab === 'trade' && <TradeTable trades={Array.isArray(trades?.data) ? trades.data : []} />}
            </div>
          </div>
        </div>
      </div>

      {confirmDel && runId && (
        <Modal onClose={() => setConfirmDel(false)} ariaLabel="确认删除回测">
          <div className="p-5 space-y-4">
            <h3 className="text-sm font-medium text-foreground">删除回测记录</h3>
            <p className="text-xs text-muted">确定删除 <span className="font-mono">{runId}</span> 及其全部数据？此操作不可恢复。</p>
            <div className="flex justify-end gap-2">
              <button onClick={() => setConfirmDel(false)} className="px-3 py-1.5 rounded-btn bg-elevated text-secondary text-xs hover:text-foreground transition-colors">取消</button>
              <button onClick={() => { api.deleteBacktest(runId).then(() => { qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] }); qc.invalidateQueries({ queryKey: ['quant', 'bt', 'runs', strategyId] }); setConfirmDel(false); setLiveRunId(null); setSelRunId(null) }) }}
                className="px-3 py-1.5 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors">删除</button>
            </div>
          </div>
        </Modal>
      )}
    </div>
  )
}

function computeExcess(equity: any[]): number | null {
  if (!equity || equity.length === 0) return null
  const first = equity[0], last = equity[equity.length - 1]
  const fv = Number(first.value), lb = Number(last.value)
  const fb = Number(first.benchmark), lbb = Number(last.benchmark)
  if (!(fv > 0) || !(fb > 0)) return null
  return (lb / fv) - (lbb / fb)
}

function isErrorLog(l: any): boolean {
  if (l && typeof l === 'object' && l.level) {
    const lv = String(l.level).toUpperCase()
    if (lv === 'ERROR' || lv === 'CRITICAL' || lv === 'EXCEPTION') return true
  }
  const s = typeof l === 'string' ? l : (l && typeof l.message === 'string' ? l.message : '')
  return /error|exception|traceback|错误/i.test(s)
}

function filterErrorLogs(logs: any[]): any[] {
  return logs.filter(isErrorLog)
}
```

- [ ] **Step 2: 在 import 行补充图标**

将文件顶部 import 改为包含新增图标：

```tsx
import { Plus, ArrowLeft, Play, Download, Trash2, FileCode2, Activity, Settings2, History } from 'lucide-react'
```

（其余 `import * as api`、`openBacktestStream`、`CodeEditor`、`EquityChart`、`pickMetrics, fmtPct, fmtNum, tone` 保持不变。）

- [ ] **Step 3: 验证 lint + build**

Run: `cd frontend && pnpm lint && pnpm build`
Expected: lint 通过，tsc + vite build 通过（无类型错误；`ProductCard`/`logTab` 类型等一致）。

- [ ] **Step 4: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(quant): 回测页合并为单屏布局，参数上提+右侧三层+三标签"
```

---

## Self-Review

- **Spec coverage:** Task 1 (CodeEditor 高度) ✓；Task 2 (metrics 字段) ✓；Task 3 含 header 一行参数、⚙高级 popover、历史 popover、左满高编辑器、8 卡(含超额/胜率/盈亏/次数)、中图、下三 tab(日志/错误/交易)、实时刷新沿用、错误红点 ✓。所有 spec 点均有对应。
- **Placeholder scan:** 无 TBD/TODO；每个 step 含完整代码或命令。
- **Type consistency:** `logTab` 三态 `'log'|'error'|'trade'` 与 TabBtn 用法一致；`excess`、`errLogs`、`hasError` 命名前后一致；`computeExcess`/`filterErrorLogs` 在 Task 3 内定义并使用。
- **Dropdowns:** `DatePicker` 的 `className`/`buttonClassName` prop 沿用现有组件签名（与编辑 tab 中用法一致）。如 `className` 不被 DatePicker 支持，退化为仅 `buttonClassName`。
