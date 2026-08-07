# 量化回测 v2（策略实体 + 标签页编辑器）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将量化回测页面改为以策略(strategy)为聚合单元的列表 + 标签页编辑器（编辑策略 / 回测详情 / 回测列表），列表显示每个策略最后一次回测的指标，代码编辑框占满左侧。

**Architecture:** 后端新增「每策略最新回测」聚合查询与 API（`strategy_id` 已在 `backtest_runs` 落库，`service.submit_backtest` 已透传）。前端把 `QuantBacktest.tsx` 重构为「策略列表视图 → 策略编辑器视图」，编辑器顶栏含编译运行/开始回测按钮与三个标签页；复用 `CodeEditor`/`DatePicker`/`Modal`/`EquityChart`/`metrics.ts` 与设计令牌。实时 SSE 沿用 `openBacktestStream` + react-query 增量。

**Tech Stack:** Python (FastAPI/SQLite via sqlite3), React + Tailwind + framer-motion + lucide-react + @tanstack/react-query + echarts-for-react。

## Global Constraints

- 设计令牌必须使用系统既有类：`rounded-card` / `rounded-btn` / `bg-surface` / `bg-base` / `border-border` / `text-muted` / `text-foreground` / `text-accent` / `text-bull` / `text-bear`；输入框统一 `INPUT_CLS = 'h-9 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground placeholder:text-muted focus:outline-none focus:border-accent/50 transition-colors'`。
- 顶层列表 = **策略列表**（每策略一行，显示该策略最后一次回测的 收益率/周期/最大回撤/夏普/回测次数），不再平铺所有 run。
- 新建 = 创建策略实体（分配 `strategy_id` + 名称），进编辑器默认「编辑策略」标签；复用现有 `/strategies` CRUD（前端 `api.saveStrategy(null, name, code)`、`api.getStrategy(id)`、`api.listStrategies()`）。
- 编辑器标签：**编辑策略 / 回测详情 / 回测列表**（无独立「编译运行列表」标签）。「编译运行」是**按钮**：用短周期（end 前约 7 天窗口）快速跑一次验证代码，其余参数同正式回测。
- 编辑策略标签：代码编辑框占满左侧空白（`grid grid-cols-1 lg:grid-cols-[1fr_20rem]`，左 `CodeEditor` 撑满高度），右侧 20rem 参数/保存列。
- 指标 key 兼容两条引擎：`total_return` / `annualized`(rqalpha) 或 `annual_return`(jqengine) / `sharpe` / `max_drawdown`；正→`text-bull`，负→`text-bear`。
- run 通过 `strategy_id` 落库（`service.submit_backtest` 已把 `params.get("strategy_id","")` 作为 `insert_run` 第 2 参）。前端 `runBacktest` 必须传 `strategy_id`。
- 前端改完须 `npx tsc -b` 通过。

---

### Task 1: 后端 — 每策略最新回测聚合查询 + API

**Files:**
- Modify: `backend/app/quant/db.py`（新增 `list_strategies_with_latest`）
- Modify: `backend/app/quant/api/quant.py`（新增 `GET /strategies/with-latest`，并让 `/backtest/runs` 支持可选 `strategy_id` 过滤）

**Interfaces:**
- Consumes: 现有 `db.list_runs`、`db.get_conn`、现有 `strategies` 表与 `backtest_runs` 表（含 `strategy_id, params_json, metrics_json, created_at, status, id`）。
- Produces: `db.list_strategies_with_latest()` 返回 `[{id, name, run_count, latest: {run_id, status, start, end, metrics_json} | None}]`；API `GET /api/quant/strategies/with-latest` 返回 `{"data": [...]}`；`GET /api/quant/backtest/runs?strategy_id=...` 可按策略过滤 —— 供前端策略列表与「回测列表」标签使用。

- [ ] **Step 1: 在 `db.py` 末尾（`delete_run` 之后或回测区）新增聚合函数**

在 `backend/app/quant/db.py` 的回测区（如 `list_runs` 函数之后）新增：
```python
def list_strategies_with_latest():
    """策略列表聚合：每个策略一行 + 最新一次回测的指标/周期/次数。"""
    with get_conn() as c:
        strat_rows = c.execute(
            "SELECT id, name FROM strategies ORDER BY updated_at DESC"
        ).fetchall()
        out = []
        for s in strat_rows:
            sid = s["id"]
            count = c.execute(
                "SELECT COUNT(*) AS n FROM backtest_runs WHERE strategy_id=?", (sid,)
            ).fetchone()["n"]
            latest = c.execute(
                "SELECT id, status, params_json, metrics_json, created_at "
                "FROM backtest_runs WHERE strategy_id=? ORDER BY created_at DESC LIMIT 1",
                (sid,),
            ).fetchone()
            item = {"id": sid, "name": s["name"], "run_count": count, "latest": None}
            if latest:
                p = {}
                try:
                    p = json.loads(latest["params_json"] or "{}")
                except Exception:
                    p = {}
                item["latest"] = {
                    "run_id": latest["id"],
                    "status": latest["status"],
                    "start": p.get("start"),
                    "end": p.get("end"),
                    "metrics_json": latest["metrics_json"],
                }
            out.append(item)
        return out
```
注意：本文件顶部已有 `import json`（确认 `db.py` 头部 import；若无则在函数文件中 `import json`）。若 `db.py` 未 import json，在文件顶部 `import os` 附近加 `import json`。

- [ ] **Step 2: 在 `api/quant.py` 新增 `GET /strategies/with-latest` 端点**

在 `backend/app/quant/api/quant.py` 的 `/backtest/runs` 端点附近新增（放在 `backtest_runs` 定义之前或之后均可）：
```python
@router.get("/strategies/with-latest")
def strategies_with_latest():
    return {"data": db.list_strategies_with_latest()}
```

- [ ] **Step 3: 让 `/backtest/runs` 支持 `strategy_id` 过滤**

`backend/app/quant/api/quant.py` 当前：
```python
@router.get("/backtest/runs")
def backtest_runs(limit: int = 50):
    return {"data": db.list_runs(limit)}
```
改为：
```python
@router.get("/backtest/runs")
def backtest_runs(limit: int = 50, strategy_id: str | None = None):
    rows = db.list_runs(limit)
    if strategy_id:
        rows = [r for r in rows if r.get("strategy_id") == strategy_id]
    return {"data": rows}
```
（注：`db.list_runs` 返回 list[dict]，dict 含 `strategy_id` 列。）

- [ ] **Step 4: 语法校验**

从 `backend/` 目录运行：
```bash
python3 -c "import ast; ast.parse(open('app/quant/db.py').read()); ast.parse(open('app/quant/api/quant.py').read()); print('syntax ok')"
```
Expected: 打印 `syntax ok`。

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/db.py backend/app/quant/api/quant.py
git commit -m "feat(quant): 策略列表聚合查询 + 回测 runs 按 strategy_id 过滤"
```

---

### Task 2: 前端 api.ts — 新增策略聚合与按策略过滤的调用

**Files:**
- Modify: `frontend/src/quant/api.ts`

**Interfaces:**
- Consumes: 现有 `j()` 封装；后端 `GET /api/quant/strategies/with-latest`、`GET /api/quant/backtest/runs?strategy_id=`。
- Produces: `listStrategiesWithLatest()`、`listBacktests(strategyId?)` —— 供 Task 3/4 使用。

- [ ] **Step 1: 在 `api.ts` 新增两个函数**

在 `frontend/src/quant/api.ts` 中 `listBacktests` 附近新增：
```ts
export const listStrategiesWithLatest = () => j('/strategies/with-latest')
export const listBacktests = (strategyId?: string) =>
  j(`/backtest/runs${strategyId ? `?strategy_id=${encodeURIComponent(strategyId)}` : ''}`)
```
（保留其余 api 不变。`runBacktest` 仍是 `(p: any)`，前端调用时传 `strategy_id`。）

- [ ] **Step 2: 类型检查**

```bash
cd /home/ubuntu/tickflow-stock-panel/frontend && npx tsc -b
```
Expected: 无报错。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/quant/api.ts
git commit -m "feat(quant): api 新增策略聚合与按 strategy_id 过滤回测"
```

---

### Task 3: 前端重写 QuantBacktest — 策略列表视图 + 编辑器骨架与标签

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx`（整体重写）

**Interfaces:**
- Consumes: Task 2 的 `api.listStrategiesWithLatest()` / `api.listBacktests(strategyId)` / `api.saveStrategy` / `api.getStrategy` / `api.runBacktest` / `api.getBacktestStatus` 等；`metrics.ts`(`pickMetrics/fmtPct/fmtNum/tone`)；`CodeEditor`；`DatePicker`；`Modal`；`EquityChart`；`openBacktestStream`。
- Produces: 策略列表视图（列：名称/最新周期/收益率/最大回撤/夏普/回测次数 + 新建）；编辑器视图（顶栏：返回/名称/编译运行/开始回测/状态 + 标签 编辑策略/回测详情/回测列表）；「编辑策略」标签左代码占满、右参数；「回测详情」「回测列表」标签；实时 SSE。

- [ ] **Step 1: 整体替换 `QuantBacktest.tsx` 为下方完整代码**

将 `frontend/src/quant/pages/QuantBacktest.tsx` 整体替换为：

```tsx
import { useEffect, useState, type ReactNode } from 'react'
import { PageHeader } from '@/components/PageHeader'
import { Modal } from '@/components/Modal'
import { DatePicker } from '@/components/DatePicker'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus, ArrowLeft, Play, Square, Download, Trash2, FileCode2, ListChecks, Activity,
} from 'lucide-react'
import * as api from '../api'
import { openBacktestStream } from '../stream'
import { CodeEditor } from '../components/CodeEditor'
import { EquityChart } from '../components/EquityChart'
import { pickMetrics, fmtPct, fmtNum, tone } from '../metrics'

const INPUT_CLS =
  'h-9 w-full rounded-btn bg-base border border-border px-2.5 text-xs text-foreground ' +
  'placeholder:text-muted focus:outline-none focus:border-accent/50 transition-colors'

const SECTION_TITLE = 'text-[11px] font-medium uppercase tracking-wide text-muted flex items-center gap-1.5'

function statusTone(s: string | undefined) {
  if (s === 'done') return 'text-bull'
  if (s === 'failed') return 'text-bear'
  if (s === 'running' || s === 'queued') return 'text-accent'
  return 'text-muted'
}

interface FormState {
  symbols: string
  start: string
  end: string
  frequency: string
  fee: number
  slippage: number
  capital: number
}

const DEFAULT_FORM: FormState = {
  symbols: '600000.XSHG', start: '', end: '', frequency: 'daily',
  fee: 0.0003, slippage: 0.001, capital: 100000,
}

export function QuantBacktest() {
  const [view, setView] = useState<'list' | 'editor'>('list')
  const [selStrategy, setSelStrategy] = useState<string | null>(null)

  const openNew = () => {
    const d: any = api.saveStrategy(null, '未命名策略', '# 新策略\n')
    setSelStrategy(d.id); setView('editor')
  }
  const openStrategy = (id: string) => { setSelStrategy(id); setView('editor') }

  return (
    <div className="flex flex-col h-full">
      {view === 'list' ? (
        <StrategyList onNew={openNew} onOpen={openStrategy} />
      ) : (
        <StrategyEditor key={selStrategy} strategyId={selStrategy!} onBack={() => setView('list')} />
      )}
    </div>
  )
}

function StrategyList({ onNew, onOpen }: { onNew: () => void; onOpen: (id: string) => void }) {
  const { data } = useQuery({ queryKey: ['quant', 'strategies', 'latest'], queryFn: () => api.listStrategiesWithLatest() })
  const list = (data?.data ?? []) as any[]

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="量化回测"
        subtitle="策略 · RQAlpha · 聚宽式 · 实时 SSE"
        right={
          <button onClick={onNew}
            className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-accent text-base text-xs font-medium hover:bg-accent/90 transition-colors">
            <Plus className="h-4 w-4" />新建
          </button>
        }
      />
      <div className="flex-1 p-4 overflow-auto">
        <div className="rounded-card border border-border bg-surface overflow-hidden">
          <table className="w-full text-xs">
            <thead className="text-muted bg-elevated/40">
              <tr className="text-left">
                <th className="px-3 py-2 font-normal">策略名称</th>
                <th className="px-3 py-2 font-normal">最新回测周期</th>
                <th className="px-3 py-2 font-normal text-right">收益率</th>
                <th className="px-3 py-2 font-normal text-right">最大回撤</th>
                <th className="px-3 py-2 font-normal text-right">夏普比率</th>
                <th className="px-3 py-2 font-normal text-right">回测次数</th>
              </tr>
            </thead>
            <tbody className="text-foreground">
              {list.length === 0 && (
                <tr><td colSpan={6} className="px-3 py-10 text-center text-muted">暂无策略，点击右上角新建</td></tr>
              )}
              {list.map((s) => {
                const m = pickMetrics(s.latest?.metrics_json)
                const period = s.latest ? `${s.latest.start ?? ''} ~ ${s.latest.end ?? ''}` : '—'
                return (
                  <tr key={s.id} onClick={() => onOpen(s.id)}
                    className="border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
                    <td className="px-3 py-2 font-medium">{s.name}</td>
                    <td className="px-3 py-2 text-muted num">{period}</td>
                    <td className={`px-3 py-2 text-right num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.max_drawdown ? -m.max_drawdown : null)}`}>{m.max_drawdown == null ? '—' : fmtPct(-m.max_drawdown)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.sharpe)}`}>{fmtNum(m.sharpe)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(null)}`}>{s.run_count}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function StrategyEditor({ strategyId, onBack }: { strategyId: string; onBack: () => void }) {
  const qc = useQueryClient()
  const { data: strategy } = useQuery({ queryKey: ['quant', 'strategy', strategyId], queryFn: () => api.getStrategy(strategyId) })

  const [tab, setTab] = useState<'edit' | 'detail' | 'runs'>('edit')
  const [code, setCode] = useState<string>('')
  const [name, setName] = useState<string>('')
  const [form, setForm] = useState<FormState>(DEFAULT_FORM)
  const [liveRunId, setLiveRunId] = useState<string | null>(null)
  const [selRunId, setSelRunId] = useState<string | null>(null)
  const [liveOn, setLiveOn] = useState(true)
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
    onSuccess: (d: any) => { setLiveRunId(d.run_id); setSelRunId(d.run_id); setTab('detail'); qc.invalidateQueries({ queryKey: ['quant', 'strategies', 'latest'] }) },
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

  return (
    <div className="flex flex-col h-full">
      <header className="px-4 py-3 border-b border-border flex items-center gap-3 flex-wrap">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border bg-base text-secondary hover:text-foreground transition-colors">
          <ArrowLeft className="h-4 w-4" />列表
        </button>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="策略名称"
          className="h-9 w-48 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground focus:outline-none focus:border-accent/50" />
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
        <div className="ml-auto flex items-center gap-1">
          <TabBtn active={tab === 'edit'} onClick={() => setTab('edit')}>编辑策略</TabBtn>
          <TabBtn active={tab === 'detail'} onClick={() => setTab('detail')}>回测详情</TabBtn>
          <TabBtn active={tab === 'runs'} onClick={() => setTab('runs')}>回测列表</TabBtn>
        </div>
      </header>

      <div className="flex-1 overflow-hidden">
        {tab === 'edit' && (
          <div className="h-full grid grid-cols-1 lg:grid-cols-[1fr_20rem] overflow-hidden">
            <div className="p-4 overflow-hidden flex flex-col">
              <div className={`${SECTION_TITLE} mb-2`}><FileCode2 className="h-3.5 w-3.5" />策略代码 (Python)</div>
              <div className="flex-1 rounded-card border border-border overflow-hidden min-h-0">
                <CodeEditor value={code} onChange={setCode} />
              </div>
            </div>
            <div className="border-l border-border p-4 overflow-auto space-y-3">
              <div className={`${SECTION_TITLE}`}><Activity className="h-3.5 w-3.5" />回测参数</div>
              <input value={form.symbols} onChange={e => setForm({ ...form, symbols: e.target.value })} placeholder="标的池(逗号分隔)" className={INPUT_CLS} />
              <div className="grid grid-cols-2 gap-2">
                <DatePicker value={form.start} onChange={d => setForm({ ...form, start: d })} placeholder="开始" buttonClassName="w-full justify-between" />
                <DatePicker value={form.end} onChange={d => setForm({ ...form, end: d })} placeholder="结束" buttonClassName="w-full justify-between" />
                <input type="number" value={form.capital} onChange={e => setForm({ ...form, capital: +e.target.value })} placeholder="初始金额" className={INPUT_CLS} />
                <select value={form.frequency} onChange={e => setForm({ ...form, frequency: e.target.value })} className={INPUT_CLS}>
                  <option value="daily">daily</option>
                  <option value="1m">1m</option>
                </select>
              </div>
              <button onClick={() => saveStrategy()} className="w-full h-9 rounded-btn bg-elevated text-secondary text-xs hover:text-foreground transition-colors">保存策略</button>
            </div>
          </div>
        )}

        {tab === 'detail' && (
          <div className="h-full p-4 overflow-auto space-y-4">
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
              <MetricCard label="收益率" value={fmtPct(metrics.total_return)} tone={tone(metrics.total_return)} />
              <MetricCard label="年化" value={fmtPct(metrics.annualized)} tone={tone(metrics.annualized)} />
              <MetricCard label="夏普" value={fmtNum(metrics.sharpe)} tone={tone(metrics.sharpe)} />
              <MetricCard label="最大回撤" value={metrics.max_drawdown == null ? '—' : fmtPct(-metrics.max_drawdown)} tone={tone(metrics.max_drawdown ? -metrics.max_drawdown : null)} />
            </div>
            <div className="rounded-card border border-border bg-surface">
              <div className="px-4 pt-3 text-xs text-foreground font-medium">收益曲线（策略 / 基准）</div>
              {runId && equityData.length > 0 ? (
                <EquityChart equity={equityData} />
              ) : (
                <div className="h-[260px] grid place-items-center text-xs text-muted">{runId ? '暂无净值数据' : '运行回测后展示实时曲线'}</div>
              )}
            </div>
            <div className="rounded-card border border-border bg-surface overflow-hidden">
              <div className="flex items-center gap-1 px-2 pt-2">
                <TabBtn active={true} onClick={() => {}}>日志</TabBtn>
                <TabBtn active={false} onClick={() => setLogTab('trade')}>交易记录</TabBtn>
                <div className="ml-auto flex items-center gap-3 pr-2">
                  {runId && <a href={api.getBacktestCsvUrl(runId)} className="inline-flex items-center gap-1 text-xs text-accent hover:underline"><Download className="h-3.5 w-3.5" />CSV</a>}
                  {runId && <button onClick={() => setConfirmDel(true)} className="inline-flex items-center gap-1 text-xs text-bear hover:underline"><Trash2 className="h-3.5 w-3.5" />删除</button>}
                </div>
              </div>
              <div className="p-3">
                {logTab === 'log' ? (
                  <LogList logs={Array.isArray(logs?.data) ? logs.data : []} />
                ) : (
                  <TradeTable trades={Array.isArray(trades?.data) ? trades.data : []} />
                )}
              </div>
            </div>
          </div>
        )}

        {tab === 'runs' && (
          <div className="h-full p-4 overflow-auto">
            <div className="rounded-card border border-border bg-surface overflow-hidden">
              <table className="w-full text-xs">
                <thead className="text-muted bg-elevated/40">
                  <tr className="text-left">
                    <th className="px-3 py-2 font-normal">回测周期</th>
                    <th className="px-3 py-2 font-normal text-right">收益率</th>
                    <th className="px-3 py-2 font-normal text-right">最大回撤</th>
                    <th className="px-3 py-2 font-normal text-right">夏普</th>
                    <th className="px-3 py-2 font-normal">状态</th>
                  </tr>
                </thead>
                <tbody className="text-foreground">
                  {runList.length === 0 && (
                    <tr><td colSpan={5} className="px-3 py-8 text-center text-muted">暂无回测</td></tr>
                  )}
                  {runList.map((r) => {
                    const p = (() => { try { return JSON.parse(r.params_json || '{}') } catch { return {} } })()
                    const m = pickMetrics(r.metrics_json)
                    return (
                      <tr key={r.id} onClick={() => { setSelRunId(r.id); setTab('detail') }}
                        className="border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
                        <td className="px-3 py-2 text-muted num">{`${p.start ?? ''} ~ ${p.end ?? ''}`}</td>
                        <td className={`px-3 py-2 text-right num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</td>
                        <td className={`px-3 py-2 text-right num ${tone(m.max_drawdown ? -m.max_drawdown : null)}`}>{m.max_drawdown == null ? '—' : fmtPct(-m.max_drawdown)}</td>
                        <td className={`px-3 py-2 text-right num ${tone(m.sharpe)}`}>{fmtNum(m.sharpe)}</td>
                        <td className={`px-3 py-2 ${statusTone(r.status)}`}>{r.status}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}
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

function shiftDays(dateStr: string, days: number): string {
  const d = new Date(dateStr)
  d.setDate(d.getDate() + days)
  return d.toISOString().slice(0, 10)
}

function MetricCard({ label, value, tone }: { label: string; value: string; tone: string }) {
  return (
    <div className="rounded-card border border-border bg-surface px-3 py-2">
      <div className="text-[10px] text-muted">{label}</div>
      <div className={`text-sm font-medium num ${tone}`}>{value}</div>
    </div>
  )
}

function TabBtn({ active, onClick, children }: { active: boolean; onClick: () => void; children: ReactNode }) {
  return (
    <button onClick={onClick}
      className={`px-3 py-1.5 rounded-btn text-xs transition-colors ${active ? 'bg-elevated text-foreground' : 'text-muted hover:text-foreground'}`}>
      {children}
    </button>
  )
}

function LogList({ logs }: { logs: any[] }) {
  if (logs.length === 0) return <div className="text-xs text-muted">暂无日志</div>
  return (
    <div className="max-h-64 overflow-auto space-y-0.5 text-[11px] text-muted font-mono">
      {logs.map((l, i) => (
        <div key={i}>{typeof l === 'string' ? l : `${l.level ?? ''} ${l.message ?? JSON.stringify(l)}`}</div>
      ))}
    </div>
  )
}

function TradeTable({ trades }: { trades: any[] }) {
  if (trades.length === 0) return <div className="text-xs text-muted">暂无成交</div>
  return (
    <div className="overflow-auto max-h-64">
      <table className="w-full text-xs">
        <thead className="text-muted sticky top-0 bg-surface">
          <tr className="text-left">
            <th className="px-2 py-1.5 font-normal">时间</th>
            <th className="px-2 py-1.5 font-normal">标的</th>
            <th className="px-2 py-1.5 font-normal">方向</th>
            <th className="px-2 py-1.5 font-normal text-right">价格</th>
            <th className="px-2 py-1.5 font-normal text-right">数量</th>
          </tr>
        </thead>
        <tbody className="text-foreground">
          {trades.map((t, i) => (
            <tr key={i} className="border-t border-border/60">
              <td className="px-2 py-1.5 text-muted">{String(t.ts ?? t.datetime ?? '')}</td>
              <td className="px-2 py-1.5">{t.code ?? t.symbol ?? ''}</td>
              <td className={`px-2 py-1.5 ${(t.action ?? t.side) === 'BUY' || (t.action ?? t.side) === 'buy' ? 'text-bull' : 'text-bear'}`}>{t.action ?? t.side ?? ''}</td>
              <td className="px-2 py-1.5 text-right">{t.price ?? ''}</td>
              <td className="px-2 py-1.5 text-right">{t.amount ?? t.qty ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
```

> 注意：上面代码中 `detail` 标签内用到了 `logTab` 状态与 `setLogTab`，以及初始 `const [logTab, setLogTab] = useState<'log' | 'trade'>('log')`。请在组件顶部（`const [confirmDel, setConfirmDel] = useState(false)` 之后）补加：
> ```ts
> const [logTab, setLogTab] = useState<'log' | 'trade'>('log')
> ```
> 完整文件以「Step 1 完整代码 + 这条补充状态」为准。

- [ ] **Step 2: 类型检查**

```bash
cd /home/ubuntu/tickflow-stock-panel/frontend && npx tsc -b
```
Expected: 无报错（确认已包含 `logTab` 状态声明）。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(quant): 策略列表 + 标签页编辑器(编辑/详情/列表)，代码占满左侧"
```

---

### Task 4: 端到端验证

**Files:** 无新增，仅验证。

**Interfaces:** 复用全部既有接口。

- [ ] **Step 1: 后端语法校验**

```bash
cd /home/ubuntu/tickflow-stock-panel/backend && python3 -c "import ast; ast.parse(open('app/quant/db.py').read()); ast.parse(open('app/quant/api/quant.py').read()); print('syntax ok')"
```
Expected: 打印 `syntax ok`。

- [ ] **Step 2: 前端全量类型检查**

```bash
cd /home/ubuntu/tickflow-stock-panel/frontend && npx tsc -b
```
Expected: 无报错。

- [ ] **Step 3: 手动验证（:3011 / :3018）**

1. 列表页显示列：策略名称 / 最新回测周期 / 收益率 / 最大回撤 / 夏普 / 回测次数；右上「新建」可创建策略并进入编辑器。
2. 编辑器顶栏含：返回列表、策略名称、编译运行、开始回测、状态徽标，右上标签 编辑策略/回测详情/回测列表。
3. 编辑策略标签：代码编辑框占满左侧空白，右侧 20rem 参数列（周期/初始金额/频率/保存策略）。
4. 编译运行：用 end 前 7 天短窗口跑一次，自动切到回测详情并实时展示曲线/日志；失败则状态 failed 并显示错误日志。
5. 开始回测：按配置周期正式跑，列表显示该策略最新指标与回测次数。

- [ ] **Step 4: 若有问题修复并追加 commit**

```bash
git add -A && git commit -m "fix(quant): 策略列表+标签页编辑器 联调修复"
```
（仅在验证发现问题时执行；无问题则跳过。）
