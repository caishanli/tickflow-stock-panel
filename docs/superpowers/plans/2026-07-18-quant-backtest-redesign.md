# 量化回测页面重构 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重构量化回测页面为「列表视图 → 新建/编辑视图」单页状态切换，列表含序号/名称/周期/收益率/最大回撤/夏普，编辑页含顶栏 + 50% 代码编辑器 + 右侧指标卡片/基准策略曲线/日志交易 Tab，并为回测记录新增 `name` 字段。

**Architecture:** 后端在 `backtest_runs` 表新增 `name` 列（带 migration 兼容旧库），API 的 `BacktestIn` 接收 `name` 并落库；前端在 `QuantBacktest.tsx` 内用 `view` 状态切换三个视图，复用现有 `CodeEditor`/`DatePicker`/`Modal` 逻辑与设计令牌。实时 SSE 沿用现有 `openBacktestStream` + react-query 增量。

**Tech Stack:** Python (FastAPI/SQLite via sqlite3), React + Tailwind + framer-motion + lucide-react + @tanstack/react-query + echarts-for-react。

## Global Constraints

- 设计令牌必须使用系统既有类：`rounded-card` / `rounded-btn` / `bg-surface` / `bg-base` / `border-border` / `text-muted` / `text-foreground` / `text-accent` / `text-bull` / `text-bear`；输入框统一 `h-9 w-full rounded-btn bg-base border border-border px-2.5 text-xs`（即 `INPUT_CLS`）。
- 不新增路由，保持 `QuantBacktest` 单文件内 `view: 'list' | 'new' | 'edit'` 状态切换。
- 编辑=克隆源 run 的 params/code/name 到表单，「开始回测」生成**新** run 记录（不覆写原记录）。
- 指标 key 兼容两条引擎：`total_return` / `annualized`(rqalpha) 或 `annual_return`(jqengine) / `sharpe` / `max_drawdown`。
- 收益率/年化/夏普为正显示 `text-bull`，为负显示 `text-bear`（红涨绿跌约定）。
- 前端改完须 `npx tsc -b` 通过。

---

### Task 1: 后端 DB 新增 name 列 + migration

**Files:**
- Modify: `backend/app/quant/db.py` (DDL `backtest_runs`, `insert_run`, `upsert_run`, `init_db`)

**Interfaces:**
- Consumes: 现有 `get_conn`、`init_db`。
- Produces: `insert_run(run_id, strategy_id, name, params_json, status)`、`upsert_run(run_id, strategy_id, name, params_json, status)` —— 后续 Task 2/3 调用。

- [ ] **Step 1: 修改 `_SCHEMA` 的 `backtest_runs` DDL，增加 `name TEXT`**

`backend/app/quant/db.py` 第 16-19 行，将：
```python
CREATE TABLE IF NOT EXISTS backtest_runs (
    id TEXT PRIMARY KEY, strategy_id TEXT, params_json TEXT, status TEXT,
    metrics_json TEXT, created_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT, error TEXT);
```
改为：
```python
CREATE TABLE IF NOT EXISTS backtest_runs (
    id TEXT PRIMARY KEY, strategy_id TEXT, name TEXT, params_json TEXT, status TEXT,
    metrics_json TEXT, created_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT, error TEXT);
```

- [ ] **Step 2: 在 `init_db` 末尾增加 migration（对已存在但无 `name` 列的旧库执行 ALTER）**

`backend/app/quant/db.py` 的 `init_db` 函数（约 47-55 行），将：
```python
def init_db(path: str | None = None) -> None:
    global _DB_PATH
    _DB_PATH = path or CONFIG.db_path
    conn = sqlite3.connect(_DB_PATH)
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
    finally:
        conn.close()
```
改为：
```python
def init_db(path: str | None = None) -> None:
    global _DB_PATH
    _DB_PATH = path or CONFIG.db_path
    conn = sqlite3.connect(_DB_PATH)
    try:
        conn.executescript(_SCHEMA)
        # 兼容旧库：若 backtest_runs 尚无 name 列则补加
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(backtest_runs)")}
        if "name" not in cols:
            conn.execute("ALTER TABLE backtest_runs ADD COLUMN name TEXT")
        conn.commit()
    finally:
        conn.close()
```

- [ ] **Step 3: 修改 `insert_run` 签名与 SQL，增加 `name` 参数**

`backend/app/quant/db.py` 第 67-72 行，将：
```python
def insert_run(run_id, strategy_id, params_json, status="queued"):
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,params_json,status) VALUES(?,?,?,?)",
            (run_id, strategy_id, params_json, status),
        )
```
改为：
```python
def insert_run(run_id, strategy_id, name, params_json, status="queued"):
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,name,params_json,status) VALUES(?,?,?,?,?)",
            (run_id, strategy_id, name, params_json, status),
        )
```

- [ ] **Step 4: 修改 `upsert_run` 签名与 SQL，增加 `name` 参数**

`backend/app/quant/db.py` 第 75-83 行，将：
```python
def upsert_run(run_id, strategy_id, params_json, status="running"):
    """插入回测记录；若 run_id 已存在（如 API 已建 'queued' 行）则更新，避免 UNIQUE 冲突。"""
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,params_json,status) VALUES(?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "status=excluded.status, strategy_id=excluded.strategy_id, params_json=excluded.params_json",
            (run_id, strategy_id, params_json, status),
        )
```
改为：
```python
def upsert_run(run_id, strategy_id, name, params_json, status="running"):
    """插入回测记录；若 run_id 已存在（如 API 已建 'queued' 行）则更新，避免 UNIQUE 冲突。"""
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,name,params_json,status) VALUES(?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "status=excluded.status, strategy_id=excluded.strategy_id, name=excluded.name, "
            "params_json=excluded.params_json",
            (run_id, strategy_id, name, params_json, status),
        )
```

- [ ] **Step 5: 验证 DB 改动（手动 sanity）**

在 `backend/` 目录下运行：
```bash
python -c "from app.quant import db; db.init_db(':memory:'); db.insert_run('t1','s1','测试名','{}','queued'); print(db.get_run('t1'))"
```
Expected: 打印含 `'name': '测试名'` 的 dict，无报错。

- [ ] **Step 6: Commit**

```bash
git add backend/app/quant/db.py
git commit -m "feat(quant): backtest_runs 新增 name 列并兼容旧库 migration"
```

---

### Task 2: 后端 service.submit_backtest 写入 name

**Files:**
- Modify: `backend/app/quant/service.py` (第 25-40 行 `submit_backtest`)

**Interfaces:**
- Consumes: Task 1 的 `db.insert_run(run_id, strategy_id, name, params_json, status)`。
- Produces: 提交的 run 记录带 `name`，供 Task 3 的 API 与前端读取。

- [ ] **Step 1: 修改 `submit_backtest` 调用 `insert_run` 时传入 `name`**

`backend/app/quant/service.py` 第 28-33 行，将：
```python
    db.insert_run(
        run_id,
        params.get("strategy_id", ""),
        json.dumps(params, ensure_ascii=False),
        "queued",
    )
```
改为：
```python
    db.insert_run(
        run_id,
        params.get("strategy_id", ""),
        params.get("name", ""),
        json.dumps(params, ensure_ascii=False),
        "queued",
    )
```

- [ ] **Step 2: Commit**

```bash
git add backend/app/quant/service.py
git commit -m "feat(quant): submit_backtest 写入回测名称"
```

---

### Task 3: 后端 API 接收 name

**Files:**
- Modify: `backend/app/quant/api/quant.py` (第 29-38 行 `BacktestIn`)

**Interfaces:**
- Consumes: Task 2 的 `submit_backtest(params)`（params 经 `model_dump()` 含 `name`）。
- Produces: `/backtest/runs` 与 `/backtest/{id}/status` 返回的记录含 `name` 字段（自动，因 `list_runs`/`get_run` 返回全列）。

- [ ] **Step 1: 在 `BacktestIn` 增加 `name` 字段**

`backend/app/quant/api/quant.py` 第 29-38 行，将：
```python
class BacktestIn(BaseModel):
    strategy_id: str = ""
    strategy_code: str = ""
    symbols: list[str] = []
    start: str
    end: str
    frequency: str = "daily"
    capital: float = 100000.0
    fee: float = 0.0003
    slippage: float = 0.001
```
改为（在首行插入 `name: str = ""`）：
```python
class BacktestIn(BaseModel):
    name: str = ""
    strategy_id: str = ""
    strategy_code: str = ""
    symbols: list[str] = []
    start: str
    end: str
    frequency: str = "daily"
    capital: float = 100000.0
    fee: float = 0.0003
    slippage: float = 0.001
```

`run_backtest` 端点（第 90-94 行）无需改动：`params = body.model_dump()` 自动包含 `name`，`submit_backtest(params)` 已落地。

- [ ] **Step 2: 验证（启动后端后 curl）**

后端已在 :3018 运行则重启后执行：
```bash
curl -s -X POST http://localhost:3018/api/quant/backtest/run -H 'Content-Type: application/json' -d '{"name":"计划验证","strategy_code":"pass","symbols":["600000.XSHG"],"start":"2024-01-01","end":"2024-01-31"}'
curl -s "http://localhost:3018/api/quant/backtest/runs?limit=1"
```
Expected: 第二条返回 JSON 中第一条记录的 `name` 为 `"计划验证"`。

- [ ] **Step 3: Commit**

```bash
git add backend/app/quant/api/quant.py
git commit -m "feat(quant): backtest/run API 接收 name 字段"
```

---

### Task 4: 前端新增指标解析与格式化工具

**Files:**
- Create: `frontend/src/quant/metrics.ts`

**Interfaces:**
- Consumes: 无。
- Produces: `pickMetrics(raw)` 返回 `{total_return, annualized, sharpe, max_drawdown}`（统一 key，归一化 null），`fmtPct(v)`、`fmtNum(v, digits?)`、`tone(v)` —— 供 Task 6/7 列表与指标卡片使用。

- [ ] **Step 1: 创建 `metrics.ts`**

新建 `frontend/src/quant/metrics.ts`：
```ts
// 统一两条回测引擎的指标 key（rqalpha: annualized；jqengine: annual_return）
export interface Metrics {
  total_return: number | null
  annualized: number | null
  sharpe: number | null
  max_drawdown: number | null
}

export function pickMetrics(raw: any): Metrics {
  if (!raw) return { total_return: null, annualized: null, sharpe: null, max_drawdown: null }
  let m: any = raw
  if (typeof raw === 'string') {
    try { m = JSON.parse(raw) } catch { m = {} }
  }
  return {
    total_return: num(m.total_return),
    annualized: num(m.annualized ?? m.annual_return),
    sharpe: num(m.sharpe),
    max_drawdown: num(m.max_drawdown),
  }
}

function num(v: any): number | null {
  if (v == null || v === '' || (typeof v === 'number' && !isFinite(v))) return null
  const n = Number(v)
  return isNaN(n) ? null : n
}

export function fmtPct(v: number | null): string {
  if (v == null) return '—'
  return `${(v * 100).toFixed(2)}%`
}

export function fmtNum(v: number | null, digits = 2): string {
  if (v == null) return '—'
  return v.toFixed(digits)
}

// 正→红涨(text-bull)，负→绿跌(text-bear)，null→muted
export function tone(v: number | null): string {
  if (v == null) return 'text-muted'
  return v >= 0 ? 'text-bull' : 'text-bear'
}
```

- [ ] **Step 2: 类型检查**

```bash
cd /home/caisl/tickflow-stock-panel/frontend && npx tsc -b
```
Expected: 无报错。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/quant/metrics.ts
git commit -m "feat(quant): 指标统一解析与格式化工具"
```

---

### Task 5: 前端抽取收益曲线组件（双 series：策略 + 基准）

**Files:**
- Create: `frontend/src/quant/components/EquityChart.tsx`

**Interfaces:**
- Consumes: `equity: any[]`（元素含 `dt, value, benchmark`）。
- Produces: `<EquityChart equity={...} />` —— 归一化净值曲线，双 series（策略 accent、基准 muted），供 Task 7 编辑器右侧使用。

- [ ] **Step 1: 创建 `EquityChart.tsx`**

新建 `frontend/src/quant/components/EquityChart.tsx`：
```tsx
import { useMemo } from 'react'
import ReactECharts from 'echarts-for-react'

function cssVar(name: string, fallback: string) {
  if (typeof window === 'undefined') return fallback
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  return v || fallback
}

export function EquityChart({ equity }: { equity: any[] }) {
  const option = useMemo(() => {
    const accent = cssVar('--accent', '#3b82f6')
    const muted = cssVar('--muted', '#94a3b8')
    const dates = equity.map((d) => String(d.dt ?? d.date ?? '').slice(0, 10))
    const strat = equity.map((d) => Number(d.value ?? 0))
    const bench = equity.map((d) => Number(d.benchmark ?? 0))
    const norm = (arr: number[]) => {
      const f = arr[0]
      return f ? arr.map((v) => v / f) : arr
    }
    const s = norm(strat)
    const b = norm(bench)
    return {
      animation: false,
      grid: { left: 56, right: 16, top: 28, bottom: 32 },
      legend: { data: ['策略', '基准'], textStyle: { color: muted, fontSize: 10 }, right: 8, top: 4 },
      tooltip: {
        trigger: 'axis',
        backgroundColor: cssVar('--surface', '#1e293b'),
        borderColor: cssVar('--border', '#334155'),
        textStyle: { color: cssVar('--foreground', '#e2e8f0'), fontSize: 12 },
      },
      xAxis: {
        type: 'category', data: dates,
        axisLabel: { color: muted, fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: cssVar('--border', '#334155') } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value', scale: true,
        axisLabel: { color: muted, fontSize: 10 },
        splitLine: { lineStyle: { color: cssVar('--border', '#334155') } },
      },
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 14, bottom: 6, borderColor: cssVar('--border', '#334155'), textStyle: { color: muted, fontSize: 10 } }],
      series: [
        { name: '策略', type: 'line', data: s, symbol: 'none', lineStyle: { color: accent, width: 2 },
          areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: accent + '26' }, { offset: 1, color: accent + '03' }] } } },
        { name: '基准', type: 'line', data: b, symbol: 'none', lineStyle: { color: muted, width: 1.5, type: 'dashed' } },
      ],
    } as any
  }, [equity])

  return <ReactECharts option={option} style={{ height: 260 }} notMerge />
}
```

- [ ] **Step 2: 类型检查**

```bash
cd /home/caisl/tickflow-stock-panel/frontend && npx tsc -b
```
Expected: 无报错。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/quant/components/EquityChart.tsx
git commit -m "feat(quant): 策略/基准双序列收益曲线组件"
```

---

### Task 6: 前端重写 QuantBacktest — 列表视图 + 编辑器骨架 + 完整组件

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx`（整体重写为列表 + 编辑器）

**Interfaces:**
- Consumes: Task 4 的 `pickMetrics`/`fmtPct`/`fmtNum`/`tone`；Task 5 的 `EquityChart`；`api.*`（`listBacktests`/`getBacktestStatus`/`getBacktestEquity`/`getBacktestTrades`/`getBacktestLogs`/`getBacktestCsvUrl`/`deleteBacktest`/`runBacktest`）；`openBacktestStream`；`CodeEditor`；`Modal`；`DatePicker`；`PageHeader`。
- Produces: 完整页面：列表视图（表格 + 新建）+ 编辑器（顶栏 + 左代码50% + 右指标常驻 + 双序列曲线 + 日志/交易 Tab）+ 实时 SSE。

- [ ] **Step 1: 整体替换 `QuantBacktest.tsx` 为下方完整代码**

将 `frontend/src/quant/pages/QuantBacktest.tsx` 整体替换为：

```tsx
import { useEffect, useState } from 'react'
import { PageHeader } from '@/components/PageHeader'
import { Modal } from '@/components/Modal'
import { DatePicker } from '@/components/DatePicker'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus, ArrowLeft, Play, Square, Download, Trash2, FileCode2,
} from 'lucide-react'
import * as api from '../api'
import { openBacktestStream } from '../stream'
import { CodeEditor } from '../components/CodeEditor'
import { EquityChart } from '../components/EquityChart'
import { pickMetrics, fmtPct, fmtNum, tone } from '../metrics'

const INPUT_CLS =
  'h-9 rounded-btn bg-base border border-border px-2.5 text-xs text-foreground ' +
  'placeholder:text-muted focus:outline-none focus:border-accent/50 transition-colors'

const SECTION_TITLE = 'text-[11px] font-medium uppercase tracking-wide text-muted flex items-center gap-1.5'

function statusTone(s: string | undefined) {
  if (s === 'done') return 'text-bull'
  if (s === 'failed') return 'text-bear'
  if (s === 'running' || s === 'queued') return 'text-accent'
  return 'text-muted'
}

interface FormState {
  name: string
  symbols: string
  start: string
  end: string
  frequency: string
  fee: number
  slippage: number
  capital: number
}

const DEFAULT_FORM: FormState = {
  name: '', symbols: '600000.XSHG', start: '', end: '', frequency: 'daily',
  fee: 0.0003, slippage: 0.001, capital: 100000,
}

export function QuantBacktest() {
  const [view, setView] = useState<'list' | 'new' | 'edit'>('list')
  const [selRun, setSelRun] = useState<string | null>(null)
  const [editorKey, setEditorKey] = useState(0)

  const openNew = () => { setSelRun(null); setEditorKey(k => k + 1); setView('new') }
  const openEdit = (id: string) => { setSelRun(id); setEditorKey(k => k + 1); setView('edit') }

  return (
    <div className="flex flex-col h-full">
      {view === 'list' ? (
        <BacktestList onNew={openNew} onOpen={openEdit} />
      ) : (
        <BacktestEditor key={editorKey} mode={view} sourceRunId={selRun} onBack={() => setView('list')} />
      )}
    </div>
  )
}

function BacktestList({ onNew, onOpen }: { onNew: () => void; onOpen: (id: string) => void }) {
  const { data: runs } = useQuery({ queryKey: ['quant', 'bt', 'runs'], queryFn: () => api.listBacktests() })
  const list = (runs ?? []) as any[]

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="量化回测"
        subtitle="RQAlpha · 聚宽式策略 · 实时 SSE"
        right={
          <button onClick={onNew}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-btn bg-accent text-base text-xs font-medium hover:bg-accent/90 transition-colors">
            <Plus className="h-4 w-4" />新建
          </button>
        }
      />
      <div className="flex-1 p-4 overflow-auto">
        <div className="rounded-card border border-border bg-surface overflow-hidden">
          <table className="w-full text-xs">
            <thead className="text-muted bg-elevated/40">
              <tr className="text-left">
                <th className="px-3 py-2 font-normal w-12">序号</th>
                <th className="px-3 py-2 font-normal">名称</th>
                <th className="px-3 py-2 font-normal">回测周期</th>
                <th className="px-3 py-2 font-normal text-right">收益率</th>
                <th className="px-3 py-2 font-normal text-right">最大回撤</th>
                <th className="px-3 py-2 font-normal text-right">夏普比率</th>
              </tr>
            </thead>
            <tbody className="text-foreground">
              {list.length === 0 && (
                <tr><td colSpan={6} className="px-3 py-10 text-center text-muted">暂无回测记录</td></tr>
              )}
              {list.map((r, i) => {
                const p = (() => { try { return JSON.parse(r.params_json || '{}') } catch { return {} } })()
                const m = pickMetrics(r.metrics_json)
                const period = `${p.start ?? ''} ~ ${p.end ?? ''}`
                return (
                  <tr key={r.id} onClick={() => onOpen(r.id)}
                    className="border-t border-border/60 cursor-pointer hover:bg-elevated/60 transition-colors">
                    <td className="px-3 py-2 text-muted num">{i + 1}</td>
                    <td className="px-3 py-2">
                      <div className="font-medium">{r.name || r.id}</div>
                      <div className="text-[10px] text-muted truncate max-w-[200px]">{p.symbols?.join(', ')}</div>
                    </td>
                    <td className="px-3 py-2 text-muted num">{period}</td>
                    <td className={`px-3 py-2 text-right num font-medium ${tone(m.total_return)}`}>{fmtPct(m.total_return)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.max_drawdown ? -m.max_drawdown : null)}`}>{m.max_drawdown == null ? '—' : fmtPct(-m.max_drawdown)}</td>
                    <td className={`px-3 py-2 text-right num ${tone(m.sharpe)}`}>{fmtNum(m.sharpe)}</td>
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

function BacktestEditor({ mode, sourceRunId, onBack }: {
  mode: 'new' | 'edit'
  sourceRunId: string | null
  onBack: () => void
}) {
  const qc = useQueryClient()
  const { data: source } = useQuery({
    queryKey: ['quant', 'bt', 'src', sourceRunId],
    queryFn: () => api.getBacktestStatus(sourceRunId as string),
    enabled: mode === 'edit' && !!sourceRunId,
  })

  const [form, setForm] = useState<FormState>(DEFAULT_FORM)
  const [code, setCode] = useState<string>('')
  const [liveRunId, setLiveRunId] = useState<string | null>(null)
  const [liveOn, setLiveOn] = useState(true)
  const [tab, setTab] = useState<'log' | 'trade'>('log')
  const [confirmDel, setConfirmDel] = useState(false)

  useEffect(() => {
    if (mode === 'edit' && source?.data) {
      const r = source.data
      const p = (() => { try { return JSON.parse(r.params_json || '{}') } catch { return {} } })()
      setForm({
        name: r.name || '',
        symbols: (p.symbols || []).join(', '),
        start: p.start || '', end: p.end || '',
        frequency: p.frequency || 'daily',
        fee: p.fee ?? 0.0003, slippage: p.slippage ?? 0.001,
        capital: p.capital ?? 100000,
      })
      setCode(p.strategy_code || '')
    }
  }, [mode, source])

  const runMut = useMutation({
    mutationFn: () => api.runBacktest({
      name: form.name,
      strategy_code: code,
      symbols: form.symbols.split(',').map(s => s.trim()).filter(Boolean),
      start: form.start, end: form.end, frequency: form.frequency,
      fee: +form.fee, slippage: +form.slippage, capital: +form.capital,
    }),
    onSuccess: (d: any) => { setLiveRunId(d.run_id); qc.invalidateQueries({ queryKey: ['quant', 'bt', 'runs'] }) },
  })

  const runId = liveRunId
  const { data: status } = useQuery({ queryKey: ['quant', 'bt', runId, 'status'], queryFn: () => api.getBacktestStatus(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: equity } = useQuery({ queryKey: ['quant', 'bt', runId, 'equity'], queryFn: () => api.getBacktestEquity(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: trades } = useQuery({ queryKey: ['quant', 'bt', runId, 'trades'], queryFn: () => api.getBacktestTrades(runId as string), enabled: !!runId, refetchInterval: 2000 })
  const { data: logs } = useQuery({ queryKey: ['quant', 'bt', runId, 'logs'], queryFn: () => api.getBacktestLogs(runId as string), enabled: !!runId, refetchInterval: 2000 })

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

  return (
    <div className="flex flex-col h-full">
      <header className="px-4 py-3 border-b border-border flex items-center gap-3 flex-wrap">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border bg-base text-secondary hover:text-foreground transition-colors">
          <ArrowLeft className="h-4 w-4" />列表
        </button>
        <input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })}
          placeholder="策略名称" className={`${INPUT_CLS} w-48`} />
        <button onClick={() => setLiveOn(v => !v)}
          className={`inline-flex items-center gap-1.5 h-9 px-3 rounded-btn border text-xs transition-colors ${liveOn ? 'border-accent/40 text-accent bg-accent/10' : 'border-border text-muted bg-base'}`}>
          {liveOn ? <Play className="h-3.5 w-3.5" /> : <Square className="h-3.5 w-3.5" />}编译运行
        </button>
        <div className="flex items-center gap-2 text-xs text-muted">
          <span>周期</span>
          <DatePicker value={form.start} onChange={d => setForm({ ...form, start: d })} placeholder="开始" buttonClassName="w-32 justify-between" />
          <span>~</span>
          <DatePicker value={form.end} onChange={d => setForm({ ...form, end: d })} placeholder="结束" buttonClassName="w-32 justify-between" />
        </div>
        <div className="flex items-center gap-2 text-xs text-muted">
          <span>初始金额</span>
          <input type="number" value={form.capital} onChange={e => setForm({ ...form, capital: +e.target.value })}
            className={`${INPUT_CLS} w-28`} />
        </div>
        <button onClick={() => runMut.mutate()} disabled={!code || !form.start || !form.end || !form.name || runMut.isPending}
          className="ml-auto inline-flex items-center gap-1.5 h-9 px-4 rounded-btn bg-accent text-base text-xs font-medium disabled:opacity-50 hover:bg-accent/90 transition-colors">
          <Play className="h-4 w-4" />{runMut.isPending ? '提交中…' : '开始回测'}
        </button>
        {lastStatus && (
          <span className={`text-xs font-medium px-2 py-0.5 rounded-btn bg-base border border-border ${statusTone(lastStatus)}`}>{lastStatus}</span>
        )}
      </header>

      <div className="flex-1 grid grid-cols-1 lg:grid-cols-2 overflow-hidden">
        <div className="border-r border-border p-4 overflow-auto">
          <div className={`${SECTION_TITLE} mb-2`}><FileCode2 className="h-3.5 w-3.5" />策略代码 (Python)</div>
          <div className="rounded-card border border-border overflow-hidden">
            <CodeEditor value={code} onChange={setCode} />
          </div>
        </div>

        <div className="p-4 overflow-auto space-y-4">
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
              <div className="h-[260px] grid place-items-center text-xs text-muted">
                {runId ? '暂无净值数据' : '运行回测后展示实时曲线'}
              </div>
            )}
          </div>

          <div className="rounded-card border border-border bg-surface overflow-hidden">
            <div className="flex items-center gap-1 px-2 pt-2">
              <TabBtn active={tab === 'log'} onClick={() => setTab('log')}>日志</TabBtn>
              <TabBtn active={tab === 'trade'} onClick={() => setTab('trade')}>交易记录</TabBtn>
              <div className="ml-auto flex items-center gap-3 pr-2">
                {runId && <a href={api.getBacktestCsvUrl(runId)} className="inline-flex items-center gap-1 text-xs text-accent hover:underline"><Download className="h-3.5 w-3.5" />CSV</a>}
                {runId && <button onClick={() => setConfirmDel(true)} className="inline-flex items-center gap-1 text-xs text-bear hover:underline"><Trash2 className="h-3.5 w-3.5" />删除</button>}
              </div>
            </div>
            <div className="p-3">
              {tab === 'log' ? (
                <LogList logs={Array.isArray(logs?.data) ? logs.data : []} />
              ) : (
                <TradeTable trades={Array.isArray(trades?.data) ? trades.data : []} />
              )}
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
              <button onClick={() => { api.deleteBacktest(runId).then(() => { qc.invalidateQueries({ queryKey: ['quant', 'bt', 'runs'] }); setConfirmDel(false); setLiveRunId(null) }) }}
                className="px-3 py-1.5 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors">删除</button>
            </div>
          </div>
        </Modal>
      )}
    </div>
  )
}

function MetricCard({ label, value, tone }: { label: string; value: string; tone: string }) {
  return (
    <div className="rounded-card border border-border bg-surface px-3 py-2">
      <div className="text-[10px] text-muted">{label}</div>
      <div className={`text-sm font-medium num ${tone}`}>{value}</div>
    </div>
  )
}

function TabBtn({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
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

- [ ] **Step 2: 类型检查**

```bash
cd /home/caisl/tickflow-stock-panel/frontend && npx tsc -b
```
Expected: 无报错。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(quant): 量化回测页面重构为列表+编辑器(指标/曲线/Tab)"
```

---

### Task 7: 端到端验证

**Files:** 无新增，仅验证。

**Interfaces:** 复用全部既有接口。

- [ ] **Step 1: 后端类型/导入 sanity**

```bash
cd /home/caisl/tickflow-stock-panel/backend && python -c "import app.quant.api.quant as q; print('api ok')"
```
Expected: 打印 `api ok`，无 ImportError。

- [ ] **Step 2: 前端全量类型检查**

```bash
cd /home/caisl/tickflow-stock-panel/frontend && npx tsc -b
```
Expected: 无报错。

- [ ] **Step 3: 重启前后端并手动验证（:3011 / :3018）**

1. 列表页显示列：序号/名称/回测周期/收益率/最大回撤/夏普；右上「新建」可进入编辑器。
2. 编辑器顶栏含：返回列表、策略名称输入、编译运行开关、回测周期、初始金额、开始回测。
3. 左半为代码编辑器（≈50% 宽），右半上方为指标卡片常驻 + 策略/基准曲线，下方 Tab 切换日志/交易。
4. 点列表项进入编辑页，表单被源 run 的 params/code/name 填充；点「开始回测」生成新 run 并实时刷新曲线/日志/交易。
5. 删除按钮弹确认 Modal，确认后记录消失。

- [ ] **Step 4: 若有问题修复并追加 commit**

```bash
git add -A && git commit -m "fix(quant): 量化回测重构联调修复"
```
（仅在 Task 3/6 验证发现问题时执行；无问题则跳过。）
