# 本地股市数据页底部多标签整合 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 LocalData 页面的服务状态从顶部标签页移到底部 stockdata 日志区域，用多标签卡片统一承载。

**Architecture:** 单文件改动（`frontend/src/pages/LocalData.tsx`）。删除顶部 `activeTab` 标签切换，数据统计表格始终展示；底部从可折叠日志面板改为固定展开的多标签卡片（"日志" / "服务状态"），日志和服务状态各自的查询/轮询逻辑不变，仅切换显隐控制。

**Tech Stack:** React 18 + TypeScript + TanStack Query + Vite + Tailwind

## Global Constraints

- 前端无测试脚本，验证用 `cd frontend && pnpm lint` + `pnpm build`（`tsc -b && vite build`）
- 不改动后端代码、API、路由配置
- `StockdataStatusPanel` 组件内部实现不变，只移动渲染位置
- 日志的无限滚动、5s 轮询、分页加载逻辑不变
- line-length 100（后端 ruff 规则，前端用 pnpm lint）
- 不加注释（AGENTS.md 规定）

---

### Task 1: 底部多标签卡片整合

**Files:**
- Modify: `frontend/src/pages/LocalData.tsx`

**Interfaces:**
- Consumes: `api.stockdataLog`, `api.stockdataStatus`, `api.localMarketStats`（均不变）
- Produces: 重构后的 `LocalData` 组件渲染（无导出签名变化）

**Steps:**

- [ ] **Step 1: 更新 import（移除不再使用的 ChevronDown）**

文件第 3 行，将：
```tsx
import { HardDrive, RefreshCw, Wrench, ChevronDown, Server, Activity, Inbox, Clock, CheckCircle2, Loader2 } from 'lucide-react'
```
改为：
```tsx
import { HardDrive, RefreshCw, Wrench, Server, Activity, Inbox, Clock, CheckCircle2, Loader2 } from 'lucide-react'
```
原因：`ChevronDown` 仅用于即将删除的折叠按钮；其余图标仍被 `StockdataStatusPanel`（`Server`/`Activity`/`Inbox`/`Clock`/`CheckCircle2`/`Loader2`）、表格区（`HardDrive`/`RefreshCw`/`Wrench`）使用。

- [ ] **Step 2: 删除 TabKey 类型（第 14 行）**

删除这一行：
```tsx
type TabKey = 'stats' | 'status'
```

- [ ] **Step 3: 替换 activeTab 状态为 bottomTab（第 217 行附近）**

将：
```tsx
  const [activeTab, setActiveTab] = useState<TabKey>('stats')
```
改为（删除 activeTab，无替换——bottomTab 在 Step 5 的 logOpen 处新增）：
直接删除该行。

- [ ] **Step 4: 替换 logOpen 状态为 bottomTab（第 270 行附近）**

将：
```tsx
  const [logOpen, setLogOpen] = useState(false)
```
改为：
```tsx
  const [bottomTab, setBottomTab] = useState<'log' | 'status'>('log')
```

- [ ] **Step 5: 更新日志 useEffect 依赖（第 294-301 行附近）**

将：
```tsx
  // 打开时首次加载 + 每 5s 轮询最新一屏
  useEffect(() => {
    if (!logOpen) return
    setLogLines([])
    setLogOffset(0)
    loadLogPage(0)
    const t = setInterval(() => loadLogPage(0), 5000)
    return () => clearInterval(t)
  }, [logOpen, loadLogPage])
```
改为：
```tsx
  const logVisible = bottomTab === 'log'
  useEffect(() => {
    if (!logVisible) return
    setLogLines([])
    setLogOffset(0)
    loadLogPage(0)
    const t = setInterval(() => loadLogPage(0), 5000)
    return () => clearInterval(t)
  }, [logVisible, loadLogPage])
```

- [ ] **Step 6: 更新 PageHeader（第 334-356 行附近）**

将：
```tsx
      <PageHeader
        title="本地股市数据"
        subtitle={activeTab === 'stats' ? (total > 0 ? `本地 Parquet 各日期去重标的数 · 共 ${total} 天` : '本地 Parquet 各日期去重标的数') : 'stockdata 服务运行状态 · 每 5 秒自动刷新'}
        right={
          <div className="inline-flex rounded-btn border border-border bg-surface/80 p-0.5 shadow-sm">
            {(['stats', 'status'] as const).map(tab => {
              const active = activeTab === tab
              return (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`inline-flex items-center gap-1.5 rounded-[5px] px-3 py-1.5 text-xs font-medium transition-colors cursor-pointer ${
                    active ? 'bg-accent text-white shadow-sm' : 'text-secondary hover:bg-elevated hover:text-foreground'
                  }`}
                >
                  {tab === 'stats' ? <HardDrive className="h-3.5 w-3.5" /> : <Server className="h-3.5 w-3.5" />}
                  {tab === 'stats' ? '数据统计' : '服务状态'}
                </button>
              )
            })}
          </div>
        }
      />
```
改为：
```tsx
      <PageHeader
        title="本地股市数据"
        subtitle={total > 0 ? `本地 Parquet 各日期去重标的数 · 共 ${total} 天` : '本地 Parquet 各日期去重标的数'}
      />
```

- [ ] **Step 7: 移除 activeTab 条件渲染外壳**

当前渲染结构有两层 fragment 嵌套：

```
357: <div className="flex-1 p-4 overflow-auto space-y-3">
358:   {activeTab === 'status' ? (           ← 要删的 activeTab 三元开头
359:     <StockdataStatusPanel />             ← 要删
360:   ) : (                                  ← 要删
361:   <>                                     ← 要删的外层 fragment
362:     {!isLoading && !isError && total > 0 && (  ← 保留，filter bar
...
413:     ) : (                                ← 保留，total>0 三元分支
414:       <>                                 ← 保留，内层 fragment（table+pagination+log）
...
515:       </>                                ← 保留，闭合内层 fragment (414)
516:     )}                                  ← 保留，闭合 total>0 三元
517:   </>                                   ← 要删，闭合外层 fragment (361)
518:   )}                                    ← 要删，闭合 activeTab 三元
519: </div>
```

**删除开头（第 358-361 行）：**

将：
```tsx
      <div className="flex-1 p-4 overflow-auto space-y-3">
        {activeTab === 'status' ? (
          <StockdataStatusPanel />
        ) : (
        <>
        {!isLoading && !isError && total > 0 && (
```
改为：
```tsx
      <div className="flex-1 p-4 overflow-auto space-y-3">
        {!isLoading && !isError && total > 0 && (
```

**删除结尾（第 517-518 行）：**

将：
```tsx
          </>
        )}
        </>
        )}
      </div>
```
改为：
```tsx
          </>
        )}
      </div>
```

即：删除第 517 行 `</>`（外层 fragment 闭合）和第 518 行 `)}`（activeTab 三元闭合）。保留第 515 行 `</>`（内层 fragment 闭合）和第 516 行 `)}`（total>0 三元闭合）。

- [ ] **Step 8: 将可折叠日志面板替换为多标签卡片（第 488-514 行附近）**

将：
```tsx
            <div className="rounded-card border border-border bg-surface overflow-hidden mt-3">
              <button
                onClick={() => setLogOpen(v => !v)}
                className="w-full flex items-center justify-between px-3 py-2 text-xs text-foreground hover:bg-elevated/40 transition-colors"
              >
                <span className="font-medium">stockdata 日志</span>
                <ChevronDown className={`h-3.5 w-3.5 text-muted transition-transform ${logOpen ? 'rotate-180' : ''}`} />
              </button>
              {logOpen && (
                <div
                  ref={logScrollRef}
                  onScroll={onLogScroll}
                  className="h-[30vh] overflow-y-auto border-t border-border/60 p-2 font-mono text-[11px] leading-relaxed text-muted"
                >
                  {logLines.length === 0 ? (
                    <div className="text-center py-6 text-muted/60">暂无日志</div>
                  ) : (
                    logLines.map(r => (
                      <div key={r.line} className="whitespace-pre-wrap break-all">
                        {r.text}
                      </div>
                    ))
                  )}
                  {logLoadingMore && <div className="text-center py-2 text-muted/50">加载更早日志...</div>}
                </div>
              )}
            </div>
```
改为：
```tsx
            <div className="rounded-card border border-border bg-surface overflow-hidden mt-3">
              <div className="flex items-center border-b border-border/60">
                {(['log', 'status'] as const).map(tab => {
                  const active = bottomTab === tab
                  return (
                    <button
                      key={tab}
                      onClick={() => setBottomTab(tab)}
                      className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium transition-colors border-b-2 ${
                        active ? 'text-accent border-accent' : 'text-secondary border-transparent hover:text-foreground'
                      }`}
                    >
                      {tab === 'log' ? <Activity className="h-3.5 w-3.5" /> : <Server className="h-3.5 w-3.5" />}
                      {tab === 'log' ? '日志' : '服务状态'}
                    </button>
                  )
                })}
              </div>
              {bottomTab === 'log' ? (
                <div
                  ref={logScrollRef}
                  onScroll={onLogScroll}
                  className="h-[30vh] overflow-y-auto p-2 font-mono text-[11px] leading-relaxed text-muted"
                >
                  {logLines.length === 0 ? (
                    <div className="text-center py-6 text-muted/60">暂无日志</div>
                  ) : (
                    logLines.map(r => (
                      <div key={r.line} className="whitespace-pre-wrap break-all">
                        {r.text}
                      </div>
                    ))
                  )}
                  {logLoadingMore && <div className="text-center py-2 text-muted/50">加载更早日志...</div>}
                </div>
              ) : (
                <div className="h-[30vh] overflow-y-auto p-3">
                  <StockdataStatusPanel />
                </div>
              )}
            </div>
```

- [ ] **Step 9: 运行 lint 验证**

Run: `cd frontend && pnpm lint`
Expected: PASS（无 error，可能有 warning 但不阻塞）

- [ ] **Step 10: 运行类型检查 + 构建验证**

Run: `cd frontend && pnpm build`
Expected: `tsc -b && vite build` 成功，无类型错误

- [ ] **Step 11: 提交**

```bash
git add frontend/src/pages/LocalData.tsx
git commit -m "feat: 本地股市数据页底部多标签整合（服务状态 + 日志）"
```

---

# 追加任务（2026-08-21 二次）

用户批准新增两个需求（详见 spec 追加部分）：菜单改名 + mootdx 服务器连通状态展示。

---

### Task 2: 菜单改名「本地股市数据」→「本地数据」

**Files:**
- Modify: `frontend/src/components/Layout.tsx:79`
- Modify: `frontend/src/pages/LocalData.tsx:332`

**Interfaces:**
- Consumes: 无
- Produces: 侧边栏导航 + 页面标题文案更新（无签名变化）

- [ ] **Step 1: 修改侧边栏菜单 label**

`frontend/src/components/Layout.tsx:79`:
```tsx
  { to: '/local-data', label: '本地股市数据', icon: HardDrive },
```
改为:
```tsx
  { to: '/local-data', label: '本地数据', icon: HardDrive },
```

- [ ] **Step 2: 修改页面标题**

`frontend/src/pages/LocalData.tsx`，`PageHeader` 的 `title="本地股市数据"` 改为 `title="本地数据"`（此时 `LocalData.tsx` 已被 Task 1 改过，title 行已精简，直接改字符串）。

- [ ] **Step 3: 类型检查 + 构建验证**

Run: `cd frontend && pnpm build`
Expected: `tsc -b && vite build` 成功，无类型错误

- [ ] **Step 4: 提交**

```bash
git add frontend/src/components/Layout.tsx frontend/src/pages/LocalData.tsx
git commit -m "feat: 本地数据页/菜单改名为「本地数据」"
```

---

### Task 3: 后端 mootdx 服务器探测 + 独立端点

**Files:**
- Modify: `backend/app/quant/jqengine/datasource/mootdx_src.py`
- Modify: `backend/app/api/data.py`
- Test: `backend/tests/quant/test_mootdx_server_probe.py`（新建）
- Test: `backend/tests/api/test_data_mootdx_servers.py`（新建）

**Interfaces:**
- Consumes: `_TDX_SERVERS`（已有列表）、`_probe(ip, port, timeout)`（已有函数）
- Produces: `probe_servers(timeout=1.5) -> list[dict]`（mootdx_src 模块级函数）、`GET /api/data/mootdx-servers` 端点

**关键约束：**
- 测试从 `backend/` 目录运行 `uv run --extra dev pytest`
- ruff：`cd backend && uv run --extra dev ruff check app`（line-length 100，E501 忽略）
- mypy：`uv run --extra dev mypy app`
- 不复用 `_patched`/`_make_client` 等私密连接逻辑，`probe_servers` 只做 TCP 探测并测延迟

- [ ] **Step 1: 写 failing 测试（probe_servers 返回形态）**

新建 `backend/tests/quant/test_mootdx_server_probe.py`:
```python
"""probe_servers 并发探测全部 mootdx 显式服务器。"""
from app.quant.jqengine.datasource import mootdx_src as msrc


def test_probe_servers_returns_all_with_status(monkeypatch):
    results = {}
    def fake_probe(ip, port, timeout=2.0):
        results[ip] = timeout
        return ip.startswith("115.")
    monkeypatch.setattr(msrc, "_probe", fake_probe)
    out = msrc.probe_servers(timeout=1.5)
    assert len(out) == len(msrc._TDX_SERVERS)
    assert all({k} <= {"ip", "port", "ok", "latency_ms"} for r in out for k in r)
    ok = [r for r in out if r["ok"]]
    assert ok and all(r["latency_ms"] is not None for r in ok)
    fail = [r for r in out if not r["ok"]]
    assert fail and all(r["latency_ms"] is None for r in fail)
    assert results and set(results.values()) == {1.5}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_server_probe.py -v`
Expected: FAIL（`probe_servers` 不存在，ImportError）

- [ ] **Step 3: 实现 probe_servers**

在 `backend/app/quant/jqengine/datasource/mootdx_src.py` 文件底部（`_probe` 之后，模块级）添加:
```python
def probe_servers(timeout: float = 1.5) -> list[dict]:
    """并发探测全部显式 mootdx 服务器 TCP 连通与延迟。

    返回按 _TDX_SERVERS 顺序的列表，每项 {ip, port, ok, latency_ms}；
    latency_ms 为连接建立耗时（毫秒，整数），不可达为 None。
    """
    from concurrent.futures import ThreadPoolExecutor
    import time

    def _one(item):
        ip, port = item
        t0 = time.perf_counter()
        try:
            ok = _probe(ip, port, timeout)
        except Exception:
            ok = False
        if ok:
            lag = int(round((time.perf_counter() - t0) * 1000))
        else:
            lag = None
        return {"ip": ip, "port": port, "ok": ok, "latency_ms": lag}

    with ThreadPoolExecutor(max_workers=len(_TDX_SERVERS)) as ex:
        return list(ex.map(_one, _TDX_SERVERS))
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_server_probe.py -v`
Expected: PASS（1 passed）

- [ ] **Step 5: 写 failing 测试（端点到响应形态 + TTL）**

新建 `backend/tests/api/test_data_mootdx_servers.py`:
```python
"""GET /api/data/mootdx-servers 端点。"""
import importlib
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _probe_stub():
    return [{"ip": "1.1.1.1", "port": 7709, "ok": True, "latency_ms": 12},
            {"ip": "2.2.2.2", "port": 7709, "ok": False, "latency_ms": None}]


def test_mootdx_servers_endpoint(monkeypatch):
    import app.api.data as data_mod
    monkeypatch.setattr(data_mod, "_mootdx_probe", lambda: _probe_stub())
    r = client.get("/api/data/mootdx-servers")
    assert r.status_code == 200
    body = r.json()
    assert body["servers"] == _probe_stub()
    assert "ts" in body
```

- [ ] **Step 6: 运行测试验证失败**

Run: `cd backend && uv run --extra dev pytest tests/api/test_data_mootdx_servers.py -v`
Expected: FAIL（端点 404/字段不存在）

- [ ] **Step 7: 实现端点 + TTL 缓存**

在 `backend/app/api/data.py` 顶部添加模块级 TTL 缓存常量与函数:
```python
_MOOTDX_PROBE_TTL = 10.0
_mootdx_probe_cache: dict = {"ts": 0.0, "data": None}


def _mootdx_probe():
    """探测 mootdx 服务器连通，带 10s TTL 缓存。"""
    import time
    now = time.monotonic()
    if _mootdx_probe_cache["data"] is not None and now - _mootdx_probe_cache["ts"] < _MOOTDX_PROBE_TTL:
        return _mootdx_probe_cache["data"]
    from app.quant.jqengine.datasource.mootdx_src import probe_servers
    data = probe_servers()
    _mootdx_probe_cache.update(ts=now, data=data)
    return data
```

在 `data.py` 中 `stockdata-log` 端点的后面添加:
```python
@router.get("/mootdx-servers")
def data_mootdx_servers():
    """mootdx 所有显式服务器 TCP 连通状态与延迟（10s 缓存）。"""
    return {"servers": _mootdx_probe(), "ts": datetime.now().isoformat()}
```

确认 `data.py` 顶部已 `from datetime import datetime`（若用 `datetime.now()`）；若无 `datetime` 导入，补 `import datetime as _dt` 并用 `_dt.datetime.now().isoformat()`。

- [ ] **Step 8: 运行测试验证通过**

Run: `cd backend && uv run --extra dev pytest tests/api/test_data_mootdx_servers.py tests/quant/test_mootdx_server_probe.py -v`
Expected: PASS（2 passed）

- [ ] **Step 9: ruff + mypy 检查**

Run: `cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app`
Expected: 无新增错误（若有既有全文件问题请记录，不强行修复）

- [ ] **Step 10: 提交**

```bash
git add backend/app/quant/jqengine/datasource/mootdx_src.py backend/app/api/data.py backend/tests/quant/test_mootdx_server_probe.py backend/tests/api/test_data_mootdx_servers.py
git commit -m "feat: mootdx 服务器连通状态探测 + /api/data/mootdx-servers 端点"
```

---

### Task 4: 前端 mootdx 服务器卡片（独立 10s 轮询）

**Files:**
- Modify: `frontend/src/lib/api.ts`
- Modify: `frontend/src/pages/LocalData.tsx`

**Interfaces:**
- Consumes: `MootdxServerRow` 类型、`api.mootdxServers()`、`_mootdx_probe` 返回形态 `{servers: [{ip, port, ok, latency_ms}]}`
- Produces: `StockdataStatusPanel` 内「mootdx 服务器」卡片（独立 `useQuery` 每 10s 刷新）

- [ ] **Step 1: api.ts 加类型 + API 函数**

在 `frontend/src/lib/api.ts` 的 `StockdataStatus` 接口附近添加:
```ts
export interface MootdxServerRow {
  ip: string
  port: number
  ok: boolean
  latency_ms: number | null
}
```

在 `stockdataStatus()` 函数附近添加:
```ts
  async mootdxServers() {
    const r = await this.fetch('/api/data/mootdx-servers')
    return r as { servers: MootdxServerRow[]; ts: string }
  },
```

（注意：先确认 `api.ts` 里 `stockdataStatus` 的定义方式与所在对象/类的结构，保持一致的调用风格。）

- [ ] **Step 2: LocalData.tsx 加 mootdx 独立查询 + 卡片**

在 `StockdataStatusPanel` 组件内（`useQuery` 主查询之后）添加独立查询:
```tsx
  const mx = useQuery({
    queryKey: ['mootdx-servers'],
    queryFn: () => api.mootdxServers(),
    refetchInterval: 10000,
  })
```

在「当前正在执行」卡片上方新增卡片渲染（放在 `isLoading ? (...) : isError ? (...) : (<>...</>)` 的 `<>` 内、最前）:
```tsx
  <div className="rounded-card border border-border bg-surface overflow-hidden">
    <div className="flex items-center justify-between px-3 py-2 border-b border-border/60 text-xs font-medium text-foreground">
      <div className="flex items-center gap-2">
        <Wifi className="h-3.5 w-3.5 text-sky-500" />
        mootdx 服务器
      </div>
      <span className="text-muted font-normal">每 10 秒刷新</span>
    </div>
    <div className="p-3">
      {mx.isLoading ? (
        <div className="space-y-2">{Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-6 w-full" />)}</div>
      ) : mx.isError ? (
        <div className="text-xs text-muted">无法获取 mootdx 服务器状态</div>
      ) : (
        <ul className="grid grid-cols-2 gap-x-4 gap-y-1.5">
          {mx.data?.servers.map(s => (
            <li key={s.ip} className="flex items-center gap-2 text-xs">
              <span className={`inline-block h-2 w-2 rounded-full ${s.ok ? 'bg-emerald-500' : 'bg-red-500'}`} />
              <span className="font-mono text-foreground">{s.ip}:{s.port}</span>
              <span className={`ml-auto ${s.ok ? 'text-secondary' : 'text-red-500'}`}>
                {s.ok ? `${s.latency_ms}ms` : '—'}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  </div>
```

注意：确认 `lucide-react` 是否已有 `Wifi` 导出（`frontend/src/pages/LocalData.tsx` 当前的 icon import 列表）。若无，改用已导入的 `Server` 或 `Activity`。

- [ ] **Step 3: 类型检查 + 构建验证**

Run: `cd frontend && pnpm build`
Expected: `tsc -b && vite build` 成功，无类型错误

- [ ] **Step 4: 提交**

```bash
git add frontend/src/lib/api.ts frontend/src/pages/LocalData.tsx
git commit -m "feat: 服务状态面板新增 mootdx 服务器连通卡片（10s 轮询）"
```
