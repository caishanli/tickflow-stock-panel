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
