# 回测页新建/返回按钮样式对齐模拟盘 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将量化回测页「新建」「返回」按钮的样式与文案对齐量化模拟盘同款按钮，并将「新建」移到与模拟盘相同的位置（列表区左上）。

**Architecture:** 纯前端单文件改动——只改 `frontend/src/quant/pages/QuantBacktest.tsx` 中两个 `<button>` 的 className 与文案、图标尺寸，以及「新建」按钮的位置（从 PageHeader right 槽移到列表区容器内首行）。样式值逐一抄自 QuantSim.tsx 同款按钮。无后端/接口/数据改动。

**Tech Stack:** React 18 + TypeScript + Tailwind CSS（lucide-react 图标）。

## Global Constraints

- 只改 `frontend/src/quant/pages/QuantBacktest.tsx`，不改其它文件。
- 目标样式字符串（抄自 QuantSim.tsx:203-206 / 554-557，逐字符一致）：
  - 新建：`inline-flex items-center gap-1.5 px-3 h-9 rounded-lg bg-accent text-white text-xs`
  - 返回：`inline-flex items-center gap-1 px-2.5 h-9 rounded-lg bg-elevated text-foreground text-xs`
- 文案：新建保持「新建」（语义=新建策略）；返回「列表」→「返回列表」。
- 位置（对齐 QuantSim.tsx:201-207）：新建按钮移到 `<div className="flex-1 p-4 overflow-auto">` 容器内、表格卡片之前，包一层 `<div className="flex items-center">`；容器 className 加 `space-y-3`。「删除选中」留在 PageHeader right 槽不动。
- 空态文案「点击右上角新建」→「点击左上角新建」。
- 图标：`<Plus size={14} />`、`<ArrowLeft size={14} />`（替换 `className="h-4 w-4"`）。
- 验证命令：`cd frontend && pnpm build`（tsc -b && vite build）必须通过。
- `pnpm lint` 仓库级预置失败（eslint 未安装/无配置），跳过不算缺陷。

---

### Task 1: 回测页两个按钮样式对齐模拟盘

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx:118-139`（列表页「新建」按钮：从 PageHeader right 槽移到列表区左上 + 空态文案）
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx:448-449`（编辑器顶栏「返回」按钮）

**Interfaces:**
- Consumes: 已有 `Plus`、`ArrowLeft`（lucide-react，文件已 import）、`onNew`/`onBack` props。
- Produces: 无新接口（纯视觉改动，后续无任务依赖）。

- [ ] **Step 1: 把「新建」按钮从 PageHeader 移到列表区左上，并改样式与图标尺寸**

定位 `QuantBacktest.tsx:118-138`，当前代码：

```tsx
  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="量化回测"
        subtitle="策略 · RQAlpha · 聚宽式 · 实时 SSE"
        right={
          <div className="flex items-center gap-2">
            {selected.size > 0 && (
              <button onClick={() => setDelIds([...selected])}
                className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors">
                <Trash2 className="h-3.5 w-3.5" />删除选中({selected.size})
              </button>
            )}
            <button onClick={onNew}
              className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-accent text-base text-xs font-medium hover:bg-accent/90 transition-colors">
              <Plus className="h-4 w-4" />新建
            </button>
          </div>
        }
      />
      <div className="flex-1 p-4 overflow-auto">
        <div className="rounded-card border border-border bg-surface overflow-hidden">
```

改为（新建按钮移入列表区容器首行，包 `<div className="flex items-center">`，容器加 `space-y-3`；right 槽只留删除选中；空态文案见 Step 2）：

```tsx
  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="量化回测"
        subtitle="策略 · RQAlpha · 聚宽式 · 实时 SSE"
        right={
          <div className="flex items-center gap-2">
            {selected.size > 0 && (
              <button onClick={() => setDelIds([...selected])}
                className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-danger/15 text-danger text-xs font-medium hover:bg-danger/25 transition-colors">
                <Trash2 className="h-3.5 w-3.5" />删除选中({selected.size})
              </button>
            )}
          </div>
        }
      />
      <div className="flex-1 p-4 overflow-auto space-y-3">
        <div className="flex items-center">
          <button onClick={onNew}
            className="inline-flex items-center gap-1.5 px-3 h-9 rounded-lg bg-accent text-white text-xs">
            <Plus size={14} />新建
          </button>
        </div>
        <div className="rounded-card border border-border bg-surface overflow-hidden">
```

- [ ] **Step 2: 修正空态文案**

定位 `QuantBacktest.tsx:160`，当前：

```tsx
                <tr><td colSpan={10} className="px-3 py-10 text-center text-muted">暂无策略，点击右上角新建</td></tr>
```

改为：

```tsx
                <tr><td colSpan={10} className="px-3 py-10 text-center text-muted">暂无策略，点击左上角新建</td></tr>
```

- [ ] **Step 3: 修改「返回」按钮 className、文案与图标尺寸**

定位 `QuantBacktest.tsx:448-449`，当前代码：

```tsx
        <button onClick={onBack} className="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border bg-base text-secondary hover:text-foreground transition-colors">
          <ArrowLeft className="h-4 w-4" />列表
        </button>
```

改为：

```tsx
        <button onClick={onBack} className="inline-flex items-center gap-1 px-2.5 h-9 rounded-lg bg-elevated text-foreground text-xs">
          <ArrowLeft size={14} />返回列表
        </button>
```

- [ ] **Step 4: 构建验证**

Run: `cd frontend && pnpm build`
Expected: `tsc -b && vite build` 均成功（退出码 0），无 TS 错误。

- [ ] **Step 5: 自查改动范围**

确认 `git diff --stat` 仅包含 `frontend/src/quant/pages/QuantBacktest.tsx`（1 file）；两个 className 与模拟盘目标字符串逐字符一致；「删除选中」按钮与 PageHeader 结构未破坏；`onNew`/`onBack` 回调保留；无意外改动。

- [ ] **Step 6: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(backtest): 新建/返回按钮样式对齐模拟盘（rounded-lg/accent-white/elevated 返回列表）+ 新建按钮移至列表左上"
```