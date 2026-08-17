# 回测页新建/返回按钮样式对齐模拟盘 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将量化回测页「新建」「返回」按钮的样式与文案对齐量化模拟盘同款按钮。

**Architecture:** 纯前端单文件改动——只改 `frontend/src/quant/pages/QuantBacktest.tsx` 中两个 `<button>` 的 className 与文案、图标尺寸，样式值逐一抄自 QuantSim.tsx 同款按钮。无后端/接口/数据改动。

**Tech Stack:** React 18 + TypeScript + Tailwind CSS（lucide-react 图标）。

## Global Constraints

- 只改 `frontend/src/quant/pages/QuantBacktest.tsx`，不改其它文件。
- 目标样式字符串（抄自 QuantSim.tsx:203-206 / 554-557，逐字符一致）：
  - 新建：`inline-flex items-center gap-1.5 px-3 h-9 rounded-lg bg-accent text-white text-xs`
  - 返回：`inline-flex items-center gap-1 px-2.5 h-9 rounded-lg bg-elevated text-foreground text-xs`
- 文案：新建保持「新建」（语义=新建策略）；返回「列表」→「返回列表」。
- 图标：`<Plus size={14} />`、`<ArrowLeft size={14} />`（替换 `className="h-4 w-4"`）。
- 按钮位置不变（新建在 PageHeader right 槽内；返回在编辑器顶栏）。
- 验证命令：`cd frontend && pnpm build`（tsc -b && vite build）必须通过。
- `pnpm lint` 仓库级预置失败（eslint 未安装/无配置），跳过不算缺陷。

---

### Task 1: 回测页两个按钮样式对齐模拟盘

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx:131-134`（列表页「新建」按钮）
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx:448-449`（编辑器顶栏「返回」按钮）

**Interfaces:**
- Consumes: 已有 `Plus`、`ArrowLeft`（lucide-react，文件已 import）、`onNew`/`onBack` props。
- Produces: 无新接口（纯视觉改动，后续无任务依赖）。

- [ ] **Step 1: 修改「新建」按钮 className 与图标尺寸**

定位 `QuantBacktest.tsx:131-134`，当前代码：

```tsx
            <button onClick={onNew}
              className="inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-accent text-base text-xs font-medium hover:bg-accent/90 transition-colors">
              <Plus className="h-4 w-4" />新建
            </button>
```

改为（文案「新建」不变）：

```tsx
            <button onClick={onNew}
              className="inline-flex items-center gap-1.5 px-3 h-9 rounded-lg bg-accent text-white text-xs">
              <Plus size={14} />新建
            </button>
```

- [ ] **Step 2: 修改「返回」按钮 className、文案与图标尺寸**

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

- [ ] **Step 3: 构建验证**

Run: `cd frontend && pnpm build`
Expected: `tsc -b && vite build` 均成功（退出码 0），无 TS 错误。

- [ ] **Step 4: 自查改动范围**

确认 `git diff --stat` 仅包含 `frontend/src/quant/pages/QuantBacktest.tsx`（1 file，~2 处改动）；两个 className 与模拟盘目标字符串逐字符一致；无意外改动。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(backtest): 新建/返回按钮样式对齐模拟盘（rounded-lg/accent-white/elevated 返回列表）"
```