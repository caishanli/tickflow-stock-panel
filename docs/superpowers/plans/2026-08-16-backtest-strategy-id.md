# 回测列表/详情展示策略 id 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 量化回测列表加「编号」列、详情页顶栏展示策略 id，交互与模拟盘一致。

**Architecture:** 纯前端改动，仅 `frontend/src/quant/pages/QuantBacktest.tsx` 一个文件。列表页在 checkbox 列后插「编号」列展示 `s.id`；详情页顶栏名称输入框后展示 `strategyId`。复制交互完全复刻 `QuantSim.tsx` 的 textarea + `document.execCommand` 模式。后端接口已返回 `s.id`，无后端改动。

**Tech Stack:** React 18 + TS + Vite（无前端测试框架）。

## Global Constraints

- 只用现有 CSS token（`font-mono` / `text-muted` / `text-accent` 等），不新增样式类、不改 tailwind 配置。
- 复制逻辑沿用 `document.execCommand`（与模拟盘一致，不引入 clipboard API）。
- 列表编号列复制时 `stopPropagation`，不得触发行点击进入详情（模拟盘用 `.copy-id` class 处理）。
- 不改模拟盘 `QuantSim.tsx`；改文件仅 `frontend/src/quant/pages/QuantBacktest.tsx`。
- 每任务完成后跑 `cd frontend && pnpm lint` 与 `pnpm build`（tsc -b && vite build）验证。

---

### Task 1: 列表页加「编号」列

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx`（StrategyList 组件，约 143-190 行）

**Interfaces:**
- Consumes: `listStrategiesWithLatest()` 返回的策略行已有 `id` 字段（8 位 hex 字符串）。
- Produces: 列表出现「编号」列，展示 `s.id`，点击复制 toast「已复制」，不触发进详情。

- [ ] **Step 1: 加表头**

在 `QuantBacktest.tsx` 的 checkbox 表头行（第 144-147 行）之后插入「编号」表头：

```tsx
<th className="px-3 py-2 font-normal">编号</th>
```

- [ ] **Step 2: 加数据单元格**

在每行 checkbox 单元格（第 170-173 行）之后插入编号单元格（复制自 `QuantSim.tsx:235-255` 的模式，`s.id` 换成 id 变量名）：

```tsx
<td className="px-3 py-2 text-muted font-mono" onClick={(e) => {
  const target = e.target as HTMLElement
  if (target.closest('.copy-id')) return
  onOpen(s.id)
}}>
  <span className="copy-id inline-flex items-center gap-1 cursor-pointer hover:text-accent transition-colors"
    onClick={(e) => {
      e.stopPropagation()
      const ta = document.createElement('textarea')
      ta.value = s.id
      ta.style.position = 'fixed'
      ta.style.left = '-9999px'
      document.body.appendChild(ta)
      ta.select()
      try { document.execCommand('copy'); toast('已复制', 'success', 'top') }
      catch { toast('复制失败', 'error') }
      document.body.removeChild(ta)
    }}>
    {s.id}
  </span>
</td>
```

注意：行点击 `onOpen(s.id)` 在行 `<tr>` 上（第 167 行），td 内也加了 onClick 防止空白处点击穿透（与模拟盘一致）。

- [ ] **Step 3: 修正空态 colSpan**

空列表提示行（第 159 行）`colSpan={9}` 改为 `colSpan={10}`。

- [ ] **Step 4: Lint + 构建验证**

Run: `cd frontend && pnpm lint && pnpm build`
Expected: 无 error，build 成功。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(backtest): 列表页加策略编号列，点击复制"
```

### Task 2: 详情页顶栏展示策略 id

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx`（StrategyEditor 组件顶栏，第 428-429 行名称输入框之后）

**Interfaces:**
- Consumes: `strategyId` prop（Task 1 无依赖，可并行）。
- Produces: 详情页顶栏展示 `strategyId`，`font-mono` 灰字，`title="点击复制策略ID"`，点击复制 toast「策略ID已复制」。

- [ ] **Step 1: 名称输入框后加 id span**

在名称输入框（第 428-429 行）之后插入（复制自 `QuantSim.tsx:559-571` 的模式）：

```tsx
<span className="text-xs text-muted font-mono cursor-pointer hover:text-accent transition-colors"
  title="点击复制策略ID"
  onClick={() => {
    const ta = document.createElement('textarea')
    ta.value = strategyId
    ta.style.position = 'fixed'
    ta.style.left = '-9999px'
    document.body.appendChild(ta)
    ta.select()
    try { document.execCommand('copy'); toast('策略ID已复制', 'success', 'top') }
    catch { toast('复制失败', 'error') }
    document.body.removeChild(ta)
  }}>{strategyId}</span>
```

- [ ] **Step 2: Lint + 构建验证**

Run: `cd frontend && pnpm lint && pnpm build`
Expected: 无 error，build 成功。

- [ ] **Step 3: 手测**

浏览器打开量化回测：列表编号列 hover 变 accent、点击复制成功且不进入详情；进入详情页顶栏 id 点击复制成功。`toast` 提示正常。

- [ ] **Step 4: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(backtest): 详情页顶栏展示策略 id，点击复制"
```
