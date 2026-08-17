# 回测页新建/返回按钮样式对齐模拟盘 — 设计

日期：2026-08-17

## 背景

量化回测（QuantBacktest.tsx）与量化模拟盘（QuantSim.tsx）同属量化工作台，但回测页两个基础按钮（列表页「新建」、编辑器顶栏「返回」）的视觉样式与模拟盘不一致：回测页新建按钮文字颜色用 `text-base`（在 accent 背景上观感发暗）、圆角用 `rounded-btn`、带 `font-medium` 与 hover 效果；返回按钮文案为「列表」而非「返回列表」，背景用 `bg-base text-secondary` 带边框。模拟盘同款按钮为 `bg-accent text-white rounded-lg` 与 `bg-elevated text-foreground rounded-lg`。

用户要求：回测页这两个按钮与模拟盘样式一致。

## 范围

- 仅前端 `frontend/src/quant/pages/QuantBacktest.tsx`，两个按钮的 className 与文案调整。
- 无后端、无接口、无数据改动。

## 改动

### 1. 列表页「新建」按钮（QuantBacktest.tsx:131-134，位于 PageHeader `right` 槽内，位置不变）

目标样式对齐 QuantSim.tsx:203-206「新建模拟」：

```
当前:  inline-flex items-center gap-1.5 h-9 px-3 rounded-btn bg-accent text-base text-xs font-medium hover:bg-accent/90 transition-colors
目标:  inline-flex items-center gap-1.5 px-3 h-9 rounded-lg bg-accent text-white text-xs
```

- 文案保持「新建」（回测页新建的是**策略**，模拟盘「新建模拟」建的是账户，语义不同，不抄文案）。

### 2. 编辑器顶栏「返回」按钮（QuantBacktest.tsx:448-449）

目标样式对齐 QuantSim.tsx:554-557「返回列表」：

```
当前:  inline-flex items-center gap-1.5 h-9 px-2.5 rounded-btn border border-border bg-base text-secondary hover:text-foreground transition-colors
目标:  inline-flex items-center gap-1 px-2.5 h-9 rounded-lg bg-elevated text-foreground text-xs
```

- 文案「列表」→「返回列表」。

### 3. 图标

沿用现有 `Plus`/`ArrowLeft` 图标组件，尺寸从 `className="h-4 w-4"` 改为 `size={14}`（与模拟盘 `<Plus size={14} />` / `<ArrowLeft size={14} />` 一致）。

## 验证

- `cd frontend && pnpm build`（tsc -b && vite build）通过。
- `pnpm lint` 仓库级预置失败（eslint 未安装/无配置），与本改动无关，跳过。
- 后端无改动，无需后端测试。

## 非目标

- 不改按钮位置（回测页新建在右上 PageHeader，模拟盘在列表左上——保持各自现状）。
- 不改其它按钮样式（保存策略/删除选中/编译运行等不在本次范围）。