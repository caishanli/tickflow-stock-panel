# 移动端响应式侧栏（抽屉 Overlay）设计

日期：2026-08-21　分支：`feature/mobile-responsive-sidebar`

## 问题

`Layout.tsx` 根容器固定 `grid-cols-[14rem_1fr]`，所有视口宽度都渲染 14rem 侧栏。
手机浏览器（~390px）上侧栏占约 2/3 屏宽，内容区挤到无法使用。全代码库此前
无任何响应式处理。

## 方案（已确认）

抽屉 Overlay + 断点 md=768px。改动仅 `frontend/src/components/Layout.tsx`，
桌面端（≥768px）零变化。

### 布局

- 根容器：`grid-cols-[14rem_1fr]` → `grid-cols-1 md:grid-cols-[14rem_1fr]`；
  `h-screen` → `h-dvh`（修复手机地址栏伸缩导致的高度误差）。
- `<aside>`：手机端 `fixed inset-y-0 left-0 z-50 w-64 max-w-[80vw]`，默认
  `-translate-x-full` 藏于屏外，打开时滑入并带阴影；`md:` 恢复 static +
  `translate-x-0` 回到网格列。
- 遮罩层：`fixed inset-0 z-40 bg-black/50 md:hidden`，点击关闭。
- 移动端顶栏：`md:hidden`、sticky top-0，置于 main 内容流首位——hamburger
  按钮 + TickFlow 迷你标识，滚动时保持可见。

### 交互

- hamburger 开抽屉；点菜单项 / 遮罩 / Esc 关闭；路由变化自动关闭
  （`useLocation` effect）。
- 抽屉内容不变（品牌区/菜单/数据源状态/实时开关/主题设置），nav 加
  `overscroll-contain` 防滚动穿透。

### 不做

页面内部布局改造、底部 Tab、手势滑动。

## 验证

`pnpm lint`、`pnpm build`；Chrome DevTools 手机模拟（390px）人工核对
看板/自选/本地数据页与桌面端回归。
