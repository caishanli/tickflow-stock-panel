# 本地股市数据页 - 底部多标签整合设计

日期: 2026-08-21
分支: `feat/local-data-bottom-tabs`（基于 `custom-main`）

## 背景与问题

当前 `LocalData.tsx` 页面（`/local-data` 路由）使用顶部两个标签页切换：

- `'stats'`（数据统计）：Parquet 各日期去重标的数表格 + 分页 + 底部可折叠的 stockdata 日志面板
- `'status'`（服务状态）：`StockdataStatusPanel` 组件（服务时间、活动任务、数据缺口、最近任务记录）

问题：

1. 顶部标签页切换时，数据统计表格的滚动位置丢失，用户在两个标签间来回切换体验差
2. 服务状态与 stockdata 日志本质都是 stockdata 服务的运行时信息，却被分到两个不同的顶层标签页，割裂了关联
3. 底部日志面板默认折叠，不易发现；且与"服务状态"分离后用户需要切到顶部另一个标签页才能看状态

## 目标

- 去掉顶部标签页切换，数据统计表格始终作为页面主内容展示
- 将服务状态与 stockdata 日志整合到页面底部，用多标签卡片统一承载
- 底部区域始终展开，用户可在"日志"与"服务状态"之间快速切换

## 设计

### 新布局

```
PageHeader
  title: "本地股市数据"
  subtitle: "本地 Parquet 各日期去重标的数 · 共 N 天"（无服务状态字样）
  right: 无（删除顶部标签切换按钮）

Main content (flex-1, 滚动):
  ├── 筛选栏（日期 + 每页条数）+ 操作按钮（刷新 / 全量检验补齐）
  ├── 数据统计表格
  ├── 分页
  └── 底部多标签卡片（固定展开，~40vh）
        Tab bar: [ 日志 ] [ 服务状态 ]
        ├── 日志 tab: 无限滚动日志查看器（现有逻辑不变）
        └── 服务状态 tab: StockdataStatusPanel 内容（现有组件逻辑不变）
```

### 变更点

1. **删除顶部标签切换**：
   - 删除 `activeTab` 状态（`useState<TabKey>('stats')`）
   - 删除 `TabKey` 类型
   - 删除 `PageHeader` 的 `right` prop（顶部标签切换按钮组）
   - `subtitle` 只显示统计信息，不再根据 `activeTab` 切换文案

2. **底部区域改造**：
   - 删除 `logOpen` 状态（不再需要折叠/展开）
   - 新增 `bottomTab` 状态（`useState<'log' | 'status'>('log')`）
   - 底部卡片从可折叠面板改为固定展开卡片
   - 卡片头部为标签栏（两个按钮切换 `bottomTab`），替换原来的折叠按钮
   - 卡片内容区根据 `bottomTab` 渲染日志或 `StockdataStatusPanel`

3. **日志逻辑保持不变**：
   - `loadLogPage`、`logLines`、`logOffset`、`logLoadingMore`、`logScrollRef` 等全部保留
   - 无限滚动、5s 轮询逻辑不变
   - 唯一变化：日志区域从 `logOpen` 控制显隐改为 `bottomTab === 'log'` 控制显隐
   - 日志的 useEffect 依赖从 `[logOpen, loadLogPage]` 改为 `[bottomTab === 'log', loadLogPage]`

4. **StockdataStatusPanel 组件保持不变**：
   - 组件内部逻辑（useQuery、refetchInterval、extractMissing、TaskRow 等）全部不动
   - 只是被渲染的位置从顶部 `'status'` 标签页移到底部卡片的"服务状态"标签

5. **布局调整**：
   - 主内容区从 `activeTab === 'status' ? <StockdataStatusPanel /> : <>...</>` 的条件渲染
   - 改为始终渲染数据统计部分 + 底部多标签卡片
   - 底部卡片高度建议 `h-[40vh]`（日志查看器内部仍 `h-[30vh]` 滚动，预留标签栏空间）

### 不涉及的变更

- API 端点（`/api/data/stockdata-log`、`/api/data/stockdata-status`、`/api/data/local-market-stats`）不变
- 后端代码不变
- 路由配置不变（仍是 `/local-data`）
- 侧边栏导航不变
- 数据统计表格、分页、筛选、行级刷新/检验逻辑不变
- `StockdataStatusPanel` 组件内部实现不变
- 日志的无限滚动、5s 轮询、分页加载逻辑不变

## 测试

- 前端无测试脚本，用 `pnpm lint` + `pnpm build`（`tsc -b && vite build`）验证
- 手动验证：
  - 页面不再有顶部标签切换按钮
  - 数据统计表格始终可见
  - 底部卡片始终展开，有两个标签可切换
  - 切到"日志"标签后无限滚动 + 5s 轮询正常
  - 切到"服务状态"标签后 5s 自动刷新 + 手动刷新按钮正常
  - 标签切换不丢失各自的滚动/数据状态（日志滚动位置保留、服务状态不重置）

## 风险

- 日志 useEffect 依赖改写时如果条件写错可能导致频繁重载日志。需确保只在 `bottomTab` 从非 `'log'` 切到 `'log'` 时重新初始化日志加载，从 `'log'` 切走时清理定时器。
- 底部卡片固定 `40vh` 在小屏可能挤压表格空间，但当前页面已有 `flex-1 overflow-auto`，表格区域会自适应。

---

# 追加需求（2026-08-21 二次）

用户批准底部多标签设计后新增两个需求：

## 需求 A：菜单改名

侧边栏菜单「本地股市数据」→「本地数据」，页面标题同步改为「本地数据」。

涉及文件：
- `frontend/src/components/Layout.tsx:79`（侧边栏菜单 label）
- `frontend/src/pages/LocalData.tsx`（`PageHeader` 的 `title`）

路由 `/local-data` 不变，组件文件名 `LocalData.tsx` 不变。

## 需求 B：mootdx 服务器连通状态展示

在 `StockdataStatusPanel`（服务状态标签页）里新增「mootdx 服务器」卡片，展示 mootdx 全部显式服务器 IP 的 TCP 连通状态与延迟，**每 10s 轮询刷新**（与主状态面板 5s 轮询解耦）。

### 后端

1. **`backend/app/quant/jqengine/datasource/mootdx_src.py`**：新增公共函数 `probe_servers(timeout=1.5) -> list[dict]`
   - 用 `ThreadPoolExecutor` 并发探测 `_TDX_SERVERS` 全部 16 个 IP（端口 7709）
   - 每个返回 `{ip, port, ok, latency_ms}`：`ok` 为 TCP 探测结果，`latency_ms` 为连接建立耗时（毫秒，整数，失败为 `None`）
   - 复用已有 `_probe(ip, port, timeout)` 函数作为探测实现

2. **`backend/app/api/data.py`**：新增端点 `GET /api/data/mootdx-servers`
   - 调用 `probe_servers()` 并返回 `{"servers": [ {ip, port, ok, latency_ms}, ... ], "ts": "..."}`
   - 加 ~10s TTL 模块级缓存（避免每 5s 主轮询时反复探测 16 个 IP）
   - 依赖 mootdx/pandas：主后端 venv 已验证 `import pandas, mootdx` 可用

3. **测试**：`backend/app/quant/jqengine/datasource/mootdx_src.py` 的 `probe_servers` 用 `monkeypatch` mock `_probe`，验证并发返回正确；`data.py` 端点测试 mock `probe_servers`，验证响应形态与 TTL 缓存。

### 前端

1. **`frontend/src/lib/api.ts`**：加 `MootdxServerRow` 类型（`{ip, port, ok, latency_ms}`）+ `mootdxServers()` API 函数（`GET /api/data/mootdx-servers`）

2. **`frontend/src/pages/LocalData.tsx`** 的 `StockdataStatusPanel`：
   - 新增「mootdx 服务器」卡片，**独立 `useQuery({ queryKey: ['mootdx-servers'], queryFn: ..., refetchInterval: 10000 })`**
   - 每行：状态点（绿=可达 `ok=true` / 红=不可达 `ok=false`）+ `ip:port` + 延迟 ms（不可达显示 `—`）
   - 布局放在「当前正在执行」卡片上方，与主状态查询互不影响

### 不在范围内

- mootdx 服务器列表来源不变（仍用 `_TDX_SERVERS`）
- 不改 mootdx 连接逻辑、轮换逻辑、超时逻辑
- 不改 `StockdataStatus` 已有字段（mootdx 服务器用独立端点，不塞进主状态响应）
- 后端 `stockdata-status` 端点行为不变
