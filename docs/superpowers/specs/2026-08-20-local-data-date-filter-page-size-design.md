# 本地股市数据页 — 日期筛选 + 每页数量 + 刷新按钮 + 日志栏 设计

## 背景

「本地股市数据」页（`frontend/src/pages/LocalData.tsx`）按日期分区展示本地 Parquet 各表去重标的数。当前：

- 后端 `GET /api/data/local-market-stats` 已支持 `page` / `page_size`（默认 15，上限 100），30s TTL 缓存。
- 前端 `PAGE_SIZE = 15` 硬编码，无日期筛选，分页仅上一页/下一页按钮。

用户需要：① 按日期范围筛选日期；② 每页可设置显示数量；③ 每行末尾刷新按钮（更新当天数据情况）；④ 表格右上角刷新按钮（更新当前显示表格数据情况）；⑤ 数据异步加载；⑥ 页面底部日志栏显示 stockdata 服务日志（倒序加载）。

## 需求

1. **日期范围筛选**：起止日期（含边界），默认不过滤（显示全部日期）。
2. **每页数量**：固定选项 10 / 20 / 50 / 100，默认 10。
3. 筛选或每页数量变化时，重置到第 1 页并重新请求。
4. **行级刷新**：每行末尾加「刷新」按钮，绕过缓存重新统计该日期各表 count（不联网、不触发补齐）。
5. **表格右上角刷新**：在「全量检验补齐」旁加「刷新」按钮，绕过缓存重新统计当前页（含当前筛选/页码）。
6. **异步加载**：右上角刷新整页 loading；单行刷新仅该行 loading，不动整页。
7. **页面底部日志栏**：表格下方空白区域显示 stockdata 服务日志，可折叠；默认倒序显示一屏，滚轮到底加载更早日志；打开时自动轮询。

## 设计

### 后端 `backend/app/api/data.py`

`GET /api/data/local-market-stats` 新增两个可选查询参数：

- `start_date: str | None` — `YYYY-MM-DD`
- `end_date: str | None` — `YYYY-MM-DD`

逻辑（在 `local_market_stats` 函数内）：

```
dates = _local_market_dates(data_dir)              # 现状：全量，降序
if start_date: dates = [d for d in dates if d >= start]
if end_date:   dates = [d for d in dates if d <= end]
total = len(dates)
page_dates = dates[(page-1)*page_size : page*page_size]
```

- 日期解析失败 → `HTTPException(400)`。
- `total` 为过滤后的天数（现有语义"共 N 天"随筛选变化）。
- TTL 缓存 key 从 `(data_dir, page, page_size)` 扩展为 `(data_dir, page, page_size, start_date, end_date)`，保证不同筛选不串缓存。

新增第三个可选查询参数：

- `refresh: bool = False` — 为 True 时**绕过 TTL 缓存**，强制重新扫描统计后回填缓存。

刷新实现：跳过缓存读（`cached` 判断分支），直接重算 `rows`，结果照常写入缓存（后续请求复用）。**不新增独立单日端点**——单行刷新复用整页端点，仅重算当前页（数据量小，每表一个分区文件，几十 ms 级）。

**新增日志接口 `GET /api/data/stockdata-log`：**

- 参数：`offset: int = 0`（从最新行往回偏移）、`limit: int = 100`（`ge=1 le=500`）。
- 读取 `data/stockdata.log`（guardian 写入路径，主后端可访问共享 `data/`）。
- 按**行号倒序**返回：`offset=0` 返回最新 `limit` 行，`offset=limit` 返回更早的 `limit` 行。
- 响应：`{ total, offset, limit, rows: [{ line: number, text: string }] }`，`total` 为文件总行数。
- 行号从 1 起，`line` 用于前端去重/排序。
- 文件不存在 → `{ total: 0, rows: [] }`；读取失败 → 500。
- 每次读取按需读文件末尾（`readlines` + 切片倒序；文件 1.7MB/13k 行，全读可接受，后续如过大再优化）。

### 前端

**`frontend/src/lib/api.ts`**

- `localMarketStats(page, pageSize, start?, end?, refresh?)` → 构造 query string，含 `start_date` / `end_date`（有值时）、`refresh=1`（为真时）。
- 新增 `stockdataLog(offset, limit)` → `GET /api/data/stockdata-log?offset=..&limit=..`，返回 `StockdataLog`。
- `LocalMarketStats` 接口不变。
- 新增 `interface StockdataLogRow { line: number; text: string }`、`interface StockdataLog { total: number; offset: number; limit: number; rows: StockdataLogRow[] }`。

**`frontend/src/lib/queryKeys.ts`**

- `localMarketStats: (page, pageSize, start?, end?, refreshNonce?) => ['local-market-stats', page, pageSize, start ?? null, end ?? null, refreshNonce ?? 0]`

**`frontend/src/pages/LocalData.tsx`**

- 新增 state：`startDate: string | ''`、`endDate: string | ''`、`pageSize: number`（默认 10）、`refreshNonce: number`（默认 0，右上角刷新计数）、`refreshingRow: string | null`（当前刷新中的单行日期）。
- 移除硬编码 `PAGE_SIZE = 15`。
- 顶部工具栏（一行，在「全量检验补齐」按钮同排、按钮左侧）：
  - `DatePicker`（起）— 复用 `@/components/DatePicker`，value=startDate
  - 分隔符（`~`）
  - `DatePicker`（止）— value=endDate
  - `<select>` 每页数量：10 / 20 / 50 / 100
- 任一控件变化：
  - `setPage(1)`（重置到第 1 页）
  - 触发 queryKey 变化 → 自动重新请求（react-query 依 queryKey 刷新）
- 查询：`useQuery({ queryKey: QK.localMarketStats(page, pageSize, startDate || undefined, endDate || undefined, refreshNonce), queryFn: () => api.localMarketStats(page, pageSize, startDate || undefined, endDate || undefined, refreshNonce > 0) })`
- 分页信息文本随 `total` 展示；空筛选结果时 `total === 0` 走现有 EmptyState（「暂无本地数据」文案在筛选场景下显示为"筛选范围内无数据"）。

**刷新按钮与异步加载：**

- **右上角「刷新」**：放在「全量检验补齐」按钮左侧。点击后整页 loading 并绕过缓存重算当前页：
  - 用 `useQuery` 的 `refetch()` 无法传参，因此单独维护 `refreshNonce` state：点击时 `setRefreshNonce(n+1)`，将 `refreshNonce` 并入 queryKey → queryKey 变化触发新请求（`isFetching` 期间整页 loading，`data` 保留避免闪烁）。
  - queryFn 统一走 `api.localMarketStats(page, pageSize, start, end, true)`（refresh 参数恒为 true，普通筛选变化也走同 queryKey 但 refresh 不同——**注意**：refresh 不能并入 queryKey，否则普通翻页也强制绕过缓存。设计：refresh 仅在 refreshNonce 触发时生效）。

  精确方案：**queryKey 包含 page/pageSize/start/end/refreshNonce**；queryFn 中 `refresh = refreshNonce > 0`（refreshNonce 初值 0，正常请求 false；点击刷新 +1 后请求 true）。翻页/筛选只改前置项，refreshNonce 保持上次值 → 需在筛选/翻页时 `setRefreshNonce(0)` 复位，避免后续请求一直绕过缓存。兜底：每次 refresh 完成后把 refreshNonce 复位为 0。

- **行级「刷新」**：表格操作列加「刷新」按钮（在「检验」旁）。点击后仅该行按钮 loading，不动整页：
  - 独立 `useMutation`：`refreshingRow = row.date`，mutationFn 调 `api.localMarketStats(page, pageSize, start, end, true)`。
  - 成功后用 `qc.setQueryData` 把返回的 rows 中对应 date 的行合并回当前页 query 缓存（或直接覆盖整页 rows），完成后清 `refreshingRow`。
  - 不触发 queryKey 变化 → 整页不 loading，仅该行按钮 spinner。

**页面底部日志栏：**

- 布局：表格下方空白区域，标题栏「stockdata 日志」+ 折叠按钮（三角/chevron）。折叠状态 `logOpen: boolean`（默认 false）。
- 展开时：日志容器固定高度（约 30vh），`overflow-y-auto`，等宽字体小字号显示。
- 数据加载：
  - `logLines: StockdataLogRow[]` 状态存已加载的行（新→旧倒序）。
  - 打开时首次加载：`api.stockdataLog(0, limit=100)` → 最新一屏，`logLines` 从新到旧排列。
  - 滚动到底（`scrollTop + clientHeight >= scrollHeight - threshold`）：`setLogOffset(offset + limit)`，加载更早的 100 行，追加到 `logLines` 末尾（保持新→旧倒序）。
  - 自动轮询：`useEffect` 在 `logOpen` 时设 `setInterval(5s)` 调 `api.stockdataLog(0, limit)` 拉最新一屏，按 `line` 去重合并到 `logLines` 头部（不重置滚动位置）；关闭时清理 interval。
- 行渲染：显示 `line`（可选）+ 原始日志文本（时间戳+级别+logger+消息），`whitespace-pre-wrap break-all` 防长行撑破。
- 空日志：显示「暂无日志」。

## 数据流

```
筛选/页大小变化 → setStart/setEnd/setPageSize + setPage(1) + setRefreshNonce(0)
                → queryKey 变化 → useQuery 重新请求
                → 后端按日期过滤 + 分页 → 表格重渲染

右上角刷新 → refreshNonce+1 → queryKey 变化 → 后端绕过缓存重算当前页 → 整页 loading
          → 完成后 refreshNonce 复位 0
行级刷新   → useMutation + setQueryData 合并单行结果 → 该行按钮 loading → 数据更新

日志栏     → 打开 → 拉最新一屏(offset=0) → 倒序显示
          → 滚动到底 → offset+=limit → 拉更早日志追加
          → 每5s → 拉最新一屏按 line 去重合并到头部
```

## 错误处理

- 后端日期格式非法 → 400。
- 起止日期为空字符串 → 前端视为"未筛选"，不传参。
- 刷新请求失败 → toast 错误提示，保留原数据，loading 态复位。
- 日志读取失败 → 500；前端 toast 提示并保留已加载日志。

## 测试

**后端 `backend/tests/test_local_market_stats.py`**

- 日期范围过滤：`start_date` 起、`end_date` 止、双端过滤、边界含端点。
- 过滤后无匹配日期 → 空列表 + total=0。
- 非法日期 → 400。
- 不同日期参数不共享 TTL 缓存（改日期参数后返回结果不同）。
- `refresh=1` 绕过缓存：修改磁盘分区后，普通请求仍返回旧缓存值，`refresh=1` 返回新值。
- `stockdata-log`：写临时日志文件（构造 fixture）→ 倒序分页返回正确、offset/limit 正确、total 正确、文件不存在返回空。

**前端**：无测试脚本；`pnpm build`（`tsc -b && vite build`）类型检查通过。

## 范围外

- 不做日期快选（近7天/近30天等快捷按钮）——YAGNI，用户未要求。
- 不改动「检验」「全量检验补齐」逻辑。
- 不做前端本地过滤（后端已按日期过滤）。
- 不加独立单日统计端点（单行刷新复用整页端点）。
- 日志栏不做关键词搜索/级别过滤/自动滚动到底——用户未要求。