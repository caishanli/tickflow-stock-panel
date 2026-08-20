# 本地股市数据页 — 日期范围筛选 + 每页数量选择 设计

## 背景

「本地股市数据」页（`frontend/src/pages/LocalData.tsx`）按日期分区展示本地 Parquet 各表去重标的数。当前：

- 后端 `GET /api/data/local-market-stats` 已支持 `page` / `page_size`（默认 15，上限 100），30s TTL 缓存。
- 前端 `PAGE_SIZE = 15` 硬编码，无日期筛选，分页仅上一页/下一页按钮。

用户需要：① 按日期范围筛选日期；② 每页可设置显示数量。

## 需求

1. **日期范围筛选**：起止日期（含边界），默认不过滤（显示全部日期）。
2. **每页数量**：固定选项 15 / 30 / 50 / 100，默认 15。
3. 筛选或每页数量变化时，重置到第 1 页并重新请求。

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

### 前端

**`frontend/src/lib/api.ts`**

- `localMarketStats(page, pageSize, start?, end?)` → 构造 query string，含 `start_date` / `end_date`（有值时）。
- `LocalMarketStats` 接口不变。

**`frontend/src/lib/queryKeys.ts`**

- `localMarketStats: (page, pageSize, start?, end?) => ['local-market-stats', page, pageSize, start ?? null, end ?? null]`

**`frontend/src/pages/LocalData.tsx`**

- 新增 state：`startDate: string | ''`、`endDate: string | ''`、`pageSize: number`（默认 15）。
- 移除硬编码 `PAGE_SIZE = 15`。
- 顶部工具栏（一行，在「全量检验补齐」按钮同排、按钮左侧）：
  - `DatePicker`（起）— 复用 `@/components/DatePicker`，value=startDate
  - 分隔符（`~`）
  - `DatePicker`（止）— value=endDate
  - `<select>` 每页数量：15 / 30 / 50 / 100
- 任一控件变化：
  - `setPage(1)`（重置到第 1 页）
  - 触发 queryKey 变化 → 自动重新请求（react-query 依 queryKey 刷新）
- 查询：`useQuery({ queryKey: QK.localMarketStats(page, pageSize, startDate || undefined, endDate || undefined), queryFn: () => api.localMarketStats(page, pageSize, startDate || undefined, endDate || undefined) })`
- 分页信息文本随 `total` 展示；空筛选结果时 `total === 0` 走现有 EmptyState（「暂无本地数据」文案在筛选场景下显示为"筛选范围内无数据"）。

## 数据流

```
筛选/页大小变化 → setStart/setEnd/setPageSize + setPage(1)
                → queryKey 变化 → useQuery 重新请求
                → 后端按日期过滤 + 分页 → 表格重渲染
```

## 错误处理

- 后端日期格式非法 → 400。
- 起止日期为空字符串 → 前端视为"未筛选"，不传参。

## 测试

**后端 `backend/tests/test_local_market_stats.py`**

- 日期范围过滤：`start_date` 起、`end_date` 止、双端过滤、边界含端点。
- 过滤后无匹配日期 → 空列表 + total=0。
- 非法日期 → 400。
- 不同日期参数不共享 TTL 缓存（改日期参数后返回结果不同）。

**前端**：无测试脚本；`pnpm build`（`tsc -b && vite build`）类型检查通过。

## 范围外

- 不做日期快选（近7天/近30天等快捷按钮）——YAGNI，用户未要求。
- 不改动「检验」「全量检验补齐」逻辑。
- 不做前端本地过滤（后端已按日期过滤）。