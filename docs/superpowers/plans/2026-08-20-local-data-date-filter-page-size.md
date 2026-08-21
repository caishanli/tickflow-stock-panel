# 本地股市数据页 — 日期筛选 + 每页数量 + 刷新 + 日志栏 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为「本地股市数据」页增加日期范围筛选、每页数量选择、行级/整页刷新按钮，以及页面底部倒序分页加载的 stockdata 日志栏。

**Architecture:** 后端 `data.py` 的 `local-market-stats` 端点扩展 `start_date`/`end_date`/`refresh` 查询参数并加缓存 key；新增 `stockdata-log` 分页端点读 `data/stockdata.log`。前端 `LocalData.tsx` 增加筛选工具栏、刷新按钮（`refreshNonce` 驱动整页刷新、`useMutation`+`setQueryData` 驱动单行刷新）、可折叠日志栏（倒序分页 + 5s 轮询）。`api.ts`/`queryKeys.ts` 签名同步扩展。

**Tech Stack:** FastAPI (Python 3.11), React 18 + Vite + TypeScript, TanStack Query, Polars/DuckDB 测试.

## Global Constraints

- 后端测试命令：`cd backend && uv run --extra dev pytest tests/test_local_market_stats.py`（绝不用裸 `uv run pytest`）。
- 后端 lint：`uv run --extra dev ruff check app`（line-length 100，忽略 E501）；类型：`uv run --extra dev mypy app`。既有 RUF002/003 中文标点噪声可忽略。
- 前端构建/类型：`cd frontend && pnpm build`（`tsc -b && vite build`）。前端无测试脚本。
- `local-market-stats` 现有 `page`(≥1)/`page_size`(1-100) 参数保持不变；`page_size` 前端默认改为 10（下拉 10/20/50/100）。
- `local-market-stats` 30s TTL 缓存 key 必须包含 `(data_dir, page, page_size, start_date, end_date)`；`refresh=1` 时绕过缓存读但照常写缓存。
- stockdata 日志路径 = `data/stockdata.log`（guardian 写入，与 `kline_daily` 同根 `data/` 下）。在端点内用 `request.app.state.repo.store.data_dir / "stockdata.log"` 解析。
- 日志按**行号倒序**返回：`offset=0` 最新，`offset=limit` 更早；行号从 1 起。
- 前端日志栏：默认倒序最新一屏，滚动到底加载更早，打开时每 5s 轮询；折叠时停止轮询。

---

### Task 1: 后端 `local-market-stats` 日期范围过滤 + `refresh` 参数

**Files:**
- Modify: `backend/app/api/data.py:666-696`（`local_market_stats` 端点）
- Test: `backend/tests/test_local_market_stats.py`

**Interfaces:**
- Consumes: `_local_market_dates(data_dir) -> list[date]`（降序）、`_count_partition_symbols(repo, data_dir, sub, d)`（已存在）。
- Produces: `GET /api/data/local-market-stats` 端点新增查询参数 `start_date: str|None`、`end_date: str|None`、`refresh: bool=False`。行为：`total` 为过滤后天数；`refresh=True` 绕过缓存读。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/test_local_market_stats.py` 末尾追加：

```python
def test_date_range_filter_start(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get("/api/data/local-market-stats?start_date=2026-08-16").json()
    assert [r["date"] for r in body["rows"]] == ["2026-08-17"]
    assert body["total"] == 1


def test_date_range_filter_end(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get("/api/data/local-market-stats?end_date=2026-08-15").json()
    assert [r["date"] for r in body["rows"]] == ["2026-08-14"]
    assert body["total"] == 1


def test_date_range_filter_both_inclusive(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get(
        "/api/data/local-market-stats?start_date=2026-08-14&end_date=2026-08-17"
    ).json()
    assert [r["date"] for r in body["rows"]] == ["2026-08-17", "2026-08-14"]
    assert body["total"] == 2


def test_date_range_filter_no_match(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get(
        "/api/data/local-market-stats?start_date=2026-09-01&end_date=2026-09-30"
    ).json()
    assert body["total"] == 0
    assert body["rows"] == []


def test_date_range_filter_invalid_date(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    assert client.get("/api/data/local-market-stats?start_date=not-a-date").status_code == 400


def test_local_market_stats_refresh_bypasses_cache(repo: _FakeRepo, tmp_path: Path) -> None:
    # 复用默认 repo 首次请求 → 命中缓存
    client = TestClient(_make_app(repo))
    first = client.get("/api/data/local-market-stats?page=1&page_size=15").json()
    assert first["rows"][0]["stock_daily"] == 2

    # 磁盘改动: 往 2026-08-17 分区加一个新 symbol
    part = repo.store.data_dir / "kline_daily" / "date=2026-08-17"
    df = pl.DataFrame({
        "symbol": ["000001.SZ", "600000.SH", "000002.SZ"],
        "date": [date(2026, 8, 17)] * 3,
    })
    df.write_parquet(part / "part.parquet")

    # 普通请求仍返回旧缓存 (2)
    cached = client.get("/api/data/local-market-stats?page=1&page_size=15").json()
    assert cached["rows"][0]["stock_daily"] == 2

    # refresh=1 绕过缓存 → 新值 (3)
    refreshed = client.get("/api/data/local-market-stats?page=1&page_size=15&refresh=1").json()
    assert refreshed["rows"][0]["stock_daily"] == 3
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run --extra dev pytest tests/test_local_market_stats.py::test_date_range_filter_start tests/test_local_market_stats.py::test_local_market_stats_refresh_bypasses_cache -v`
Expected: FAIL（端点尚未接受新参数/未过滤/未绕过缓存）。

- [ ] **Step 3: 实现最小代码**

修改 `backend/app/api/data.py` 的 `local_market_stats`（当前 666-696 行）。将函数签名与缓存 key、过滤逻辑、refresh 分支一并替换：

```python
@router.get("/local-market-stats")
def local_market_stats(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(15, ge=1, le=100),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    refresh: bool = Query(False),
) -> dict:
    """按日期分区统计本地各表去重标的数(服务端分页, 30s TTL, 可筛选日期范围/强制刷新)。"""
    def _parse(d: str | None) -> date | None:
        if not d:
            return None
        try:
            return datetime.fromisoformat(d).date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期需为 YYYY-MM-DD")

    start = _parse(start_date)
    end = _parse(end_date)

    repo = request.app.state.repo
    data_dir = repo.store.data_dir
    key = (str(data_dir), page, page_size, start_date, end_date)
    now = time.time()
    if not refresh:
        with _local_stats_lock:
            cached = _local_stats_cache.get(key)
            if cached is not None and (now - cached[0]) < _LOCAL_STATS_TTL:
                return cached[1]

    dates = _local_market_dates(data_dir)
    if start:
        dates = [d for d in dates if d >= start]
    if end:
        dates = [d for d in dates if d <= end]
    total = len(dates)
    page_dates = dates[(page - 1) * page_size : page * page_size]

    rows: list[dict] = []
    for d in page_dates:
        row: dict = {"date": d.isoformat()}
        for key_, sub in _LOCAL_MARKET_TABLES.items():
            row[key_] = _count_partition_symbols(repo, data_dir, sub, d)
        rows.append(row)

    result = {"total": total, "page": page, "page_size": page_size, "rows": rows}
    with _local_stats_lock:
        _local_stats_cache[key] = (now, result)
    return result
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run --extra dev pytest tests/test_local_market_stats.py -q`
Expected: 全部通过（含既有 11 个测试）。

- [ ] **Step 5: Commit**

```bash
git add backend/app/api/data.py backend/tests/test_local_market_stats.py
git commit -m "feat: local-market-stats 支持日期范围筛选与强制刷新参数"
```

---

### Task 2: 后端 `stockdata-log` 倒序分页端点

**Files:**
- Modify: `backend/app/api/data.py`（追加端点，放在 `local_market_stats` 之后）
- Test: `backend/tests/test_local_market_stats.py`

**Interfaces:**
- Consumes: `request.app.state.repo.store.data_dir`（日志路径 `data_dir/stockdata.log`）。
- Produces: `GET /api/data/stockdata-log?offset=&limit=` → `{ total, offset, limit, rows: [{ line, text }] }`。`offset=0` 最新；`limit` 默认 100（1-500）。

- [ ] **Step 1: 写失败测试**

在测试文件追加（需要控制日志路径，用 monkeypatch 指向临时文件）：

```python
def _write_log(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_stockdata_log_reverse_pagination(monkeypatch, repo: _FakeRepo, tmp_path: Path) -> None:
    from fastapi import FastAPI
    log_path = tmp_path / "data" / "stockdata.log"
    _write_log(log_path, [f"line-{i}" for i in range(1, 11)])  # 10 行
    # repo 的 data_dir 指向临时数据目录
    repo.store = SimpleNamespace(data_dir=tmp_path / "data")
    app = FastAPI()
    app.include_router(api.router)
    app.state.repo = repo
    app.state.capabilities = SimpleNamespace(has=lambda *_: True)
    client = TestClient(app)

    first = client.get("/api/data/stockdata-log?offset=0&limit=5").json()
    assert first["total"] == 10
    assert [r["text"] for r in first["rows"]] == ["line-10", "line-9", "line-8", "line-7", "line-6"]
    assert [r["line"] for r in first["rows"]] == [10, 9, 8, 7, 6]

    second = client.get("/api/data/stockdata-log?offset=5&limit=5").json()
    assert [r["text"] for r in second["rows"]] == ["line-5", "line-4", "line-3", "line-2", "line-1"]


def test_stockdata_log_missing_file(repo: _FakeRepo, tmp_path: Path) -> None:
    from fastapi import FastAPI
    repo.store = SimpleNamespace(data_dir=tmp_path / "data" / "none")
    app = FastAPI()
    app.include_router(api.router)
    app.state.repo = repo
    app.state.capabilities = SimpleNamespace(has=lambda *_: True)
    client = TestClient(app)
    body = client.get("/api/data/stockdata-log").json()
    assert body == {"total": 0, "offset": 0, "limit": 100, "rows": []}


def test_stockdata_log_limit_validation(repo: _FakeRepo, tmp_path: Path) -> None:
    from fastapi import FastAPI
    repo.store = SimpleNamespace(data_dir=tmp_path / "data")
    app = FastAPI()
    app.include_router(api.router)
    app.state.repo = repo
    app.state.capabilities = SimpleNamespace(has=lambda *_: True)
    client = TestClient(app)
    assert client.get("/api/data/stockdata-log?limit=0").status_code == 422
    assert client.get("/api/data/stockdata-log?limit=501").status_code == 422
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run --extra dev pytest tests/test_local_market_stats.py::test_stockdata_log_reverse_pagination -v`
Expected: FAIL（404，端点不存在）。

- [ ] **Step 3: 实现最小代码**

在 `backend/app/api/data.py` 的 `local_market_stats` 之后追加：

```python
@router.get("/stockdata-log")
def stockdata_log(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
) -> dict:
    """stockdata 服务日志, 按行号倒序分页返回(offset=0 最新)。"""
    log_path = request.app.state.repo.store.data_dir / "stockdata.log"
    if not log_path.is_file():
        return {"total": 0, "offset": offset, "limit": limit, "rows": []}
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        logger.warning("stockdata-log read failed: %s", e)
        raise HTTPException(status_code=500, detail="读取日志失败")
    lines = text.splitlines()
    total = len(lines)
    # 倒序切片: offset 从最新行往回
    start = max(0, total - offset - limit)
    end = max(0, total - offset)
    selected = lines[start:end]  # 文件顺序正序切片
    selected.reverse()  # 倒序返回
    rows = [{"line": total - offset - i, "text": ln} for i, ln in enumerate(selected)]
    return {"total": total, "offset": offset, "limit": limit, "rows": rows}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run --extra dev pytest tests/test_local_market_stats.py -q`
Expected: 全部通过。

- [ ] **Step 5: Commit**

```bash
git add backend/app/api/data.py backend/tests/test_local_market_stats.py
git commit -m "feat: 新增 stockdata-log 倒序分页端点"
```

---

### Task 3: 前端 api.ts / queryKeys.ts 扩展

**Files:**
- Modify: `frontend/src/lib/api.ts`（`localMarketStats` 签名 + 新增 `stockdataLog` + 接口）
- Modify: `frontend/src/lib/queryKeys.ts`（`localMarketStats` 签名）

**Interfaces:**
- Consumes: 现有 `request<T>(path, init?)` 辅助函数。
- Produces: `api.localMarketStats(page, pageSize, start?, end?, refresh?)`、`api.stockdataLog(offset, limit)`、`StockdataLogRow`、`StockdataLog` 类型；`QK.localMarketStats(page, pageSize, start?, end?, refreshNonce?)`。

- [ ] **Step 1: 修改 `api.ts` 的 `localMarketStats` 与接口**

替换现有（1532-1533 行）：

```ts
  localMarketStats: (page: number, pageSize: number, start?: string, end?: string, refresh?: boolean) => {
    const params = new URLSearchParams({ page: String(page), page_size: String(pageSize) })
    if (start) params.set('start_date', start)
    if (end) params.set('end_date', end)
    if (refresh) params.set('refresh', '1')
    return request<LocalMarketStats>(`/api/data/local-market-stats?${params.toString()}`)
  },
  stockdataLog: (offset: number, limit: number) =>
    request<StockdataLog>(`/api/data/stockdata-log?offset=${offset}&limit=${limit}`),
```

在 `LocalMarketStats` 接口定义之后（2296 行附近）追加：

```ts
export interface StockdataLogRow {
  line: number
  text: string
}

export interface StockdataLog {
  total: number
  offset: number
  limit: number
  rows: StockdataLogRow[]
}
```

- [ ] **Step 2: 修改 `queryKeys.ts`**

替换现有（50 行）：

```ts
  localMarketStats:     (page: number, pageSize: number, start?: string, end?: string, refreshNonce?: number) =>
                          ['local-market-stats', page, pageSize, start ?? null, end ?? null, refreshNonce ?? 0] as const,
```

- [ ] **Step 3: 构建类型检查**

Run: `cd frontend && pnpm build`
Expected: 通过（此时 LocalData.tsx 仍用旧签名，`refresh`/`start`/`end` 可选，不报错）。

- [ ] **Step 4: Commit**

```bash
git add frontend/src/lib/api.ts frontend/src/lib/queryKeys.ts
git commit -m "feat: api/queryKeys 扩展 localMarketStats 参数并新增 stockdataLog"
```

---

### Task 4: 前端 LocalData.tsx — 筛选工具栏 + 每页数量 + 刷新按钮

**Files:**
- Modify: `frontend/src/pages/LocalData.tsx`

**Interfaces:**
- Consumes: `api.localMarketStats(page, pageSize, start?, end?, refresh?)`、`QK.localMarketStats(page, pageSize, start?, end?, refreshNonce?)`、`DatePicker` 组件。
- Produces: 完整页面功能（筛选、分页、行级/整页刷新）。日志栏在 Task 5 追加。

- [ ] **Step 1: 先读当前文件确认结构**

Run: 已在前文给出完整 `LocalData.tsx`（155 行）。以它为基础修改。

- [ ] **Step 2: 整体重写 `LocalData.tsx`**

将整个文件内容替换为：

```tsx
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { HardDrive, RefreshCw, Wrench, ChevronDown } from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { Skeleton } from '@/components/data/Skeleton'
import { DatePicker } from '@/components/DatePicker'
import { toast } from '@/components/Toast'
import { api, type LocalMarketStatsRow } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

const PAGE_SIZE_OPTIONS = [10, 20, 50, 100]

type CountKey = Exclude<keyof LocalMarketStatsRow, 'date'>

const COLUMNS: { key: CountKey; label: string }[] = [
  { key: 'stock_daily', label: '股市日线' },
  { key: 'stock_minute', label: '股市分钟线' },
  { key: 'etf_daily', label: 'ETF日线' },
  { key: 'etf_minute', label: 'ETF分钟线' },
  { key: 'index_daily', label: '指数日线' },
  { key: 'index_minute', label: '指数分钟线' },
]

function fmtCount(n: number): string {
  return n.toLocaleString('zh-CN')
}

export function LocalData() {
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [refreshNonce, setRefreshNonce] = useState(0)
  const [refreshingRow, setRefreshingRow] = useState<string | null>(null)
  const qc = useQueryClient()

  const start = startDate || undefined
  const end = endDate || undefined

  const { data, isLoading, isFetching, isError } = useQuery({
    queryKey: QK.localMarketStats(page, pageSize, start, end, refreshNonce),
    queryFn: () => api.localMarketStats(page, pageSize, start, end, refreshNonce > 0),
  })

  const refreshStats = () => {
    qc.invalidateQueries({ queryKey: ['local-market-stats'] })
  }

  const checkDayMut = useMutation({
    mutationFn: (date: string) => api.checkDay(date),
    onSuccess: (_data, date) => {
      toast(`已触发 ${date} 检验补齐`, 'success', 'top')
      setTimeout(refreshStats, 3000)
    },
  })

  const checkFullMut = useMutation({
    mutationFn: () => api.checkFull(),
    onSuccess: () => {
      toast('已触发全量检验补齐', 'success', 'top')
      setTimeout(refreshStats, 3000)
    },
  })

  const refreshPageMut = useMutation({
    mutationFn: () => api.localMarketStats(page, pageSize, start, end, true),
    onSuccess: () => {
      setRefreshNonce(0)
      refreshStats()
    },
  })

  const refreshRowMut = useMutation({
    mutationFn: (date: string) => api.localMarketStats(page, pageSize, start, end, true),
    onSuccess: (data, date) => {
      qc.setQueryData(
        QK.localMarketStats(page, pageSize, start, end, refreshNonce),
        (old: typeof data | undefined) => {
          if (!old) return old
          const newRow = data.rows.find(r => r.date === date)
          if (!newRow) return old
          return { ...old, rows: old.rows.map(r => (r.date === date ? newRow : r)) }
        },
      )
    },
    onSettled: () => setRefreshingRow(null),
  })

  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / pageSize))
  const safePage = Math.min(page, totalPages)
  const rows = data?.rows ?? []

  const onFilterChange = () => {
    setPage(1)
    setRefreshNonce(0)
  }

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="本地股市数据"
        subtitle={total > 0 ? `本地 Parquet 各日期去重标的数 · 共 ${total} 天` : '本地 Parquet 各日期去重标的数'}
      />
      <div className="flex-1 p-4 overflow-auto space-y-3">
        {!isLoading && !isError && total > 0 && (
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <DatePicker value={startDate} onChange={v => { setStartDate(v); onFilterChange() }} placeholder="起始日期" />
              <span className="text-muted text-xs">~</span>
              <DatePicker value={endDate} onChange={v => { setEndDate(v); onFilterChange() }} placeholder="结束日期" />
              <select
                value={pageSize}
                onChange={e => { setPageSize(Number(e.target.value)); onFilterChange() }}
                className="h-7 rounded-btn border border-border bg-elevated px-2 text-xs text-foreground"
              >
                {PAGE_SIZE_OPTIONS.map(n => (
                  <option key={n} value={n}>{n} 条/页</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-1.5">
              <button
                onClick={() => refreshPageMut.mutate()}
                disabled={refreshPageMut.isPending}
                className="px-3 py-1.5 rounded-btn border border-border bg-elevated text-secondary hover:text-foreground disabled:opacity-40 transition-colors flex items-center gap-1.5"
                title="刷新当前页统计"
              >
                <RefreshCw className={`h-3 w-3 ${refreshPageMut.isPending ? 'animate-spin' : ''}`} />
                {refreshPageMut.isPending ? '刷新中...' : '刷新'}
              </button>
              <button
                onClick={() => checkFullMut.mutate()}
                disabled={checkFullMut.isPending}
                className="px-3 py-1.5 rounded-btn border border-border bg-elevated text-secondary hover:text-foreground disabled:opacity-40 transition-colors flex items-center gap-1.5"
              >
                <Wrench className="h-3 w-3" />
                {checkFullMut.isPending ? '校验中...' : '全量检验补齐'}
              </button>
            </div>
          </div>
        )}
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 8 }).map((_, i) => (
              <Skeleton key={i} className="h-9 w-full" />
            ))}
          </div>
        ) : isError ? (
          <EmptyState title="加载失败" hint="无法获取本地数据统计，请稍后重试或检查后端服务。" />
        ) : total === 0 ? (
          <EmptyState
            icon={HardDrive}
            title="暂无本地数据"
            hint={startDate || endDate ? '当前日期范围内无数据。' : '本地尚无任何行情数据，数据同步完成后会在此展示各日期的标的覆盖情况。'}
          />
        ) : (
          <>
            <div className="rounded-card border border-border bg-surface overflow-hidden relative">
              {isFetching && !isLoading && (
                <div className="absolute inset-0 bg-elevated/30 flex items-center justify-center z-10">
                  <div className="text-xs text-muted flex items-center gap-2">
                    <RefreshCw className="h-4 w-4 animate-spin" />
                    刷新中...
                  </div>
                </div>
              )}
              <table className="w-full text-xs">
                <thead className="text-muted bg-elevated/40">
                  <tr className="text-left">
                    <th className="px-3 py-2 font-normal">日期</th>
                    {COLUMNS.map(c => (
                      <th key={c.key} className="px-3 py-2 font-normal text-right">{c.label}</th>
                    ))}
                    <th className="px-3 py-2 font-normal text-right">操作</th>
                  </tr>
                </thead>
                <tbody className="text-foreground">
                  {rows.map(row => (
                    <tr key={row.date} className="border-t border-border/60 hover:bg-elevated/60 transition-colors">
                      <td className="px-3 py-2 font-mono num">{row.date}</td>
                      {COLUMNS.map(c => (
                        <td key={c.key} className="px-3 py-2 text-right num text-muted">
                          {fmtCount(row[c.key])}
                        </td>
                      ))}
                      <td className="px-3 py-2 text-right whitespace-nowrap">
                        <button
                          onClick={() => { setRefreshingRow(row.date); refreshRowMut.mutate(row.date) }}
                          disabled={refreshRowMut.isPending}
                          className="px-2 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors inline-flex items-center gap-1"
                        >
                          {refreshingRow === row.date
                            ? <RefreshCw className="h-3 w-3 animate-spin" />
                            : <RefreshCw className="h-3 w-3" />}
                          刷新
                        </button>
                        <button
                          onClick={() => checkDayMut.mutate(row.date)}
                          disabled={checkDayMut.isPending}
                          className="px-2 py-1 ml-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                        >
                          检验
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between mt-3 text-xs text-muted">
              <span>共 {total} 天 · 第 {safePage}/{totalPages} 页</span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => { setPage(p => Math.max(1, p - 1)); setRefreshNonce(0) }}
                  disabled={safePage <= 1}
                  className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                >
                  上一页
                </button>
                <button
                  onClick={() => { setPage(p => Math.min(totalPages, p + 1)); setRefreshNonce(0) }}
                  disabled={safePage >= totalPages}
                  className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                >
                  下一页
                </button>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
```

- [ ] **Step 3: 构建类型检查**

Run: `cd frontend && pnpm build`
Expected: 通过（日志栏未加，`ChevronDown` 暂未使用——若 lint 报未使用则从 import 移除，见下）。

- [ ] **Step 4: 移除未使用 import**

`ChevronDown` 用于 Task 5 日志栏；本任务若 `pnpm build` 不报未使用（vite/tsc 对未使用 import 默认允许），可保留；若 `tsc -b` 的 `noUnusedLocals` 报错，从 import 中移除 `ChevronDown`（Task 5 再加回）。按实际报错处理。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/pages/LocalData.tsx
git commit -m "feat: 本地股市数据页日期筛选/每页数量/行级与整页刷新"
```

---

### Task 5: 前端 LocalData.tsx — 底部 stockdata 日志栏

**Files:**
- Modify: `frontend/src/pages/LocalData.tsx`
- Modify: `frontend/src/lib/queryKeys.ts`（可选，若日志用 query 则加 key；本计划用本地 state + 手写轮询，不引入 query key）

**Interfaces:**
- Consumes: `api.stockdataLog(offset, limit) -> StockdataLog`、`StockdataLogRow`。
- Produces: 页面底部可折叠日志栏（`logOpen`、`logLines`、`logOffset`、`logLoadingMore`）。

- [ ] **Step 1: 加日志栏状态与逻辑**

在 `LocalData` 组件内、`return` 之前追加状态与 effect（保持现有 import，补 `useEffect`、`useRef`、`useCallback`）：

在文件顶部 import 处，把 `import { useState } from 'react'` 改为：

```ts
import { useState, useEffect, useRef } from 'react'
```

在 `refreshRowMut` 之后追加：

```ts
  const [logOpen, setLogOpen] = useState(false)
  const [logLines, setLogLines] = useState<import('@/lib/api').StockdataLogRow[]>([])
  const [logOffset, setLogOffset] = useState(0)
  const [logLoadingMore, setLogLoadingMore] = useState(false)
  const logScrollRef = useRef<HTMLDivElement>(null)
  const LOG_LIMIT = 100

  const loadLogPage = useCallback(async (offset: number) => {
    try {
      const res = await api.stockdataLog(offset, LOG_LIMIT)
      setLogLines(prev => {
        const seen = new Set(prev.map(r => r.line))
        const merged = [...prev]
        for (const r of res.rows) {
          if (!seen.has(r.line)) merged.push(r)
        }
        return merged
      })
      return res
    } catch {
      toast('加载日志失败', 'error')
      return null
    }
  }, [])

  // 打开时首次加载 + 每 5s 轮询最新一屏
  useEffect(() => {
    if (!logOpen) return
    setLogLines([])
    setLogOffset(0)
    loadLogPage(0)
    const t = setInterval(() => loadLogPage(0), 5000)
    return () => clearInterval(t)
  }, [logOpen, loadLogPage])

  // 滚动到底加载更早日志
  const onLogScroll = useCallback(() => {
    const el = logScrollRef.current
    if (!el || logLoadingMore) return
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 40) {
      setLogLoadingMore(true)
      const nextOffset = logOffset + LOG_LIMIT
      loadLogPage(nextOffset).then(res => {
        if (res && res.rows.length > 0) setLogOffset(nextOffset)
        setLogLoadingMore(false)
      })
    }
  }, [logLoadingMore, logOffset, loadLogPage])
```

- [ ] **Step 2: 渲染日志栏**

在分页信息 `<div className="flex items-center justify-between mt-3 ...">...</div>` 之后、`</>` 之前（即表格区块末尾）追加日志栏：

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

- [ ] **Step 3: 构建类型检查**

Run: `cd frontend && pnpm build`
Expected: 通过。

- [ ] **Step 4: Commit**

```bash
git add frontend/src/pages/LocalData.tsx
git commit -m "feat: 本地股市数据页底部倒序分页 stockdata 日志栏"
```

---

### Task 6: 全量验证

- [ ] **Step 1: 后端测试**

Run: `cd backend && uv run --extra dev pytest tests/test_local_market_stats.py tests/quant/test_mootdx_backfill_coverage.py tests/quant/test_stockdata_scheduler.py tests/quant/test_stockdata_handlers.py -q`
Expected: 全部通过。

- [ ] **Step 2: 后端 lint + mypy**

Run: `uv run --extra dev ruff check app/api/data.py` 与 `uv run --extra dev mypy app/api/data.py`
Expected: 无新增错误（既有 RUF002/003 中文标点噪声可忽略；mypy 仅既有错误）。

- [ ] **Step 3: 前端构建**

Run: `cd frontend && pnpm build`
Expected: 通过。

- [ ] **Step 4: 人工冒烟（可选）**

后端运行中（3018），浏览器访问本地股市数据页，验证：日期筛选、每页切换、右上角刷新、单行刷新、日志栏打开/滚动加载更早/自动刷新。

- [ ] **Step 5: 提交验证（无改动则跳过）**

若冒烟发现问题，修复后单独提交。
