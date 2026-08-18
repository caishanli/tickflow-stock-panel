# 本地股市数据页 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增「本地股市数据」页面，展示本地 Parquet 各日期分区的去重标的数（6 类：股市日线/股市分钟线/ETF日线/ETF分钟线/指数日线/指数分钟线），服务端分页。

**Architecture:** 后端新增 `GET /api/data/local-market-stats?page=&page_size=` 端点（`backend/app/api/data.py`，30s TTL 缓存），按 `data/<dir>/date=YYYY-MM-DD/` 分区目录并集得到日期列表，每页日期逐表 `COUNT(DISTINCT symbol)`。前端新增 `/local-data` 页面（react-query + 服务端分页），导航插入「量化模拟盘」下方。

**Tech Stack:** FastAPI + DuckDB + Polars（仅测试造数用），React 18 + TanStack Query + Tailwind（无 UI 库，原生 table）。

**Spec:** `docs/superpowers/specs/2026-08-18-local-market-data-page-design.md`

## Global Constraints

- 后端 lint：`ruff`，line-length 100（E501 忽略），select E,F,I,N,UP,B,SIM,RUF。测试命令必须 `uv run --extra dev`，且从 `backend/` 目录运行。
- 数据目录一律用 `repo.store.data_dir`（= 仓库根 `data/`），**禁止**硬编码 `"data/..."` 相对路径（从 `backend/` 运行会写到 `backend/data/` 遗留库）。
- 列顺序固定：`stock_daily, stock_minute, etf_daily, etf_minute, index_daily, index_minute`；日期行降序（最新在前）。
- 数量口径 = 去重 `symbol` 数；目录不存在/无 parquet → 0；page 越界 → `rows: []`（total 不变）。
- 前端数字千分位显示（`toLocaleString('zh-CN')`），单元格加 `num` class。
- 页面风格与系统一致：`PageHeader` + `rounded-card border border-border bg-surface` 表格容器 + 底部翻页样式复刻 `QuantBacktest.tsx` 的 `StrategyList`。
- 前端无测试脚本，验证 = `pnpm lint` + `pnpm build`。
- 菜单图标用 `HardDrive`（lucide-react），**不用** `Database`——`/data`（数据）菜单已占用该图标，避免菜单里出现两个相同图标。

---

### Task 1: 后端端点 `GET /api/data/local-market-stats`

**Files:**
- Modify: `backend/app/api/data.py`（模块级常量区加 TTL 缓存，`/status` 端点后加新端点）
- Create: `backend/tests/test_local_market_stats.py`

**Interfaces:**
- Consumes: `request.app.state.repo`（`repo.store.data_dir: Path`，`repo.execute_one(sql) -> tuple | None`，见 `backend/app/tickflow/repository.py:347`）
- Produces: `GET /api/data/local-market-stats?page=1&page_size=15` →
  ```json
  { "total": 5230, "page": 1, "page_size": 15,
    "rows": [{ "date": "2026-08-17", "stock_daily": 5214, "stock_minute": 4987,
               "etf_daily": 865, "etf_minute": 0, "index_daily": 562, "index_minute": 0 }] }
  ```
  前端 Task 2/3 依赖此字段名。

- [ ] **Step 1: 写失败测试**

创建 `backend/tests/test_local_market_stats.py`：

```python
"""本地股市数据统计端点测试。"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import data as api


def _write_partition(root: Path, sub: str, d: str, symbols: list[str]) -> None:
    """写一个 date=YYYY-MM-DD 分区, 含 symbol 列。"""
    part = root / sub / f"date={d}"
    part.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "symbol": symbols,
        "date": [date.fromisoformat(d)] * len(symbols),
    })
    df.write_parquet(part / "part.parquet")


class _FakeRepo:
    """最小 repo: data_dir + 真实 duckdb 执行 SQL (SQL 内 read_parquet 读真实文件)。"""

    def __init__(self, data_dir: Path):
        self.store = SimpleNamespace(data_dir=data_dir)
        self._db = duckdb.connect()

    def execute_one(self, sql: str, params: list | None = None) -> tuple | None:
        return self._db.execute(sql, params or []).fetchone()


def _make_app(repo: _FakeRepo) -> FastAPI:
    app = FastAPI()
    app.include_router(api.router)
    app.state.repo = repo
    return app


@pytest.fixture
def repo(tmp_path: Path) -> _FakeRepo:
    root = tmp_path / "data"
    root.mkdir()
    _write_partition(root, "kline_daily", "2026-08-14", ["000001.SZ", "600000.SH", "000002.SZ"])
    _write_partition(root, "kline_daily", "2026-08-17", ["000001.SZ", "600000.SH"])
    _write_partition(root, "kline_minute", "2026-08-17", ["000001.SZ", "600000.SH"])
    _write_partition(root, "kline_etf_daily", "2026-08-17", ["510050.SH", "159001.SZ"])
    _write_partition(root, "kline_etf_minute", "2026-08-17", ["510050.SH"])
    _write_partition(root, "kline_index_daily", "2026-08-17", ["000001.SH"])
    # 故意不建 kline_index_minute 目录
    return _FakeRepo(root)


def test_counts_per_date(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    r = client.get("/api/data/local-market-stats?page=1&page_size=15")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2
    assert [row["date"] for row in body["rows"]] == ["2026-08-17", "2026-08-14"]
    newest = body["rows"][0]
    assert newest["stock_daily"] == 2
    assert newest["stock_minute"] == 2
    assert newest["etf_daily"] == 2
    assert newest["etf_minute"] == 1
    assert newest["index_daily"] == 1
    assert newest["index_minute"] == 0  # 目录不存在 → 0
    older = body["rows"][1]
    assert older["stock_daily"] == 3
    assert older["stock_minute"] == 0  # 该日无分钟数据


def test_pagination(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    body = client.get("/api/data/local-market-stats?page=2&page_size=1").json()
    assert body["total"] == 2
    assert [row["date"] for row in body["rows"]] == ["2026-08-14"]
    empty = client.get("/api/data/local-market-stats?page=99&page_size=15").json()
    assert empty["total"] == 2
    assert empty["rows"] == []


def test_page_size_validation(repo: _FakeRepo) -> None:
    client = TestClient(_make_app(repo))
    assert client.get("/api/data/local-market-stats?page=0").status_code == 422
    assert client.get("/api/data/local-market-stats?page_size=101").status_code == 422


def test_empty_data_dir(tmp_path: Path) -> None:
    repo = _FakeRepo(tmp_path / "data" / "nope")
    client = TestClient(_make_app(repo))
    body = client.get("/api/data/local-market-stats").json()
    assert body == {"total": 0, "page": 1, "page_size": 15, "rows": []}
```

- [ ] **Step 2: 运行确认失败**

Run（在 `backend/` 目录）:
```bash
uv run --extra dev pytest tests/test_local_market_stats.py -q
```
Expected: FAIL — `AttributeError: module 'app.api.data' has no attribute ...` 或 404（端点不存在）。

- [ ] **Step 3: 实现端点**

在 `backend/app/api/data.py` 中：

1. 改导入（文件头 `from datetime import datetime, timezone` → 补 `date`；`from fastapi import APIRouter, Request` → 补 `Query`）：
```python
from datetime import date, datetime, timezone
from fastapi import APIRouter, Query, Request
```

2. 模块级常量区（`_LAST_FINISHED_LOCK` 定义之后，约第 54 行）加：
```python
# ===== 本地股市数据统计(local-market-stats) =====
_LOCAL_MARKET_TABLES: dict[str, str] = {
    "stock_daily": "kline_daily",
    "stock_minute": "kline_minute",
    "etf_daily": "kline_etf_daily",
    "etf_minute": "kline_etf_minute",
    "index_daily": "kline_index_daily",
    "index_minute": "kline_index_minute",
}
_LOCAL_STATS_TTL = 30.0
# 实现修订（Task 1 评审确认）：键含 str(data_dir)，否则多 data_dir 的测试进程内共享缓存
# 会交叉污染（test_empty_data_dir 读到 test_counts_per_date 的陈旧结果）；生产单 data_dir 行为不变。
_local_stats_cache: dict[tuple[str, int, int], tuple[float, dict]] = {}
_local_stats_lock = threading.Lock()
```

3. `status()` 端点之后（第 619 行后）加：
```python
def _local_market_dates(data_dir: Path) -> list[date]:
    """各表 date=* 分区目录并集, 降序。"""
    seen: set[date] = set()
    for sub in _LOCAL_MARKET_TABLES.values():
        d = data_dir / sub
        if not d.is_dir():
            continue
        for child in d.iterdir():
            if not child.is_dir() or not child.name.startswith("date="):
                continue
            try:
                seen.add(datetime.strptime(child.name[5:], "%Y-%m-%d").date())
            except ValueError:
                continue
    return sorted(seen, reverse=True)


def _count_partition_symbols(repo, data_dir: Path, sub: str, d: date) -> int:
    """某表某日期的去重 symbol 数; 目录缺失/异常 → 0。"""
    part = data_dir / sub / f"date={d.isoformat()}"
    if not part.is_dir() or not any(part.rglob("*.parquet")):
        return 0
    try:
        row = repo.execute_one(
            f"SELECT COUNT(DISTINCT symbol) FROM read_parquet('{part.as_posix()}/**/*.parquet')"
        )
        return int(row[0] or 0) if row else 0
    except Exception as e:  # noqa: BLE001
        logger.debug("local-market-stats count failed %s %s: %s", sub, d, e)
        return 0


@router.get("/local-market-stats")
def local_market_stats(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(15, ge=1, le=100),
) -> dict:
    """按日期分区统计本地各表去重标的数(服务端分页, 30s TTL)。"""
    key = (page, page_size)
    now = time.time()
    with _local_stats_lock:
        cached = _local_stats_cache.get(key)
        if cached is not None and (now - cached[0]) < _LOCAL_STATS_TTL:
            return cached[1]

    repo = request.app.state.repo
    data_dir = repo.store.data_dir
    dates = _local_market_dates(data_dir)
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

注意：`data.py` 顶部已导入 `datetime`（`from datetime import datetime, timezone`）、`threading`、`time`、`Path`；Step 1 已补 `date` 导入，勿重复。

- [ ] **Step 4: 运行确认通过**

Run（在 `backend/` 目录）:
```bash
uv run --extra dev pytest tests/test_local_market_stats.py -q
```
Expected: 4 passed。

- [ ] **Step 5: lint + 类型检查**

```bash
uv run --extra dev ruff check app/api/data.py tests/test_local_market_stats.py
uv run --extra dev mypy app/api/data.py
```
Expected: 无报错。

- [ ] **Step 6: 提交**

```bash
git add backend/app/api/data.py backend/tests/test_local_market_stats.py
git commit -m "feat: 本地股市数据统计端点 /api/data/local-market-stats"
```

---

### Task 2: 前端 API 客户端 + query key

**Files:**
- Modify: `frontend/src/lib/api.ts`（`export const api = {` 的 data 区块，约第 1530 行 `dataStatus` 附近）
- Modify: `frontend/src/lib/queryKeys.ts`（`QK` 对象的 `// Data / Pipeline` 区块）

**Interfaces:**
- Consumes: `request<T>`（`frontend/src/lib/api.ts:10`）
- Produces: `api.localMarketStats(page: number, pageSize: number) => Promise<LocalMarketStats>`、`QK.localMarketStats(page, pageSize)`。Task 3 依赖。

- [ ] **Step 1: 加接口类型 + 客户端函数**

在 `frontend/src/lib/api.ts` 的 `DataStatus` 接口（第 2221 行）附近加：

```ts
export interface LocalMarketStatsRow {
  date: string
  stock_daily: number
  stock_minute: number
  etf_daily: number
  etf_minute: number
  index_daily: number
  index_minute: number
}

export interface LocalMarketStats {
  total: number
  page: number
  page_size: number
  rows: LocalMarketStatsRow[]
}
```

在 `export const api = {` 内、`dataStatus` 行（第 1530 行）下方加：

```ts
  dataStatus: () => request<DataStatus>('/api/data/status'),
  localMarketStats: (page: number, pageSize: number) =>
    request<LocalMarketStats>(`/api/data/local-market-stats?page=${page}&page_size=${pageSize}`),
```

- [ ] **Step 2: 加 query key**

在 `frontend/src/lib/queryKeys.ts` 的 `// Data / Pipeline` 区块（`dataStatus` 行附近）加：

```ts
  dataStatus:           ['data-status'] as const,
  localMarketStats:     (page: number, pageSize: number) => ['local-market-stats', page, pageSize] as const,
```

- [ ] **Step 3: 验证类型**

```bash
cd frontend && pnpm build
```
Expected: `tsc -b` + vite build 通过（无未使用导出报错；`pnpm build` 里 tsc 全量类型检查）。

- [ ] **Step 4: 提交**

```bash
git add frontend/src/lib/api.ts frontend/src/lib/queryKeys.ts
git commit -m "feat: 前端本地股市数据 API 客户端与 query key"
```

---

### Task 3: 前端页面 `LocalData.tsx`

**Files:**
- Create: `frontend/src/pages/LocalData.tsx`

**Interfaces:**
- Consumes: `api.localMarketStats`、`QK.localMarketStats`（Task 2）；`PageHeader`（`@/components/PageHeader`）；`EmptyState`（`@/components/EmptyState`）；`Skeleton`（`@/components/data/Skeleton`，注意路径带 `data/`）。
- Produces: 命名导出 `export function LocalData()`。Task 4 的 router 依赖。

- [ ] **Step 1: 写页面组件**

创建 `frontend/src/pages/LocalData.tsx`：

```tsx
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { HardDrive } from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { Skeleton } from '@/components/data/Skeleton'
import { api, type LocalMarketStatsRow } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

const PAGE_SIZE = 15

const COLUMNS: { key: keyof LocalMarketStatsRow; label: string }[] = [
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
  const { data, isLoading, isError } = useQuery({
    queryKey: QK.localMarketStats(page, PAGE_SIZE),
    queryFn: () => api.localMarketStats(page, PAGE_SIZE),
  })

  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const safePage = Math.min(page, totalPages)
  const rows = data?.rows ?? []

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="本地股市数据"
        subtitle={total > 0 ? `本地 Parquet 各日期去重标的数 · 共 ${total} 天` : '本地 Parquet 各日期去重标的数'}
      />
      <div className="flex-1 p-4 overflow-auto space-y-3">
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
            hint="本地尚无任何行情数据，数据同步完成后会在此展示各日期的标的覆盖情况。"
          />
        ) : (
          <>
            <div className="rounded-card border border-border bg-surface overflow-hidden">
              <table className="w-full text-xs">
                <thead className="text-muted bg-elevated/40">
                  <tr className="text-left">
                    <th className="px-3 py-2 font-normal">日期</th>
                    {COLUMNS.map(c => (
                      <th key={c.key} className="px-3 py-2 font-normal text-right">{c.label}</th>
                    ))}
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
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between mt-3 text-xs text-muted">
              <span>共 {total} 天 · 第 {safePage}/{totalPages} 页</span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={safePage <= 1}
                  className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                >
                  上一页
                </button>
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
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

注意：subtitle 用动态文本——加载完成后显示「共 N 天」，避免渲染 `{total}` 字面量。

- [ ] **Step 2: lint + 构建**

```bash
cd frontend && pnpm lint && pnpm build
```
Expected: 无 lint 错误，`tsc -b` + vite build 通过。

（`Skeleton` 签名已确认：`{ w?, h?, rounded?, className? }`，Step 1 代码兼容。）

- [ ] **Step 3: 提交**

```bash
git add frontend/src/pages/LocalData.tsx
git commit -m "feat: 本地股市数据页面"
```

---

### Task 4: 路由 + 侧边栏菜单

**Files:**
- Modify: `frontend/src/router.tsx`
- Modify: `frontend/src/components/Layout.tsx`

**Interfaces:**
- Consumes: `LocalData` 命名导出（Task 3）。
- Produces: 路由 `/local-data`、菜单项（`/quant-sim` 之后、`/stock-analysis` 之前）。

- [ ] **Step 1: 加路由**

`frontend/src/router.tsx`：

1. 第 30 行 `QuantSim` lazy 定义后加：
```tsx
const LocalData = lazy(() => import('./pages/LocalData').then(m => ({ default: m.LocalData })))
```
2. 第 83 行 `{ path: 'quant-sim', element: <QuantSim /> },` 后加：
```tsx
      { path: 'local-data', element: <LocalData /> },
```

- [ ] **Step 2: 加菜单项**

`frontend/src/components/Layout.tsx` 第 71-87 行 `nav` 数组，`/quant-sim` 行后加：

```tsx
  { to: '/quant-sim',     label: '量化模拟盘', icon: Wallet },
  { to: '/local-data', label: '本地股市数据', icon: HardDrive },
  { to: '/stock-analysis',    label: '个股分析', icon: TrendingUp },
```

并确认 `HardDrive` 已在 lucide-react 导入列表（第 20-58 行的 import 块）。若未导入，加入 `HardDrive`（按字母顺序）。

- [ ] **Step 3: lint + 构建**

```bash
cd frontend && pnpm lint && pnpm build
```
Expected: 无 lint 错误，构建通过。

- [ ] **Step 4: 手动冒烟验证（可选，需 dev 环境）**

```bash
setsid ./dev.sh > /tmp/tickflow-dev.log 2>&1 </dev/null & disown
```
另起命令确认 `:3018`/`:3011` 存活后，浏览器访问 `http://localhost:3011/local-data`，确认：
- 侧边栏「量化模拟盘」下方出现「本地股市数据」。
- 表格按日期降序，6 列数字千分位显示，翻页正常。
- 日期数 = `ls data/kline_daily | wc -l` 等目录并集（可抽查某日期与 DuckDB 直查一致）。

- [ ] **Step 5: 提交**

```bash
git add frontend/src/router.tsx frontend/src/components/Layout.tsx
git commit -m "feat: 本地股市数据路由与侧边栏菜单"
```

---

### Task 5: 全量回归

- [ ] **Step 1: 后端全量测试 + lint + mypy**

```bash
cd backend
uv run --extra dev pytest -q
uv run --extra dev ruff check app tests
uv run --extra dev mypy app
```
Expected: 全绿（含 Task 1 的 4 个新用例）。

- [ ] **Step 2: 前端 lint + build**

```bash
cd frontend && pnpm lint && pnpm build
```
Expected: 全绿。

- [ ] **Step 3: 检查 spec 覆盖**

对照 `docs/superpowers/specs/2026-08-18-local-market-data-page-design.md` 逐条核对：
- [ ] 菜单位置（quant-sim 之后）— Task 4 Step 2
- [ ] 6 列表头与列序 — Task 3 Step 1
- [ ] 日期=并集降序、服务端分页 15 行 — Task 1
- [ ] 去重 symbol 口径、目录缺失计 0、越界空 rows — Task 1 测试
- [ ] 千分位 + 底部翻页样式 — Task 3 Step 1
- [ ] 风格一致（PageHeader/表格容器/空态/Skeleton）— Task 3 Step 1

- [ ] **Step 4: 提交（如无未提交改动则跳过）**

```bash
git status
git log --oneline -6
```
Expected: 4 个 feature commit 串行，工作区干净。