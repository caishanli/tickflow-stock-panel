# 量化模拟盘成交记录持仓时长 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 模拟盘「成交记录」表格新增「持仓时长」列（买入/卖出行都显示，单位=交易日个数），并让鼠标悬停整行高亮。

**Architecture:** 后端 `sim_status` 响应 data 附带真实交易日历 `trade_days`（`StockDataClient.get_trade_days`，失败降级工作日）；前端持有可能未全量成交列表，按数据变化用 FIFO 分标的配对重算每行持仓时长（买入行在对应卖出行到达时自动回填）。不改 schema、不改 SSE 结构。

**Tech Stack:** 后端 FastAPI + `app.quant.datasource.network_client.StockDataClient`；前端 React 18 + TS + TanStack Query + tailwind。

## Global Constraints

- 后端测试命令（backend/ 下）：`uv run --extra dev pytest tests/quant/test_api_quant.py -v`
- 后端 lint / 类型检查：
  - `uv run --extra dev ruff check app`（line-length 100，select E,F,I,N,UP,B,SIM,RUF，忽略 E501）
  - `uv run --extra dev mypy app`
- 前端无测试脚本；验证：`cd frontend && pnpm lint` 与 `pnpm build`（`tsc -b && vite build`）
- `asyncio_mode = "auto"`；测试从 `backend/` 目录跑，`from app...` 导入
- 只改量化模拟盘成交记录（复测 `sim_status` + `QuantSim.tsx`），回到测成交表不动

---

### Task 1: 后端 `sim_status` 附带交易日历

**Files:**
- Modify: `backend/app/quant/api/quant.py:4-20`（imports）、`quant.py:296-306`（`sim_status`）、`quant.py:309` 附近（新增 `_build_trade_days` 助手，放在 `sim_status` 之后）
- Test: `backend/tests/quant/test_api_quant.py`

**Interfaces:**
- Consumes: `StockDataClient().get_trade_days(start_date: str, end_date: str) -> list[str]`（返回 `YYYY-MM-DD`，见 `stockdata/sources.py:466`）
- Produces: `_build_trade_days(account: dict, trades: list[dict]) -> list[str]`；`sim_status` data 新增键 `trade_days`。前端 Task 2 依赖 `st.trade_days`（`getSimStatus` 的 `data` 解包后直接可读）。

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_api_quant.py` 末尾追加：

```python
def test_sim_status_returns_trade_days(client, monkeypatch):
    import types

    from app.quant import db
    import app.quant.datasource.network_client as nc

    db.insert_sim_account("a1", "acc", 100000.0, 0.03, "created",
                          strategy_id="", start_date="2026-01-05")
    days = ["2026-01-05", "2026-01-06", "2026-01-07"]
    fake = types.SimpleNamespace(get_trade_days=lambda s, e: days)
    monkeypatch.setattr(nc, "StockDataClient", lambda: fake)

    r = client.get("/api/quant/sim/accounts/a1/status")
    assert r.status_code == 200
    assert r.json()["data"]["trade_days"] == days


def test_build_trade_days_network_failure_fallback(monkeypatch):
    import app.quant.datasource.network_client as nc
    from app.quant.api import quant as qmod

    class Boom:
        def __init__(self):
            raise RuntimeError("network down")

    monkeypatch.setattr(nc, "StockDataClient", Boom)

    acct = {"start_date": "2026-08-03"}  # 周一，当天为工作日
    days = qmod._build_trade_days(acct, [])
    assert days and days[0] == "2026-08-03"
    assert all(len(d) == 10 for d in days)
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/quant/test_api_quant.py::test_sim_status_returns_trade_days tests/quant/test_api_quant.py::test_build_trade_days_network_failure_fallback -v`
Expected: FAIL（`trade_days` 不存在 / `AttributeError`）。

- [ ] **Step 3: 实现**

在 `quant.py` 顶部加 `import datetime`：

```python
import csv
import datetime
import io
```

在 `sim_status` 之后、`_build_benchmark_map` 之前新增助手：

```python
def _build_trade_days(account: dict, trades: list[dict]) -> list[str]:
    """账户区间 [start_date, 今天] 的真实交易日列表（YYYY-MM-DD）。

    start_date 为空时取最早成交日，仍为空则取今天。真实日历取数失败/异常
    时降级为工作日（周一~周五）日历，保证接口不挂。
    """
    today = datetime.date.today().isoformat()
    start = (account.get("start_date") or "").strip()[:10]
    if not start and trades:
        start = str(trades[0].get("ts") or "")[:10]
    if not start or start > today:
        start = today
    try:
        from ..datasource.network_client import StockDataClient
        days = StockDataClient().get_trade_days(start, today)
        if days:
            return sorted({str(d)[:10] for d in days})
    except Exception:  # noqa: BLE001
        pass
    try:
        import pandas as _pd
        return [d.strftime("%Y-%m-%d") for d in _pd.bdate_range(start, today)]
    except Exception:  # noqa: BLE001
        return [start, today]
```

修改 `sim_status` 返回体，data 加 `trade_days`：

```python
@router.get("/sim/accounts/{aid}/status")
def sim_status(aid: str):
    acct = db.get_sim_account(aid)
    if not acct:
        raise HTTPException(404, "not found")
    sid = acct.get("strategy_id") or ""
    strat = get_strategy(sid) if sid else None
    trades = db.get_sim_trades(aid)
    return {"data": {"account": acct, "strategy_name": (strat or {}).get("name", ""),
                     "state": db.read_sim_state(aid),
                     "stop_loss": db.get_sim_stoploss(aid),
                     "trade_days": _build_trade_days(acct, trades)}}
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/quant/test_api_quant.py -v`
Expected: PASS（含新增两个测试，原有测试不受影响）。

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/api/quant.py backend/tests/quant/test_api_quant.py
git commit -m "feat(sim): sim_status 附带交易日历 trade_days（供前端持仓时长计算）"
```

---

### Task 2: 前端「持仓时长」列 + 整行高亮

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx`（`computeHoldDays` 助手放 `fmtPct` 之后；SimDetail 内 `tradeList` 附近接入；成交记录表 TP `tab === 'trades'` 段）

**Interfaces:**
- Consumes: Task 1 的 `st.trade_days: string[]`（`getSimStatus` 的 `j()` 解包后即 `st.trade_days`）；SSE `trade` 事件行字段不变（`ts/code/name/action/price/amount/pnl/pnl_pct/commission`）
- Produces: `computeHoldDays(trades: any[], tradeDays: string[]) -> Map<number, { hold: number | null; open: boolean }>`（key 为按 ts 排序后的下标，hold 为交易日数，open 表示未平仓买入）；渲染层直接消费 `holdMap`.

- [ ] **Step 1: 新增 `computeHoldDays` 纯函数**

在 `frontend/src/quant/pages/QuantSim.tsx` 的 `fmtPct` 之后追加：

```tsx
/** 交易日历查找表：tradeDays 外的日期（如实时会话跨日新增）按工作日索引兜底插补 */
function buildDayLookup(trades: any[], tradeDays: string[]): Map<string, number> {
  const idx = new Map<string, number>()
  tradeDays.filter(Boolean).sort().forEach((d: string, i: number) => idx.set(d, i))
  let next = idx.size
  const isWeekday = (d: string) => {
    const t = new Date(`${d}T00:00:00`)
    const w = t.getUTCDay()
    return !Number.isNaN(t.getTime()) && w >= 1 && w <= 5
  }
  for (const t of trades) {
    const d = String(t?.ts ?? '').slice(0, 10)
    if (d && !idx.has(d) && isWeekday(d)) idx.set(d, next++)
  }
  return idx
}

/** 分标的 FIFO 配对，返回每行（按 ts 排序后的下标）持仓交易日数 */
function computeHoldDays(trades: any[], tradeDays: string[]): Map<number, { hold: number | null; open: boolean }> {
  const days = buildDayLookup(trades, tradeDays)
  const out = new Map<number, { hold: number | null; open: boolean }>()
  const acc = new Map<number, { sum: number; qty: number }>() // key: 买入行 sorted 下标
  const lots: Record<string, { buyOrder: number; buyDay: number; amount: number }[]> = {}
  const sorted = [...trades].sort((a, b) => String(a.ts).localeCompare(String(b.ts)))
  for (let oi = 0; oi < sorted.length; oi++) {
    const t = sorted[oi]
    const d = String(t.ts ?? '').slice(0, 10)
    const day = days.get(d) ?? -1
    const amt = Number(t.amount) || 0
    if (String(t.action).toUpperCase() === 'BUY') {
      (lots[String(t.code ?? '')] ??= []).push({ buyOrder: oi, buyDay: day, amount: amt })
      out.set(oi, { hold: null, open: true })
    } else {
      const q = lots[String(t.code ?? '')] ?? []
      let remaining = amt
      let sum = 0, qty = 0
      while (remaining > 0 && q.length > 0) {
        const lot = q[0]
        const take = Math.min(remaining, lot.amount)
        if (day >= 0 && lot.buyDay >= 0) {
          const diff = day - lot.buyDay
          sum += diff * take
          qty += take
          const a = acc.get(lot.buyOrder) ?? { sum: 0, qty: 0 }
          a.sum += diff * take
          a.qty += take
          acc.set(lot.buyOrder, a)
        }
        lot.amount -= take
        remaining -= take
        if (lot.amount <= 0) q.shift()
      }
      out.set(oi, { hold: qty > 0 ? Math.round(sum / qty) : null, open: false })
    }
  }
  for (const [oi, a] of acc) {
    if (a.qty > 0) out.set(oi, { hold: Math.round(a.sum / a.qty), open: false })
  }
  return out
}
```

- [ ] **Step 2: SimDetail 接入计算**

在 `QuantSim.tsx` 中 `const tradeList = ...`（约 365 行）附近追加：

```tsx
const tradeDays: string[] = Array.isArray(st?.trade_days) ? st.trade_days : []
// 成交记录持仓时长：按 ts 排序下标重算，SSE 追加卖出后买入行自动回填
const sortedTrades = useMemo(() => [...tradeList].sort((a: any, b: any) => String(a.ts).localeCompare(String(b.ts))), [tradeList])
const holdMap = useMemo(() => computeHoldDays(sortedTrades, tradeDays), [sortedTrades, tradeDays])
const holdOf = (origIdx: number) => holdMap.get(origIdx)
```

同时把成交记录渲染处（`{[...(tradeList)].reverse().map((t: any, i: number) => (`，约 547 行）改为使用 sorted 下标：替换该行 `{[...tradeList].reverse().map((t: any, i: number) => (` 为：

```tsx
{[...sortedTrades].reverse().map((t: any, i: number) => {
    const h = holdOf(sortedTrades.length - 1 - i)
```

- [ ] **Step 3: 表头/单元格渲染 + 整行高亮**

把成交记录 `<thead>` 的 `<tr className="text-left">` 段（目前含 时间/名称/代码/方向/价格/数量/手续费/盈亏，约 535-544 行）在「盈亏」前加一列，同时给 `<tr>`（约 548 行）加 hover 高亮。替换「盈亏」表头前一行为：

```tsx
<th className="px-3 py-1.5 font-normal text-right">持仓时长</th>
```

并把该表渲染行的 `<tr key={i} className="border-t border-border/60">` 改为：

```tsx
<tr key={i} className="border-t border-border/60 hover:bg-elevated/60 transition-colors">
```

在「盈亏」单元格（`<td ...>` 盈亏）之后追加持仓时长单元格：

```tsx
<td className="px-3 py-1.5 text-right num">
  {h?.open ? '持有中' : h?.hold == null ? '—' : h.hold === 0 ? '<1天' : `${h.hold}个交易日`}
</td>
```

- [ ] **Step 4: 前端验证**

Run: `cd frontend && pnpm lint && pnpm build`
Expected: 通过（无未使用变量/类型错误；`tsc -b && vite build` 成功）。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/quant/pages/QuantSim.tsx
git commit -m "feat(sim): 成交记录新增持仓时长列（FIFO配对·交易日数）+ 整行悬停高亮"
```

---

## Self-Review 补注

- 表头「持仓时长」列渲染在盈亏列之后；`sortedTrades.length - 1 - i` 在 `.reverse().map` 中即原始 sorted 下标（reverse 后第 i 个的原始下标），与 `computeHoldDays` 的 key 对齐。
- `h?.open` 仅在 BUY 未平仓时为 true；SELL 恒 `open:false`，无匹配数据时 hold 为 null 显示 `—`。