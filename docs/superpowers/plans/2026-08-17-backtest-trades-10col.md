# 回测成交表对齐模拟盘 10 列 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 量化回测成交表从 6 列扩展为与量化模拟盘一致的 10 列（时间/名称/代码/持仓时长/方向/价格/数量/手续费/盈亏/收益率），名称由后端在读取层补齐。

**Architecture:** 后端 `quant.py` 在 trades 接口与 SSE trade 事件里用既有 `resolve_name(code)`（simulate/names.py，通达信名优先、聚宽快照回退、缺名回退代码）补 `name` 字段，零 schema 变更、历史行同样生效。前端 `QuantBacktest.tsx` 重建 `TradeTable`：移植模拟盘 `buildDayLookup`/`computeHoldDays` FIFO 持仓时长逻辑（`tradeDays=[]` 走交易日兜底，买卖判定 `/BUY/i` 兼容 `SIDE.BUY`），盈亏/收益率渲染与模拟盘同款，展示顺序最新在上。

**Tech Stack:** FastAPI / SQLite (quant.db) / React 18 + TS + Vite。

## Global Constraints

- 买卖判定统一用 `/BUY/i.test(String(t.action))`，兼容 `SIDE.BUY`/`SIDE.SELL`（与方向列既有归一化同口径）。
- 名称字段：后端 `resolve_name(code)` 恒返回非空（缺名回退代码本身），前端直接 `t.name ?? ''`。
- 不改 `backtest_trades` 表结构；不改 `trades.csv` 导出列；不改 `BacktestResult.tsx`；不改模拟盘任何代码。
- 后端验证：`cd backend && uv run --extra dev pytest tests/quant -q`（本机 3.6Gi 全量会 OOM，quant 范围为验收口径）。
- 前端验证：`cd frontend && pnpm build`（tsc -b && vite build）；`pnpm lint` 仓库级预置失败，跳过。
- 前端文件 `QuantBacktest.tsx` 现有 import：`useEffect, useRef, useState, type ReactNode`（第 1 行，**无 useMemo**，本任务需补 `useMemo`）。

---

### Task 1: 后端 trades 接口与 SSE 事件补 name

**Files:**
- Modify: `backend/app/quant/api/quant.py`（import 区 :1-21、`backtest_trades` :130-132、`backtest_stream` SSE trade 事件 :192-195）
- Test: `backend/tests/quant/test_api_quant.py`（文件末尾追加）

**Interfaces:**
- Consumes: `app.quant.db.insert_run(run_id, strategy_id, name, params_json, status)`、`app.quant.db.insert_trade(run_id, ts, code, action, price, amount, pnl, pnl_pct, commission)`、`app.quant.simulate.names.resolve_name(code: str) -> str`（模块级函数，进程内缓存，恒返回非空字符串，缺名回退 code；内部网络调用有超时与异常兜底）。
- Produces: `GET /api/quant/backtest/{run_id}/trades` 返回的每行多一个 `name` 字段；SSE `trade` 事件 data 里多一个 `name` 字段。

- [ ] **Step 1: 追加失败测试**

在 `backend/tests/quant/test_api_quant.py` 文件末尾（最后一个测试之后）追加：

```python
def test_backtest_trades_includes_name(client):
    rid = db.insert_run("r1", "s1", "n1", "{}", "done")
    db.insert_trade(rid, "2026-08-17 10:30:00", "600000.XSHG", "SIDE.BUY", 10.0, 100.0, 0.0, 0.0, 1.0)
    r = client.get(f"/api/quant/backtest/{rid}/trades")
    assert r.status_code == 200
    rows = r.json()["data"]
    assert len(rows) == 1
    row = rows[0]
    assert row["code"] == "600000.XSHG"
    assert "name" in row
    assert row["name"]
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_api_quant.py::test_backtest_trades_includes_name -q`
Expected: FAIL（`KeyError: 'name'` 或 assert 报缺 name 键）。

- [ ] **Step 3: 实现后端补 name**

3a. `backend/app/quant/api/quant.py` import 区（:21 后）追加一行：

```python
from ..simulate.names import resolve_name
```

3b. `backtest_trades`（:130-132）改为：

```python
@router.get("/backtest/{run_id}/trades")
def backtest_trades(run_id: str):
    rows = db.get_trades(run_id)
    for row in rows:
        row["name"] = resolve_name(row["code"])
    return {"data": rows}
```

3c. `backtest_stream` SSE trade 事件（:192-195）改为：

```python
            for row in db.get_trades_after(run_id, off_trade):
                off_trade = row["rowid"]
                d = {k: row[k] for k in ("ts", "code", "action", "price", "amount", "pnl", "pnl_pct", "commission")}
                d["name"] = resolve_name(row["code"])
                yield f"event: trade\ndata: {_json.dumps(d, ensure_ascii=False)}\n\n"
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_api_quant.py -q`
Expected: PASS（原有用例 + 新用例全绿）。若测试环境 stockdata 服务未起，`resolve_name` 走回退返回代码本身，断言仍成立。

- [ ] **Step 5: 提交**

```bash
git add backend/app/quant/api/quant.py backend/tests/quant/test_api_quant.py
git commit -m "feat: 回测成交 trades 接口与 SSE 事件补名称字段"
```

---

### Task 2: 前端 TradeTable 重建为 10 列

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx`（import 第 1 行、`TradeTable` :800-832）

**Interfaces:**
- Consumes: 后端 trades/SSE 行数据（`ts, code, action, price, amount, commission, pnl, pnl_pct, name`）；已有 `fmtNum`/`fmtPct`（`../metrics`，已 import）；模拟盘移植函数 `buildDayLookup`/`computeHoldDays`（本任务内定义，见 Step 2 代码）。
- Produces: `TradeTable({ trades })` 渲染 10 列，最新成交在上。

- [ ] **Step 1: 补 useMemo import**

`frontend/src/quant/pages/QuantBacktest.tsx` 第 1 行改为：

```tsx
import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
```

- [ ] **Step 2: 移植持仓时长辅助函数**

在 `TradeTable` 函数（:800）之前插入两个模块级函数（来自 QuantSim.tsx:56-117，买卖判定改为 `/BUY/i`）：

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

/** 分标的 FIFO 配对，返回每行（按 ts 排序后的下标）持仓交易日数（买入判定兼容 SIDE.BUY） */
function computeHoldDays(trades: any[], tradeDays: string[]): Map<number, { hold: number | null; open: boolean }> {
  const days = buildDayLookup(trades, tradeDays)
  const out = new Map<number, { hold: number | null; open: boolean }>()
  const acc = new Map<number, { sum: number; qty: number }>()
  const lots: Record<string, { buyOrder: number; buyDay: number; amount: number }[]> = {}
  const sorted = [...trades].sort((a, b) => String(a.ts).localeCompare(String(b.ts)))
  for (let oi = 0; oi < sorted.length; oi++) {
    const t = sorted[oi]
    const d = String(t.ts ?? '').slice(0, 10)
    const day = days.get(d) ?? -1
    const amt = Number(t.amount) || 0
    if (/BUY/i.test(String(t.action))) {
      (lots[String(t.code ?? '')] ??= []).push({ buyOrder: oi, buyDay: day, amount: amt })
      const now = days.size - 1
      out.set(oi, { hold: day >= 0 ? Math.max(0, now - day) : null, open: true })
    } else {
      const q = lots[String(t.code ?? '')] ?? []
      let remaining = amt
      let sum = 0
      let qty = 0
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

- [ ] **Step 3: 重建 TradeTable 为 10 列**

将 `frontend/src/quant/pages/QuantBacktest.tsx` :800-832 整个 `TradeTable` 函数替换为：

```tsx
function TradeTable({ trades }: { trades: any[] }) {
  if (trades.length === 0) return <div className="text-xs text-muted">暂无成交</div>
  const isBuy = (t: any) => /BUY/i.test(String(t.action ?? t.side ?? ''))
  const sorted = useMemo(() => [...trades].sort((a: any, b: any) => String(a.ts).localeCompare(String(b.ts))), [trades])
  const holdMap = useMemo(() => computeHoldDays(sorted, []), [sorted])
  const holdOf = (origIdx: number) => holdMap.get(origIdx)
  return (
    <div className="overflow-auto">
      <table className="w-full text-xs">
        <thead className="text-muted sticky top-0 bg-surface">
          <tr className="text-left">
            <th className="px-2 py-1.5 font-normal">时间</th>
            <th className="px-2 py-1.5 font-normal">名称</th>
            <th className="px-2 py-1.5 font-normal">代码</th>
            <th className="px-2 py-1.5 font-normal">持仓时长</th>
            <th className="px-2 py-1.5 font-normal">方向</th>
            <th className="px-2 py-1.5 font-normal text-right">价格</th>
            <th className="px-2 py-1.5 font-normal text-right">数量</th>
            <th className="px-2 py-1.5 font-normal text-right">手续费</th>
            <th className="px-2 py-1.5 font-normal text-right">盈亏</th>
            <th className="px-2 py-1.5 font-normal text-right">收益率</th>
          </tr>
        </thead>
        <tbody className="text-foreground">
          {[...sorted].reverse().map((t, i) => {
            const origIdx = sorted.length - 1 - i
            const h = holdOf(origIdx)
            const buy = isBuy(t)
            const holdText = buy
              ? (h?.open
                  ? (h?.hold == null ? '持仓中' : h.hold === 0 ? '<1天（持仓中）' : `${h.hold}个交易日（持仓中）`)
                  : '—')
              : h?.hold == null ? '—' : h.hold === 0 ? '<1天' : `${h.hold}个交易日`
            return (
              <tr key={origIdx} className="border-t border-border/60">
                <td className="px-2 py-1.5 text-muted">{String(t.ts ?? t.datetime ?? '')}</td>
                <td className="px-2 py-1.5">{t.name ?? ''}</td>
                <td className="px-2 py-1.5 text-muted">{t.code ?? t.symbol ?? ''}</td>
                <td className={`px-2 py-1.5 num ${holdText === '—' ? 'text-muted' : ''}`}>{holdText}</td>
                <td className={`px-2 py-1.5 ${buy ? 'text-bull' : 'text-bear'}`}>{buy ? '买入' : '卖出'}</td>
                <td className="px-2 py-1.5 text-right num">{fmtNum(t.price, 3)}</td>
                <td className="px-2 py-1.5 text-right">{t.amount ?? t.qty ?? ''}</td>
                <td className="px-2 py-1.5 text-right num">{fmtNum(t.commission, 2)}</td>
                <td className={`px-2 py-1.5 text-right num ${typeof t.pnl === 'number' && t.pnl !== 0 ? (t.pnl >= 0 ? 'text-bull' : 'text-bear') : 'text-muted'}`}>
                  {typeof t.pnl === 'number' && t.pnl !== 0 ? fmtNum(t.pnl) : '—'}
                </td>
                <td className={`px-2 py-1.5 text-right num ${!buy && typeof t.pnl_pct === 'number' ? (t.pnl_pct >= 0 ? 'text-bull' : 'text-bear') : 'text-muted'}`}>
                  {buy ? '—' : fmtPct(t.pnl_pct)}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
```

- [ ] **Step 4: 构建验证**

Run: `cd frontend && pnpm build`
Expected: `tsc -b` 与 `vite build` 均成功（无 TS 报错）。

- [ ] **Step 5: 提交**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat: 回测成交表对齐模拟盘 10 列（名称/持仓时长/盈亏/收益率）"
```