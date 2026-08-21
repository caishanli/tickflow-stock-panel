# 模拟盘持仓现价逐股行情时间 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 模拟盘持仓表「现价」列在价格后显示该价格对应行情 bar 的逐股精确时间。

**Architecture:** feed（`live_feed.refresh` / `runner._hist_feed`）返回值扩为三元组 `(prices, bar_dt, price_ts)`；新增 `Position.price_ts` 字段，经 `_state_from_portfolio` 序列化进 `positions_json`（无 schema JSON，零迁移），API/SSE 全量透传；前端现价单元格追加小字时间。Spec：`docs/superpowers/specs/2026-08-21-sim-position-price-ts-design.md`。

**Tech Stack:** FastAPI + pandas（后端）、React 18 + Vite + TS（前端）、pytest。

## Global Constraints

- 分支：`feature/sim-position-price-ts`（已建，自 `custom-main`）。
- 后端命令一律从 `backend/` 目录跑且带 dev 依赖：`uv run --extra dev pytest` / `uv run --extra dev ruff check app` / `uv run --extra dev mypy app`。
- ruff line-length 100（E501 已忽略，但仍尽量 ≤100）。
- 不改 DB schema、不改 API 路由（positions_json 与 state 全量透传）。
- 除回测边界外不引入 pandas 新用法（本改动只消费现有 DataFrame index）。
- 提交信息风格参照仓库近期提交（中文、`feat:`/`test:`/`fix:` 前缀）。

---

### Task 1: live_feed.refresh 返回逐股 price_ts

**Files:**
- Modify: `backend/app/quant/simulate/live_feed.py`
- Test: `backend/tests/quant/test_live_feed.py`

**Interfaces:**
- Produces: `live_feed.refresh(dm, codes, now=None, fresh_acc=None, loader=None, enabled=False) -> tuple[dict[str, float], pd.Timestamp | None, dict[str, str]]`，第三元素 `price_ts = {code: 该 code 现价 bar 时刻字符串}`。Task 3 的 runner 接线依赖此签名。

- [ ] **Step 1: 更新测试为三元组解包并加 price_ts 断言**

`backend/tests/quant/test_live_feed.py` 六处修改：

```python
# test_refresh_default_loader_uses_client_snapshot（原 line 70-72）
    prices, bar_dt, price_ts = live_feed.refresh(dm, ["510300.XSHG"], now)
    assert prices == {"510300.XSHG": 1.0}
    assert bar_dt == now
    assert price_ts == {"510300.XSHG": "2026-07-17 09:31:30"}
```

```python
# test_refresh_custom_loader_overrides_default（原 line 90-91）
    prices, bar_dt, price_ts = live_feed.refresh(dm, ["510300.XSHG"], now, loader=_loader)
    assert prices == {} and bar_dt is None and price_ts == {}
```

```python
# test_refresh_merges_into_minute_mem_and_snapshots（原 line 103-105）
    prices, bar_dt, price_ts = live_feed.refresh(dm, ["510300.XSHG"], now, acc)
    assert prices == {"510300.XSHG": 10.2}
    assert bar_dt == pd.Timestamp("2026-07-17 09:31")
    assert price_ts == {"510300.XSHG": "2026-07-17 09:31"}
```

```python
# test_refresh_dedupes_overlapping_bars_keep_last（原 line 118）
    prices, _, _ = live_feed.refresh(dm, ["510300.XSHG"], now)
```

```python
# test_refresh_failure_falls_back_to_old_frame（原 line 131-133）
    prices, bar_dt, price_ts = live_feed.refresh(dm, ["510300.XSHG"], now)
    assert prices == {"510300.XSHG": 10.0}      # 失败沿用旧帧最后价
    assert bar_dt == pd.Timestamp("2026-07-17 09:30")
    assert price_ts == {"510300.XSHG": "2026-07-17 09:30"}   # 旧帧的 bar 时间
```

```python
# test_refresh_no_data_returns_none_bar_dt（原 line 138-139）
    prices, bar_dt, price_ts = live_feed.refresh(
        dm, ["510300.XSHG"], pd.Timestamp("2026-07-17 10:00"))
    assert prices == {} and bar_dt is None and price_ts == {}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_live_feed.py -q`
Expected: FAIL — `ValueError: not enough values to unpack (expected 3, got 2)` 或断言失败。

- [ ] **Step 3: 实现 refresh 三元组返回**

`backend/app/quant/simulate/live_feed.py`：

docstring（line 24-27 区域）改为：

```python
    """刷新 watch 集合的实时分钟帧，返回 ``(prices, bar_dt, price_ts)``。

    - prices: ``{code: 截至 now 最新 bar 收盘价}``；
    - bar_dt: 全场最新 bar 时刻（``pd.Timestamp``；全部无数据时为 None）；
    - price_ts: ``{code: 该 code 现价对应 bar 时刻字符串}``（逐股行情时间，
      停牌/无新数据标的为旧帧时刻）；
```

循环初始化与收尾改为：

```python
    prices, latest, price_ts = {}, None, {}
```

（原 line 45：`prices, latest = {}, None`）

价格与时间提取处（原 line 69-72）：

```python
        prices[code] = float(sub["close"].iloc[-1])
        bar = sub.index[-1]
        price_ts[code] = str(bar)
        if latest is None or bar > latest:
            latest = bar
    return prices, latest, price_ts
```

（原最后一行 `return prices, latest` 删除）

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_live_feed.py -q`
Expected: PASS（8 passed）

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/simulate/live_feed.py backend/tests/quant/test_live_feed.py
git commit -m "feat: live_feed.refresh 返回逐股现价时间 price_ts"
```

---

### Task 2: Position.price_ts 字段 + state 序列化/恢复/matcher 回写

**Files:**
- Modify: `backend/app/quant/jqengine/engine/jq/context.py`
- Modify: `backend/app/quant/simulate/runner.py`（`_state_from_portfolio` / `_restore_portfolio` / `_apply_matcher_result`）
- Test: `backend/tests/quant/test_runner_mark.py`

**Interfaces:**
- Consumes: 无（独立字段）。
- Produces: `Position(amount=..., avg_cost=..., price=..., today_amount=..., entry_ts=..., price_ts=None)`；state positions 条目新增键 `"price_ts"`（str | None）。Task 3 在设置 `pos.price` 处写 `pos.price_ts`；Task 4 前端读 `p.price_ts`。

- [ ] **Step 1: 写失败测试（state 往返保留 price_ts）**

`backend/tests/quant/test_runner_mark.py` 末尾追加：

```python
def test_state_roundtrip_preserves_price_ts(tmp_quant):
    """positions_json 序列化/恢复保留逐股行情时间 price_ts。"""
    from app.quant.jqengine.engine.jq.context import Position

    aid = _revalue_at_close_setup(tmp_quant)
    st = protocol.read_state(aid)
    ctx = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {"510300.XSHG": Position(amount=5000.0, avg_cost=10.0,
                                              price=12.0,
                                              price_ts="2026-07-17 10:31")},
        "cash": 0.0})()})()

    runner._state_from_portfolio(ctx, st)
    assert st["positions"]["510300.XSHG"]["price_ts"] == "2026-07-17 10:31"

    ctx2 = type("Ctx", (), {"portfolio": type("Pf", (), {
        "positions": {}, "cash": 0.0})()})()
    runner._restore_portfolio(ctx2, st)
    assert ctx2.portfolio.positions["510300.XSHG"].price_ts == "2026-07-17 10:31"
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py::test_state_roundtrip_preserves_price_ts -q`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'price_ts'`

- [ ] **Step 3: 实现 Position 字段与序列化**

`backend/app/quant/jqengine/engine/jq/context.py`（line 13-19）：

```python
    def __init__(self, amount=0, avg_cost=0.0, price=0.0, today_amount=0.0,
                 entry_ts=None, price_ts=None):
        self.amount = amount
        self.avg_cost = avg_cost
        self.price = price
        self.today_amount = today_amount
        self.entry_ts = entry_ts  # 首次建仓时间（模拟盘展示用）
        self.price_ts = price_ts  # 现价对应行情 bar 时间（模拟盘展示用）
```

`backend/app/quant/simulate/runner.py` `_restore_portfolio`（line 419-425）：

```python
        pf.positions[code] = Position(
            amount=float(sp.get("amount", 0.0) or 0.0),
            avg_cost=float(sp.get("avg_cost", 0.0) or 0.0),
            price=float(sp.get("price", 0.0) or 0.0),
            today_amount=float(sp.get("today_amount", 0.0) or 0.0),
            entry_ts=sp.get("entry_ts"),
            price_ts=sp.get("price_ts"),
        )
```

`_state_from_portfolio`（line 441-450）每持仓 dict 增加：

```python
            "entry_ts": _entry_ts_str(getattr(p, "entry_ts", None)),
            "price_ts": getattr(p, "price_ts", None),
            "name": names.resolve_name(code),
```

`_apply_matcher_result`（line 466-469）同步回写：

```python
        pos.amount = sp["amount"]
        pos.avg_cost = sp.get("avg_cost", pos.avg_cost)
        pos.price = sp.get("price", pos.price)
        pos.today_amount = sp.get("today_amount", 0.0)
        pos.price_ts = sp.get("price_ts", getattr(pos, "price_ts", None))
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py -q`
Expected: PASS（含原有用例，全绿）

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/jqengine/engine/jq/context.py backend/app/quant/simulate/runner.py backend/tests/quant/test_runner_mark.py
git commit -m "feat: Position 增加 price_ts 并随 sim_state 持久化"
```

---

### Task 3: runner 接线三元组 feed（hist/mark/tick/revalue）+ 测试 double 更新

**Files:**
- Modify: `backend/app/quant/simulate/runner.py`（`_hist_feed` / `_mark_to_market` / `_strategy_tick` / `_revalue_at_close`）
- Test: `backend/tests/quant/test_runner_mark.py`、`backend/tests/quant/test_runner_strategy.py`

**Interfaces:**
- Consumes: Task 1 的 `refresh -> (prices, bar_dt, price_ts)`；Task 2 的 `Position.price_ts`。
- Produces: 内部 feed 约定统一为三元组 `(prices, bar_dt, price_ts)`；持仓价格更新点（tick/mark/revalue）均写 `pos.price_ts`。

- [ ] **Step 1: 更新测试 double 为三元组并加断言**

`backend/tests/quant/test_runner_mark.py` `_feed` helper（line 169-172）：

```python
def _feed(price):
    def _fe(dm, codes, now, acc):
        ts = str(pd.Timestamp(now))
        return {c: price for c in codes}, pd.Timestamp(now), {c: ts for c in codes}
    return _fe
```

同文件两处 `_hist_feed` 解包（line 225、238）：

```python
# test_hist_feed_falls_back_to_current_snapshot_when_minute_empty
    prices, bar_dt, price_ts = runner._hist_feed(dm, ["510300.XSHG"], now, {})

    assert prices["510300.XSHG"] == 12.0
    assert bar_dt is not None
    assert price_ts["510300.XSHG"] == str(now)   # 兜底快照 bar = as_of
```

```python
# test_hist_feed_skips_fallback_for_historical_day
    prices, bar_dt, price_ts = runner._hist_feed(dm, ["510300.XSHG"], now, {})

    assert prices == {}
    assert bar_dt is None
    assert price_ts == {}
```

`backend/tests/quant/test_runner_strategy.py` 四个 feed double 各补第三元素 `{}`：

```python
# _feed_factory（line 113-115）
def _feed_factory(price=10.0, bar=None):
    def _feed(dm, codes, now, acc):
        return {c: price for c in codes}, (bar or _today_bar()), {}
    return _feed
```

```python
# line 171-172（_patch_one_loop 内）
    def _feed(dm, codes, now, acc):
        return {c: 10.0 for c in codes}, next(bars), {}
```

```python
# line 265-268（越界钳制版）
    def _feed(dm, codes, now, acc):
        p = next(bars, None)
        # 主循环盘中 mark 子循环会基于持仓反复取价，越界后钳制在最后一根 bar
        return {c: 10.0 for c in codes}, (p or _today_bar(minute=31)), {}
```

```python
# line 632-633
    def _feed(dm, codes, now, acc):
        return {c: 10.0 for c in codes}, next(bars), {}
```

- [ ] **Step 2: 运行相关测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_runner_mark.py tests/quant/test_runner_strategy.py -q`
Expected: FAIL — `_hist_feed` 返回 2 元素导致解包错误 / `too many values to unpack`。

- [ ] **Step 3: 实现 runner 四处接线**

`backend/app/quant/simulate/runner.py`：

`_hist_feed`（line 717-755）整体改为：

```python
def _hist_feed(dm, codes, now, _acc):
    """补跑馈送：取各标的截至 now（历史时刻）的最后一分钟收盘价。

    走 ``dm.get_minute_price_at`` 滑窗加载（C1 近 3 月真实 1m / 更早 baostock 5m
    插值，均在内存，不落盘）；无数据标的缺席，全部无数据则 bar_dt=None（该 bar
    跳过，如停牌/数据空洞）。返回 ``(prices, bar_dt, price_ts)``，price_ts 为
    ``{code: 该 code 现价 bar 时刻字符串}``。

    当日（now 与真实今天同一天）全部取不到价时回退 ``current_snapshot`` 实时
    兜底：stock data 服务刚重启/当日分区尚未落盘的竞态下，get_minute 分区取数
    为空，但实时源可回源当日真实 1m——补跑不再整批静默跳过（复现：dev.sh 重启
    后 11:51 补跑 ETF 分钟分区 11:51:51 才落盘，全部 bar 被跳过、持仓价停旧值）。
    历史日分区应已存在，缺失即真实缺失（停牌），不做兜底，避免错配今日价。
    """
    prices, price_ts = {}, {}
    for code in dict.fromkeys(codes):
        p = dm.get_minute_price_at(code, now)
        if p is not None:
            prices[code] = float(p)
            price_ts[code] = str(pd.Timestamp(now))
    if prices:
        return prices, now, price_ts
    now_ts = pd.Timestamp(now)
    if now_ts.date() != pd.Timestamp(datetime.datetime.now()).date():
        return prices, None, price_ts
    client = getattr(dm, "client", None)
    if client is None:
        return prices, None, price_ts
    try:
        snap = client.current_snapshot(list(dict.fromkeys(codes)), as_of=now_ts)
    except Exception as e:  # noqa: BLE001
        log.warning("[hist_feed] 当日实时兜底取数失败: %s", e)
        return prices, None, price_ts
    for code, df in (snap or {}).items():
        if df is None or (hasattr(df, "empty") and df.empty):
            continue
        sub = df[df.index <= now_ts]
        if sub.empty:
            continue
        prices[code] = float(sub["close"].iloc[-1])
        price_ts[code] = str(sub.index[-1])
    return prices, (now_ts if prices else None), price_ts
```

`_mark_to_market`（line 965-967 与 982-983）：

```python
    prices, _bar, bar_ts_map = feed(dm, engine_codes, now, None)
    if prices and conv is not None:
        prices = {conv[1](c): v for c, v in prices.items()}
        bar_ts_map = {conv[1](c): v for c, v in (bar_ts_map or {}).items()}
```

```python
        pos.price = float(px)
        ts = bar_ts_map.get(code)
        if ts:
            pos.price_ts = str(ts)
        last_mark[code] = float(px)
```

`_strategy_tick`（line 1008-1010）：

```python
    prices, bar_dt, price_ts = feed(dm, [_to_engine(c) for c in watch], now, aux["fresh_frames"])
    if prices:
        prices = {_to_pt(c): v for c, v in prices.items()}
        price_ts = {_to_pt(c): v for c, v in (price_ts or {}).items()}
```

同函数持仓回写（line 1061-1063）：

```python
    for code, pos in ctx.portfolio.positions.items():
        if code in prices:
            pos.price = prices[code]
            ts = price_ts.get(code)
            if ts:
                pos.price_ts = str(ts)
```

`_revalue_at_close`（line 638）：

```python
        pos.price = float(price)
        pos.price_ts = str(close_ts)
        changed = True
```

- [ ] **Step 4: 运行 quant 测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant -q`
Expected: PASS（全绿，无回归）

- [ ] **Step 5: lint + 类型检查**

Run: `cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app`
Expected: 无新增告警

- [ ] **Step 6: Commit**

```bash
git add backend/app/quant/simulate/runner.py backend/tests/quant/test_runner_mark.py backend/tests/quant/test_runner_strategy.py
git commit -m "feat: 模拟盘 tick/mark/收盘重估写入逐股现价时间"
```

---

### Task 4: 前端现价单元格显示行情时间

**Files:**
- Modify: `frontend/src/quant/pages/QuantSim.tsx`

**Interfaces:**
- Consumes: state positions 条目可选字段 `p.price_ts`（`"YYYY-MM-DD HH:MM:SS"` | null），由 `/sim/accounts/{aid}/status` 与 SSE `status` 事件透传。
- Produces: 无（纯展示）。

- [ ] **Step 1: 加格式化 helper**

`fmtPct` 函数后（line 39-48 区域之后）新增：

```tsx
function fmtPriceTs(ts: unknown): string | null {
  if (!ts) return null
  const m = String(ts).match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/)
  if (!m) return null
  const now = new Date()
  const pad = (n: number) => String(n).padStart(2, '0')
  const isToday =
    Number(m[1]) === now.getFullYear() &&
    Number(m[2]) === now.getMonth() + 1 &&
    Number(m[3]) === now.getDate()
  return isToday ? `${m[4]}:${m[5]}` : `${m[2]}-${m[3]} ${m[4]}:${m[5]}`
}
```

- [ ] **Step 2: 现价单元格渲染时间小字**

现价 `<td>`（line 727）改为：

```tsx
                        <td className="px-3 py-1.5 text-right num">
                          {fmtNum(p.price, 3)}
                          {fmtPriceTs(p.price_ts) && (
                            <span className="ml-1 text-[10px] text-muted font-normal">
                              {fmtPriceTs(p.price_ts)}
                            </span>
                          )}
                        </td>
```

- [ ] **Step 3: lint + 构建验证**

Run: `cd frontend && pnpm lint && pnpm build`
Expected: 均通过（tsc 无类型错误）

- [ ] **Step 4: Commit**

```bash
git add frontend/src/quant/pages/QuantSim.tsx
git commit -m "feat: 模拟盘持仓现价后显示逐股行情时间"
```

---

### Task 5: 全量回归验证

**Files:** 无新改动（验证任务；如有修复一并提交）。

- [ ] **Step 1: 后端全量测试**

Run: `cd backend && uv run --extra dev pytest -q`
Expected: 全绿（若存在与本改动无关的既有失败，记录并与用户确认，不静默跳过）

- [ ] **Step 2: 后端 lint + mypy**

Run: `cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app`
Expected: 通过

- [ ] **Step 3: 前端 lint + build**

Run: `cd frontend && pnpm lint && pnpm build`
Expected: 通过

- [ ] **Step 4: 手工验收（可选，需运行中的服务）**

启动 `./dev.sh` 后打开模拟盘页：持仓表现价右侧出现 `HH:MM` 小字；有停牌/旧仓的账户显示 `MM-DD HH:MM`。

- [ ] **Step 5: 如有修复则提交**

```bash
git add -A && git commit -m "fix: 回归修复（如有）"
```
