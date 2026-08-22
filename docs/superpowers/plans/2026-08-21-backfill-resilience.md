# 回源链路统一加固实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 任一回源链路的单次 socket 失败不再产生静默数据缺口——可发现、当轮自动补、补不上有告警。

**Architecture:** 共享原语不抽框架。因子链路：xdxr 原始事件落本地 parquet，因子重建纯本地计算，断点审计驱动当轮重试；日线/分钟/NAV 链路：查询失败收集为 query_failed，当轮换实例重试一轮，仍失败走既有 failures.csv + 00:00 巡检兜底。

**Tech Stack:** Python 3.12 / polars / pandas / pytest（asyncio_mode=auto）/ mootdx

## Global Constraints

- 规格文档：`docs/superpowers/specs/2026-08-21-backfill-resilience-design.md`
- 因子数学与现版本完全一致（cat==11: 1/suogu；cat==1: (prev_close-fh+pgj*pg)/(1+sg+pg)/prev_close），不得改变数值口径
- 审计阈值 0.2，严格大于（排除 20cm ETF 恰好 ±20% 合法涨跌）
- 告警统一 `logger.warning`（钉钉消费），消息含链路名+数量+标的样例≤10 只
- 测试从 `backend/` 目录跑：`uv run --extra dev pytest tests/quant/test_sync_adj_factor.py -q`
- lint：`uv run --extra dev ruff check app`；类型：`uv run --extra dev mypy app`
- 提交信息用中文 conventional 风格（如 `fix(quant): ...`）

---

### Task 1: 第 1 层——_xdxr_rows 失败不缓存（工作区已完成，验证+提交）

**Files:**
- Modify: `backend/app/quant/jqengine/datasource/mootdx_src.py`（`_xdxr_rows`，已改好）
- Test: `backend/tests/quant/test_sync_adj_factor.py::test_xdxr_failure_not_cached`（已写好）

**Interfaces:**
- Produces: `MootdxSource._xdxr_rows(sym) -> list[dict] | None`——成功返回 `[]` 或事件列表并缓存；整轮轮换失败返回 `None` 且不缓存。

- [ ] **Step 1: 运行验证**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_sync_adj_factor.py::test_xdxr_failure_not_cached -q`
Expected: PASS（该层已实现）

- [ ] **Step 2: Commit**

```bash
git add backend/app/quant/jqengine/datasource/mootdx_src.py backend/tests/quant/test_sync_adj_factor.py
git commit -m "fix(quant): _xdxr_rows 失败不缓存，防 socket 抖动静默漏标的"
```

### Task 2: 因子链路——事件落本地 + 断点审计 + 当轮重试

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（`sync_adj_factor` 及新 helper）
- Test: `backend/tests/quant/test_sync_adj_factor.py`（4 个 RED 测试已在位：
  `test_raw_events_persisted` / `test_query_failure_keeps_local_events` /
  `test_audit_retries_and_warns`；`test_xdxr_failure_not_cached` 在 Task 1 已 GREEN）

**Interfaces:**
- Consumes: `MootdxSource._xdxr_rows(sym6) -> list|None`（Task 1）
- Produces:
  - `_symbol_factor_frame(jq: str, closes: pd.Series, rows: list[dict] | None) -> pl.DataFrame | None`
  - `_audit_uncovered_breakpoints(daily: dict, factor_df: pl.DataFrame, threshold: float = 0.2) -> list[str]`（已在工作区）
  - `_normalize_xdxr_rows / _load_xdxr_events / _save_xdxr_events`（已在工作区）
  - `sync_adj_factor() -> dict` 增加 `"query_failed": list[str]`, `"audit_uncovered": list[str]`

- [ ] **Step 1: 抽取单标的因子帧 helper（模块级，放在 `_audit_uncovered_breakpoints` 之后）**

把 `sync_adj_factor` 主循环里 `events = []` 起到 `frames.append(...)` 为止的数学块原样搬入：

```python
def _symbol_factor_frame(jq: str, closes: pd.Series,
                         rows: list[dict] | None) -> pl.DataFrame | None:
    """单标的：本地事件 rows × 日线 close → 逐日因子帧；无有效事件返回 None。

    数学与历史版本完全一致（cat==11: 1/suogu；cat==1 含红利/配股摊薄）。
    """
    events = []
    for r in (rows or []):
        cat = r.get("category")
        year = r.get("year")
        if not year or int(year) < _SINCE_YEAR:
            continue
        try:
            ex_dt = pd.Timestamp(int(r["year"]), int(r["month"]), int(r["day"]))
        except Exception:
            continue
        if cat == 11:
            suogu = float(r.get("suogu") or 0)
            if suogu <= 0:
                continue
            events.append((ex_dt, 1.0 / suogu))
        elif cat == 1:
            fh = float(r.get("fenhong") or 0) / 10.0      # 每股现金红利(元)
            sg = float(r.get("songzhuangu") or 0) / 10.0  # 每股送转
            pg = float(r.get("peigu") or 0) / 10.0        # 每股配股
            pgj = float(r.get("peigujia") or 0)           # 配股价
            if fh == 0 and sg == 0 and pg == 0:
                continue
            prev = closes.loc[closes.index < ex_dt].dropna()
            if prev.empty:
                continue
            prev_close = float(prev.iloc[-1])
            if prev_close <= 0:
                continue
            ex_price = (prev_close - fh + pgj * pg) / (1.0 + sg + pg)
            if ex_price <= 0:
                continue
            events.append((ex_dt, ex_price / prev_close))
    if not events:
        return None
    events = [(e, f) for e, f in events if e < closes.index.max()]
    if not events:
        return None
    adj = pd.Series(1.0, index=closes.index)
    for ex_dt, f in events:
        adj.loc[adj.index < ex_dt] *= f
    return pl.DataFrame({
        "symbol": jq,
        "trade_date": [d.isoformat() for d in closes.index.date],
        "ex_factor": adj.values,
    })
```

- [ ] **Step 2: 主循环改用 helper**

把主循环里从 `rows = events_map.get(sym6)` 到 `frames.append(...)` 的整段替换为：

```python
        fr = _symbol_factor_frame(jq, closes, events_map.get(sym6))
        if fr is not None:
            frames.append(fr)
```

（查询分支 `rows = src._xdxr_rows(sym6)` / None 处理 / `events_map[sym6] = _normalize_xdxr_rows(rows)` 保持不变。）

- [ ] **Step 3: 循环结束后保存事件表**

在主循环结束、`if not frames:` 之前插入：

```python
    _save_xdxr_events(events_path, events_map)
```

- [ ] **Step 4: 尾部重构——合并写入抽成闭包 + 审计 + 重试轮 + 告警**

把从 `if not frames:` 到函数末尾 `return {...}` 的整段替换为：

```python
    def _merge_write(frames_):
        """合并写入因子表；frames_ 为空时保留既有表。返回最新表内容。"""
        ADJ_FACTOR_PATH.parent.mkdir(parents=True, exist_ok=True)
        out = pl.concat(frames_) if frames_ else None
        if out is not None:
            out = out.with_columns(pl.col("trade_date").cast(pl.Date))
            out = out.unique(subset=["symbol", "trade_date"], keep="last").sort(
                ["symbol", "trade_date"])
        if ADJ_FACTOR_PATH.exists():
            old = pl.read_parquet(ADJ_FACTOR_PATH)
            out = old if out is None else pl.concat([old, out]).unique(
                subset=["symbol", "trade_date"], keep="last").sort(
                ["symbol", "trade_date"])
        if out is None or out.is_empty():
            return pl.DataFrame({"symbol": [], "trade_date": [], "ex_factor": []})
        tmp = ADJ_FACTOR_PATH.parent / "all.tmp.parquet"
        out.write_parquet(tmp)
        tmp.rename(ADJ_FACTOR_PATH)
        logger.info("mootdx_service: 因子表更新 %d 行 / %d 只 → %s",
                    out.height, out["symbol"].n_unique(), ADJ_FACTOR_PATH)
        return out

    out = _merge_write(frames)
    audit_uncovered = _audit_uncovered_breakpoints(daily, out)

    # 第3层：查询失败 ∪ 审计缺口 → 新实例（新缓存+换服务器）重试一轮
    retry_syms = sorted(set(query_failed)
                        | {s.split(".")[0] for s in audit_uncovered})
    if retry_syms:
        logger.warning("mootdx_service: 因子缺口重试 %d 只: %s",
                       len(retry_syms), retry_syms[:10])
        src2 = MootdxSource()
        got = []
        for sym6 in retry_syms:
            rows = src2._xdxr_rows(sym6)
            if rows is not None:
                events_map[sym6] = _normalize_xdxr_rows(rows)
                got.append(sym6)
        if got:
            _save_xdxr_events(events_path, events_map)
            frames = []
            for jq, pdf in daily.items():
                closes = pdf["close"].dropna()
                if closes.empty:
                    continue
                fr = _symbol_factor_frame(jq, closes,
                                          events_map.get(jq.split(".")[0]))
                if fr is not None:
                    frames.append(fr)
            out = _merge_write(frames)
            audit_uncovered = _audit_uncovered_breakpoints(daily, out)

    if audit_uncovered:
        logger.warning("mootdx_service: 因子断点审计未覆盖 %d 只: %s",
                       len(audit_uncovered), audit_uncovered[:10])
    return {"written_symbols": len(frames), "rows": out.height,
            "total_symbols": len(codes),
            "query_failed": query_failed, "audit_uncovered": audit_uncovered}
```

注意：删除旧的 `if not frames: return ...` 早退（事件表保存与审计必须执行）；
旧尾部 `out = pl.concat(frames)...tmp.rename(ADJ_FACTOR_PATH)` 一并删除（已被
`_merge_write` 取代）。

- [ ] **Step 5: 运行因子链路全部测试**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_sync_adj_factor.py -q`
Expected: 7 passed

- [ ] **Step 6: Commit**

```bash
git add backend/app/services/mootdx_service.py backend/tests/quant/test_sync_adj_factor.py
git commit -m "feat(quant): 因子同步事件落本地+断点审计+当轮重试（防 socket 静默漏标的）"
```

### Task 3: 日线/分钟链路——query_failed + 当轮重试

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（`sync_daily` ~1560 行起；
  `sync_etf_minute` ~153 行起；`sync_stock_minute` ~461 行起）
- Test: `backend/tests/quant/test_sync_adj_factor.py`（追加）

**Interfaces:**
- Consumes: Task 1 的 `_xdxr_rows` 语义（None=失败）
- Produces: `sync_daily() -> dict` 增加 `"query_failed": list[str]`；
  分钟链路失败标的当轮经既有单日/定向入口重试

- [ ] **Step 1: 写失败测试（追加到 test_sync_adj_factor.py）**

```python
def test_sync_daily_query_failed_field(monkeypatch, tmp_path):
    """日线链路：单标的查询异常计入 query_failed 并当轮重试。"""
    from app.services import mootdx_service as ms
    calls = {"n": 0}

    class _FlakySrc:
        def get_daily(self, code, start, end):
            calls["n"] += 1
            if calls["n"] <= 2:      # 首轮每只都失败
                raise TimeoutError("socket down")
            import pandas as pd
            return pd.DataFrame(
                {"close": [1.0], "volume": [100.0]},
                index=pd.to_datetime(["2026-08-20"]))

    monkeypatch.setattr(ms, "_stock_universe", lambda: ["600000.XSHG"])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [])
    monkeypatch.setattr(ms, "MootdxSource", lambda: _FlakySrc())
    monkeypatch.setattr(ms, "_active", lambda s: True)
    res = ms.sync_daily(__import__("datetime").date(2026, 8, 20))
    assert set(res["query_failed"]) == {"600000.XSHG"} or res["query_failed"] == []
    assert calls["n"] >= 2, "失败标的应被重试"
```

- [ ] **Step 2: 运行确认 RED**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_sync_adj_factor.py::test_sync_daily_query_failed_field -q`
Expected: FAIL（`query_failed` 键不存在）

- [ ] **Step 3: sync_daily 接入——收集失败 + 当轮重试**

在 `sync_daily` 主循环里把两处裸 `except ...: continue` 改为记录失败标的：

```python
    failed = []
    for i, sym in enumerate(syms):
        try:
            df = src.get_daily(sym, day_str, day_str)
        except Exception:
            failed.append(sym)
            continue
```

主循环结束后追加：

```python
    # 第3层：失败标的换实例当轮重试一轮
    if failed:
        logger.warning("mootdx_service: 日线回源失败重试 %d 只: %s",
                       len(failed), failed[:10])
        src = MootdxSource()
        syms2, failed = failed, []
        for sym in syms2:
            try:
                df = src.get_daily(sym, day_str, day_str)
            except Exception:
                failed.append(sym)
                _append_failure(sym, "daily_retry_timeout")
                continue
            <原循环体内「取 day 那根写分区」的同一段逻辑，原样复用>
    return {"stocks": ..., "etfs": ..., "query_failed": failed}
```

（`<原循环体>` 指该函数现有「hit = df[...] → 写分区」段，逐字复用不改逻辑；
若函数原本返回 int，改为 dict 并同步更新调用方 `_run_sync` 的日志字段。）

- [ ] **Step 4: 分钟链路接入**

`sync_stock_minute` / `sync_etf_minute`：现有 `except` 分支已调 `_append_failure`
（timeout/exception/empty 三处），在各循环结束后追加同模式重试：

```python
    if failed_syms:
        logger.warning("mootdx_service: 分钟回源失败重试 %d 只: %s",
                       len(failed_syms), failed_syms[:10])
        <以既有单日/定向入口重试一轮：
         股票当日缺口 → sync_stock_minute_day(day, symbols=failed_syms)；
         ETF 全日链路 → 对 failed 集合再跑一次本函数的逐只循环体>
    仍失败 → 保留既有 _append_failure 记录（00:00 巡检兜底）
```

返回值增加 `query_failed` 字段。

- [ ] **Step 5: 运行测试**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_sync_adj_factor.py -q`
Expected: all passed

- [ ] **Step 6: Commit**

```bash
git add backend/app/services/mootdx_service.py backend/tests/quant/test_sync_adj_factor.py
git commit -m "feat(quant): 日线/分钟回源失败标的当轮重试+query_failed 字段"
```

### Task 4: NAV 重试 + 全量验证

**Files:**
- Modify: `backend/app/services/etf_nav_service.py`（`sync_etf_nav` ~83 行起）

- [ ] **Step 1: NAV 空结果重试一次**

```python
    raw = _fund_etf_fund_daily_em()
    if raw.is_empty():
        logger.warning("etf_nav_service: 东财净值空结果，重试一次")
        raw = _fund_etf_fund_daily_em()
    if raw.is_empty():
        logger.warning("etf_nav_service: 净值源连续为空，本轮跳过")
        return 0
```

- [ ] **Step 2: 全量验证**

Run:
```bash
cd backend && uv run --extra dev pytest -q --ignore=tests/backtest/test_fix_api.py
cd backend && uv run --extra dev ruff check app
cd backend && uv run --extra dev mypy app
```
Expected: 失败清单与基线一致（29 个既有失败）；ruff/mypy 无新增违规

- [ ] **Step 3: 手工验收**

```bash
cd backend && uv run python -c "
from app.services.mootdx_service import sync_adj_factor
r = sync_adj_factor()
print(r['audit_uncovered'], r['query_failed'])"
```
Expected: 两个列表均空（当前因子表已 79/79 覆盖）

- [ ] **Step 4: Commit**

```bash
git add backend/app/services/etf_nav_service.py
git commit -m "feat(quant): NAV 同步空结果重试一次"
```
