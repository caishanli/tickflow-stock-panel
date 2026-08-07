# 00:00 全量数据缺失检测 + 自动补全 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 每日 00:00 全量巡检 5 类数据分区缺失（4/1~今天，含中间洞），缺失日自动回源补全（有数据才回）。

**Architecture:** 在 `mootdx_service.py` 新增「完整交易日历推导 + 分区缺失扫描 + 逐缺失日补全」三个能力，在 `stockdata/scheduler.py` 新增 00:00 定时循环（与 15:35 共用 `_sync_lock` 串行）。补全复用现有 `sync_daily` / `sync_index_daily` / `sync_etf_minute` / `sync_stock_minute`，仅股票分钟缺失需新增 `sync_stock_minute_day`。

**Tech Stack:** Python 3.11, Polars, pandas（已有）, mootdx（已有）, threading 定时（已有）。

## Global Constraints

- 数据根目录统一走 `DATA_ROOT`（`PARTITION_DATA_ROOT` env 或 `backend/../data`）。
- 回源起点 `STOCK_MINUTE_START = _date(2026, 4, 1)`（已在代码定义）。
- 分区符号格式：`date=YYYY-MM-DD/part.parquet`。
- 股票 symbol 带 `.SH`/`.SZ`；北交所（`.BJ`）跳过（mootdx 无数据）。
- 「有数据才回」：上市日晚于目标日 / 当日停牌 → 跳过，不落盘。
- 不引入 pandas 之外的新依赖；测试用 pytest（`uv run --extra dev`）。
- 单日失败记 errors 不阻断其它缺失日。

---

### Task 1: `_trade_days_in_range` — 完整交易日历推导

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（在 `_trade_days_up_to` 附近新增）
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`

**Interfaces:**
- Produces: `mootdx_service._trade_days_in_range(start: _date, end: _date) -> list[_date]`
  - 返回 `[start, end]` 内的 A 股交易日（升序），从 `get_daily("000300.XSHG", start, end)` 推导；失败回退工作日近似。

- [ ] **Step 1: 写失败测试**

```python
def test_trade_days_in_range(monkeypatch):
    import datetime as _dt
    import pandas as pd
    from app.services import mootdx_service as ms

    class _FakeSrc:
        def get_daily(self, code, start, end):
            idx = pd.DatetimeIndex([
                _dt.datetime(2026, 8, 3), _dt.datetime(2026, 8, 4),
                _dt.datetime(2026, 8, 5)])
            return pd.DataFrame({"open": [1.0] * 3}, index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _FakeSrc())
    days = ms._trade_days_in_range(_dt.date(2026, 8, 1), _dt.date(2026, 8, 6))
    assert days == [_dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)]


def test_trade_days_in_range_fallback_weekday(monkeypatch):
    import datetime as _dt
    from app.services import mootdx_service as ms

    class _FailSrc:
        def get_daily(self, code, start, end):
            raise RuntimeError("boom")

    monkeypatch.setattr(ms, "MootdxSource", lambda: _FailSrc())
    days = ms._trade_days_in_range(_dt.date(2026, 8, 3), _dt.date(2026, 8, 5))
    # 周一(3)周二(4)周三(5)都是工作日
    assert days == [_dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)]
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k trade_days_in_range -v`
Expected: FAIL — `AttributeError: module 'app.services.mootdx_service' has no attribute '_trade_days_in_range'`

- [ ] **Step 3: 实现**

在 `_trade_days_up_to` 函数定义之后新增：

```python
def _trade_days_in_range(start: _date, end: _date) -> list[_date]:
    """返回 [start, end] 内 A 股交易日（从沪深300 日线推导，全区间不截断）。

    与 ``_trade_days_up_to``（90 天窗口）不同，本函数支持 4/1 至今的全区间
    扫描。取数失败回退工作日近似（不阻断检测）。
    """
    src = MootdxSource()
    try:
        df = src.get_daily("000300.XSHG", start.strftime("%Y%m%d"),
                           end.strftime("%Y%m%d"))
        if df is not None and not df.empty:
            return sorted(d.date() for d in df.index if d.date() <= end)
    except Exception as e:  # noqa: BLE001
        logger.warning("mootdx_service: 交易日历获取失败: %s", e)
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += _dt.timedelta(days=1)
    return days
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k trade_days_in_range -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
cd backend
git add app/services/mootdx_service.py tests/quant/test_mootdx_backfill_coverage.py
git commit -m "feat(mootdx): _trade_days_in_range 全区间交易日历推导"
```

---

### Task 2: `scan_missing_partitions` — 5 类数据分区缺失扫描

**Files:**
- Modify: `backend/app/services/mootdx_service.py`
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`

**Interfaces:**
- Consumes: `_trade_days_in_range(start, end)`, `_partition_dates(root)`
- Produces: `mootdx_service.scan_missing_partitions(start: _date | None = None) -> dict[str, list[_date]]`
  - 返回 `{"kline_daily": [...], "kline_etf_daily": [...], "kline_index_daily": [...], "kline_etf_minute": [...], "kline_minute": [...]}`，键名与现有 `backfill_to_now` 的 `result["missing"]` 一致风格；值为缺失交易日列表。

- [ ] **Step 1: 写失败测试**

```python
def test_scan_missing_partitions_finds_middle_gap(tmp_path, monkeypatch):
    import datetime as _dt
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_trade_days_in_range", lambda s, e: [
        _dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])

    # 只有 8/3、8/5 有分区，8/4 缺失（中间洞）
    for name in ["kline_daily", "kline_etf_daily", "kline_index_daily"]:
        for d in ["2026-08-03", "2026-08-05"]:
            (tmp_path / name / f"date={d}").mkdir(parents=True)
    # 分钟类 8/3、8/5 也有，8/4 缺失
    for name in ["kline_etf_minute", "kline_minute"]:
        for d in ["2026-08-03", "2026-08-05"]:
            (tmp_path / name / f"date={d}").mkdir(parents=True)

    missing = ms.scan_missing_partitions()
    assert missing["kline_daily"] == [_dt.date(2026, 8, 4)]
    assert missing["kline_etf_daily"] == [_dt.date(2026, 8, 4)]
    assert missing["kline_index_daily"] == [_dt.date(2026, 8, 4)]
    assert missing["kline_etf_minute"] == [_dt.date(2026, 8, 4)]
    assert missing["kline_minute"] == [_dt.date(2026, 8, 4)]
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k scan_missing_partitions -v`
Expected: FAIL — `AttributeError: ... has no attribute 'scan_missing_partitions'`

- [ ] **Step 3: 实现**

在 `_trade_days_in_range` 之后新增：

```python
def _missing_days_in(calendar: list[_date], root: Path) -> list[_date]:
    """calendar 中不在 root 的 date= 分区里的日期（中间洞也检）。"""
    existing = set(_partition_dates(root))
    return [d for d in calendar if d.isoformat() not in existing]


def scan_missing_partitions(start: _date | None = None) -> dict[str, list[_date]]:
    """分区级缺失扫描：4/1（或 start）至今，5 类数据按交易日历逐日比对。

    检测「交易日历上有、但分区目录无 date= 分区」的日期，含中间洞。
    仅分区级（分区存在即视为该日已覆盖），不逐 symbol 校验。
    """
    today = _date.today()
    calendar = _trade_days_in_range(start or STOCK_MINUTE_START, today)
    return {
        "kline_daily":       _missing_days_in(calendar, STOCK_DAILY_ROOT),
        "kline_etf_daily":   _missing_days_in(calendar, ETF_DAILY_ROOT),
        "kline_index_daily": _missing_days_in(calendar, INDEX_DAILY_ROOT),
        "kline_etf_minute":  _missing_days_in(calendar, ETF_MINUTE_ROOT),
        "kline_minute":      _missing_days_in(calendar, STOCK_MINUTE_ROOT),
    }
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k scan_missing_partitions -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
cd backend
git add app/services/mootdx_service.py tests/quant/test_mootdx_backfill_coverage.py
git commit -m "feat(mootdx): scan_missing_partitions 5 类数据分区缺失扫描"
```

---

### Task 3: `sync_etf_minute` 支持历史日（`get_minute` 分支）

**Files:**
- Modify: `backend/app/services/mootdx_service.py:80-127`（`sync_etf_minute`）
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`

**Interfaces:**
- Consumes: `_etf_universe()`, `MootdxSource.get_minute` / `get_minute_recent`, `_write_minute_partition`
- Produces: 保持 `sync_etf_minute(day: _date | None = None) -> int` 签名不变；历史日（距今天 > 5 个自然日）改用 `get_minute` 全量拉 + 过滤当日。

- [ ] **Step 1: 写失败测试**

```python
def test_sync_etf_minute_historical_day_uses_get_minute(tmp_path, monkeypatch):
    import datetime as _dt
    import pandas as pd
    import polars as pl
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159518.XSHE"])

    class _Src:
        def get_minute(self, code, max_bars=30000):
            idx = pd.DatetimeIndex([
                _dt.datetime(2026, 6, 15, 10, 30), _dt.datetime(2026, 6, 15, 10, 31)])
            return pd.DataFrame({"open": [1.0, 1.0], "close": [1.0, 1.0],
                                 "volume": [100.0, 100.0], "amount": [100.0, 100.0]},
                                index=idx)

    # 强制走历史日分支（day 距今天 > 5 天）
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _dt.date(2026, 8, 6))})())
    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    n = ms.sync_etf_minute(_dt.date(2026, 6, 15))
    assert n == 2
    part = tmp_path / "kline_etf_minute" / "date=2026-06-15" / "part.parquet"
    assert part.exists()
    df = pl.read_parquet(part)
    assert len(df) == 2
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k sync_etf_minute_historical -v`
Expected: FAIL（当前 `sync_etf_minute` 用 `get_minute_recent`，`_Src` 无该方法 → 抛 AttributeError）

- [ ] **Step 3: 实现**

修改 `sync_etf_minute`，在取数处按 `day` 是否历史日分支：

```python
def sync_etf_minute(day: _date | None = None) -> int:
    """收盘后同步指定交易日（默认今天）全部 ETF 分钟到按日分区。

    逐标的拉真实 1m：近期日（≤5 天）用 ``get_minute_recent``（含当日盘中），
    历史日（>5 天）用 ``get_minute`` 全量拉再过滤当日（支持 4/1 起缺失日回补）。
    以 ``date={day}/part.parquet`` 原子写盘。返回写入行数。
    """
    day = day or _date.today()
    src = MootdxSource()
    codes = _etf_universe()
    if not codes:
        logger.warning("mootdx_service: ETF 宇宙为空，跳过分钟同步")
        return 0
    historical = (day < _date.today() - _dt.timedelta(days=5))
    frames = []
    for i, jq in enumerate(codes):
        try:
            if historical:
                df = src.get_minute(jq, max_bars=40000)
            else:
                df = src.get_minute_recent(jq, pages=2)
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", jq, e)
            continue
        if df is None or df.empty:
            continue
        df = df.copy()
        df["symbol"] = _to_tf_symbol(jq)
        df = df.reset_index()
        keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date == day]
        if df.empty:
            continue
        frames.append(pl.from_pandas(df))
        if (i + 1) % 500 == 0:
            try:
                src._client = None
                src._server_idx = -1
            except Exception:  # noqa: BLE001
                pass
    if not frames:
        return 0
    out = pl.concat(frames).unique(
        subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
    out = out.with_columns(
        pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
    return _write_minute_partition(out, ETF_MINUTE_ROOT, day)
```

> 注意：`_Src.get_minute` 返回的帧无 `open`/`high`/`low`/`amount` 列时，`keep` 补齐逻辑已处理（`if c not in df.columns: df[c] = None`）。测试帧补全了 close/volume/amount，`open`/`high`/`low` 走 None 分支。若 polars 对全 None 列 cast 报错，测试帧补 `open`/`high`/`low` 列。

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k sync_etf_minute_historical -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
cd backend
git add app/services/mootdx_service.py tests/quant/test_mootdx_backfill_coverage.py
git commit -m "feat(mootdx): sync_etf_minute 历史日走 get_minute 回补"
```

---

### Task 4: `sync_stock_minute_day` — 按缺失日补全股票分钟

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（`sync_stock_minute` 之后新增）
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`

**Interfaces:**
- Consumes: `_stock_universe()`, `_listing_date_map()`, `_guarded_get_minute()`, `_append_failure()`, `_flush_stock_minute_chunk()`
- Produces: `mootdx_service.sync_stock_minute_day(day: _date) -> int`
  - 按缺失日补全 `kline_minute/date={day}/part.parquet`；返回写入行数。

- [ ] **Step 1: 写失败测试**

```python
def test_sync_stock_minute_day_filters_listing_and_writes(tmp_path, monkeypatch):
    import datetime as _dt
    import pandas as pd
    from app.services import mootdx_service as ms

    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [
        "000001.SZ", "600000.SH", "999999.SZ"])
    # 600000 上市晚于目标日 → 跳过不取数；000001 停牌（无该日 bar）；999999 有
    monkeypatch.setattr(ms, "_listing_date_map", lambda: {
        "000001.SZ": _dt.date(2020, 1, 1),
        "600000.SH": _dt.date(2026, 8, 1),   # 上市晚于 6/15 → 跳过
        "999999.SZ": _dt.date(2020, 1, 1),
    })

    class _Src:
        def get_minute(self, sym, max_bars=40000):
            if sym == "000001.SZ":  # 停牌：无该日 bar
                idx = pd.DatetimeIndex([_dt.datetime(2026, 6, 16, 9, 31)])
            else:  # 999999 有 6/15 的两根
                idx = pd.DatetimeIndex([
                    _dt.datetime(2026, 6, 15, 9, 31),
                    _dt.datetime(2026, 6, 15, 9, 32)])
            return pd.DataFrame({"open": [1.0] * len(idx), "close": [1.0] * len(idx),
                                 "volume": [100.0] * len(idx), "amount": [100.0] * len(idx)},
                                index=idx)

    monkeypatch.setattr(ms, "MootdxSource", lambda: _Src())
    n = ms.sync_stock_minute_day(_dt.date(2026, 6, 15))
    # 只有 999999 写入 2 根（600000 上市过滤跳过，000001 停牌该日无 bar）
    assert n == 2
    part = tmp_path / "kline_minute" / "date=2026-06-15" / "part.parquet"
    assert part.exists()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k sync_stock_minute_day -v`
Expected: FAIL — `AttributeError: ... has no attribute 'sync_stock_minute_day'`

- [ ] **Step 3: 实现**

在 `sync_stock_minute` 定义之后新增：

```python
def sync_stock_minute_day(day: _date) -> int:
    """按缺失日补全全市场股票分钟到 ``kline_minute/date={day}``。

    「有数据才回」：上市日晚于目标日的 symbol 跳过（该日尚未上市）；
    当日停牌/无 bar 自然跳过不落盘。逐 symbol 用 ``get_minute`` 全量拉，
    过滤到 ``day`` 后批量写分区（复用 ``_flush_stock_minute_chunk``）。
    返回写入行数。
    """
    src = MootdxSource()
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    if not stocks:
        logger.warning("mootdx_service: 股票宇宙为空，跳过分钟同步")
        return 0
    listing = _listing_date_map()
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    total = 0
    chunk: list[pl.DataFrame] = []
    for i, sym in enumerate(stocks):
        ld = listing.get(sym)
        if ld is not None and ld > day:
            continue  # 上市晚于目标日，该日无数据
        try:
            df = _guarded_get_minute(src, sym, max_bars=40000)
        except TimeoutError:
            src = MootdxSource()
            _append_failure(sym, "timeout")
            continue
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", sym, e)
            _append_failure(sym, f"exception:{str(e)[:60]}")
            continue
        if df is None or df.empty:
            _append_failure(sym, "empty")
            continue
        df = df.copy()
        df["symbol"] = sym
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date == day]
        if df.empty:
            continue  # 当日停牌/无 bar，跳过
        sub = pl.from_pandas(df)
        sub = sub.with_columns(pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
        sub = sub.unique(subset=["symbol", "datetime"], keep="last")
        chunk.append(sub)
        total += sub.height
        if len(chunk) >= _STOCK_MINUTE_BATCH:
            _flush_stock_minute_chunk(chunk)
            chunk = []
            src = MootdxSource()
    if chunk:
        _flush_stock_minute_chunk(chunk)
    logger.info("mootdx_service: 股票分钟按日回源 %s 完成, %d 行", day, total)
    return total
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k sync_stock_minute_day -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
cd backend
git add app/services/mootdx_service.py tests/quant/test_mootdx_backfill_coverage.py
git commit -m "feat(mootdx): sync_stock_minute_day 按缺失日回补股票分钟"
```

---

### Task 5: `backfill_missing_partitions` + `scan_and_backfill_full`

**Files:**
- Modify: `backend/app/services/mootdx_service.py`
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`

**Interfaces:**
- Consumes: `sync_daily`, `sync_index_daily`, `sync_etf_minute`, `sync_stock_minute_day`, `scan_missing_partitions`
- Produces:
  - `mootdx_service.backfill_missing_partitions(missing: dict[str, list[_date]]) -> dict`
    - 返回 `{"daily_days": [...], "index_daily_days": [...], "etf_minute_days": [...], "stock_minute_days": [...], "errors": [...]}`
  - `mootdx_service.scan_and_backfill_full() -> dict`
    - 返回 `{"missing": missing, "backfilled": backfilled, "errors": [...]}`

- [ ] **Step 1: 写失败测试**

```python
def test_backfill_missing_partitions_routes_to_sync(monkeypatch):
    import datetime as _dt
    from app.services import mootdx_service as ms

    calls = {"daily": [], "index": [], "etf_min": [], "stock_min": []}
    monkeypatch.setattr(ms, "sync_daily", lambda d: calls["daily"].append(d) or {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: calls["index"].append(d) or {"written": 1})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d: calls["etf_min"].append(d) or 5)
    monkeypatch.setattr(ms, "sync_stock_minute_day", lambda d: calls["stock_min"].append(d) or 7)

    missing = {
        "kline_daily":       [_dt.date(2026, 6, 15)],
        "kline_etf_daily":   [_dt.date(2026, 6, 16)],
        "kline_index_daily": [_dt.date(2026, 6, 17)],
        "kline_etf_minute":  [_dt.date(2026, 6, 18)],
        "kline_minute":      [_dt.date(2026, 6, 19)],
    }
    res = ms.backfill_missing_partitions(missing)
    assert calls["daily"] == [_dt.date(2026, 6, 15), _dt.date(2026, 6, 16)]
    assert calls["index"] == [_dt.date(2026, 6, 17)]
    assert calls["etf_min"] == [_dt.date(2026, 6, 18)]
    assert calls["stock_min"] == [_dt.date(2026, 6, 19)]
    assert res["errors"] == []


def test_backfill_missing_partitions_survives_per_day_error(monkeypatch):
    import datetime as _dt
    from app.services import mootdx_service as ms

    def _boom(d):
        raise RuntimeError("sync failed")

    monkeypatch.setattr(ms, "sync_daily", _boom)
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: {"written": 1})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d: 0)
    monkeypatch.setattr(ms, "sync_stock_minute_day", lambda d: 0)

    missing = {
        "kline_daily": [_dt.date(2026, 6, 15), _dt.date(2026, 6, 16)],
        "kline_index_daily": [_dt.date(2026, 6, 17)],
    }
    res = ms.backfill_missing_partitions(missing)
    assert len(res["errors"]) == 2  # 两日都失败，但 index 仍补了
    assert "2026-06-17" in res["index_daily_days"]
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k backfill_missing -v`
Expected: FAIL — `AttributeError: ... has no attribute 'backfill_missing_partitions'`

- [ ] **Step 3: 实现**

在 `sync_stock_minute_day` 之后新增：

```python
def backfill_missing_partitions(missing: dict[str, list[_date]]) -> dict:
    """逐缺失日复用现有 sync 函数补全，单日失败记 errors 不阻断。

    ``missing`` 键名与 ``scan_missing_partitions`` 一致：
    kline_daily / kline_etf_daily → sync_daily；kline_index_daily →
    sync_index_daily；kline_etf_minute → sync_etf_minute；
    kline_minute → sync_stock_minute_day。
    """
    result: dict = {
        "daily_days": [], "index_daily_days": [],
        "etf_minute_days": [], "stock_minute_days": [], "errors": [],
    }

    for day in missing.get("kline_daily", []) + missing.get("kline_etf_daily", []):
        try:
            sync_daily(day)
            result["daily_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"daily {day}: {e}")
    for day in missing.get("kline_index_daily", []):
        try:
            sync_index_daily(day)
            result["index_daily_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"index_daily {day}: {e}")
    for day in missing.get("kline_etf_minute", []):
        try:
            sync_etf_minute(day)
            result["etf_minute_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"etf_minute {day}: {e}")
    for day in missing.get("kline_minute", []):
        try:
            sync_stock_minute_day(day)
            result["stock_minute_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"stock_minute {day}: {e}")

    return result


def scan_and_backfill_full() -> dict:
    """00:00 全量巡检 + 补全入口：扫描缺失 → 逐日补全 → 汇总。"""
    missing = scan_missing_partitions()
    backfilled = backfill_missing_partitions(missing)
    total = sum(len(v) for v in missing.values())
    logger.info("mootdx_service: 全量扫描 %d 缺失日, 补全 %s, errors=%s",
                total, {k: len(v) for k, v in backfilled.items()},
                len(backfilled["errors"]))
    return {"missing": missing, "backfilled": backfilled,
            "errors": backfilled["errors"]}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k backfill_missing -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
cd backend
git add app/services/mootdx_service.py tests/quant/test_mootdx_backfill_coverage.py
git commit -m "feat(mootdx): backfill_missing_partitions + scan_and_backfill_full"
```

---

### Task 6: `_midnight_scan_loop` — 00:00 定时巡检

**Files:**
- Modify: `backend/app/services/stockdata/scheduler.py`
- Test: `backend/tests/quant/test_scheduler_scan.py`（新文件）

**Interfaces:**
- Consumes: `mootdx_service.scan_and_backfill_full()`
- Produces: `stockdata.scheduler._midnight_scan_loop()`（00:00 触发，`_sync_lock` 串行）

- [ ] **Step 1: 写失败测试**

```python
def test_midnight_scan_loop_triggers_full_scan(monkeypatch):
    import threading
    from app.services.stockdata import scheduler as sched

    fired = {"n": 0}
    monkeypatch.setattr(sched, "_sync_lock", threading.Lock())
    monkeypatch.setattr(sched, "_lock", threading.Lock())
    monkeypatch.setattr(sched, "_scheduler_state", {"last_full_scan": None, "full_scan_result": None})
    monkeypatch.setattr(sched, "_stop", threading.Event())
    monkeypatch.setattr(
        sched, "_dt",
        type("DT", (), {
            "datetime": type("DT2", (), {
                "now": staticmethod(lambda: __import__("datetime").datetime(2026, 8, 6, 0, 0, 10)),
            }),
            "date": __import__("datetime").date,
            "time": __import__("datetime").time,
        })())

    import app.services.mootdx_service as ms
    def _fake_full():
        fired["n"] += 1
        return {"missing": {}, "backfilled": {}, "errors": []}
    monkeypatch.setattr(ms, "scan_and_backfill_full", _fake_full)

    # 手动跑一轮循环体（内部 while 依赖 _stop，这里直接调用私有方法一次）
    sched._midnight_scan_loop.run_once = True  # noqa: BLE001
    # 因 _midnight_scan_loop 是 while 循环，测试改为直接验证调度函数存在且
    # 触发逻辑正确：调用其内部单次执行体（提取为 _run_full_scan_once）
    assert hasattr(sched, "_run_full_scan_once")
    sched._run_full_scan_once()
    assert fired["n"] == 1
```

> 说明：为避免测试长时间 while 循环，实现将「单次执行体」提取为 `_run_full_scan_once()`，`_midnight_scan_loop` 只是 00:00 定时调用它。测试直接调 `_run_full_scan_once`。

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_scheduler_scan.py -v`
Expected: FAIL — `AttributeError: module '...scheduler' has no attribute '_run_full_scan_once'`

- [ ] **Step 3: 实现**

修改 `scheduler.py`：

```python
def _run_full_scan_once() -> None:
    """00:00 全量缺失巡检 + 补全（单次执行体，与 15:35 用 _sync_lock 串行）。"""
    with _sync_lock:
        try:
            from app.services import mootdx_service
            res = mootdx_service.scan_and_backfill_full()
            with _lock:
                _scheduler_state["last_full_scan"] = str(_dt.datetime.now())
                _scheduler_state["full_scan_result"] = res
            logger.info("stockdata midnight full scan done: %s",
                        {k: len(v) for k, v in (res.get("missing") or {}).items()}
                        if isinstance(res.get("missing"), dict) else res)
        except Exception:  # noqa: BLE001
            logger.exception("stockdata midnight full scan failed")


def _midnight_scan_loop():
    """00:00 触发全量缺失巡检；每日一次，跨日重置。"""
    last_date = None
    while not _stop.is_set():
        now = _dt.datetime.now()
        if (now.time() >= _dt.time(0, 0) and now.time() < _dt.time(0, 1)
                and now.date() != last_date):
            last_date = now.date()
            threading.Thread(target=_run_full_scan_once, daemon=True).start()
        time.sleep(20)
```

并在 `start_scheduler` 的 `targets` 列表加入 `_midnight_scan_loop`：

```python
    targets = [_backfill_loop, _sync_cron_loop, _midnight_scan_loop]
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_scheduler_scan.py -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
cd backend
git add app/services/stockdata/scheduler.py tests/quant/test_scheduler_scan.py
git commit -m "feat(stockdata): 00:00 全量缺失巡检 _midnight_scan_loop"
```

---

### Task 7: 全量回归 + lint/typecheck

**Files:**
- Verify: `backend/app/services/mootdx_service.py`, `backend/app/services/stockdata/scheduler.py`

- [ ] **Step 1: 跑相关测试**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py tests/quant/test_sync_adj_factor.py -q`
Expected: 全部通过（含既有用例）

- [ ] **Step 2: 跑 lint**

Run: `cd backend && uv run --extra dev ruff check app/services/mootdx_service.py app/services/stockdata/scheduler.py`
Expected: 仅预存的 RUF002/RUF003 中文标点类告警（与改动前基线一致），无新增功能性错误。

- [ ] **Step 3: 跑 typecheck**

Run: `cd backend && uv run --extra dev mypy app/services/mootdx_service.py`
Expected: 仅预存的 5 个 mypy 错误（`timedelta` 相关，与改动前基线一致），无新增。

- [ ] **Step 4: 提交**

```bash
cd backend
git add -A
git commit -m "chore: 全量回归通过（00:00 巡检功能）"  # 若无可提交内容则跳过
```

---
