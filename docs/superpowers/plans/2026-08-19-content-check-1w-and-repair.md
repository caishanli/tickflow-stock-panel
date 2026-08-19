# 本地数据内容校验（近 1 周自动 + 手动单日/全量补齐）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 mootdx 5 类 Parquet 分区加内容级校验（symbol 覆盖率），每日自动只查近 1 周交易日，支持手动单日/全量检验补齐，并修复新交易日股票分钟当日不落盘问题。

**Architecture:** 在 `mootdx_service.py` 加通用内容校验 helper + 5 类 `_incomplete_*_days` 包装；`scan_missing_partitions` 加 `content_recent` 参数（默认近 1 周）；新增 `check_and_repair_day` / `check_and_repair_full`；`sync_stock_minute` 开头先 range 补整日缺失。经 stockdata 服务 `trigger_sync` 暴露新 kinds，主后端 `data.py` 加两个 POST 端点，前端 LocalData 页加行内「检验」与顶部「全量检验补齐」按钮。

**Tech Stack:** Python FastAPI + Polars + stockdata TCP 服务（msgpack）；React 18 + Vite + TS + @tanstack/react-query。

## Global Constraints

- 内容校验窗口常量：`_CONTENT_CHECK_RECENT_DAYS`（env `CONTENT_CHECK_RECENT_DAYS`，默认 `250`，全量用）；`_DAILY_CHECK_RECENT_PARTITIONS`（env `DAILY_CHECK_RECENT_PARTITIONS`，默认 `7`，每日自动用）；`_CONTENT_CHECK_MIN_COVERAGE`（env `CONTENT_CHECK_MIN_COVERAGE`，默认 `0.5`）。
- 保留 `_STOCK_MINUTE_RECENT_LIMIT`（env 默认改为 `250`）与 `_STOCK_MINUTE_MIN_COVERAGE`（env 默认 `0.5`）作股票分钟 legacy 覆盖；删除 `_ETF_DAILY_RECENT_LIMIT` / `_ETF_DAILY_MIN_COVERAGE`。
- 覆盖率 = `|分区symbol ∩ 基准宇宙| / |基准宇宙|`，`< 阈值` 即判残缺需重写。
- 宇宙为空 / 分区根为空 → 跳过该类型校验（返回 `[]`，不误判）。
- 盘中（`_market_closed()` 为 False）跳过当日分区；收盘后纳入。
- symbol 归一化：股票/指数直接 `.SH/.SZ`；ETF 用 `_to_tf_symbol` 把 JQ 码（`.XSHG/.XSHE`）转 `.SH/.SZ`。
- 既有函数签名不可变：`_missing_minute_days(now=None)`（调用方按位置传 `now`）、`_incomplete_etf_daily_days(recent=None)`、`_incomplete_stock_minute_days(recent=None)`。
- 板块分区类型→重写函数：stock/ETF 日线→`sync_daily(day)`；指数日线→`sync_index_daily(day)`；ETF 分钟→`sync_etf_minute(day)`；股票分钟单日→`sync_stock_minute_day(day, symbols=missing)`（只补缺失）；股票分钟整日缺失→`sync_stock_minute_range(days)`。
- 前端无测试脚本，验证用 `pnpm lint` + `pnpm build`。
- 全部验证命令：
  ```bash
  cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -q
  cd backend && uv run --extra dev pytest tests/quant/test_stockdata_scheduler.py tests/quant/test_stockdata_handlers.py -q
  cd backend && uv run --extra dev pytest tests/test_local_market_stats.py -q
  cd backend && uv run --extra dev ruff check app
  cd backend && uv run --extra dev mypy app
  cd frontend && pnpm lint && pnpm build
  ```

---

### Task 1: 内容校验框架（常量 + 通用 helper + 5 类包装）

**Files:**
- Modify: `backend/app/services/mootdx_service.py:47-57`（常量）、新增 helper 与包装（替换 `_incomplete_etf_daily_days`/`_incomplete_stock_minute_days` 现有实现）
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`（追加）

**Interfaces:**
- Consumes: `_partition_dates(root)`、`_stock_universe()`、`_etf_universe()`、`_index_universe()`、`_to_tf_symbol()`、`_market_closed(now=None)`、`_date.today()`、`pl`
- Produces:
  - `_CONTENT_CHECK_RECENT_DAYS: int`、`_DAILY_CHECK_RECENT_PARTITIONS: int`、`_CONTENT_CHECK_MIN_COVERAGE: float`
  - `_incomplete_partition_days(root, target, recent, min_coverage, skip_today_intraday=True) -> list[_date]`
  - `_incomplete_stock_daily_days(recent=None) -> list[_date]`
  - `_incomplete_etf_daily_days(recent=None) -> list[_date]`
  - `_incomplete_index_daily_days(recent=None) -> list[_date]`
  - `_incomplete_etf_minute_days(recent=None) -> list[_date]`
  - `_incomplete_stock_minute_days(recent=None) -> list[_date]`

- [ ] **Step 1: 追加失败测试**

在 `backend/tests/quant/test_mootdx_backfill_coverage.py` 末尾追加：

```python
# ---------------------------------------------------------------------------
# 近一年内容校验：5 类统一（07-31 指数日线残帧回归）
# ---------------------------------------------------------------------------

def test_incomplete_index_daily_flags_fallback_fragment(tmp_path, monkeypatch):
    """07-31 回归：指数日线用兜底 4 只写入的残帧应被判残缺、完整日不误报。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "_index_universe",
                        lambda: [f"000{i:03d}.SH" for i in range(1, 61)] + ["399006.SZ"])
    root = tmp_path / "kline_index_daily"
    (root / "date=2026-07-31").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["000300.SH", "000510.SH", "399006.SZ", "399101.SZ"],
        "open": [1.0] * 4, "close": [1.0] * 4,
    }).write_parquet(root / "date=2026-07-31" / "part.parquet")
    (root / "date=2026-08-03").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"000{i:03d}.SH" for i in range(1, 61)] + ["399006.SZ"],
        "open": [1.0] * 61, "close": [1.0] * 61,
    }).write_parquet(root / "date=2026-08-03" / "part.parquet")
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)

    leftovers = ms._incomplete_index_daily_days()
    assert _d(2026, 7, 31) in leftovers, "4/61 残帧应被判残缺"
    assert _d(2026, 8, 3) not in leftovers, "完整日不应误报"


def test_incomplete_stock_daily_detects_sparse(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(90)])
    root = tmp_path / "kline_daily"
    for d in ["2026-08-03", "2026-08-04"]:
        (root / f"date={d}").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"6000{dd:03d}.SH" for dd in range(90)],
        "open": [1.0] * 90, "close": [1.0] * 90,
    }).write_parquet(root / "date=2026-08-03" / "part.parquet")
    pl.DataFrame({"symbol": ["600000.SH"], "open": [1.0], "close": [1.0]}).write_parquet(
        root / "date=2026-08-04" / "part.parquet")
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)

    leftovers = ms._incomplete_stock_daily_days()
    assert _d(2026, 8, 4) in leftovers
    assert _d(2026, 8, 3) not in leftovers


def test_incomplete_etf_minute_detects_sparse(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "_etf_universe",
                        lambda: [f"1599{dd:02d}.XSHE" for dd in range(90)])
    root = tmp_path / "kline_etf_minute"
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["159900.SZ"],
        "datetime": [_dt.datetime(2026, 8, 4, 9, 31)],
        "close": [1.0],
    }).write_parquet(root / "date=2026-08-04" / "part.parquet")
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)

    assert _d(2026, 8, 4) in ms._incomplete_etf_minute_days()


def test_incomplete_etf_daily_window_250_vs_7(tmp_path, monkeypatch):
    """残缺分区位于第 31~250 位之间：recent=250 识别、recent=7 不识别。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "_etf_universe",
                        lambda: [f"1599{dd:02d}.XSHE" for dd in range(90)])
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 1))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    root = tmp_path / "kline_etf_daily"
    for i in range(60):
        ds = (_d(2026, 8, 1) - _dt.timedelta(days=i)).isoformat()
        (root / f"date={ds}").mkdir(parents=True, exist_ok=True)
        if i == 40:  # 第 41 新的分区为残帧（1 只）
            df = pl.DataFrame({"symbol": ["159900.SZ"], "open": [1.0], "close": [1.0]})
        else:
            df = pl.DataFrame({
                "symbol": [f"1599{dd:02d}.SZ" for dd in range(90)],
                "open": [1.0] * 90, "close": [1.0] * 90,
            })
        df.write_parquet(root / f"date={ds}" / "part.parquet")

    assert ms._incomplete_etf_daily_days(recent=7) == [], "近 7 分区内无残缺"
    left = ms._incomplete_etf_daily_days(recent=250)
    assert len(left) == 1, f"recent=250 应识别 1 个残帧, 实际 {left}"
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k "index_daily_flags or stock_daily_detects or etf_minute_detects or daily_window_250" -q
```

Expected: FAIL（`_incomplete_index_daily_days` / `_incomplete_stock_daily_days` / `_incomplete_etf_minute_days` 未定义等）。

- [ ] **Step 3: 实现常量与通用 helper**

把 `mootdx_service.py:47-57` 的常量块替换为：

```python
# 内容完整性校验（symbol 覆盖率 vs 基准宇宙）：
# 覆盖率低于阈值即判残缺重写，防止"目录存在但只剩几只"的残帧永久污染。
# 全量手动校验回看近一年（默认 250 个交易分区）；每日自动只查近 1 周。
_CONTENT_CHECK_RECENT_DAYS = int(os.getenv("CONTENT_CHECK_RECENT_DAYS", "250"))
_DAILY_CHECK_RECENT_PARTITIONS = int(os.getenv("DAILY_CHECK_RECENT_PARTITIONS", "7"))
_CONTENT_CHECK_MIN_COVERAGE = float(os.getenv("CONTENT_CHECK_MIN_COVERAGE", "0.5"))
# 股票分钟 legacy 覆盖（env 可调；默认与共享常量一致）。
_STOCK_MINUTE_MIN_COVERAGE = float(os.getenv("STOCK_MINUTE_MIN_COVERAGE", "0.5"))
_STOCK_MINUTE_RECENT_LIMIT = int(os.getenv("STOCK_MINUTE_RECENT_LIMIT", "250"))
```

在 `_incomplete_stock_minute_days`（当前约 1114-1169 行）之前插入通用 helper 与 5 类包装，并**删除**旧的 `_incomplete_etf_daily_days`（当前约 1067-1111 行）与旧 `_incomplete_stock_minute_days`（当前约 1114-1169 行）的实现（其 docstring 已并入新实现）：

```python
def _incomplete_partition_days(root: Path, target: set[str], recent: int,
                               min_coverage: float,
                               skip_today_intraday: bool = True) -> list[_date]:
    """最近 recent 个分区 symbol 覆盖率 < min_coverage 即判残缺（目录存在≠完整）。

    root: 分区根目录；target: 已归一化的基准宇宙 symbol 集合。
    分区根 / 宇宙为空 → []（无基线可比）。盘中跳过当日分区（防半程误伤）。
    """
    existing = _partition_dates(root)
    if not existing or not target:
        return []
    today = _date.today()
    out: list[_date] = []
    for ds in existing[-recent:]:
        d = _dt.date.fromisoformat(ds)
        if skip_today_intraday and d == today and not _market_closed():
            logger.info("mootdx_service: 当日 %s 盘中未收盘，跳过内容校验", ds)
            continue
        pdir = root / f"date={ds}"
        parts = sorted(pdir.glob("*.parquet"))
        if not parts:
            continue
        syms: set[str] = set()
        for p in parts:
            try:
                df = pl.read_parquet(p, columns=["symbol"])
                syms |= set(df["symbol"].to_list())
            except Exception:  # noqa: BLE001
                continue
        coverage = len(syms & target) / len(target)
        if coverage < min_coverage:
            out.append(d)
    logger.info("mootdx_service: 内容校验 %s 最近 %d 分区, 残缺 %d: %s",
                root.name, len(existing[-recent:]), len(out),
                [d.isoformat() for d in out])
    return out


def _incomplete_stock_daily_days(recent: int | None = None) -> list[_date]:
    """股票日线内容残缺分区（symbol 覆盖率 << 股票宇宙）。"""
    try:
        codes = _stock_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 股票宇宙读取失败，跳过股票日线内容校验",
                       exc_info=True)
        return []
    return _incomplete_partition_days(
        STOCK_DAILY_ROOT, set(codes),
        recent or _CONTENT_CHECK_RECENT_DAYS, _CONTENT_CHECK_MIN_COVERAGE)


def _incomplete_etf_daily_days(recent: int | None = None) -> list[_date]:
    """ETF 日线内容残缺分区（symbol 覆盖率 << ETF 宇宙）。"""
    try:
        codes = _etf_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: ETF 宇宙读取失败，跳过 ETF 日线内容校验",
                       exc_info=True)
        return []
    if not codes:
        return []
    target = set(_to_tf_symbol(c) for c in codes)
    return _incomplete_partition_days(
        ETF_DAILY_ROOT, target,
        recent or _CONTENT_CHECK_RECENT_DAYS, _CONTENT_CHECK_MIN_COVERAGE)


def _incomplete_index_daily_days(recent: int | None = None) -> list[_date]:
    """指数日线内容残缺分区（symbol 覆盖率 << 指数宇宙）。

    07-31 案例：instruments_index 缺失时曾用兜底 4 只指数写入，4/600 残帧
    目录存在 → 分区级扫描永不重写；此处内容校验兜住。
    """
    try:
        codes = _index_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 指数宇宙读取失败，跳过指数日线内容校验",
                       exc_info=True)
        return []
    if not codes:
        return []
    return _incomplete_partition_days(
        INDEX_DAILY_ROOT, set(codes),
        recent or _CONTENT_CHECK_RECENT_DAYS, _CONTENT_CHECK_MIN_COVERAGE)


def _incomplete_etf_minute_days(recent: int | None = None) -> list[_date]:
    """ETF 分钟内容残缺分区（symbol 覆盖率 << ETF 宇宙）。"""
    try:
        codes = _etf_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: ETF 宇宙读取失败，跳过 ETF 分钟内容校验",
                       exc_info=True)
        return []
    if not codes:
        return []
    target = set(_to_tf_symbol(c) for c in codes)
    return _incomplete_partition_days(
        ETF_MINUTE_ROOT, target,
        recent or _CONTENT_CHECK_RECENT_DAYS, _CONTENT_CHECK_MIN_COVERAGE)


def _incomplete_stock_minute_days(recent: int | None = None) -> list[_date]:
    """股票分钟内容残缺分区（symbol 覆盖率 << 股票宇宙）。"""
    try:
        codes = _stock_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 股票宇宙读取失败，跳过分钟内容校验",
                       exc_info=True)
        return []
    if not codes:
        return []
    recent = _STOCK_MINUTE_RECENT_LIMIT if recent is None else recent
    return _incomplete_partition_days(
        STOCK_MINUTE_ROOT, set(codes),
        recent, _STOCK_MINUTE_MIN_COVERAGE)
```

- [ ] **Step 4: 运行新增测试确认通过**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k "index_daily_flags or stock_daily_detects or etf_minute_detects or daily_window_250" -q
```

Expected: PASS（4 passed）。

- [ ] **Step 5: 跑全量既有测试确认无回归**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -q
```

Expected: PASS（既有 `_incomplete_etf_daily_days`/`_incomplete_stock_minute_days` 相关测试仍通过——窗口默认变 250，但测试分区数 < 250，行为不变；`recent=3` 显式传参不受影响）。

- [ ] **Step 6: 提交**

```bash
cd /home/caisl/tickflow-stock-panel && git add backend/app/services/mootdx_service.py backend/tests/quant/test_mootdx_backfill_coverage.py && git commit -m "feat: 5类数据内容校验框架(helper+包装, 全量窗口250)"
```

---

### Task 2: `_missing_stock_minute_days` + `sync_stock_minute` resume 架空修复

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（新增 `_missing_stock_minute_days`；改 `sync_stock_minute`）
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`（追加）

**Interfaces:**
- Consumes: `_partition_dates(STOCK_MINUTE_ROOT)`、`_trade_days_up_to(today)`、`_market_closed(now)`、`sync_stock_minute_range(days)`、`_minute_fragment_days`、`_existing_minute_symbols`
- Produces: `_missing_stock_minute_days(now: _dt.datetime | None = None) -> list[_date]`（Task 3/4 依赖）；`sync_stock_minute(limit=None) -> int` 现在先 range 补缺失交易日。

- [ ] **Step 1: 追加失败测试**

```python
# ---------------------------------------------------------------------------
# 新交易日股票分钟当日落盘（resume 架空修复）
# ---------------------------------------------------------------------------

def test_missing_stock_minute_days_intraday_and_after_close(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    assert ms._missing_stock_minute_days(_dt.datetime(2026, 8, 5, 10, 55)) == [], \
        "盘中今天不补（防半程数据）"
    assert ms._missing_stock_minute_days(_dt.datetime(2026, 8, 5, 16, 0)) == [_dt.date(2026, 8, 5)]


def test_missing_stock_minute_days_past_gap(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    (root / "date=2026-08-03").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 3), _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    days = ms._missing_stock_minute_days(_dt.datetime(2026, 8, 6, 10, 55))
    assert days == [_dt.date(2026, 8, 4), _dt.date(2026, 8, 5)], "盘中补真实历史缺失日，排除今天"


def test_sync_stock_minute_pulls_missing_day_first(monkeypatch, tmp_path):
    """回归：最新分区=昨天完整、今天无分区、已收盘 → 先 range 补今天再 resume。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(3)])
    (root / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"6000{dd:03d}.SH" for dd in range(3)],
        "datetime": [_dt.datetime(2026, 8, 4, 9, 31)] * 3, "close": [1.0] * 3,
    }).write_parquet(root / "date=2026-08-04" / "part.parquet")
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 4), _dt.date(2026, 8, 5)])
    monkeypatch.setattr(ms, "_minute_fragment_days", lambda: {})
    ranges = []
    monkeypatch.setattr(ms, "sync_stock_minute_range",
                        lambda days: ranges.append(list(days)) or 10)

    n = ms.sync_stock_minute(limit=None)

    assert ranges == [[_d(2026, 8, 5)]], "应先 range 补今天"
    assert n == 10, "返回值应包含 range 写入行数"


def test_sync_stock_minute_no_range_when_current(monkeypatch, tmp_path):
    """最新分区=今天 → 不触发 range，正常 resume。"""
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    root = tmp_path / "kline_minute"
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", root)
    monkeypatch.setattr(ms, "_stock_universe",
                        lambda: [f"6000{dd:03d}.SH" for dd in range(3)])
    from datetime import date as _d
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    monkeypatch.setattr(ms, "_market_closed", lambda now=None: True)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [
        _dt.date(2026, 8, 5)])
    (root / "date=2026-08-05").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [f"6000{dd:03d}.SH" for dd in range(3)],
        "datetime": [_dt.datetime(2026, 8, 5, 9, 31)] * 3, "close": [1.0] * 3,
    }).write_parquet(root / "date=2026-08-05" / "part.parquet")
    monkeypatch.setattr(ms, "_minute_fragment_days", lambda: {})
    ranges = []
    monkeypatch.setattr(ms, "sync_stock_minute_range",
                        lambda days: ranges.append(list(days)) or 10)

    n = ms.sync_stock_minute(limit=None)

    assert ranges == [], "最新分区已是今天，不应触发 range"
    assert n == 0, "resume todo 空 → 0 行"
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k "missing_stock_minute or pulls_missing_day or no_range_when_current" -q
```

Expected: FAIL（`_missing_stock_minute_days` 未定义 / 行为不符）。

- [ ] **Step 3: 实现 `_missing_stock_minute_days`**

紧邻 `_missing_minute_days`（当前约 973 行）之后插入：

```python
def _missing_stock_minute_days(now: _dt.datetime | None = None) -> list[_date]:
    """找出股票分钟分区缺失的交易日（latest < d <= today，盘中排除今天）。

    镜像 ``_missing_minute_days``（ETF 分钟），但读 ``STOCK_MINUTE_ROOT``。
    resume 逻辑只看最新分区，新交易日无分区时会被误判"已覆盖"而跳过——
    本函数供 ``sync_stock_minute`` 在 resume 之前先 range 补整日缺失。
    """
    existing = _partition_dates(STOCK_MINUTE_ROOT)
    if not existing:
        return []
    latest = _date.fromisoformat(existing[-1])
    now = now or _dt.datetime.now()
    today = now.date()
    days = [d for d in _trade_days_up_to(today) if latest < d <= today]
    if not _market_closed(now):
        return [d for d in days if d < today]
    return days
```

- [ ] **Step 4: 改 `sync_stock_minute`**

在 `sync_stock_minute`（约 454-475 行）中，`listing = _listing_date_map()` 之后插入：

```python
    listing = _listing_date_map()
    # 新交易日整日缺失：resume 只按最新分区判断，最新分区是昨天且完整时会
    # 误判"已覆盖"而跳过今天（08-18 案例）——先 range 补缺失交易日。盘中
    # 排除今天，不写半程数据（见 _missing_stock_minute_days）。
    range_rows = 0
    missing_days = _missing_stock_minute_days()
    if missing_days:
        range_rows = sync_stock_minute_range(missing_days)
        logger.info("mootdx_service: 股票分钟补齐缺失交易日 %s 共 %d 行",
                    [d.isoformat() for d in missing_days], range_rows)
        src = MootdxSource()  # 长批后重建连接
```

并把两处返回改为并入 `range_rows`：
- `return fragment_rows`（"已全部覆盖"分支）→ `return range_rows + fragment_rows`
- `return total + fragment_rows`（末尾）→ `return range_rows + total + fragment_rows`

- [ ] **Step 5: 运行新增测试确认通过**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k "missing_stock_minute or pulls_missing_day or no_range_when_current" -q
```

Expected: PASS（4 passed）。

- [ ] **Step 6: 全量回归 + 提交**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -q
cd /home/caisl/tickflow-stock-panel && git add backend/app/services/mootdx_service.py backend/tests/quant/test_mootdx_backfill_coverage.py && git commit -m "fix: 新交易日股票分钟当日落盘(sync_stock_minute 先 range 补整日缺失)"
```

---

### Task 3: 内容校验接入扫描/巡检/启动回源

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（`scan_missing_partitions`、`scan_and_backfill_full`、`backfill_to_now`）
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`（追加）

**Interfaces:**
- Consumes: Task 1 的 `_CONTENT_CHECK_RECENT_DAYS` / `_DAILY_CHECK_RECENT_PARTITIONS`、5 类 `_incomplete_*_days(recent=...)`；Task 2 的 `_missing_stock_minute_days()`
- Produces: `scan_missing_partitions(start=None, content_recent=None) -> dict[str, list[_date]]`；`scan_and_backfill_full(content_recent=None) -> dict`（Task 4 依赖）

- [ ] **Step 1: 追加失败测试**

```python
# ---------------------------------------------------------------------------
# 内容校验接入巡检/启动回源
# ---------------------------------------------------------------------------

def test_scan_missing_partitions_content_recent_default(tmp_path, monkeypatch):
    """content_recent 默认近 1 周（7），显式传 250 时透传给全部 _incomplete_*。"""
    import datetime as _dt
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    for key in ["STOCK_DAILY_ROOT", "ETF_DAILY_ROOT", "INDEX_DAILY_ROOT",
                "ETF_MINUTE_ROOT", "STOCK_MINUTE_ROOT"]:
        monkeypatch.setattr(ms, key, tmp_path / key.lower())
    monkeypatch.setattr(ms, "_trade_days_in_range", lambda s, e: [_dt.date(2026, 8, 4)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: ["159900.XSHE"])
    monkeypatch.setattr(ms, "_stale_daily_days", lambda root, now=None: [])
    seen: dict[str, int | None] = {}

    def _mk(name):
        def f(recent=None):
            seen[name] = recent
            return []
        return f

    monkeypatch.setattr(ms, "_incomplete_stock_daily_days", _mk("stock_daily"))
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days", _mk("etf_daily"))
    monkeypatch.setattr(ms, "_incomplete_index_daily_days", _mk("index_daily"))
    monkeypatch.setattr(ms, "_incomplete_etf_minute_days", _mk("etf_minute"))
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", _mk("stock_minute"))

    ms.scan_missing_partitions()
    assert set(seen.values()) == {ms._DAILY_CHECK_RECENT_PARTITIONS}, \
        f"默认应传近 1 周窗口, 实际 {seen}"

    seen.clear()
    ms.scan_missing_partitions(content_recent=250)
    assert set(seen.values()) == {250}, f"显式 250 应透传, 实际 {seen}"


def test_backfill_to_now_resyncs_content_flagged_index(monkeypatch, tmp_path):
    """启动回源用近 1 周窗口：内容残缺的指数日线触发 sync_index_daily 重写。"""
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "ADJ_FACTOR_PATH", tmp_path / "adj_factor_etf" / "all.parquet")
    _stub_etf_nav(monkeypatch)
    monkeypatch.setattr(ms, "_date", type("D", (), {"today": staticmethod(
        lambda: _d(2026, 8, 5))})())
    for name in ["kline_etf_minute", "kline_etf_daily", "kline_index_daily", "kline_daily"]:
        (tmp_path / name / "date=2026-08-04").mkdir(parents=True, exist_ok=True)
    (tmp_path / "adj_factor_etf").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["510300.XSHG"], "trade_date": [_d(2026, 8, 4)], "ex_factor": [1.0],
    }).write_parquet(tmp_path / "adj_factor_etf" / "all.parquet")
    monkeypatch.setattr(ms, "_missing_daily_days", lambda root: [])
    monkeypatch.setattr(ms, "_missing_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(ms, "_missing_stock_minute_days", lambda: [])
    monkeypatch.setattr(ms, "_incomplete_etf_minute_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_daily_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_incomplete_etf_daily_days", lambda recent=None: [])
    monkeypatch.setattr(ms, "_incomplete_stock_minute_days", lambda recent=None: [])

    def _flagged_index(recent=None):
        w = recent or ms._CONTENT_CHECK_RECENT_DAYS
        return [_d(2026, 7, 31)] if w <= ms._DAILY_CHECK_RECENT_PARTITIONS else []

    monkeypatch.setattr(ms, "_incomplete_index_daily_days", _flagged_index)
    monkeypatch.setattr(ms, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(ms, "_adj_factor_stale", lambda: False)
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: 0)
    monkeypatch.setattr(ms, "sync_daily", lambda d: {"stock": 1, "etf": 1})
    monkeypatch.setattr(ms, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(ms, "sync_stock_minute_range", lambda days: 0)
    monkeypatch.setattr(ms, "_notify_missing", lambda m: None)
    days = []
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: days.append(d) or {"written": 1})

    res = ms.backfill_to_now()

    assert days == [_d(2026, 7, 31)], f"启动回源应重写内容残缺指数日线, 实际 {days}"
    assert res["missing"]["kline_index_daily"]["missing"] is True
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k "content_recent_default or resyncs_content_flagged" -q
```

Expected: FAIL（`content_recent` 未实现 / 残缺日未触发重写）。

- [ ] **Step 3: 改 `scan_missing_partitions` 与 `scan_and_backfill_full`**

`scan_missing_partitions` 签名改为 `(start: _date | None = None, content_recent: int | None = None)`，函数体开头：

```python
    today = _date.today()
    calendar = _trade_days_in_range(start or STOCK_MINUTE_START, today)
    from app.services.etf_nav_service import _missing_etf_nav_days as _missing_nav
    content = (_DAILY_CHECK_RECENT_PARTITIONS
               if content_recent is None else content_recent)
    missing_etf_daily = set(_missing_days_in(calendar, ETF_DAILY_ROOT))
    missing_etf_daily |= set(_incomplete_etf_daily_days(recent=content))
    # 盘中半程快照自愈（08-11 案例）：昨日/历史日分区 mtime 早于自身日期
    # 15:00 即判残缺重写。00:00 巡检跨天也能识别（旧实现只查今天）。
    missing_etf_daily |= set(_stale_daily_days(ETF_DAILY_ROOT))
    missing_stock_daily = set(_missing_days_in(calendar, STOCK_DAILY_ROOT))
    missing_stock_daily |= set(_stale_daily_days(STOCK_DAILY_ROOT))
    missing_stock_daily |= set(_incomplete_stock_daily_days(recent=content))
    missing_index_daily = set(_missing_days_in(calendar, INDEX_DAILY_ROOT))
    missing_index_daily |= set(_incomplete_index_daily_days(recent=content))
    missing_etf_minute = set(_missing_days_in(calendar, ETF_MINUTE_ROOT))
    missing_etf_minute |= set(_incomplete_etf_minute_days(recent=content))
    missing_stock_minute = set(_missing_days_in(calendar, STOCK_MINUTE_ROOT))
    missing_stock_minute |= set(_incomplete_stock_minute_days(recent=content))
```

并把 docstring 补充一句：`content_recent` 默认 `_DAILY_CHECK_RECENT_PARTITIONS`（近 1 周），全量手动传 250。

`scan_and_backfill_full` 改为：

```python
def scan_and_backfill_full(content_recent: int | None = None) -> dict:
    """00:00 全量巡检 + 补全入口：扫描缺失 → 逐日补全 → 汇总。

    ``content_recent``：内容校验窗口，默认近 1 周（``_DAILY_CHECK_RECENT_PARTITIONS``）；
    手动全量检验传 250（``_CONTENT_CHECK_RECENT_DAYS``）。
    """
    missing = scan_missing_partitions(content_recent=content_recent)
    backfilled = backfill_missing_partitions(missing)
    total = sum(len(v) for v in missing.values())
    msg = "mootdx_service: 全量扫描 %d 缺失日, 补全 %s, errors=%s"
    args = (total, {k: len(v) for k, v in backfilled.items()},
            len(backfilled["errors"]))
    if backfilled["errors"]:
        logger.warning(msg, *args)
    else:
        logger.info(msg, *args)
    return {"missing": missing, "backfilled": backfilled,
            "errors": backfilled["errors"]}
```

- [ ] **Step 4: 改 `backfill_to_now`**

在 `backfill_to_now` 的 `result` 初始化之后、`result["missing"]` 之前插入内容校验结果（recent=近 1 周）：

```python
    content = _DAILY_CHECK_RECENT_PARTITIONS
    incomplete_etf_minute = set(_incomplete_etf_minute_days(recent=content))
    incomplete_stock_daily = set(_incomplete_stock_daily_days(recent=content))
    incomplete_etf_daily = set(_incomplete_etf_daily_days(recent=content))
    incomplete_index_daily = set(_incomplete_index_daily_days(recent=content))
    incomplete_stock_minute = set(_incomplete_stock_minute_days(recent=content))
    missing_stock_minute_days = set(_missing_stock_minute_days())
```

`result["missing"]` 对应项改为：

```python
        "kline_index_daily":  {"latest": index_daily_days[-1] if index_daily_days else None,
                               "empty": not index_daily_days,
                               "missing": bool(_missing_index_daily_days() or incomplete_index_daily)},
        "kline_etf_minute":   {"latest": etf_minute_days[-1] if etf_minute_days else None,
                               "empty": not etf_minute_days,
                               "missing": bool(_missing_minute_days() or incomplete_etf_minute)},
        "kline_minute":       {"latest": stock_minute_days[-1] if stock_minute_days else None,
                               "empty": not stock_minute_days,
                               "missing": bool(missing_stock_minute_days or incomplete_stock_minute)},
```

ETF 分钟 loop（当前 `for day in _missing_minute_days():`）改为：

```python
    for day in sorted(set(_missing_minute_days()) | incomplete_etf_minute):
```

日线缺失集合（当前 1601-1603 行）改为：

```python
    today = _date.today()
    daily_days = sorted(set(_missing_daily_days(STOCK_DAILY_ROOT))
                        | set(_missing_daily_days(ETF_DAILY_ROOT))
                        | set(_incomplete_etf_daily_days())
                        | set(incomplete_stock_daily)
                        | set(incomplete_etf_daily))
```

seed 兜底分支（当前 1606-1608 行）改为：

```python
    if _missing_daily_days(STOCK_DAILY_ROOT) == [] and not stocks_daily:
        seed = set(_trade_days_up_to(today)) - set(_partition_dates(STOCK_DAILY_ROOT))
        daily_days = sorted(seed | set(_incomplete_etf_daily_days())
                            | set(incomplete_stock_daily) | set(incomplete_etf_daily))
```

指数日线（当前 1620-1622 行）改为：

```python
    idx_days = sorted(set(_missing_index_daily_days()) | set(incomplete_index_daily))
    if not idx_days and not index_daily_days:
        idx_days = sorted(set(_trade_days_up_to(today))
                          - set(_partition_dates(INDEX_DAILY_ROOT)))
```

股票分钟（当前 1643-1658 行）改为：

```python
    incomplete_minute = incomplete_stock_minute | missing_stock_minute_days
    if incomplete_minute:
        try:
            min_days = sorted(incomplete_minute)
            n = sync_stock_minute_range(min_days)
            result["stock_minute_rows"] = result.get("stock_minute_rows", 0) + n
            result["stock_minute_days"] = [d.isoformat() for d in min_days]
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 股票分钟残缺/缺失分区重写失败 %s: %s",
                           sorted(incomplete_minute), e)
            result["errors"].append(f"stock_minute_range {sorted(incomplete_minute)}: {e}")
    try:
        n = sync_stock_minute(limit=STOCK_MINUTE_BATCH_LIMIT)
        result["stock_minute_rows"] = result.get("stock_minute_rows", 0) + n
    except Exception as e:  # noqa: BLE001
        logger.warning("mootdx_service: 股票分钟回源失败: %s", e)
        result["errors"].append(f"stock_minute: {e}")
```

> 注意：`incomplete_minute` 现在是 `set`，去掉了原代码对空集合的 `if incomplete_minute:` 直接判断差异——上面已显式 `if incomplete_minute:` 包裹，语义一致。

- [ ] **Step 5: 运行新增测试确认通过**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k "content_recent_default or resyncs_content_flagged" -q
```

Expected: PASS（2 passed）。

- [ ] **Step 6: 全量回归 + 提交**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -q
cd /home/caisl/tickflow-stock-panel && git add backend/app/services/mootdx_service.py backend/tests/quant/test_mootdx_backfill_coverage.py && git commit -m "feat: 内容校验接入00:00巡检与启动回源(近1周窗口)"
```

---

### Task 4: 单日/全量手动检验补齐入口

**Files:**
- Modify: `backend/app/services/mootdx_service.py`（新增 `_partition_symbols`、`_coverage`、`check_and_repair_day`、`check_and_repair_full`）
- Test: `backend/tests/quant/test_mootdx_backfill_coverage.py`（追加）

**Interfaces:**
- Consumes: `_CONTENT_CHECK_MIN_COVERAGE`、`sync_daily(day)`、`sync_index_daily(day)`、`sync_etf_minute(day)`、`sync_stock_minute_day(day, symbols=...)`、Task 3 的 `scan_and_backfill_full(content_recent=...)`
- Produces: `check_and_repair_day(day: _date) -> dict`（Task 5 依赖）；`check_and_repair_full(content_recent: int | None = None) -> dict`

- [ ] **Step 1: 追加失败测试**

```python
# ---------------------------------------------------------------------------
# 单日 / 全量检验补齐
# ---------------------------------------------------------------------------

def test_check_and_repair_day_repairs_sparse_index(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [f"6000{dd:03d}.SH" for dd in range(3)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [f"1599{dd:02d}.XSHE" for dd in range(3)])
    monkeypatch.setattr(ms, "_index_universe", lambda: ["000001.SH", "000300.SH", "399006.SZ"])
    root = tmp_path / "kline_index_daily"
    (root / "date=2026-07-31").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["000300.SH"], "open": [1.0], "close": [1.0]}).write_parquet(
        root / "date=2026-07-31" / "part.parquet")
    calls = {"daily": 0, "index": [], "etf_minute": 0, "stock_minute": []}
    monkeypatch.setattr(ms, "sync_daily", lambda d: calls.__setitem__("daily", calls["daily"] + 1))
    monkeypatch.setattr(ms, "sync_index_daily", lambda d: calls["index"].append(d) or {"written": 3})
    monkeypatch.setattr(ms, "sync_etf_minute", lambda d=None: calls.__setitem__("etf_minute", calls["etf_minute"] + 1))
    monkeypatch.setattr(ms, "sync_stock_minute_day",
                        lambda day, symbols=None: calls["stock_minute"].append(sorted(symbols or [])) or 0)

    res = ms.check_and_repair_day(_d(2026, 7, 31))

    assert calls["index"] == [_d(2026, 7, 31)], "指数日线残缺应重写"
    assert res["results"]["index_daily"]["status"] == "repaired"
    # 其它类型该日无分区 → 覆盖率为 0 → 也重写（股票分钟只补缺失 symbol）
    assert calls["daily"] >= 1
    assert calls["etf_minute"] >= 1
    assert calls["stock_minute"] == [[
        "600000.SH", "600001.SH", "600002.SH"]], "股票分钟应只补缺失 symbol"


def test_check_and_repair_day_noop_when_complete(tmp_path, monkeypatch):
    from datetime import date as _d
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "kline_daily")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(ms, "ETF_MINUTE_ROOT", tmp_path / "kline_etf_minute")
    monkeypatch.setattr(ms, "STOCK_MINUTE_ROOT", tmp_path / "kline_minute")
    monkeypatch.setattr(ms, "_stock_universe", lambda: [f"6000{dd:03d}.SH" for dd in range(3)])
    monkeypatch.setattr(ms, "_etf_universe", lambda: [f"1599{dd:02d}.XSHE" for dd in range(3)])
    monkeypatch.setattr(ms, "_index_universe", lambda: ["000001.SH", "000300.SH", "399006.SZ"])
    for sub, syms in [
        ("kline_daily", ["600000.SH", "600001.SH", "600002.SH"]),
        ("kline_etf_daily", ["159900.SZ", "159901.SZ", "159902.SZ"]),
        ("kline_index_daily", ["000001.SH", "000300.SH", "399006.SZ"]),
        ("kline_etf_minute", ["159900.SZ", "159901.SZ", "159902.SZ"]),
        ("kline_minute", ["600000.SH", "600001.SH", "600002.SH"]),
    ]:
        pdir = tmp_path / sub / "date=2026-08-04"
        pdir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": syms, "close": [1.0] * len(syms)}).write_parquet(
            pdir / "part.parquet")
    calls = {"n": 0}
    for fn in ["sync_daily", "sync_index_daily", "sync_etf_minute", "sync_stock_minute_day"]:
        monkeypatch.setattr(ms, fn, lambda *a, **k: calls.__setitem__("n", calls["n"] + 1))

    res = ms.check_and_repair_day(_d(2026, 8, 4))

    assert calls["n"] == 0, "全部完整时不应触发任何重写"
    assert all(v["status"] == "ok" for v in res["results"].values())


def test_check_and_repair_full_uses_250_window(monkeypatch, tmp_path):
    monkeypatch.setattr(ms, "DATA_ROOT", tmp_path)
    seen = {}
    monkeypatch.setattr(ms, "scan_and_backfill_full",
                        lambda content_recent=None: seen.setdefault(
                            "recent", content_recent)
                        or {"missing": {}, "backfilled": {}, "errors": []})

    res = ms.check_and_repair_full()

    assert seen["recent"] == ms._CONTENT_CHECK_RECENT_DAYS, "全量默认用近一年窗口"
    assert res["errors"] == []
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k "check_and_repair" -q
```

Expected: FAIL（`check_and_repair_day` / `check_and_repair_full` 未定义）。

- [ ] **Step 3: 实现**

在 `scan_and_backfill_full` 之后插入：

```python
def _partition_symbols(root: Path, day: _date) -> set[str]:
    """读某日分区所有 parquet 的 symbol 集合（异常/缺失 → 空集）。"""
    pdir = root / f"date={day.isoformat()}"
    syms: set[str] = set()
    for p in sorted(pdir.glob("*.parquet")):
        try:
            syms |= set(pl.read_parquet(p, columns=["symbol"])["symbol"].to_list())
        except Exception:  # noqa: BLE001
            continue
    return syms


def _coverage(root: Path, day: _date, target: set[str]) -> tuple[float, set[str]]:
    """返回 (覆盖率, 缺失 symbol)。分区不存在/宇宙空 → (0.0, target)。"""
    if not target:
        return 0.0, set()
    have = _partition_symbols(root, day)
    if not have:
        return 0.0, target
    inter = have & target
    return len(inter) / len(target), target - have


def check_and_repair_day(day: _date) -> dict:
    """单日检验补齐：对该日 5 类逐类查内容，残缺/缺失则重写。

    返回 {"day": str, "results": {type: {"status": "ok"|"repaired"|"skip"|"failed",
                                          "coverage": float|None, "symbols": int}}}。
    单类失败不阻断其它类。
    """
    results: dict[str, dict] = {}
    try:
        stocks = _stock_universe()
    except Exception:  # noqa: BLE001
        stocks = []
    try:
        etf_tf = set(_to_tf_symbol(c) for c in _etf_universe())
    except Exception:  # noqa: BLE001
        etf_tf = set()
    try:
        idx = _index_universe()
    except Exception:  # noqa: BLE001
        idx = []

    for key, root, target, repair in [
        ("stock_daily", STOCK_DAILY_ROOT, set(stocks), lambda: sync_daily(day)),
        ("etf_daily", ETF_DAILY_ROOT, etf_tf, lambda: sync_daily(day)),
        ("index_daily", INDEX_DAILY_ROOT, set(idx), lambda: sync_index_daily(day)),
        ("etf_minute", ETF_MINUTE_ROOT, etf_tf, lambda: sync_etf_minute(day)),
    ]:
        if not target:
            results[key] = {"status": "skip", "coverage": None, "symbols": 0}
            continue
        cov, _missing = _coverage(root, day, target)
        have = len(_partition_symbols(root, day))
        if cov >= _CONTENT_CHECK_MIN_COVERAGE:
            results[key] = {"status": "ok", "coverage": round(cov, 4), "symbols": have}
            continue
        try:
            repair()
            results[key] = {"status": "repaired", "coverage": round(cov, 4), "symbols": have}
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 单日补齐 %s %s 失败: %s", key, day, e)
            results[key] = {"status": "failed", "coverage": round(cov, 4), "symbols": have}

    # 股票分钟：只补缺失 symbol（残缺少时快；停牌标的该日无 bar 由 sync 内部跳过）
    stock_target = set(stocks)
    cov, missing = _coverage(STOCK_MINUTE_ROOT, day, stock_target)
    have = len(_partition_symbols(STOCK_MINUTE_ROOT, day))
    if not stock_target:
        results["stock_minute"] = {"status": "skip", "coverage": None, "symbols": 0}
    elif cov >= _CONTENT_CHECK_MIN_COVERAGE:
        results["stock_minute"] = {"status": "ok", "coverage": round(cov, 4), "symbols": have}
    else:
        try:
            sync_stock_minute_day(day, symbols=sorted(missing))
            results["stock_minute"] = {"status": "repaired", "coverage": round(cov, 4),
                                       "symbols": have}
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 单日补齐 stock_minute %s 失败: %s", day, e)
            results["stock_minute"] = {"status": "failed", "coverage": round(cov, 4),
                                       "symbols": have}

    return {"day": day.isoformat(), "results": results}


def check_and_repair_full(content_recent: int | None = None) -> dict:
    """全量检验补齐：全窗口内容校验 + 全量分区缺失补全。

    content_recent 默认 ``_CONTENT_CHECK_RECENT_DAYS``（250，≈1 年交易日）。
    """
    if content_recent is None:
        content_recent = _CONTENT_CHECK_RECENT_DAYS
    return scan_and_backfill_full(content_recent=content_recent)
```

- [ ] **Step 4: 运行新增测试确认通过**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -k "check_and_repair" -q
```

Expected: PASS（3 passed）。

- [ ] **Step 5: 全量回归 + 提交**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py -q
cd /home/caisl/tickflow-stock-panel && git add backend/app/services/mootdx_service.py backend/tests/quant/test_mootdx_backfill_coverage.py && git commit -m "feat: 单日/全量检验补齐入口 check_and_repair_day/full"
```

---

### Task 5: stockdata 服务暴露 check_day / check_full

**Files:**
- Modify: `backend/app/services/stockdata/scheduler.py`（`_run_check_day`/`_run_check_full` + `trigger_sync`）
- Modify: `backend/app/services/stockdata/handlers.py:105-109`（`h_trigger_sync` 透传 params）
- Test: `backend/tests/quant/test_stockdata_scheduler.py`、`backend/tests/quant/test_stockdata_handlers.py`（追加）

**Interfaces:**
- Consumes: `mootdx_service.check_and_repair_day(day)`、`mootdx_service.check_and_repair_full(content_recent=None)`
- Produces: `trigger_sync(kind: str, **params) -> dict`（kinds 增加 `check_day`/`check_full`；Task 6 依赖）；`h_trigger_sync` 透传非 kind 参数

- [ ] **Step 1: 追加失败测试**

在 `tests/quant/test_stockdata_scheduler.py` 追加：

```python
def test_run_check_day_runs_repair(monkeypatch):
    """check_day 后台执行体：解析日期并调 mootdx_service.check_and_repair_day。"""
    from datetime import date as _d
    from app.services import mootdx_service
    calls = []
    monkeypatch.setattr(sch, "_sync_lock", threading.Lock())
    monkeypatch.setattr(sch, "_lock", threading.Lock())
    monkeypatch.setattr(mootdx_service, "check_and_repair_day",
                        lambda day: calls.append(day) or {"day": str(day), "results": {}})
    sch._run_check_day("2026-08-05")
    assert calls == [_d(2026, 8, 5)]
    assert sch._scheduler_state["check_day_result"]["day"] == "2026-08-05"


def test_run_check_full_runs_repair(monkeypatch):
    """check_full 后台执行体：调 check_and_repair_full 并记录汇总。"""
    from app.services import mootdx_service
    calls = []
    monkeypatch.setattr(sch, "_sync_lock", threading.Lock())
    monkeypatch.setattr(sch, "_lock", threading.Lock())
    monkeypatch.setattr(mootdx_service, "check_and_repair_full",
                        lambda content_recent=None: calls.append(content_recent)
                        or {"missing": {}, "backfilled": {}, "errors": []})
    sch._run_check_full()
    assert calls == [None]
    assert sch._scheduler_state["check_full_result"]["errors"] == []


def test_trigger_sync_check_kinds_spawn_thread(monkeypatch):
    """trigger_sync 的 check_day/check_full 走后台线程（start 不阻塞）。"""
    spawned = []

    class _T(threading.Thread):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            spawned.append(k.get("args", ()))

        def start(self):
            pass

    monkeypatch.setattr(sch, "threading.Thread", _T)
    assert sch.trigger_sync("check_day", day="2026-08-05") == {"ok": True}
    assert sch.trigger_sync("check_full") == {"ok": True}
    assert len(spawned) == 2
```

在 `tests/quant/test_stockdata_handlers.py` 追加（先读该文件确认导入方式，若无 `from app.services.stockdata import handlers` 则加）：

```python
def test_h_trigger_sync_passes_params(monkeypatch):
    """trigger_sync handler 应透传 kind 之外的参数（如 day）。"""
    from app.services.stockdata import handlers
    got = {}

    def fake_trigger(kind, **params):
        got["kind"] = kind
        got["params"] = params

    monkeypatch.setattr("app.services.stockdata.scheduler.trigger_sync", fake_trigger)
    out = handlers.h_trigger_sync({"kind": "check_day", "day": "2026-08-05"}, None)
    assert got == {"kind": "check_day", "params": {"day": "2026-08-05"}}
    assert out == ("json", {"ok": True, "kind": "check_day"})
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_stockdata_scheduler.py tests/quant/test_stockdata_handlers.py -q
```

Expected: FAIL（`_run_check_day` 未定义 / params 未透传）。

- [ ] **Step 3: 实现 scheduler**

在 `scheduler.py` 的 `_run_sync` 之后插入：

```python
def _run_check_day(day: str) -> None:
    """单日检验补齐（后台线程）：解析日期并执行。"""
    with _sync_lock:
        try:
            from app.services import mootdx_service
            d = _dt.date.fromisoformat(day)
            res = mootdx_service.check_and_repair_day(d)
            with _lock:
                _scheduler_state["last_check_day"] = day
                _scheduler_state["check_day_result"] = res
            logger.info("stockdata check_day %s done: %s", day,
                        {k: v["status"] for k, v in res["results"].items()})
        except Exception:  # noqa: BLE001
            logger.exception("stockdata check_day %s failed", day)


def _run_check_full() -> None:
    """全量检验补齐（后台线程）：执行并记录汇总。"""
    with _sync_lock:
        try:
            from app.services import mootdx_service
            res = mootdx_service.check_and_repair_full()
            with _lock:
                _scheduler_state["last_check_full"] = str(_dt.datetime.now())
                _scheduler_state["check_full_result"] = res
            logger.info("stockdata check_full done: %s",
                        {k: len(v) for k, v in (res.get("missing") or {}).items()}
                        if isinstance(res.get("missing"), dict) else res)
        except Exception:  # noqa: BLE001
            logger.exception("stockdata check_full failed")
```

`trigger_sync` 改为：

```python
def trigger_sync(kind: str, **params) -> dict:
    """手动触发同步（供 handler 调用）。

    kind: backfill|daily|etf_minute|stock_minute|adj_factor|check_day|check_full
    check_day 需传 ``day``（YYYY-MM-DD）。
    """
    if kind == "backfill":
        threading.Thread(target=_backfill_loop, daemon=True).start()
    elif kind == "check_day":
        threading.Thread(target=_run_check_day, args=(params["day"],),
                         daemon=True).start()
    elif kind == "check_full":
        threading.Thread(target=_run_check_full, daemon=True).start()
    else:
        threading.Thread(target=_run_sync, daemon=True).start()
    return {"ok": True}
```

- [ ] **Step 4: 改 handler**

`h_trigger_sync` 改为：

```python
def h_trigger_sync(p, s: DataSources):
    from .scheduler import trigger_sync
    kind = p.get("kind", "backfill")
    params = {k: v for k, v in p.items() if k != "kind"}
    trigger_sync(kind, **params)
    return "json", {"ok": True, "kind": kind}
```

- [ ] **Step 5: 运行测试确认通过**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_stockdata_scheduler.py tests/quant/test_stockdata_handlers.py -q
```

Expected: PASS。

- [ ] **Step 6: 提交**

```bash
cd /home/caisl/tickflow-stock-panel && git add backend/app/services/stockdata/scheduler.py backend/app/services/stockdata/handlers.py backend/tests/quant/test_stockdata_scheduler.py backend/tests/quant/test_stockdata_handlers.py && git commit -m "feat: stockdata 服务暴露 check_day/check_full 触发"
```

---

### Task 6: 主后端 check-day / check-full API + StockDataClient 参数透传

**Files:**
- Modify: `backend/app/api/data.py`（新增两个 POST 端点）
- Modify: `backend/app/quant/datasource/network_client.py:198-199`（`trigger_sync` 接受 `**params`）
- Test: `backend/tests/test_local_market_stats.py`（追加）

**Interfaces:**
- Consumes: `StockDataClient.trigger_sync(kind, **params)`（TCP `trigger_sync` handler）
- Produces: `POST /api/data/check-day`（body `{"date": "YYYY-MM-DD"}`，400 非法日期 / 503 服务不可达）、`POST /api/data/check-full`（503 服务不可达）

- [ ] **Step 1: 改 `network_client.trigger_sync`**

```python
    def trigger_sync(self, kind: str, **params) -> dict:
        return self._request("trigger_sync", {"kind": kind, **params})["d"]
```

- [ ] **Step 2: 追加失败测试**

在 `tests/test_local_market_stats.py` 末尾追加：

```python
def test_check_day_endpoint_triggers(monkeypatch) -> None:
    from app.quant.datasource import network_client as nc
    calls: list[tuple] = []

    class _FakeClient:
        def __init__(self) -> None:
            pass

        def trigger_sync(self, kind: str, **params):
            calls.append((kind, params))

    monkeypatch.setattr(nc, "StockDataClient", _FakeClient)
    client = TestClient(_make_app(repo))
    r = client.post("/api/data/check-day", json={"date": "2026-08-05"})
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    assert calls == [("check_day", {"day": "2026-08-05"})]
    bad = client.post("/api/data/check-day", json={"date": "not-a-date"})
    assert bad.status_code == 400


def test_check_full_endpoint_triggers(monkeypatch) -> None:
    from app.quant.datasource import network_client as nc
    calls: list[tuple] = []

    class _FakeClient:
        def __init__(self) -> None:
            pass

        def trigger_sync(self, kind: str, **params):
            calls.append((kind, params))

    monkeypatch.setattr(nc, "StockDataClient", _FakeClient)
    client = TestClient(_make_app(repo))
    r = client.post("/api/data/check-full")
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    assert calls == [("check_full", {})]


def test_check_day_endpoint_503_when_service_down(monkeypatch) -> None:
    from app.quant.datasource import network_client as nc

    class _BrokenClient:
        def __init__(self) -> None:
            pass

        def trigger_sync(self, kind: str, **params):
            raise ConnectionError("stockdata down")

    monkeypatch.setattr(nc, "StockDataClient", _BrokenClient)
    client = TestClient(_make_app(repo))
    r = client.post("/api/data/check-day", json={"date": "2026-08-05"})
    assert r.status_code == 503
```

- [ ] **Step 3: 运行测试确认失败**

```bash
cd backend && uv run --extra dev pytest tests/test_local_market_stats.py -k "check_day or check_full" -q
```

Expected: FAIL（404 无端点）。

- [ ] **Step 4: 实现端点**

`data.py` 导入行 `from fastapi import APIRouter, Query, Request` 改为：

```python
from fastapi import APIRouter, HTTPException, Query, Request
```

在 `local_market_stats` 之后、`@router.post("/clear")` 之前插入：

```python
@router.post("/check-day")
def data_check_day(payload: dict, request: Request):
    """触发单日检验补齐（stockdata 服务后台执行，异步）。"""
    day = payload.get("date")
    try:
        datetime.fromisoformat(day or "")
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="date 需为 YYYY-MM-DD")
    try:
        from app.quant.datasource.network_client import StockDataClient
        StockDataClient().trigger_sync("check_day", day=day)
    except Exception as e:
        logger.warning("check-day 触发失败: %s", e)
        raise HTTPException(status_code=503, detail="stockdata 服务不可达")
    return {"ok": True}


@router.post("/check-full")
def data_check_full(request: Request):
    """触发全量检验补齐（stockdata 服务后台执行，异步）。"""
    try:
        from app.quant.datasource.network_client import StockDataClient
        StockDataClient().trigger_sync("check_full")
    except Exception as e:
        logger.warning("check-full 触发失败: %s", e)
        raise HTTPException(status_code=503, detail="stockdata 服务不可达")
    return {"ok": True}
```

- [ ] **Step 5: 运行测试确认通过**

```bash
cd backend && uv run --extra dev pytest tests/test_local_market_stats.py -q
```

Expected: PASS（6 passed，含既有 3 个）。

- [ ] **Step 6: 提交**

```bash
cd /home/caisl/tickflow-stock-panel && git add backend/app/api/data.py backend/app/quant/datasource/network_client.py backend/tests/test_local_market_stats.py && git commit -m "feat: 主后端 /api/data/check-day 与 /api/data/check-full 端点"
```

---

### Task 7: 前端「本地股市数据」页检验补齐按钮

**Files:**
- Modify: `frontend/src/lib/api.ts`（新增 `checkDay`/`checkFull`）
- Modify: `frontend/src/pages/LocalData.tsx`（顶部全量按钮 + 每行检验按钮）

**Interfaces:**
- Consumes: `POST /api/data/check-day`、`POST /api/data/check-full`
- Produces: 按钮交互（触发后 Toast + 延迟刷新 local-market-stats）

- [ ] **Step 1: `api.ts` 新增方法**

在 `frontend/src/lib/api.ts` 的 `localMarketStats` 之后追加：

```ts
  checkDay: (date: string) => request<{ ok: boolean }>('/api/data/check-day', {
    method: 'POST',
    body: JSON.stringify({ date }),
  }),
  checkFull: () => request<{ ok: boolean }>('/api/data/check-full', { method: 'POST' }),
```

- [ ] **Step 2: 改写 `LocalData.tsx`**

整体替换为（含行内「检验」+ 顶部「全量检验补齐」，样式对齐系统按钮）：

```tsx
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { HardDrive, Wrench } from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { Skeleton } from '@/components/data/Skeleton'
import { toast } from '@/components/Toast'
import { api, type LocalMarketStatsRow } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

const PAGE_SIZE = 15

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
  const qc = useQueryClient()
  const { data, isLoading, isError } = useQuery({
    queryKey: QK.localMarketStats(page, PAGE_SIZE),
    queryFn: () => api.localMarketStats(page, PAGE_SIZE),
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
        {!isLoading && !isError && total > 0 && (
          <div className="flex items-center justify-end">
            <button
              onClick={() => checkFullMut.mutate()}
              disabled={checkFullMut.isPending}
              className="px-3 py-1.5 rounded-btn border border-border bg-elevated text-secondary hover:text-foreground disabled:opacity-40 transition-colors flex items-center gap-1.5"
            >
              <Wrench className="h-3 w-3" />
              {checkFullMut.isPending ? '校验中...' : '全量检验补齐'}
            </button>
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
                      <td className="px-3 py-2 text-right">
                        <button
                          onClick={() => checkDayMut.mutate(row.date)}
                          disabled={checkDayMut.isPending}
                          className="px-2 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
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

- [ ] **Step 3: 前端验证**

```bash
cd frontend && pnpm lint
cd frontend && pnpm build
```

Expected: lint 无错误；`tsc -b && vite build` 成功。

- [ ] **Step 4: 提交**

```bash
cd /home/caisl/tickflow-stock-panel && git add frontend/src/lib/api.ts frontend/src/pages/LocalData.tsx && git commit -m "feat: 本地股市数据页单日检验+全量检验补齐按钮"
```

---

### Task 8: 全量验证 + 修复存量 07-31 指数日线

**Files:**
- None（运行验证命令）

**Interfaces:**
- Consumes: 全部既有功能

- [ ] **Step 1: 后端全量测试**

```bash
cd backend && uv run --extra dev pytest tests/quant/test_mootdx_backfill_coverage.py tests/quant/test_stockdata_scheduler.py tests/quant/test_stockdata_handlers.py tests/test_local_market_stats.py -q
cd backend && uv run --extra dev ruff check app
cd backend && uv run --extra dev mypy app
```

Expected: 全部 PASS；ruff/mypy 无错误。

- [ ] **Step 2: 前端验证**

```bash
cd frontend && pnpm lint && pnpm build
```

Expected: 无错误。

- [ ] **Step 3: 若主后端/stockdata 服务在跑，触发一次真实单日补齐验证存量残帧**

```bash
curl -s -X POST http://localhost:3018/api/data/check-day -H 'Content-Type: application/json' -d '{"date":"2026-07-31"}'
# 等待后（stockdata 服务后台执行），验证指数日线 07-31 分区 symbol 数恢复正常
ss -tlnp | grep -E ":3322|:3018"
```

Expected: 返回 `{"ok": true}`；随后 `kline_index_daily/date=2026-07-31` 的 symbol 数从 4 恢复到 ~600。

- [ ] **Step 4: 提交（若有额外修改）**

```bash
cd /home/caisl/tickflow-stock-panel && git status
```
