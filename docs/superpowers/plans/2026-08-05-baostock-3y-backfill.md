# baostock 全市场近 3 年回源脚本 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 独立脚本回源 baostock 全市场近 3 年数据（股票 5min 真实 + ETF/指数日线 + 复权因子 + 分红送转明细），带进度报告、断点续传，落 parquet 到项目 data/。

**Architecture:** 逻辑全部放 `backend/app/services/baostock_backfill.py`（纯函数、可测、不依赖 DataManager/运行时），`backend/scripts/backfill_baostock_3y.py` 只做 CLI 薄壳。复用 mootdx_service 的分区原子写、批量 flush、失败 CSV 模式。三 stage 可独立运行（minute / daily / corporate），共用 `data/baostock_backfill_state.json` 断点。

**Tech Stack:** Python 3.12, baostock 0.9.3（已装）, polars, argparse；测试 pytest + monkeypatch（不打真实网络）。

## Global Constraints

- 实测：baostock 无 1min/ETF分钟/指数分钟（frequency="1" → 10004012）；ETF 日线仅 2026-01-05 起；指数日线 3 年可用
- 落盘格式与现有分区一致：`date=YYYY-MM-DD/part.parquet`，schema `symbol, datetime|date, open, high, low, close, volume, amount`
- volume 单位：股票 5min = 股（不换算）；指数日线 = baostock 股 ÷100 → 手（对齐现有 kline_index_daily）；ETF 日线 = 股（不换算）；amount = 元
- symbol 规范：`.SH`/`.SZ`（如 `600036.SH`），baostock 侧 `sh.600036`；跳过 `.BJ`（baostock 无北交所）
- 原子写：tmp 文件 + rename；写前读旧→concat→unique keep=last
- 断点：`data/baostock_backfill_state.json` 原子更新；失败 CSV `data/baostock_backfill_failures.csv`（symbol, 原因, 时间）
- 串行执行（baostock 连接是进程级全局）；每请求墙钟超时（默认 300s）+ 重试 3 次递增退避（2/8/18s）
- 数据根：`PARTITION_DATA_ROOT` 环境变量可覆盖，默认 `Path(__file__).resolve().parents[3] / "data"`（= 项目 data/）
- 测试运行：`cd backend && uv run --extra dev pytest`；lint：`uv run --extra dev ruff check app scripts`；mypy：`uv run --extra dev mypy app`
- 不引入新依赖（baostock 已在 venv）

---

### Task 1: 模块骨架 + baostock 查询包装（墙钟超时 + 重试）

**Files:**
- Create: `backend/app/services/baostock_backfill.py`
- Test: `backend/tests/quant/test_baostock_backfill.py`

**Interfaces:**
- Produces: `DATA_ROOT`, `KLINE_5MIN_ROOT`, `KLINE_INDEX_DAILY_ROOT`, `KLINE_ETF_DAILY_ROOT`, `ADJ_FACTOR_PATH`, `DIVIDENDS_PATH`, `STATE_PATH`, `FAILURE_CSV`；`KLINE_5MIN_FIELDS`, `DAILY_FIELDS`；函数 `_bs()`, `_guarded(fn, timeout)`, `_rows(rs)`, `_retry(fn, timeout, retries)`, `query_kline(code, fields, start, end, frequency, adjustflag, timeout, retries)`, `query_all_stock(day, timeout, retries)`, `query_adjust_factor_rows(code, start, end, timeout, retries)`, `query_dividend_rows(code, year, timeout, retries)`（返回 `list[dict]`）, `to_baostock_code(sym)`, `from_baostock_code(code)`, `_safe_float(v)`

- [ ] **Step 1: 写失败测试**

创建 `backend/tests/quant/test_baostock_backfill.py`：

```python
"""baostock_backfill 单元测试（不打真实网络，monkeypatch 假 baostock 模块）。"""
import time

import polars as pl
import pytest

from app.services import baostock_backfill as bb


class _FakeRS:
    """伪造 baostock QueryResult：iter_rows 遍历 rows。"""

    def __init__(self, rows, error_code="0", error_msg="success"):
        self.error_code = error_code
        self.error_msg = error_msg
        self._rows = rows
        self._i = -1

    def next(self):
        self._i += 1
        return self._i < len(self._rows)

    def get_row_data(self):
        return self._rows[self._i]


class _FakeBS:
    """伪造 baostock 模块（含 fields 的 query_dividend_data）。"""

    def __init__(self):
        self.calls = []

    def query_history_k_data_plus(self, code, fields, start_date, end_date,
                                  frequency, adjustflag):
        self.calls.append(("kline", code, frequency))
        return _FakeRS([["2025-07-01", "20250701093500000",
                         "1.0", "2.0", "1.5", "1.8", "100", "200"]])

    def query_all_stock(self, day=None):
        self.calls.append(("all_stock", day))
        return _FakeRS([["sh.600036", "1", "招商银行"], ["sh.000001", "1", "上证指数"]])

    def query_adjust_factor(self, code, start_date, end_date):
        self.calls.append(("adj", code))
        return _FakeRS([["sh.600036", "2025-07-16", "0.95", "12.76", "12.76"]])

    def query_dividend_data(self, code, year, yearType):
        self.calls.append(("dividend", code, year))
        return _FakeRS([["sh.600036", "2025-07-11", "2025-07-11", "2", "1.8",
                         "0.000000", "10派20元", "0"]],
                       fields=["code", "dividOperateDate", "dividPayDate",
                               "dividCashPsBeforeTax", "dividCashPsAfterTax",
                               "dividStocksPs", "dividCashStock",
                               "dividReserveToStockPs"])


@pytest.fixture
def fake_bs(monkeypatch):
    fb = _FakeBS()
    monkeypatch.setattr(bb, "_bs_module", fb)
    return fb


def test_code_conversion():
    assert bb.to_baostock_code("600036.SH") == "sh.600036"
    assert bb.to_baostock_code("000001.SZ") == "sz.000001"
    assert bb.from_baostock_code("sh.600036") == "600036.SH"
    assert bb.from_baostock_code("sz.000001") == "000001.SZ"


def test_query_kline(fake_bs):
    rows = bb.query_kline("sh.600036", bb.KLINE_5MIN_FIELDS,
                          "2025-07-01", "2025-07-15", "5", "3", timeout=5)
    assert rows == [["2025-07-01", "20250701093500000",
                     "1.0", "2.0", "1.5", "1.8", "100", "200"]]
    assert fake_bs.calls[0] == ("kline", "sh.600036", "5")


def test_query_kline_error_retries(monkeypatch):
    class _ErrBS:
        def query_history_k_data_plus(self, *a, **k):
            return _FakeRS([], error_code="10001003", error_msg="失败")

    monkeypatch.setattr(bb, "_bs_module", _ErrBS())
    monkeypatch.setattr(bb.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="baostock 查询失败"):
        bb.query_kline("sh.600036", "f", "s", "e", "5", "3",
                       timeout=5, retries=1)


def test_query_all_stock(fake_bs):
    rows = bb.query_all_stock()
    assert len(rows) == 2


def test_query_adjust_factor_rows(fake_bs):
    rows = bb.query_adjust_factor_rows("sh.600036", "2025-01-01", "2025-12-31")
    assert rows == [["sh.600036", "2025-07-16", "0.95", "12.76", "12.76"]]


def test_query_dividend_rows(fake_bs):
    recs = bb.query_dividend_rows("sh.600036", 2025)
    assert recs[0]["dividOperateDate"] == "2025-07-11"
    assert recs[0]["dividCashPsBeforeTax"] == "2"


def test_guarded_timeout():
    def slow():
        time.sleep(0.3)
        return 1

    with pytest.raises(TimeoutError):
        bb._guarded(slow, timeout=0.05)


def test_safe_float():
    assert bb._safe_float("2.5") == 2.5
    assert bb._safe_float("") is None
    assert bb._safe_float("-") is None
    assert bb._safe_float("abc") is None
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py -v`
Expected: FAIL（ModuleNotFoundError: app.services.baostock_backfill）

- [ ] **Step 3: 写实现（创建模块）**

创建 `backend/app/services/baostock_backfill.py`：

```python
"""baostock 全市场近 3 年回源（股票 5min + ETF/指数日线 + 复权因子 + 分红送转明细）。

独立于运行时（不依赖 DataManager/服务层），由 scripts/backfill_baostock_3y.py
CLI 驱动。要点（均实测验证）：
- baostock 无 1min/ETF分钟/指数分钟（frequency="1" 返回 10004012 错误）
- 股票 5min 真实数据 → data/kline_5min/date=YYYY-MM-DD/part.parquet
- ETF 日线 baostock 仅 2026-01-05 起；指数日线 3 年可用
- 断点续传 data/baostock_backfill_state.json；墙钟超时 + 重试 + 失败 CSV
- volume 单位：股票 5min=股；指数日线=股÷100(手，对齐 kline_index_daily)；
  ETF 日线=股；amount 元
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date as _date
from datetime import datetime as _datetime
from pathlib import Path
from typing import Any, Callable

import polars as pl

logger = logging.getLogger("app.services.baostock_backfill")

_env_root = os.getenv("PARTITION_DATA_ROOT", "").strip()
DATA_ROOT = Path(_env_root) if _env_root else Path(__file__).resolve().parents[3] / "data"

KLINE_5MIN_ROOT = DATA_ROOT / "kline_5min"
KLINE_INDEX_DAILY_ROOT = DATA_ROOT / "kline_index_daily"
KLINE_ETF_DAILY_ROOT = DATA_ROOT / "kline_etf_daily"
ADJ_FACTOR_PATH = DATA_ROOT / "adj_factor" / "all.parquet"
DIVIDENDS_PATH = DATA_ROOT / "dividends" / "all.parquet"
STATE_PATH = DATA_ROOT / "baostock_backfill_state.json"
FAILURE_CSV = DATA_ROOT / "baostock_backfill_failures.csv"

KLINE_5MIN_FIELDS = "date,time,open,high,low,close,volume,amount"
DAILY_FIELDS = "date,open,high,low,close,volume,amount"

_bs_module = None


def _bs():
    """惰性加载 baostock 模块（测试可 monkeypatch _bs_module）。"""
    global _bs_module
    if _bs_module is None:
        import baostock as bs

        _bs_module = bs
    return _bs_module


def _guarded(fn: Callable[[], Any], timeout: float) -> Any:
    """墙钟超时守护：baostock socket 可能永久阻塞，超时弃帧不卡死整批。"""
    box: dict = {}

    def _run() -> None:
        try:
            box["out"] = fn()
        except Exception as e:  # noqa: BLE001
            box["err"] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"baostock 调用超时({timeout}s)")
    if "err" in box:
        raise box["err"]
    return box.get("out")


def _rows(rs) -> list[list[str]]:
    out = []
    while rs.error_code == "0" and rs.next():
        out.append(rs.get_row_data())
    return out


def _retry(fn: Callable[[], Any], timeout: float, retries: int) -> Any:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return _guarded(fn, timeout)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2.0 * (attempt + 1) ** 2)  # 2/8/18s 递增退避
    raise last  # type: ignore[misc]


def query_kline(code: str, fields: str, start: str, end: str, frequency: str = "5",
                adjustflag: str = "3", timeout: float = 300, retries: int = 3) -> list[list[str]]:
    bs = _bs()

    def _q():
        rs = bs.query_history_k_data_plus(
            code, fields, start_date=start, end_date=end,
            frequency=frequency, adjustflag=adjustflag)
        if rs is None or rs.error_code != "0":
            raise RuntimeError(
                f"baostock 查询失败 {getattr(rs, 'error_code', None)}: "
                f"{getattr(rs, 'error_msg', '')}")
        return _rows(rs)

    return _retry(_q, timeout, retries)


def query_all_stock(day: str | None = None, timeout: float = 120, retries: int = 3) -> list[list[str]]:
    bs = _bs()

    def _q():
        rs = bs.query_all_stock(day=day or _date.today().strftime("%Y-%m-%d"))
        if rs is None or rs.error_code != "0":
            raise RuntimeError(f"query_all_stock 失败: {getattr(rs, 'error_code', None)}")
        return _rows(rs)

    return _retry(_q, timeout, retries)


def query_adjust_factor_rows(code: str, start: str, end: str,
                             timeout: float = 120, retries: int = 3) -> list[list[str]]:
    """复权因子事件行：每行 [code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor]。"""
    bs = _bs()

    def _q():
        rs = bs.query_adjust_factor(code, start_date=start, end_date=end)
        if rs is None or rs.error_code != "0":
            raise RuntimeError(f"query_adjust_factor 失败: {getattr(rs, 'error_code', None)}")
        return _rows(rs)

    return _retry(_q, timeout, retries)


def query_dividend_rows(code: str, year: int, timeout: float = 120, retries: int = 3) -> list[dict]:
    """分红送转明细（yearType=operate 除权除息口径），返回 dict 列表（字段名见 baostock）。"""
    bs = _bs()

    def _q():
        rs = bs.query_dividend_data(code, year=year, yearType="operate")
        if rs is None or rs.error_code != "0":
            raise RuntimeError(f"query_dividend_data 失败: {getattr(rs, 'error_code', None)}")
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(dict(zip(rs.fields, rs.get_row_data())))
        return rows

    return _retry(_q, timeout, retries)


def to_baostock_code(sym: str) -> str:
    """分区 symbol (.SH/.SZ) -> baostock 码 (sh.600036)。"""
    pure, mkt = sym.split(".")
    return f"{mkt.lower()}.{pure}"


def from_baostock_code(code: str) -> str:
    """baostock 码 (sh.600036) -> 分区 symbol (.SH)。"""
    mkt, pure = code.split(".")
    return f"{pure}.{mkt.upper()}"


def _safe_float(v) -> float | None:
    try:
        if v is None or str(v).strip() in ("", "-"):
            return None
        return float(v)
    except Exception:  # noqa: BLE001
        return None
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/baostock_backfill.py backend/tests/quant/test_baostock_backfill.py
git commit -m "feat(quant): baostock backfill module skeleton + guarded query wrappers"
```

---

### Task 2: 断点状态 IO + 失败 CSV

**Files:**
- Modify: `backend/app/services/baostock_backfill.py`（追加）
- Test: `backend/tests/quant/test_baostock_backfill.py`（追加）

**Interfaces:**
- Consumes: `STATE_PATH`, `FAILURE_CSV`（Task 1）
- Produces: `load_state(path=STATE_PATH) -> dict`, `save_state(state, path=STATE_PATH)`, `mark_done(state, stage, sym)`, `mark_failed(state, stage, sym, reason)`, `append_failure(sym, reason)`；stage 取值 `minute|daily|adj|dividends`

- [ ] **Step 1: 写失败测试（追加到测试文件末尾）**

```python
def test_state_roundtrip(tmp_path):
    p = tmp_path / "state.json"
    st = bb.load_state(p)
    assert st["minute_done"] == []
    bb.mark_done(st, "minute", "600036.SH")
    bb.mark_failed(st, "minute", "000001.SZ", "timeout")
    bb.save_state(st, p)
    st2 = bb.load_state(p)
    assert st2["minute_done"] == ["600036.SH"]
    assert st2["failed"]["minute"]["000001.SZ"] == "timeout"
    assert bb.load_state(tmp_path / "missing.json")["daily_done"] == []


def test_state_atomic_no_tmp_left(tmp_path):
    p = tmp_path / "state.json"
    bb.save_state({"a": 1}, p)
    assert not (tmp_path / "state.json.tmp").exists()


def test_mark_done_and_failed_mutate_inplace(tmp_path):
    st = bb.load_state(tmp_path / "missing.json")
    bb.mark_done(st, "daily", "000001.SH")
    bb.mark_failed(st, "daily", "510300.SH", "empty")
    assert st["daily_done"] == ["000001.SH"]
    assert st["failed"]["daily"] == {"510300.SH": "empty"}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py::test_state_roundtrip -v`
Expected: FAIL（AttributeError: module has no attribute 'load_state'）

- [ ] **Step 3: 写实现（追加到模块）**

```python
def load_state(path: Path = STATE_PATH) -> dict:
    """读断点状态；不存在/损坏时返回默认空状态。"""
    default = {
        "start": None, "end": None,
        "minute_done": [], "daily_done": [], "adj_done": [], "dividends_done": [],
        "failed": {},
    }
    if not path.exists():
        return default
    try:
        st = json.loads(path.read_text())
        for k, v in default.items():
            st.setdefault(k, v)
        return st
    except Exception:  # noqa: BLE001
        logger.warning("断点状态损坏，重置: %s", path)
        return default


def save_state(state: dict, path: Path = STATE_PATH) -> None:
    """原子写状态文件（tmp + rename）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1))
    tmp.rename(path)


def mark_done(state: dict, stage: str, sym: str) -> None:
    state[f"{stage}_done"].append(sym)


def mark_failed(state: dict, stage: str, sym: str, reason: str) -> None:
    state["failed"].setdefault(stage, {})[sym] = str(reason)[:200]


def append_failure(sym: str, reason: str) -> None:
    """把回源失败标的追加到 failure csv（symbol, 原因, 时间）。"""
    try:
        from datetime import datetime as _dt
        line = f"{sym},{reason},{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        FAILURE_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(FAILURE_CSV, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:  # noqa: BLE001
        logger.warning("失败记录写入失败: %s", sym)
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py -v`
Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/baostock_backfill.py backend/tests/quant/test_baostock_backfill.py
git commit -m "feat(quant): baostock backfill state IO + failure csv"
```

---

### Task 3: 分区写入器（5min 按日分区 + 日线分区，幂等原子写）

**Files:**
- Modify: `backend/app/services/baostock_backfill.py`（追加）
- Test: `backend/tests/quant/test_baostock_backfill.py`（追加）

**Interfaces:**
- Consumes: 无
- Produces: `write_minute_partition(df, root, day)`, `flush_minute_batch(frames, root)`, `write_daily_partition(df, root)`；`df` 为 polars DataFrame，5min 含 `symbol, datetime, open, high, low, close, volume, amount`，日线含 `symbol, date, ...`

- [ ] **Step 1: 写失败测试（追加到测试文件末尾）**

```python
def _m5(sym, day, hour=10):
    return pl.DataFrame({
        "symbol": [sym, sym],
        "datetime": [pl.datetime(2025, 7, day, hour, 0), pl.datetime(2025, 7, day, hour, 5)],
        "open": [1.0, 1.1], "high": [1.2, 1.3], "low": [0.9, 1.0],
        "close": [1.1, 1.2], "volume": [100.0, 200.0], "amount": [110.0, 240.0],
    })


def test_write_minute_partition_idempotent(tmp_path):
    root = tmp_path / "k5"
    bb.write_minute_partition(_m5("600036.SH", 1), root, _date(2025, 7, 1))
    bb.write_minute_partition(_m5("600036.SH", 1), root, _date(2025, 7, 1))
    df = pl.read_parquet(root / "date=2025-07-01" / "part.parquet")
    assert df.height == 2
    assert not (root / "date=2025-07-01" / "part.tmp").exists()


def test_flush_minute_batch_two_days(tmp_path):
    root = tmp_path / "k5"
    bb.flush_minute_batch([_m5("600036.SH", 1), _m5("000001.SZ", 2)], root)
    d1 = pl.read_parquet(root / "date=2025-07-01" / "part.parquet")
    d2 = pl.read_parquet(root / "date=2025-07-02" / "part.parquet")
    assert set(d1["symbol"].to_list()) == {"600036.SH"}
    assert set(d2["symbol"].to_list()) == {"000001.SZ"}


def test_write_daily_partition_merge_with_date_col(tmp_path):
    root = tmp_path / "kd"
    df = pl.DataFrame({
        "symbol": ["000001.SH"], "date": [_date(2025, 7, 1)],
        "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
        "volume": [100.0], "amount": [100.0],
    })
    bb.write_daily_partition(df, root)
    bb.write_daily_partition(df, root)
    out = pl.read_parquet(root / "date=2025-07-01" / "part.parquet")
    assert out.height == 1
    assert "date" in out.columns
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py::test_write_minute_partition_idempotent -v`
Expected: FAIL（AttributeError: no attribute 'write_minute_partition'）

- [ ] **Step 3: 写实现（追加到模块）**

```python
def write_minute_partition(df: pl.DataFrame, root: Path, day: _date) -> None:
    """按 date 分区原子写 5min（读旧→concat→unique keep=last→tmp→rename），幂等。"""
    pdir = root / f"date={day}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    tmp = pdir / "part.tmp"
    if part.exists():
        old = pl.read_parquet(part)
        df = pl.concat([old, df]).unique(
            subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
    df = df.sort(["symbol", "datetime"])
    df.write_parquet(tmp)
    tmp.rename(part)


def flush_minute_batch(frames: list[pl.DataFrame], root: Path) -> None:
    """一批股票的 5min 按交易日分组一次性写分区（降 IO 一个量级）。"""
    if not frames:
        return
    all_df = pl.concat(frames).unique(
        subset=["symbol", "datetime"], keep="last")
    all_df = all_df.with_columns(pl.col("datetime").dt.date().alias("_day"))
    for d, g in all_df.group_by("_day"):
        write_minute_partition(g.drop("_day"), root, d[0])


def write_daily_partition(df: pl.DataFrame, root: Path) -> None:
    """按 date 分区原子写日线（兼容有无 date 列的既有分区）。"""
    ds = str(df["date"][0])[:10]
    pdir = root / f"date={ds}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    tmp = pdir / "part.tmp"
    if part.exists():
        old = pl.read_parquet(part)
        if "date" not in old.columns:
            df = df.drop("date")
            merged = pl.concat([old, df]).unique(
                subset=["symbol"], keep="last").sort(["symbol"])
        else:
            merged = pl.concat([old, df]).unique(
                subset=["symbol", "date"], keep="last").sort(["symbol", "date"])
    else:
        merged = df.sort(["symbol", "date"])
    merged.write_parquet(tmp)
    tmp.rename(part)
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py -v`
Expected: 15 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/baostock_backfill.py backend/tests/quant/test_baostock_backfill.py
git commit -m "feat(quant): baostock backfill partition writers (idempotent atomic)"
```

---

### Task 4: Universe 与上市日期

**Files:**
- Modify: `backend/app/services/baostock_backfill.py`（追加）
- Test: `backend/tests/quant/test_baostock_backfill.py`（追加）

**Interfaces:**
- Consumes: `DATA_ROOT`, `query_all_stock`（Task 1）
- Produces: `stock_universe() -> list[str]`, `index_universe() -> list[str]`, `etf_universe() -> list[str]`, `listing_date_map() -> dict[str, _date]`

- [ ] **Step 1: 写失败测试（追加到测试文件末尾）**

```python
@pytest.fixture
def tmp_data(tmp_path, monkeypatch):
    """把模块全部路径常量重定向到 tmp 目录。"""
    monkeypatch.setattr(bb, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(bb, "KLINE_5MIN_ROOT", tmp_path / "kline_5min")
    monkeypatch.setattr(bb, "KLINE_INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(bb, "KLINE_ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(bb, "ADJ_FACTOR_PATH", tmp_path / "adj_factor" / "all.parquet")
    monkeypatch.setattr(bb, "DIVIDENDS_PATH", tmp_path / "dividends" / "all.parquet")
    monkeypatch.setattr(bb, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(bb, "FAILURE_CSV", tmp_path / "failures.csv")
    return tmp_path


def test_stock_universe_from_instruments(tmp_data, fake_bs):
    inst = tmp_data / "instruments"
    inst.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600036.SH", "000001.SZ", "920001.BJ"],
        "listing_date": ["2020-01-01", "1991-04-03", "2023-01-01"],
    }).write_parquet(inst / "instruments.parquet")
    assert bb.stock_universe() == ["000001.SZ", "600036.SH"]  # 排除北交所


def test_stock_universe_fallback_all_stock(tmp_data, fake_bs):
    # 无 instruments 文件 → 回退 query_all_stock
    assert bb.stock_universe() == ["000001.SH", "600036.SH"]


def test_index_universe_from_parquet(tmp_data):
    inst = tmp_data / "instruments_index"
    inst.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SH", "399001.SZ"],
        "name": ["上证指数", "深证成指"],
    }).write_parquet(inst / "instruments_index.parquet")
    assert bb.index_universe() == ["000001.SH", "399001.SZ"]


def test_listing_date_map(tmp_data):
    inst = tmp_data / "instruments"
    inst.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600036.SH", "000001.SZ"],
        "listing_date": ["2002-04-09", "1991-04-03"],
    }).write_parquet(inst / "instruments.parquet")
    m = bb.listing_date_map()
    assert m["600036.SH"] == _date(2002, 4, 9)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py::test_stock_universe_from_instruments -v`
Expected: FAIL（AttributeError: no attribute 'stock_universe'）

- [ ] **Step 3: 写实现（追加到模块）**

```python
def stock_universe() -> list[str]:
    """全市场 A 股 symbol 列表（.SH/.SZ，排除北交所；优先 instruments parquet）。"""
    inst = DATA_ROOT / "instruments" / "instruments.parquet"
    if inst.exists():
        try:
            df = pl.read_parquet(inst, columns=["symbol"])
            syms = [s for s in df["symbol"].to_list() if not s.endswith(".BJ")]
            if syms:
                return sorted(syms)
        except Exception as e:  # noqa: BLE001
            logger.warning("instruments 读取失败: %s", e)
    # 兜底：baostock query_all_stock（沪深 A 股）
    out = []
    for r in query_all_stock():
        code = r[0]
        if code.startswith(("sh.6", "sz.0", "sz.3")):
            out.append(from_baostock_code(code))
    return sorted(set(out))


def index_universe() -> list[str]:
    """指数 universe（优先 instruments_index parquet）。"""
    inst_dir = DATA_ROOT / "instruments_index"
    fs = sorted(inst_dir.glob("*.parquet")) if inst_dir.is_dir() else []
    if fs:
        try:
            df = pl.read_parquet(fs[-1], columns=["symbol"])
            return sorted(df["symbol"].to_list())
        except Exception as e:  # noqa: BLE001
            logger.warning("instruments_index 读取失败: %s", e)
    # 兜底：query_all_stock 指数码
    out = []
    for r in query_all_stock():
        code = r[0]
        if code.startswith(("sh.000", "sz.399")):
            out.append(from_baostock_code(code))
    return sorted(set(out))


def etf_universe() -> list[str]:
    """ETF universe（优先 etf_universe_snapshot.json，JQ 码转 .SH/.SZ）。"""
    snap = DATA_ROOT / "quant_kline" / "etf_universe_snapshot.json"
    if snap.exists():
        try:
            codes = json.loads(snap.read_text()).get("codes", [])
            if codes:
                return sorted(
                    c.replace(".XSHG", ".SH").replace(".XSHE", ".SZ") for c in codes)
        except Exception as e:  # noqa: BLE001
            logger.warning("ETF 快照读取失败: %s", e)
    # 兜底：已有 kline_etf_daily 分区里的标的
    if KLINE_ETF_DAILY_ROOT.is_dir():
        try:
            lf = pl.scan_parquet(
                str(KLINE_ETF_DAILY_ROOT / "**" / "*.parquet"), hive_partitioning=True)
            return sorted(lf.select("symbol").unique().collect()["symbol"].to_list())
        except Exception:  # noqa: BLE001
            pass
    return []


def listing_date_map() -> dict[str, _date]:
    """{symbol: 上市日期}（instruments parquet；缺失返回空 dict）。"""
    inst = DATA_ROOT / "instruments" / "instruments.parquet"
    out: dict[str, _date] = {}
    if not inst.exists():
        return out
    try:
        df = pl.read_parquet(inst, columns=["symbol", "listing_date"])
        for sym, ld in df.iter_rows():
            if not sym or not ld:
                continue
            s = str(ld).strip()
            try:
                if "-" in s:
                    out[sym] = _date.fromisoformat(s[:10])
                else:
                    out[sym] = _date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            except Exception:  # noqa: BLE001
                continue
    except Exception as e:  # noqa: BLE001
        logger.warning("上市日期读取失败: %s", e)
    return out
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py -v`
Expected: 19 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/baostock_backfill.py backend/tests/quant/test_baostock_backfill.py
git commit -m "feat(quant): baostock backfill universe + listing dates"
```

---

### Task 5: 股票 5min 同步主循环（进度 + 断点 + 批量 flush）

**Files:**
- Modify: `backend/app/services/baostock_backfill.py`（追加）
- Test: `backend/tests/quant/test_baostock_backfill.py`（追加）

**Interfaces:**
- Consumes: `KLINE_5MIN_ROOT`, `query_kline`, `to_baostock_code`, `from_baostock_code`, `stock_universe`, `listing_date_map`, `flush_minute_batch`, `mark_done`, `mark_failed`, `save_state`, `append_failure`（Task 1-4）
- Produces: `_to_5min_df(code, rows) -> pl.DataFrame`（schema `symbol, datetime(dt[us]), open, high, low, close, volume, amount`，volume 股不换算，datetime 解析 baostock time `YYYYMMDDHHMMSSsss` 前 14 位）, `make_progress_printer() -> Callable[[str, int, int, int], None]`, `sync_minute(start: _date, end: _date, state: dict, timeout=300, flush_batch=100, retry_failed=False, limit=None, progress=None) -> dict`

- [ ] **Step 1: 写失败测试（追加到测试文件末尾）**

```python
def test_to_5min_df_parses_baostock_time():
    df = bb._to_5min_df("sh.600036", [
        ["2025-07-01", "20250701093500000", "1.0", "2.0", "1.5", "1.8", "100", "200"],
    ])
    assert df["symbol"].to_list() == ["600036.SH"]
    assert str(df["datetime"][0]) == "2025-07-01 09:35:00"
    assert df["volume"][0] == 100.0
    empty = bb._to_5min_df("sh.600036", [])
    assert empty.is_empty() and "symbol" in empty.columns


def test_sync_minute_writes_partitions_and_state(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "stock_universe",
                        lambda: ["600036.SH", "000001.SZ", "600519.SH"])
    monkeypatch.setattr(bb, "listing_date_map", lambda: {})

    calls = []

    def fake_query(code, fields, start, end, frequency, adjustflag, timeout, retries=3):
        calls.append(code)
        return [["2025-07-01", "20250701093500000", "1", "2", "1.5", "1.8", "100", "200"]]

    monkeypatch.setattr(bb, "query_kline", fake_query)
    st = bb.load_state(tmp_data / "state.json")
    out = bb.sync_minute(_date(2025, 7, 1), _date(2025, 7, 2), st,
                         timeout=5, flush_batch=2, limit=3)
    assert out["symbols"] == 3
    assert len(calls) == 3
    df = pl.read_parquet(tmp_data / "kline_5min" / "date=2025-07-01" / "part.parquet")
    assert df["symbol"].n_unique() == 3
    assert set(st["minute_done"]) == {"600036.SH", "000001.SZ", "600519.SH"}


def test_sync_minute_resume_skips_done(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "stock_universe", lambda: ["600036.SH", "000001.SZ"])
    monkeypatch.setattr(bb, "listing_date_map", lambda: {})
    calls = []
    monkeypatch.setattr(bb, "query_kline",
                        lambda *a, **k: (calls.append(a[0]) or
                                         [["2025-07-01", "20250701093500000",
                                           "1", "2", "1.5", "1.8", "100", "200"]]))
    st = bb.load_state(tmp_data / "state.json")
    bb.mark_done(st, "minute", "600036.SH")
    bb.save_state(st, tmp_data / "state.json")
    st2 = bb.load_state(tmp_data / "state.json")
    bb.sync_minute(_date(2025, 7, 1), _date(2025, 7, 2), st2, timeout=5)
    assert calls == ["sz.000001"]  # 已完成的 600036.SH 不重拉


def test_sync_minute_failure_recorded(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "stock_universe", lambda: ["600036.SH"])
    monkeypatch.setattr(bb, "listing_date_map", lambda: {})
    monkeypatch.setattr(bb, "query_kline",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    st = bb.load_state(tmp_data / "state.json")
    bb.sync_minute(_date(2025, 7, 1), _date(2025, 7, 2), st, timeout=5)
    assert "600036.SH" in st["failed"]["minute"]
    assert st["minute_done"] == []
    assert tmp_data / "failures.csv" in list(tmp_data.iterdir()) or \
        (tmp_data / "failures.csv").exists()


def test_make_progress_printer_prints(capsys):
    p = bb.make_progress_printer()
    p("minute", 10, 100, 1234)
    out = capsys.readouterr().out
    assert "minute" in out and "10/100" in out
```

注意：`sync_minute` 内部对超时/异常调用 `append_failure` 前会 try 包住 CSV 写入（已容错）；测试断言失败记录进 state 即可。

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py::test_to_5min_df_parses_baostock_time -v`
Expected: FAIL（AttributeError: no attribute '_to_5min_df'）

- [ ] **Step 3: 写实现（追加到模块）**

```python
_MIN5_SCHEMA = {
    "symbol": pl.Utf8, "datetime": pl.Datetime("us"),
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
    "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
}


def _to_5min_df(code: str, rows: list[list[str]]) -> pl.DataFrame:
    """baostock 5min 行 → polars 帧（symbol .SH/.SZ；volume 股不换算）。"""
    if not rows:
        return pl.DataFrame(schema=_MIN5_SCHEMA)
    ts = [_datetime.strptime(r[1][:14], "%Y%m%d%H%M%S") for r in rows]
    return pl.DataFrame({
        "symbol": [from_baostock_code(code)] * len(rows),
        "datetime": ts,
        "open": [float(r[2]) for r in rows],
        "high": [float(r[3]) for r in rows],
        "low": [float(r[4]) for r in rows],
        "close": [float(r[5]) for r in rows],
        "volume": [float(r[6]) for r in rows],
        "amount": [float(r[7]) for r in rows],
    }).with_columns(pl.col("datetime").cast(pl.Datetime("us")))


def make_progress_printer():
    """返回 progress(stage, i, total, rows) 回调：stdout 打印进度 + 速率 + ETA。"""
    t0 = time.time()

    def _p(stage: str, i: int, total: int, rows: int) -> None:
        elapsed = max(time.time() - t0, 0.001)
        rate = i / elapsed * 60.0
        eta = (total - i) / (i / elapsed) / 3600.0 if i > 0 else float("nan")
        print(f"[{stage}] {i}/{total} 累计{rows}行 "
              f"速率={rate:.1f}只/min ETA={eta:.1f}h", flush=True)

    return _p


def sync_minute(start: _date, end: _date, state: dict, timeout: float = 300,
                flush_batch: int = 100, retry_failed: bool = False,
                limit: int | None = None, progress=None) -> dict:
    """股票 5min 回源主循环：逐只拉 3 年 5min → 批量 flush 分区 → 断点标记。

    - resume：跳过 state['minute_done']；非 --retry-failed 时跳过 failed
    - 上市日期约束：起始 = max(start, listing_date)；1970 占位（退市/异常）跳过
    - 进度：每 flush_batch 只打一次 progress；失败落 failed + CSV
    """
    syms = stock_universe()
    listing = listing_date_map()
    done = set(state["minute_done"])
    failed = state["failed"].get("minute", {})
    todo = [s for s in syms if s not in done]
    if not retry_failed:
        todo = [s for s in todo if s not in failed]
    if limit is not None:
        todo = todo[:limit]
    pre_delisted = [s for s in todo if listing.get(s) == _date(1970, 1, 1)]
    if pre_delisted:
        logger.info("[minute] 跳过 %d 只退市/异常标的（上市日期占位）", len(pre_delisted))
        todo = [s for s in todo if s not in set(pre_delisted)]
    logger.info("[minute] 回源 %d 只（已覆盖 %d）", len(todo), len(done))
    total = 0
    chunk: list[pl.DataFrame] = []
    chunk_syms: list[str] = []
    for i, sym in enumerate(todo, 1):
        sym_start = start
        ld = listing.get(sym)
        if ld is not None and ld > sym_start:
            sym_start = ld
        code = to_baostock_code(sym)
        try:
            rows = query_kline(code, KLINE_5MIN_FIELDS,
                               sym_start.isoformat(), end.isoformat(),
                               "5", "3", timeout)
            df = _to_5min_df(code, rows)
            if df.is_empty():
                raise RuntimeError("empty")
            df = df.filter(pl.col("datetime").dt.date() >= sym_start)
            if df.is_empty():
                raise RuntimeError(f"no_data_since_{sym_start}")
        except Exception as e:  # noqa: BLE001
            mark_failed(state, "minute", sym, str(e)[:120])
            append_failure(sym, f"minute:{str(e)[:120]}")
            save_state(state)
            continue
        chunk.append(df)
        chunk_syms.append(sym)
        total += df.height
        if len(chunk) >= flush_batch:
            flush_minute_batch(chunk, KLINE_5MIN_ROOT)
            for s in chunk_syms:
                mark_done(state, "minute", s)
            save_state(state)
            chunk, chunk_syms = [], []
            if progress:
                progress("minute", i, len(todo), total)
    if chunk:
        flush_minute_batch(chunk, KLINE_5MIN_ROOT)
        for s in chunk_syms:
            mark_done(state, "minute", s)
        save_state(state)
    logger.info("[minute] 完成 %d 只, 累计 %d 行", len(todo), total)
    return {"symbols": len(todo), "rows": total}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py -v`
Expected: 23 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/baostock_backfill.py backend/tests/quant/test_baostock_backfill.py
git commit -m "feat(quant): baostock backfill stock 5min sync loop (progress/resume/batch)"
```

---

### Task 6: ETF/指数日线同步

**Files:**
- Modify: `backend/app/services/baostock_backfill.py`（追加）
- Test: `backend/tests/quant/test_baostock_backfill.py`（追加）

**Interfaces:**
- Consumes: `DAILY_FIELDS`, `index_universe`, `etf_universe`, `write_daily_partition`（Task 3/4）
- Produces: `_to_daily_df(code, rows, volume_div=1.0) -> pl.DataFrame`（schema `symbol, date, open, high, low, close, volume, amount`；volume = baostock 值 ÷ volume_div；指数传 100.0 转手）, `_flush_daily_batch(frames, root)`, `sync_daily(start, end, state, timeout=300, retry_failed=False, limit=None, progress=None) -> dict`

- [ ] **Step 1: 写失败测试（追加到测试文件末尾）**

```python
def test_to_daily_df_volume_div():
    rows = [["2025-07-01", "3513.25", "3532.11", "3513.25", "3519.65",
             "57208470500", "623102482278"]]
    df = bb._to_daily_df("sh.000001", rows, volume_div=100.0)
    assert df["symbol"].to_list() == ["000001.SH"]
    assert df["volume"][0] == 572084705.0  # 股 ÷100 → 手
    assert str(df["date"][0]) == "2025-07-01"


def test_sync_daily_writes_both_universes(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "index_universe", lambda: ["000001.SH"])
    monkeypatch.setattr(bb, "etf_universe", lambda: ["510300.SH"])
    monkeypatch.setattr(bb, "query_kline", lambda *a, **k: [
        ["2025-07-01", "1", "2", "1.5", "1.8", "100000000", "200000000"],
    ])
    st = bb.load_state()
    out = bb.sync_daily(_date(2025, 7, 1), _date(2025, 7, 2), st, timeout=5)
    assert out["index"]["rows"] == 1 and out["etf"]["rows"] == 1
    idx = pl.read_parquet(tmp_data / "kline_index_daily" / "date=2025-07-01" / "part.parquet")
    etf = pl.read_parquet(tmp_data / "kline_etf_daily" / "date=2025-07-01" / "part.parquet")
    assert idx["symbol"].to_list() == ["000001.SH"]
    assert etf["symbol"].to_list() == ["510300.SH"]
    assert idx["volume"][0] == 1000000.0  # 指数 ÷100
    assert etf["volume"][0] == 100000000.0  # ETF 不换算
    assert set(st["daily_done"]) == {"000001.SH", "510300.SH"}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py::test_to_daily_df_volume_div -v`
Expected: FAIL（AttributeError: no attribute '_to_daily_df'）

- [ ] **Step 3: 写实现（追加到模块）**

```python
_DAILY_SCHEMA = {
    "symbol": pl.Utf8, "date": pl.Date,
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
    "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
}


def _to_daily_df(code: str, rows: list[list[str]], volume_div: float = 1.0) -> pl.DataFrame:
    """baostock 日线行 → polars 帧。volume_div=100 时 baostock 股→手（指数口径）。"""
    if not rows:
        return pl.DataFrame(schema=_DAILY_SCHEMA)
    return pl.DataFrame({
        "symbol": [from_baostock_code(code)] * len(rows),
        "date": [_date.fromisoformat(r[0]) for r in rows],
        "open": [float(r[1]) for r in rows],
        "high": [float(r[2]) for r in rows],
        "low": [float(r[3]) for r in rows],
        "close": [float(r[4]) for r in rows],
        "volume": [float(r[5]) / volume_div for r in rows],
        "amount": [float(r[6]) for r in rows],
    })


def _flush_daily_batch(frames: list[pl.DataFrame], root: Path) -> None:
    if not frames:
        return
    all_df = pl.concat(frames).unique(subset=["symbol", "date"], keep="last")
    for d, g in all_df.group_by("date"):
        write_daily_partition(g, root)


def sync_daily(start: _date, end: _date, state: dict, timeout: float = 300,
               retry_failed: bool = False, limit: int | None = None,
               progress=None) -> dict:
    """ETF + 指数日线回源：逐只拉 3 年日线 → 按 date 批量写分区。

    指数 volume 股÷100 转手（对齐现有 kline_index_daily）；ETF 不换算。
    baostock ETF 日线仅 2026-01-05 起（更早返回空），空帧记失败但不阻塞。
    """
    groups = [
        ("index", index_universe(), KLINE_INDEX_DAILY_ROOT, 100.0),
        ("etf", etf_universe(), KLINE_ETF_DAILY_ROOT, 1.0),
    ]
    stats = {}
    for name, syms, root, vol_div in groups:
        done = set(state["daily_done"])
        failed = state["failed"].get("daily", {})
        todo = [s for s in syms if s not in done]
        if not retry_failed:
            todo = [s for s in todo if s not in failed]
        if limit is not None:
            todo = todo[:limit]
        logger.info("[daily:%s] 回源 %d 只（已覆盖 %d）", name, len(todo), len(done))
        batch: list[tuple[str, pl.DataFrame]] = []
        total = 0
        for i, sym in enumerate(todo, 1):
            code = to_baostock_code(sym)
            try:
                rows = query_kline(code, DAILY_FIELDS,
                                   start.isoformat(), end.isoformat(),
                                   "d", "3", timeout)
                df = _to_daily_df(code, rows, vol_div)
                if df.is_empty():
                    raise RuntimeError("empty")
            except Exception as e:  # noqa: BLE001
                mark_failed(state, "daily", sym, str(e)[:120])
                append_failure(sym, f"daily:{str(e)[:120]}")
                save_state(state)
                continue
            batch.append((sym, df))
            total += df.height
            if len(batch) >= 100:
                _flush_daily_batch([d for _, d in batch], root)
                for s, _ in batch:
                    mark_done(state, "daily", s)
                save_state(state)
                batch = []
                if progress:
                    progress(f"daily:{name}", i, len(todo), total)
        if batch:
            _flush_daily_batch([d for _, d in batch], root)
            for s, _ in batch:
                mark_done(state, "daily", s)
            save_state(state)
        stats[name] = {"symbols": len(todo), "rows": total}
    return stats
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py -v`
Expected: 25 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/baostock_backfill.py backend/tests/quant/test_baostock_backfill.py
git commit -m "feat(quant): baostock backfill ETF/index daily sync"
```

---

### Task 7: 复权因子转换 + 分红送转明细（corporate stage）

**Files:**
- Modify: `backend/app/services/baostock_backfill.py`（追加）
- Test: `backend/tests/quant/test_baostock_backfill.py`（追加）

**Interfaces:**
- Consumes: `query_adjust_factor_rows`, `query_dividend_rows`, `stock_universe`, `_safe_float`, `_write_append_table`（见本任务 Step 3）
- Produces: `build_ex_factor_table(events: dict[str, list[tuple[_date, float]]]) -> pl.DataFrame`（schema `symbol, trade_date, ex_factor`；事件行 `ex_factor = back(d)/back(latest)`，锚定最新=1.0）, `_write_append_table(df, path, keys)`, `sync_corporate(start, end, state, timeout=120, retry_failed=False, limit=None, progress=None) -> dict`（写 `ADJ_FACTOR_PATH` 与 `DIVIDENDS_PATH`）

- [ ] **Step 1: 写失败测试（追加到测试文件末尾）**

```python
def test_build_ex_factor_table():
    # back 因子在除权日单调累积：1.0 → 2.0 → 4.0（两次 10送10）
    events = {"sh.600036": [
        (_date(2024, 6, 1), 1.0),
        (_date(2025, 6, 1), 2.0),
        (_date(2026, 6, 1), 4.0),
    ]}
    df = bb.build_ex_factor_table(events)
    assert df["symbol"].to_list() == ["600036.SH"] * 3
    # ex_factor = back/latest：1/4, 2/4, 4/4
    assert [round(x, 4) for x in df["ex_factor"].to_list()] == [0.25, 0.5, 1.0]
    # DataManager._adj_events 口径：相邻行 prev/curr = 0.5（10送10 事件因子）
    f1 = df["ex_factor"][0] / df["ex_factor"][1]
    f2 = df["ex_factor"][1] / df["ex_factor"][2]
    assert f1 == 0.5 and f2 == 0.5


def test_write_append_table_idempotent(tmp_path):
    p = tmp_path / "all.parquet"
    df = pl.DataFrame({"symbol": ["600036.SH"], "trade_date": [_date(2025, 7, 16)],
                       "ex_factor": [0.9]})
    bb._write_append_table(df, p, ["symbol", "trade_date"])
    bb._write_append_table(df, p, ["symbol", "trade_date"])
    out = pl.read_parquet(p)
    assert out.height == 1
    assert not (p.with_name(p.name + ".tmp")).exists()


def test_sync_corporate_writes_adj_and_dividends(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "stock_universe", lambda: ["600036.SH"])
    monkeypatch.setattr(bb, "query_adjust_factor_rows", lambda *a, **k: [
        ["sh.600036", "2025-07-16", "0.954887", "12.763991", "12.763991"],
    ])
    monkeypatch.setattr(bb, "query_dividend_rows", lambda *a, **k: [
        {"code": "sh.600036", "dividOperateDate": "2025-07-11",
         "dividPayDate": "2025-07-11", "dividCashPsBeforeTax": "2",
         "dividCashPsAfterTax": "1.8", "dividStocksPs": "0.000000",
         "dividCashStock": "", "dividReserveToStockPs": ""},
    ])
    st = bb.load_state()
    out = bb.sync_corporate(_date(2025, 1, 1), _date(2025, 12, 31), st, timeout=5)
    assert out["adj"] == 1 and out["dividends"] == 1
    adj = pl.read_parquet(tmp_data / "adj_factor" / "all.parquet")
    assert adj["symbol"].to_list() == ["600036.SH"]
    assert adj["trade_date"][0] == _date(2025, 7, 16)
    assert adj["ex_factor"][0] == pytest.approx(1.0)  # 唯一事件=最新 → 1.0
    div = pl.read_parquet(tmp_data / "dividends" / "all.parquet")
    assert div["cash_ps_before_tax"][0] == 2.0
    assert set(st["adj_done"]) == {"600036.SH"}
    assert set(st["dividends_done"]) == {"600036.SH"}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py::test_build_ex_factor_table -v`
Expected: FAIL（AttributeError: no attribute 'build_ex_factor_table'）

- [ ] **Step 3: 写实现（追加到模块）**

```python
def build_ex_factor_table(events: dict[str, list[tuple[_date, float]]]) -> pl.DataFrame:
    """backAdjustFactor 事件 → 动态前复权累计 ex_factor（锚定最新事件日 = 1.0）。

    事件行 ex_factor = back(d) / back(latest)。DataManager._adj_events 用相邻行
    比例重建事件因子（prev/curr）：10送10 → 0.5 跳变 ✓（与 adj_factor_etf 同构）。
    """
    frames = []
    for code, evs in events.items():
        evs = sorted(set(evs))
        if not evs:
            continue
        latest_back = evs[-1][1]
        if latest_back <= 0:
            continue
        frames.append(pl.DataFrame({
            "symbol": [from_baostock_code(code)] * len(evs),
            "trade_date": [d for d, _ in evs],
            "ex_factor": [b / latest_back for _, b in evs],
        }))
    if not frames:
        return pl.DataFrame(schema={
            "symbol": pl.Utf8, "trade_date": pl.Date, "ex_factor": pl.Float64})
    return pl.concat(frames).sort(["symbol", "trade_date"])


def _write_append_table(df: pl.DataFrame, path: Path, keys: list[str]) -> None:
    """整表原子写（读旧→concat→unique keep=last→tmp→rename），幂等。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        old = pl.read_parquet(path)
        df = pl.concat([old, df]).unique(subset=keys, keep="last").sort(keys)
    tmp = path.with_name(path.name + ".tmp")
    df.write_parquet(tmp)
    tmp.rename(path)


def sync_corporate(start: _date, end: _date, state: dict, timeout: float = 120,
                   retry_failed: bool = False, limit: int | None = None,
                   progress=None) -> dict:
    """复权因子 + 分红送转明细回源（全市场股票）。

    - adj：逐只 query_adjust_factor → 事件行 ex_factor 表 → data/adj_factor/all.parquet
    - dividends：逐只 × 逐年（start.year..end.year）query_dividend_data →
      data/dividends/all.parquet；只保留 start<=ex_date<=end 且 ex_date 非空的行
    - 断点：state['adj_done'] / state['dividends_done']（按 symbol 粒度）
    """
    stats = {"adj": 0, "dividends": 0}
    syms = stock_universe()
    # ---- 复权因子 ----
    done_adj = set(state["adj_done"])
    failed_adj = state["failed"].get("adj", {})
    todo_adj = [s for s in syms if s not in done_adj]
    if not retry_failed:
        todo_adj = [s for s in todo_adj if s not in failed_adj]
    if limit is not None:
        todo_adj = todo_adj[:limit]
    logger.info("[adj] 回源 %d 只（已覆盖 %d）", len(todo_adj), len(done_adj))
    events: dict[str, list[tuple[_date, float]]] = {}
    for i, sym in enumerate(todo_adj, 1):
        code = to_baostock_code(sym)
        try:
            rows = query_adjust_factor_rows(code, start.isoformat(), end.isoformat(), timeout)
            evs = []
            for r in rows:
                if len(r) < 4 or not r[1]:
                    continue
                b = _safe_float(r[3])  # backAdjustFactor
                if b is None or b <= 0:
                    continue
                try:
                    evs.append((_date.fromisoformat(r[1]), b))
                except Exception:  # noqa: BLE001
                    continue
            if evs:
                events[code] = evs
        except Exception as e:  # noqa: BLE001
            mark_failed(state, "adj", sym, str(e)[:120])
            append_failure(sym, f"adj:{str(e)[:120]}")
            save_state(state)
            continue
        mark_done(state, "adj", sym)
        if i % 100 == 0:
            save_state(state)
            if progress:
                progress("adj", i, len(todo_adj), len(events))
    save_state(state)
    if events:
        df = build_ex_factor_table(events)
        _write_append_table(df, ADJ_FACTOR_PATH, ["symbol", "trade_date"])
        stats["adj"] = df.height
        logger.info("[adj] 因子表 %d 行 → %s", df.height, ADJ_FACTOR_PATH)
    # ---- 分红送转明细 ----
    years = list(range(start.year, end.year + 1))
    done_div = set(state["dividends_done"])
    failed_div = state["failed"].get("dividends", {})
    todo_div = [s for s in syms if s not in done_div]
    if not retry_failed:
        todo_div = [s for s in todo_div if s not in failed_div]
    if limit is not None:
        todo_div = todo_div[:limit]
    logger.info("[dividends] 回源 %d 只 × %d 年（已覆盖 %d）",
                len(todo_div), len(years), len(done_div))
    div_frames: list[pl.DataFrame] = []
    for i, sym in enumerate(todo_div, 1):
        code = to_baostock_code(sym)
        all_ok = True
        for y in years:
            try:
                recs = query_dividend_rows(code, y, timeout)
            except Exception as e:  # noqa: BLE001
                all_ok = False
                mark_failed(state, "dividends", sym, f"{y}:{str(e)[:100]}")
                append_failure(sym, f"dividend:{y}:{str(e)[:100]}")
                continue
            for rec in recs:
                ex = (rec.get("dividOperateDate") or "").strip()
                if not ex:
                    continue
                try:
                    ex_d = _date.fromisoformat(ex)
                except Exception:  # noqa: BLE001
                    continue
                if not (start <= ex_d <= end):
                    continue
                div_frames.append(pl.DataFrame({
                    "symbol": [sym], "ex_date": [ex_d],
                    "cash_ps_before_tax": [_safe_float(rec.get("dividCashPsBeforeTax"))],
                    "cash_ps_after_tax": [_safe_float(rec.get("dividCashPsAfterTax"))],
                    "stocks_ps": [_safe_float(rec.get("dividStocksPs"))],
                    "reserve_to_stock_ps": [_safe_float(rec.get("dividReserveToStockPs"))],
                }))
        if all_ok:
            mark_done(state, "dividends", sym)
        if i % 100 == 0:
            save_state(state)
            if progress:
                progress("dividends", i, len(todo_div), len(div_frames))
    save_state(state)
    if div_frames:
        df = pl.concat(div_frames).unique(
            subset=["symbol", "ex_date"], keep="last").sort(["symbol", "ex_date"])
        _write_append_table(df, DIVIDENDS_PATH, ["symbol", "ex_date"])
        stats["dividends"] = df.height
        logger.info("[dividends] 明细 %d 行 → %s", df.height, DIVIDENDS_PATH)
    return stats
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_baostock_backfill.py -v`
Expected: 28 passed

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/baostock_backfill.py backend/tests/quant/test_baostock_backfill.py
git commit -m "feat(quant): baostock backfill adj factor conversion + dividend detail"
```

---

### Task 8: CLI 脚本 + AGENTS.md 说明 + 全量验证

**Files:**
- Create: `backend/scripts/backfill_baostock_3y.py`
- Modify: `AGENTS.md`（追加小节）
- Test: 无新单测（CLI 冒烟手动执行）

**Interfaces:**
- Consumes: `baostock_backfill` 全部导出（Task 1-7）
- Produces: CLI 入口 `python scripts/backfill_baostock_3y.py`

- [ ] **Step 1: 写 CLI 脚本**

创建 `backend/scripts/backfill_baostock_3y.py`：

```python
#!/usr/bin/env python
"""baostock 全市场近 3 年回源 CLI（股票 5min + ETF/指数日线 + 复权因子 + 分红送转）。

用法:
  python scripts/backfill_baostock_3y.py                         # 全部 stage
  python scripts/backfill_baostock_3y.py --stage minute          # 只回源股票 5min
  python scripts/backfill_baostock_3y.py --stage daily           # ETF/指数日线
  python scripts/backfill_baostock_3y.py --stage corporate       # 因子+分红
  python scripts/backfill_baostock_3y.py --limit 3               # 冒烟（各 stage 只处理 3 只）

断点续传：data/baostock_backfill_state.json，中断后重跑自动跳过已完成标的；
--retry-failed 重试上次失败标的，--reset-state 清空状态重跑。
"""
import argparse
import logging
import os
import sys
from datetime import date as _date
from datetime import timedelta as _td

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import baostock_backfill as bb


def _default_start() -> _date:
    return _date.today() - _td(days=365 * 3)


def main() -> None:
    ap = argparse.ArgumentParser(description="baostock 全市场近 3 年回源")
    ap.add_argument("--start", type=_date.fromisoformat, default=None,
                    help="起始日期 YYYY-MM-DD（默认 3 年前）")
    ap.add_argument("--end", type=_date.fromisoformat, default=None,
                    help="结束日期 YYYY-MM-DD（默认今天）")
    ap.add_argument("--stage", choices=["minute", "daily", "corporate", "all"],
                    default="all")
    ap.add_argument("--reset-state", action="store_true", help="清空断点状态重跑")
    ap.add_argument("--retry-failed", action="store_true", help="重试失败标的")
    ap.add_argument("--timeout", type=float, default=300.0, help="单请求墙钟超时秒")
    ap.add_argument("--flush-batch", type=int, default=100, help="攒满多少只批量写分区")
    ap.add_argument("--limit", type=int, default=None, help="每 stage 最多处理标的数（冒烟用）")
    args = ap.parse_args()

    start = args.start or _default_start()
    end = args.end or _date.today()
    if start >= end:
        print(f"start({start}) 必须早于 end({end})")
        sys.exit(1)

    if args.reset_state and bb.STATE_PATH.exists():
        bb.STATE_PATH.unlink()
        print("已清空断点状态")
    state = bb.load_state()
    state["start"] = start.isoformat()
    state["end"] = end.isoformat()
    bb.save_state(state)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    progress = bb.make_progress_printer()
    print(f"[baostock-backfill] {start} ~ {end} stage={args.stage} ...", flush=True)

    if args.stage in ("minute", "all"):
        bb.sync_minute(start, end, state, timeout=args.timeout,
                       flush_batch=args.flush_batch, retry_failed=args.retry_failed,
                       limit=args.limit, progress=progress)
    if args.stage in ("daily", "all"):
        bb.sync_daily(start, end, state, timeout=args.timeout,
                      retry_failed=args.retry_failed, limit=args.limit,
                      progress=progress)
    if args.stage in ("corporate", "all"):
        bb.sync_corporate(start, end, state, timeout=args.timeout,
                          retry_failed=args.retry_failed, limit=args.limit,
                          progress=progress)
    print("[baostock-backfill] 完成", flush=True)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 冒烟测试（真实网络，小样本）**

Run:
```bash
cd backend && timeout 600 uv run python scripts/backfill_baostock_3y.py --stage minute --limit 2
```
Expected: 打印 `[minute] 回源 2 只`、进度行、`[minute] 完成`；`data/kline_5min/date=.../part.parquet` 生成，schema 为 `symbol, datetime, open, high, low, close, volume, amount`。

再跑一遍同样命令：Expected: `[minute] 回源 0 只（已覆盖 2）`（断点续传生效）。

Run:
```bash
cd backend && timeout 600 uv run python scripts/backfill_baostock_3y.py --stage daily --limit 1
```
Expected: 指数 + ETF 各 1 只，日线分区合并进 `kline_index_daily` / `kline_etf_daily`。

Run:
```bash
cd backend && timeout 600 uv run python scripts/backfill_baostock_3y.py --stage corporate --limit 1
```
Expected: `data/adj_factor/all.parquet` 与 `data/dividends/all.parquet` 生成。

（冒烟样本数据会真实写进 data/，如需清理：删掉冒烟涉及的 `kline_5min`、对应日线分区、`adj_factor`、`dividends`、state.json、failures.csv 再全量跑。）

- [ ] **Step 3: lint + mypy + 全量测试**

Run:
```bash
cd backend && uv run --extra dev ruff check app/services/baostock_backfill.py scripts/backfill_baostock_3y.py tests/quant/test_baostock_backfill.py
cd backend && uv run --extra dev mypy app/services/baostock_backfill.py
cd backend && uv run --extra dev pytest
```
Expected: ruff 0 errors；mypy 无 error；pytest 全绿（含既有用例）。

- [ ] **Step 4: AGENTS.md 追加小节**

在 AGENTS.md 的 mootdx 数据服务小节后追加：

```markdown
## baostock 回源脚本（一次性全量回源）

- `backend/scripts/backfill_baostock_3y.py`（逻辑在 `backend/app/services/baostock_backfill.py`）：
  回源全市场近 3 年 **股票 5min 真实数据** → `data/kline_5min/date=YYYY-MM-DD/part.parquet`
  （baostock 无 1min/ETF分钟/指数分钟，实测 `frequency="1"` 返回错误）；
  ETF/指数**日线** → `kline_etf_daily` / `kline_index_daily`（指数 volume 股÷100 转手，
  ETF 不换算；baostock ETF 日线仅 2026-01-05 起）；复权因子（分红/送转/配股/缩股净效果）
  → `data/adj_factor/all.parquet`（与 `adj_factor_etf` 同构，DataManager 自动加载）；
  分红送转明细 → `data/dividends/all.parquet`。
- 断点续传：`data/baostock_backfill_state.json`；`--retry-failed` 重试失败，
  `--reset-state` 清空重跑；失败记录 `data/baostock_backfill_failures.csv`。
- baostock 服务器吞吐波动大（单只 3 年 5min 实测 47s~100s+），串行执行，全量约几十小时，
  靠 resume 分多轮跑完。运行：`cd backend && uv run python scripts/backfill_baostock_3y.py [--stage minute|daily|corporate|all]`。
```

- [ ] **Step 5: Commit**

```bash
git add backend/scripts/backfill_baostock_3y.py AGENTS.md
git commit -m "feat(quant): baostock backfill CLI + AGENTS.md docs"
```

---

## 验收标准

1. `cd backend && uv run --extra dev pytest` 全绿（新增 ~28 用例 + 既有用例）
2. `uv run --extra dev ruff check app scripts` 与 `uv run --extra dev mypy app` 干净
3. 冒烟：`--limit 2 --stage minute` 生成 `data/kline_5min/date=*/part.parquet`，重跑显示「已覆盖」跳过（断点生效）
4. `--limit 1 --stage daily` / `--stage corporate` 生成对应分区与 `adj_factor`/`dividends` 表
5. 复权因子抽样验证：对某只有分红/送转的股票，`data/adj_factor/all.parquet` 事件日 ex_factor 跳变比例与实际除权比例一致（如 10送10 → 0.5）
