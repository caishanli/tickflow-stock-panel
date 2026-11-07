# 多源数据核验与自动修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给本地行情分区加一层「多源交叉核验 + 自动修复」，覆盖 5 个数据集全部历史，并修掉当前已知的科创板分钟量额 ×100 污染。

**Architecture:** 新包 `backend/app/quant/data_verify/`，纯函数做核验、独立模块做修复，runner 负责分批与断点。核验分三层：L0 结构（无网络）、L1 分钟→日线内部聚合（无网络，覆盖全部历史）、L2 外部源比对（盘后包/腾讯/新浪/baostock/eltdx）。修复只碰已收盘分区，先备份再原子替换，最后重算受影响日期的 enriched。

**Tech Stack:** Python 3.14、Polars、DuckDB（只读查询）、requests、JobStore（`app/services/pipeline_jobs.py`）、APScheduler。

**Spec:** `docs/superpowers/specs/2026-11-07-multisource-data-verification-design.md`

## Global Constraints

- 数据根目录以 `app/services/mootdx_service.py` 的 `DATA_ROOT` 为准（不是相对路径）。
- 计算一律 Polars；不得引入 pandas（回测之外）。
- 不新增分区列、不改读侧契约：`kline_daily` / `kline_etf_daily` 仍存不复权原始价。
- 只修 `day < today` 的已收盘分区；写入必须 `tmp` → `os.replace` 原子替换，且在
  `raw_partition_lock.daily_partition_lock(part)` 内完成。
- 手动跑默认 dry-run；定时跑只自动应用「确定性类 + 近 7 日 confirmed 外部覆盖」。
- 所有外部请求必须有硬超时；baostock 5min 查询超时 ≤60s，失败即跳过。
- `eltdx` 只允许作为可选依赖（`--extra verify`），import 失败必须自动退化，不得阻塞主流程。
- Lint/测试命令：`cd backend && uv run --extra dev pytest tests/quant/<file> -v`；
  `uv run --extra dev ruff check app/quant/data_verify`（line-length 100）。

## Review Focus

1. **停牌 / 新股 / 退市整理**：分钟 bar 数不足 241 是正常现象，L0 不能按「不足即坏」处理。
2. **提前收盘（熔断半日市）**：分钟尾段 0 量平推，收盘价必须取「最后一根有量 bar」，否则每天误报。
3. **ETF 日线没有 `date` 列**（日期由 hive 分区名隐含）：重写分区时若混入 `date` 列会写坏格式。
4. **腾讯分钟 amount 是估算值**（`vol × typical`，实测偏差 0.5%~1%、极端更大）：只能当 `soft`，永不作为修复依据。
5. **重复执行幂等**：同一天跑两次核验/修复，不能重复写盘、不能重复追加 receipt、不能每次重建备份。

---

### Task 1: 单位契约 + 修生产端 `parse_m1_rows`

**Files:**
- Create: `backend/app/quant/data_verify/__init__.py`
- Create: `backend/app/quant/data_verify/units.py`
- Modify: `backend/app/services/tencent_minute.py:117-175`（`parse_m1_rows` 内 `vol` 计算）
- Test: `backend/tests/quant/test_data_verify_units.py`
- Test: `backend/tests/quant/test_tencent_minute_units.py`

**Interfaces:**
- Consumes: 无。
- Produces:
  - `tencent_m1_volume_factor(symbol: str) -> float` — 科创板 `688`/`689` 返回 `1.0`，其余返回 `100.0`。
  - `minute_volume_to_daily(volume_shares: float, table: str) -> float` — `kline_minute` 返回 `volume_shares / 100`（股→手），`kline_etf_minute` 原样返回。
  - `DAILY_VOLUME_UNIT: dict[str, str]`，值为 `"lot"` / `"share"`。

- [ ] **Step 1: 写失败测试**

`backend/tests/quant/test_data_verify_units.py`：

```python
import pytest
from app.quant.data_verify.units import (
    DAILY_VOLUME_UNIT, minute_volume_to_daily, tencent_m1_volume_factor,
)


@pytest.mark.parametrize("symbol,expected", [
    ("688001.SH", 1.0), ("689009.SH", 1.0),   # 科创板：腾讯 m1 原始量已是股
    ("600000.SH", 100.0), ("000001.SZ", 100.0),
    ("300750.SZ", 100.0), ("510300.SH", 100.0), ("588000.SH", 100.0),
])
def test_tencent_m1_volume_factor(symbol, expected):
    assert tencent_m1_volume_factor(symbol) == expected


def test_minute_volume_to_daily():
    assert minute_volume_to_daily(8_630_559.0, "kline_minute") == pytest.approx(86_305.59)
    assert minute_volume_to_daily(9_802_800_000.0, "kline_etf_minute") == 9_802_800_000.0


def test_daily_volume_unit_contract():
    assert DAILY_VOLUME_UNIT["kline_daily"] == "lot"
    assert DAILY_VOLUME_UNIT["kline_etf_daily"] == "share"
```

`backend/tests/quant/test_tencent_minute_units.py`：

```python
import polars as pl
from datetime import date
from app.services.tencent_minute import parse_m1_rows

ROW = ["202611061500", "35.69", "36.68", "36.68", "35.69", "95429.00", {}, "2.02"]


def test_star_minute_volume_kept_as_shares():
    df = parse_m1_rows("688001.SH", [ROW], {date(2026, 11, 6)})
    assert df["volume"][0] == 95429.0          # 不能再 ×100


def test_main_board_minute_volume_converted_to_shares():
    df = parse_m1_rows("600000.SH", [ROW], {date(2026, 11, 6)})
    assert df["volume"][0] == 9_542_900.0
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_units.py tests/quant/test_tencent_minute_units.py -v`
Expected: FAIL（`ModuleNotFoundError: app.quant.data_verify` / 688001 量断言 9542900 != 95429）

- [ ] **Step 3: 实现 `units.py` 并接入 `parse_m1_rows`**

`units.py` 要点：

```python
STAR_PREFIXES = ("688", "689")

def tencent_m1_volume_factor(symbol: str) -> float:
    """腾讯 mkline 返回量单位：主板/创业板/ETF 给「手」，科创板 688/689 给「股」。"""
    code = symbol.split(".")[0]
    return 1.0 if code.startswith(STAR_PREFIXES) else 100.0

DAILY_VOLUME_UNIT = {
    "kline_daily": "lot", "kline_etf_daily": "share", "kline_index_daily": "share",
}

def minute_volume_to_daily(volume_shares: float, table: str) -> float:
    return volume_shares / 100.0 if DAILY_VOLUME_UNIT.get(table) == "lot" else volume_shares
```

`parse_m1_rows` 中把 `vol = float(r[5]) * 100.0` 改为
`vol = float(r[5]) * tencent_m1_volume_factor(symbol)`，并在 docstring 补一句单位说明。

- [ ] **Step 4: 运行确认通过 + 回归**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_units.py tests/quant/test_tencent_minute_units.py -v`
Expected: PASS
Run: `cd backend && uv run --extra dev pytest tests/quant/test_alt_daily.py tests/quant/test_stockdata_sources.py -q`
Expected: PASS（不破坏既有回源测试）

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/__init__.py backend/app/quant/data_verify/units.py \
  backend/app/services/tencent_minute.py \
  backend/tests/quant/test_data_verify_units.py backend/tests/quant/test_tencent_minute_units.py
git commit -m "fix(stockdata): 腾讯 m1 科创板成交量按股入库，新增单位契约"
```

---

### Task 2: Finding 模型 + L0 结构检查

**Files:**
- Create: `backend/app/quant/data_verify/findings.py`
- Create: `backend/app/quant/data_verify/checks_structure.py`
- Test: `backend/tests/quant/test_data_verify_structure.py`

**Interfaces:**
- Consumes: Task 1 的 `minute_volume_to_daily`（本任务暂不用）。
- Produces:
  - `Finding(table: str, day: date, symbol: str | None, code: str, severity: str, detail: dict)`
  - `check_structure(df: pl.DataFrame, *, table: str, day: date, expected_bars: int = 241,
    suspended_symbols: set[str] | None = None) -> list[Finding]`
  - 错误码常量：`MISSING_BARS`、`DUPLICATE_BAR`、`OUT_OF_SESSION`、`FORBIDDEN_1300`、
    `OHLC_INVALID`、`NON_FINITE`、`ALL_ZERO`。

- [ ] **Step 1: 写失败测试**

```python
import polars as pl
from datetime import date, datetime
from app.quant.data_verify.checks_structure import check_structure

DAY = date(2026, 11, 6)


def _frame(rows):
    return pl.DataFrame(rows, schema={
        "symbol": pl.String, "datetime": pl.Datetime("us"),
        "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
        "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
    })


def _day_rows(sym, n=241, start_hour=9, start_min=30):
    out, h, m = [], start_hour, start_min
    for _ in range(n):
        out.append((sym, datetime(2026, 11, 6, h, m), 10.0, 10.1, 9.9, 10.0, 100.0, 1000.0))
        m += 1
        if m == 60:
            h, m = h + 1, 0
        if h == 11 and m == 31:
            h, m = 13, 1
    return out


def test_missing_bars_flagged():
    df = _frame(_day_rows("600000.SH", n=200))
    codes = [f.code for f in check_structure(df, table="kline_minute", day=DAY)]
    assert "MISSING_BARS" in codes


def test_suspended_symbol_with_zero_volume_not_flagged():
    df = _frame([(r[0], r[1], 10.0, 10.0, 10.0, 10.0, 0.0, 0.0) for r in _day_rows("600000.SH")])
    codes = [f.code for f in check_structure(
        df, table="kline_minute", day=DAY, suspended_symbols={"600000.SH"})]
    assert codes == []


def test_duplicate_and_1300_bar_flagged():
    base = _day_rows("600000.SH")
    rows = base + [base[0], ("600000.SH", datetime(2026, 11, 6, 13, 0), 1, 1, 1, 1, 1, 1)]
    codes = {f.code for f in check_structure(_frame(rows), table="kline_minute", day=DAY)}
    assert "DUPLICATE_BAR" in codes and "FORBIDDEN_1300" in codes


def test_ohlc_invalid_and_non_finite():
    rows = _day_rows("600000.SH")
    rows[0] = ("600000.SH", datetime(2026, 11, 6, 9, 30), 10.0, 9.0, 11.0, 10.0, 100.0, 1000.0)
    rows[1] = ("600000.SH", datetime(2026, 11, 6, 9, 31), float("nan"), 10, 9, 10, 100, 1000)
    codes = {f.code for f in check_structure(_frame(rows), table="kline_minute", day=DAY)}
    assert "OHLC_INVALID" in codes and "NON_FINITE" in codes
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_structure.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现 `findings.py` 与 `checks_structure.py`**

`findings.py`：`@dataclass(frozen=True)`，`to_dict()` 把 `date` 转 ISO。
`checks_structure.py`：按 symbol 分组，逐项产出 Finding；`MISSING_BARS` 的
`detail={"expected": 241, "actual": n}`；停牌名单内的 symbol 跳过 `MISSING_BARS`/`ALL_ZERO`。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_structure.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/findings.py backend/app/quant/data_verify/checks_structure.py \
  backend/tests/quant/test_data_verify_structure.py
git commit -m "feat(data-verify): L0 结构检查"
```

---

### Task 3: L1 分钟→日线内部聚合比对

**Files:**
- Create: `backend/app/quant/data_verify/checks_internal.py`
- Test: `backend/tests/quant/test_data_verify_internal.py`

**Interfaces:**
- Consumes: Task 1 `minute_volume_to_daily`；Task 2 `Finding`。
- Produces:
  - `aggregate_minute(minute: pl.DataFrame) -> pl.DataFrame`（按 symbol 聚合，
    列为 `symbol, m_open, m_high, m_low, m_close, m_volume, m_amount, bars`；
    `m_close` 取**最后一根 `volume>0` 的 bar**，全 0 量则取最后一根）。
  - `compare_minute_to_daily(minute: pl.DataFrame, daily: pl.DataFrame, *, table: str,
    price_abs_tol: float = 0.011, price_rel_tol: float = 0.001,
    volume_rel_tol: float = 0.005, amount_rel_tol: float = 0.02) -> list[Finding]`
  - 错误码：`MINUTE_VOLUME_X100`、`MINUTE_VOLUME_MISMATCH`（hard）、
    `MINUTE_CLOSE_MISMATCH`（soft）、`MINUTE_AMOUNT_MISMATCH`（soft）。

- [ ] **Step 1: 写失败测试**

```python
import polars as pl
from datetime import date, datetime
from app.quant.data_verify.checks_internal import aggregate_minute, compare_minute_to_daily

DAY = date(2026, 11, 6)


def _minute(sym, volumes):
    return pl.DataFrame(
        [(sym, datetime(2026, 11, 6, 9, 30 + i), 10.0, 10.1, 9.9, 10.0, v, v * 10.0)
         for i, v in enumerate(volumes)],
        schema={"symbol": pl.String, "datetime": pl.Datetime("us"),
                "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
                "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64},
    )


def _daily(sym, volume, amount, close=10.0):
    return pl.DataFrame([(sym, DAY, 10.0, 10.1, 9.9, close, volume, amount)],
                        schema={"symbol": pl.String, "date": pl.Date, "open": pl.Float64,
                                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                                "volume": pl.Float64, "amount": pl.Float64})


def test_close_uses_last_nonzero_bar():
    m = _minute("600004.SH", [100.0, 200.0, 0.0, 0.0])
    m = m.with_columns(pl.when(pl.int_range(pl.len()) >= 2).then(pl.lit(13.27)).otherwise(pl.col("close")).alias("close"))
    agg = aggregate_minute(m)
    assert agg["m_close"][0] == 10.0      # 尾段 0 量平推不参与收盘价


def test_star_volume_x100_detected():
    m = _minute("688001.SH", [8_630_559.0])          # 股
    d = _daily("688001.SH", volume=86_305.59, amount=8.63e8)
    codes = {f.code for f in compare_minute_to_daily(m, d, table="kline_minute")}
    assert "MINUTE_VOLUME_X100" in codes


def test_etf_volume_compared_share_to_share():
    m = _minute("510300.SH", [9_802_800.0])
    d = _daily("510300.SH", volume=9_802_800.0, amount=9.8028e9)
    codes = {f.code for f in compare_minute_to_daily(m, d, table="kline_etf_minute")}
    assert "MINUTE_VOLUME_MISMATCH" not in codes


def test_amount_mismatch_is_soft():
    m = _minute("600000.SH", [1_000_000.0])          # amount = 1e7
    d = _daily("600000.SH", volume=10_000.0, amount=1.08e7)   # 8% 偏差
    findings = compare_minute_to_daily(m, d, table="kline_minute")
    amt = [f for f in findings if f.code == "MINUTE_AMOUNT_MISMATCH"]
    assert amt and amt[0].severity == "soft"


def test_suspended_day_zero_daily_volume_no_finding():
    m = _minute("600000.SH", [0.0, 0.0])
    d = _daily("600000.SH", volume=0.0, amount=0.0)
    assert compare_minute_to_daily(m, d, table="kline_minute") == []


def test_minute_close_mismatch_is_soft():
    # 2016-01-04 600004：分钟最后一根有量 bar 收 13.27，日线收 13.04（提前收盘）
    m = _minute("600004.SH", [100.0, 0.0])
    d = _daily("600004.SH", volume=1.0, amount=10.0, close=13.04)
    findings = compare_minute_to_daily(m, d, table="kline_minute")
    close_f = [f for f in findings if f.code == "MINUTE_CLOSE_MISMATCH"]
    assert close_f and close_f[0].severity == "soft"
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_internal.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现聚合与比对**

`aggregate_minute`：先 `sort(["symbol","datetime"])`，`group_by("symbol").agg(...)`；
`m_close` 用 `pl.col("close").filter(pl.col("volume") > 0).last()`，空则回退
`pl.col("close").last()`。

`compare_minute_to_daily`：inner join；日线 `volume == 0` 的标的直接跳过（停牌正常）。
先算 `ratio = m_volume / volume`，
`ratio ∈ [95,105]` → `MINUTE_VOLUME_X100`（hard）；否则按容差比较
`minute_volume_to_daily(m_volume, table)` 与 `volume`。价格用
`max(abs, rel)`；amount 只产出 `soft`。缺 symbol 的只在 `detail` 计数，不逐条报。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_internal.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/checks_internal.py backend/tests/quant/test_data_verify_internal.py
git commit -m "feat(data-verify): L1 分钟→日线聚合比对"
```

---

### Task 4: 备份 / 原子写 / receipt / 确定性缩放修复

**Files:**
- Create: `backend/app/quant/data_verify/repair.py`
- Test: `backend/tests/quant/test_data_verify_repair.py`

**Interfaces:**
- Consumes: `app.raw_partition_lock.daily_partition_lock`。
- Produces:
  - `backup_partition(part: Path, backup_root: Path, run_id: str) -> Path`
  - `write_partition_atomic(df: pl.DataFrame, part: Path) -> None`
  - `scale_minute_volume_amount(part: Path, *, table: str, factor: float,
    backup_root: Path, run_id: str, dry_run: bool) -> dict`
    （返回 `{"changed_rows": int, "backup": str|None}`；`factor` 为除数，如 100 表示 /100。
    幂等性由调用方负责：runner 只在 L1 判定 `MINUTE_VOLUME_X100` 时才调用本函数）

- [ ] **Step 1: 写失败测试**

```python
import polars as pl
from datetime import date, datetime
from pathlib import Path
from app.quant.data_verify.repair import backup_partition, scale_minute_volume_amount


def _write(part: Path):
    part.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [("688001.SH", datetime(2026, 11, 6, 9, 30), 1.0, 1.0, 1.0, 1.0, 863055900.0, 3.13e10)],
        schema={"symbol": pl.String, "datetime": pl.Datetime("us"), "open": pl.Float64,
                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                "volume": pl.Float64, "amount": pl.Float64},
    ).write_parquet(part)


def test_dry_run_does_not_write(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    _write(part)
    res = scale_minute_volume_amount(part, table="kline_minute", factor=100.0,
                                     backup_root=tmp_path / "bk", run_id="r1", dry_run=True)
    assert res["changed_rows"] == 1
    assert pl.read_parquet(part)["volume"][0] == 863055900.0
    assert not (tmp_path / "bk").exists()


def test_apply_scales_and_backs_up(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    _write(part)
    res = scale_minute_volume_amount(part, table="kline_minute", factor=100.0,
                                     backup_root=tmp_path / "bk", run_id="r1", dry_run=False)
    df = pl.read_parquet(part)
    assert df["volume"][0] == 8630559.0 and df["amount"][0] == 3.13e8
    assert Path(res["backup"]).exists()
    assert pl.read_parquet(res["backup"])["volume"][0] == 863055900.0
```

> 重复执行的安全性由 runner 保证（Task 5 的 `test_runner_is_idempotent`）。

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_repair.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现 repair.py**

`backup_partition`：`backup_root/run_id/<table>/date=.../part.parquet`，用 `shutil.copy2`，
已存在同内容则跳过（比较 `st_size` + mtime 不必要，直接覆盖幂等）。
`write_partition_atomic`：`with daily_partition_lock(part): tmp = part.with_suffix(".tmp");
df.write_parquet(tmp); os.replace(tmp, part)`，`finally` 清理 tmp。
`scale_minute_volume_amount`：读分区，只对 `volume/amount` 除以 `factor`，其余列不动；
`dry_run` 直接返回；写前先备份。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_repair.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/repair.py backend/tests/quant/test_data_verify_repair.py
git commit -m "feat(data-verify): 备份/原子写/缩放修复"
```

---

### Task 5: Runner（L0+L1）+ 断点 + CLI（M1 可交付）

**Files:**
- Create: `backend/app/quant/data_verify/runner.py`
- Create: `backend/app/quant/data_verify/__main__.py`
- Test: `backend/tests/quant/test_data_verify_runner.py`

**Interfaces:**
- Consumes: Task 1-4 全部接口；`mootdx_service.DATA_ROOT`。
- Produces:
  - `run_verify(stage: str, *, apply: bool = False, days: list[date] | None = None,
    data_root: Path | None = None, resume: bool = False, progress=None) -> dict`
    （`stage ∈ {"structure","internal","all"}`；`days=None` = 全部已有分区；
    返回 `{"findings": int, "repaired": int, "days": int, "state_path": str}`）
  - 状态文件 `data/data_verify_state.json`：`{"last_run_at","stage","last_day_done","applied"}`
  - CLI：`python -m app.quant.data_verify --stage internal --apply`

- [ ] **Step 1: 写失败测试**

```python
import json
import polars as pl
from datetime import date, datetime
from app.quant.data_verify.runner import run_verify


def _write_day(root, day, stars_bad=True):
    mdir = root / "kline_minute" / f"date={day}" / "part.parquet"
    mdir.parent.mkdir(parents=True, exist_ok=True)
    vol = 8_630_559.0 * (100 if stars_bad else 1)
    pl.DataFrame(
        [("688001.SH", datetime.fromisoformat(f"{day}T09:30:00"), 1.0, 1.0, 1.0, 1.0, vol, vol)],
        schema={"symbol": pl.String, "datetime": pl.Datetime("us"), "open": pl.Float64,
                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                "volume": pl.Float64, "amount": pl.Float64},
    ).write_parquet(mdir)
    ddir = root / "kline_daily" / f"date={day}" / "part.parquet"
    ddir.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [("688001.SH", datetime.fromisoformat(f"{day}T00:00:00").date(), 1.0, 1.0, 1.0, 1.0, 86_305.59, 8.63e8)],
        schema={"symbol": pl.String, "date": pl.Date, "open": pl.Float64, "high": pl.Float64,
                "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64},
    ).write_parquet(ddir)


def test_runner_detects_and_repairs_star_x100(tmp_path):
    _write_day(tmp_path, "2026-11-06")
    rep = run_verify("internal", apply=True, days=None, data_root=tmp_path)
    assert rep["findings"] >= 1 and rep["repaired"] == 1
    assert pl.read_parquet(tmp_path / "kline_minute/date=2026-11-06/part.parquet")["volume"][0] == 8_630_559.0
    state = json.loads((tmp_path / "data_verify_state.json").read_text())
    assert state["last_day_done"] == "2026-11-06"


def test_runner_dry_run_by_default(tmp_path):
    _write_day(tmp_path, "2026-11-06")
    rep = run_verify("internal", days=None, data_root=tmp_path)
    assert rep["repaired"] == 0
    assert pl.read_parquet(tmp_path / "kline_minute/date=2026-11-06/part.parquet")["volume"][0] == 863_055_900.0


def test_runner_is_idempotent(tmp_path):
    _write_day(tmp_path, "2026-11-06")
    run_verify("internal", apply=True, days=None, data_root=tmp_path)
    rep = run_verify("internal", apply=True, days=[date(2026, 11, 6)], data_root=tmp_path)
    assert rep["repaired"] == 0
    assert pl.read_parquet(tmp_path / "kline_minute/date=2026-11-06/part.parquet")["volume"][0] == 8_630_559.0


def test_runner_resume_skips_done_days(tmp_path):
    _write_day(tmp_path, "2026-11-05", stars_bad=False)
    _write_day(tmp_path, "2026-11-06", stars_bad=True)
    (tmp_path / "data_verify_state.json").write_text(
        json.dumps({"last_run_at": "2026-11-07T00:00:00", "stage": "internal",
                    "last_day_done": "2026-11-05", "applied": 0}))
    rep = run_verify("internal", apply=True, days=None, data_root=tmp_path, resume=True)
    assert rep["days"] == 1                      # 只处理 2026-11-06
    assert pl.read_parquet(tmp_path / "kline_minute/date=2026-11-06/part.parquet")["volume"][0] == 8_630_559.0
```

> `days=None` 表示「扫描 `data_root` 下全部已有分区」；`resume=True` 时从
> 状态文件的 `last_day_done` 之后继续，配合 `--apply` 实现全量历史断点续跑。

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_runner.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现 runner + CLI**

`run_verify` 流程：枚举 `kline_minute` / `kline_etf_minute` 分区日期（`days=None` 或 `[]`
= 全部），逐日：L0 → L1；`apply` 时只对 `MINUTE_VOLUME_X100` 调
`scale_minute_volume_amount(factor=100)`；每完成一天更新状态文件（原子写）；
`progress(stage, pct, msg)` 可选回调。
CLI 用 `argparse`，打印 JSON 摘要。

- [ ] **Step 4: 运行确认通过 + 冒烟**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_runner.py -v`
Expected: PASS
Run: `cd backend && uv run --extra dev python -m app.quant.data_verify --stage internal --days 2026-11-06`
Expected: 输出 JSON，`findings>0`、`repaired=0`（dry-run），不写盘

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/runner.py backend/app/quant/data_verify/__main__.py \
  backend/tests/quant/test_data_verify_runner.py
git commit -m "feat(data-verify): runner + 断点状态 + CLI"
```

---

### Task 6: 通达信官网盘后包 probe

**Files:**
- Create: `backend/app/quant/data_verify/probes/__init__.py`
- Create: `backend/app/quant/data_verify/probes/tdx_package.py`
- Test: `backend/tests/quant/test_data_verify_tdx_package.py`
- Test: `backend/tests/quant/test_data_verify_tdx_package_live.py`（`ASTOCK_LIVE=1` 才跑）

**Interfaces:**
- Consumes: 无。
- Produces:
  - `fetch_daily_package(day: date, *, timeout: float = 90.0) -> pl.DataFrame`
    （列 `symbol,date,open,high,low,close,volume,amount`；`symbol` 形如 `600000.SH`；
    `volume` 为股、`amount` 为元；非交易日/未发布抛 `ValueError`）
  - `parse_package(content: bytes, ymd: str) -> pl.DataFrame`

- [ ] **Step 1: 写失败测试（合成 zip）**

```python
import io, struct, zipfile
import pytest
from app.quant.data_verify.probes.tdx_package import parse_package


def _build_zip(ymd="20261106", n=3, close=10.0):
    cod = b""
    md1 = b""
    for i in range(n):
        code = f"60{i:04d}".encode()
        rec = code.ljust(6, b"\x00") + b"\x00" * 26 + struct.pack("<H", i) + b"\x00" * 6
        rec += "测试".encode("gbk") + b"\x00" * 30
        cod += rec.ljust(150, b"\x00")
        block = struct.pack("<d", close - 0.1) + struct.pack("<4d", 10, 10.2, 9.8, close)
        block += struct.pack("<Q", 1_000_000) + struct.pack("<d", 1e7)
        md1 += block.ljust(512, b"\x00")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(f"sh{ymd[2:]}.cod", cod)
        z.writestr(f"sh{ymd[2:]}.md1", md1)
    return buf.getvalue()


def test_parse_package_rows():
    df = parse_package(_build_zip(), "20261106")
    assert df.height == 3
    assert df["symbol"][0] == "600000.SH"
    assert df["close"][0] == 10.0


def test_missing_market_file_raises():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("sh261106.cod", b"")
    with pytest.raises(RuntimeError):
        parse_package(buf.getvalue(), "20261106")
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_tdx_package.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现 probe（移植 a-stock-data §1.3）**

从 `~/.hermes/skills/financial-data/a-stock-data/SKILL.md` §1.3 的
`_tdx_parse_package` 移植（MIT），改动两处：`symbol` 输出为 `{code}.{SH|SZ|BJ}`；
北交所文件缺失规则保留（2022-05-06 前放行）。`fetch_daily_package` 走
`https://www.tdx.com.cn/products/data/data/g4day/{ymd}.zip`，404 → `ValueError`。

- [ ] **Step 4: 运行确认通过（含一次真实抓取）**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_tdx_package.py -v`
Expected: PASS
Run: `cd backend && ASTOCK_LIVE=1 uv run --extra dev pytest tests/quant/test_data_verify_tdx_package_live.py -v`
Expected: PASS（`2026-11-06` 返回 ≥5000 行，600519 与本地日线收盘一致）

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/probes/ backend/tests/quant/test_data_verify_tdx_package*.py
git commit -m "feat(data-verify): 通达信盘后包探针"
```

---

### Task 7: 日线 L2 比对 + 仲裁策略

**Files:**
- Create: `backend/app/quant/data_verify/checks_external.py`
- Create: `backend/app/quant/data_verify/policy.py`
- Test: `backend/tests/quant/test_data_verify_policy.py`

**Interfaces:**
- Consumes: Task 2 `Finding`；Task 6 `fetch_daily_package`。
- Produces:
  - `compare_daily_external(local: pl.DataFrame, external: pl.DataFrame, *, source: str,
    price_abs_tol: float = 0.011, price_rel_tol: float = 0.001) -> list[Finding]`
  - `decide(findings: list[Finding], verdicts: dict[str, str]) -> list[Decision]`，
    `Decision(action: str, finding: Finding, source: str)`，
    `action ∈ {"keep_local","repair_local","quarantine"}`；`verdicts` 为
    `{source: "agree_local"|"agree_external"|"unknown"}`。
  - 规则：≥2 个外部源与本地不一致 → `repair_local`；单源 → `repair_local` 且
    `finding.detail["single_source"]=True`；外部源互相矛盾 → `quarantine`。

- [ ] **Step 1: 写失败测试**

```python
from datetime import date
import polars as pl
from app.quant.data_verify.checks_external import compare_daily_external
from app.quant.data_verify.policy import decide

DAY = date(2026, 11, 6)


def _df(close, volume=1000.0):
    return pl.DataFrame([("600000.SH", DAY, 10.0, 10.2, 9.8, close, volume, close * volume)],
                        schema={"symbol": pl.String, "date": pl.Date, "open": pl.Float64,
                                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                                "volume": pl.Float64, "amount": pl.Float64})


def test_close_mismatch_detected():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    assert [x.code for x in f] == ["DAILY_CLOSE_MISMATCH"]


def test_two_external_agree_external_repairs():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    d = decide(f, {"tdx": "agree_external", "baostock": "agree_external"})
    assert d[0].action == "repair_local"


def test_single_source_marks_single_source():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    d = decide(f, {"tdx": "agree_external"})
    assert d[0].action == "repair_local" and d[0].finding.detail["single_source"] is True


def test_conflict_quarantines():
    f = compare_daily_external(_df(10.0), _df(10.5), source="tdx")
    d = decide(f, {"tdx": "agree_external", "baostock": "agree_local"})
    assert d[0].action == "quarantine"
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_policy.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现比对与仲裁**

`compare_daily_external`：按 `symbol` join；逐字段比较，产出
`DAILY_CLOSE_MISMATCH` / `DAILY_VOLUME_MISMATCH`（`DAILY_AMOUNT_MISMATCH` 为 `soft`）。
`decide`：按 §5.4 优先级与多数规则产出 `Decision`；`single_source` 写进
`finding.detail`（`dataclasses.replace`）。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_policy.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/checks_external.py backend/app/quant/data_verify/policy.py \
  backend/tests/quant/test_data_verify_policy.py
git commit -m "feat(data-verify): 日线外部比对与仲裁"
```

---

### Task 8: 外部覆盖修复 + 下游 enriched 重算

**Files:**
- Modify: `backend/app/quant/data_verify/repair.py`（新增 `replace_partition_rows`）
- Create: `backend/app/quant/data_verify/recompute.py`
- Test: `backend/tests/quant/test_data_verify_recompute.py`

**Interfaces:**
- Consumes: Task 4 `backup_partition` / `write_partition_atomic`；
  `app.services.data_integrity.prune_enriched_partitions`。
- Produces:
  - `replace_symbols(part: Path, replacement: pl.DataFrame, *, backup_root: Path,
    run_id: str, dry_run: bool) -> dict`（按 `symbol` 整行替换；**保留分区原有列结构**，
    `kline_etf_daily` 没有 `date` 列时不得引入）
  - `recompute_enriched(days: list[date], *, tables: list[str], prune, runner) -> dict`
    （`prune`/`runner` 为可注入实现，测试用 fake；返回 `{"ok": bool, "days": int, "error": str|None}`）

- [ ] **Step 1: 写失败测试**

```python
from datetime import date
import polars as pl
from app.quant.data_verify.recompute import recompute_enriched
from app.quant.data_verify.repair import replace_symbols


def test_replace_symbols_preserves_etf_daily_without_date_column(tmp_path):
    part = tmp_path / "kline_etf_daily" / "date=2026-11-06" / "part.parquet"
    part.parent.mkdir(parents=True)
    schema = {"symbol": pl.String, "open": pl.Float64, "high": pl.Float64,
              "low": pl.Float64, "close": pl.Float64,
              "volume": pl.Float64, "amount": pl.Float64}
    pl.DataFrame([("510300.SH", 4.0, 4.1, 3.9, 4.05, 1000.0, 4000.0)],
                 schema=schema, orient="row").write_parquet(part)
    repl = pl.DataFrame([("510300.SH", 4.0, 4.1, 3.9, 4.12, 1000.0, 4000.0)],
                        schema=schema, orient="row")
    replace_symbols(part, repl, backup_root=tmp_path / "bk", run_id="r1", dry_run=False)
    out = pl.read_parquet(part)
    assert out["close"][0] == 4.12 and "date" not in out.columns


def test_recompute_prunes_and_calls_runner(tmp_path):
    called = []

    def fake_prune(data_dir, start, table):
        called.append(("prune", str(start), table))

    def fake_runner(repo, capset, days):
        called.append(("run", tuple(str(d) for d in days)))

    res = recompute_enriched([date(2026, 11, 5), date(2026, 11, 6)],
                             tables=["kline_daily_enriched"],
                             prune=fake_prune, runner=fake_runner)
    assert res["days"] == 2 and res["ok"] is True
    assert called[0] == ("prune", "2026-11-05", "kline_daily_enriched")
    assert called[-1] == ("run", ("2026-11-05", "2026-11-06"))


def test_recompute_failure_reported_not_raised():
    def boom(*a, **k):
        raise RuntimeError("pipeline down")
    res = recompute_enriched([date(2026, 11, 6)], tables=["kline_daily_enriched"],
                             prune=lambda *a, **k: None, runner=boom)
    assert res["ok"] is False and "pipeline down" in res["error"]
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_recompute.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现**

`replace_symbols`：读旧分区 → `join(replacement, on="symbol", how="left")` +
`coalesce` 覆盖被替换列 → 备份 → 原子写。
`recompute_enriched(days, tables, prune, runner)`：按表逐个 `prune(data_dir, min(days), table)`，
然后一次 `runner(repo, capset, sorted(days))`；异常捕获成 `{"ok": False, "error": str(e)}`。
真实接线在 Task 11 的调度里，注入 `daily_pipeline` 的实现。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_recompute.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/repair.py backend/app/quant/data_verify/recompute.py \
  backend/tests/quant/test_data_verify_recompute.py
git commit -m "feat(data-verify): 外部覆盖修复 + enriched 重算"
```

---

### Task 9: 分钟外部源（腾讯 / 新浪 / eltdx 可选）

**Files:**
- Create: `backend/app/quant/data_verify/probes/tencent_m1.py`
- Create: `backend/app/quant/data_verify/probes/sina_m1.py`
- Create: `backend/app/quant/data_verify/probes/eltdx_m1.py`
- Modify: `backend/pyproject.toml`（新增可选 extra `verify = ["eltdx>=3.3"]`）
- Modify: `backend/app/quant/data_verify/runner.py`（新增 `external_minute` stage）
- Test: `backend/tests/quant/test_data_verify_minute_probes.py`

**Interfaces:**
- Consumes: 无（独立 probe）；输出统一 schema
  `symbol, datetime, open, high, low, close, volume, amount`（volume 股）。
- Produces:
  - `fetch_tencent_m1(symbol: str, *, days: set[date] | None = None) -> pl.DataFrame`
  - `fetch_sina_m1(symbol: str, *, count: int = 1023) -> pl.DataFrame`
  - `fetch_eltdx_m1(symbol: str, *, all_pages: bool = True) -> pl.DataFrame`；
    未安装 `eltdx` 时抛 `ImportError`（调用方捕获后跳过）
  - `available_sources() -> list[str]`
  - 在 `runner.run_verify` 增加 `external_minute` stage：三方 probe → 与本地分区逐 bar 比对
    （按 `datetime` join，容差同 L1；缺 bar/close/volume 不一致产出 finding）→
    `repair.replace_symbols` 修被 ≥2 个源确认的 symbol；`"all"` 定义为
    `structure + internal + external_minute`。
  - stage 参数：`sample: int = 100`（每天按 symbol 排序后等距抽样）、
    `lookback_days: int = 94`（eltdx 深度）。外部源不可用时该 stage 直接跳过并记入报告。

- [ ] **Step 1: 写失败测试（离线 fixture）**

```python
import polars as pl
from app.quant.data_verify.probes.tencent_m1 import parse_tencent_m1
from app.quant.data_verify.probes.sina_m1 import parse_sina_m1


def test_parse_tencent_m1_star_keeps_shares():
    rows = [["202611061500", "35.69", "36.68", "36.68", "35.69", "95429.00", {}, "2.02"]]
    df = parse_tencent_m1("688001.SH", rows)
    assert df["volume"][0] == 95429.0 and df.schema["datetime"] == pl.Datetime("us")


def test_parse_sina_m1_volume_is_shares():
    payload = [{"day": "2026-11-06 15:00:00", "open": "10.03", "high": "10.03",
                "low": "10.03", "close": "10.03", "volume": "955500", "amount": "9583665"}]
    df = parse_sina_m1("600000.SH", payload)
    assert df["volume"][0] == 955500.0
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_minute_probes.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现三个 probe**

- 腾讯：复用 `tencent_minute.fetch_m1` 的 URL 与解析规则（不要重复实现 HTTP 细节，
  调 `parse_m1_rows` 保证单位一致）；
- 新浪：`CN_MarketDataService.getKLineData?scale=1&datalen=1023`，`amount` 字段存在则用，
  缺失时按 `volume × typical` 估算并打 `attrs["amount"]="estimated"`；
- eltdx：`from eltdx import TdxClient` 延迟导入，`bars.get(symbol, period="1m",
  all_pages=True)`，把 `KlineBar` 转成统一 schema。

- [ ] **Step 4: 运行确认通过 + 联网冒烟（可选）**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_minute_probes.py -v`
Expected: PASS
Run: `cd backend && uv run --extra dev python -c "from app.quant.data_verify.probes.tencent_m1 import fetch_tencent_m1 as f; from datetime import date; print(f('688001.SH', days={date(2026,11,6)}).tail(1))"`
Expected: 打印 15:00 bar，`volume` 与日线量×100 同量级（不再 ×100 两次）

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/probes/tencent_m1.py \
  backend/app/quant/data_verify/probes/sina_m1.py \
  backend/app/quant/data_verify/probes/eltdx_m1.py \
  backend/tests/quant/test_data_verify_minute_probes.py
git commit -m "feat(data-verify): 分钟外部源探针（腾讯/新浪/eltdx 可选）"
```

---

### Task 10: 报告 / 状态 / API

**Files:**
- Create: `backend/app/quant/data_verify/report.py`
- Modify: `backend/app/services/pipeline_jobs.py`（不改代码，仅复用 `job_store`）
- Modify: `backend/app/api/settings.py`（新增两个端点）
- Test: `backend/tests/quant/test_data_verify_api.py`

**Interfaces:**
- Consumes: Task 5 `run_verify`；`app.services.pipeline_jobs.job_store`。
- Produces:
  - `write_status(report: dict, data_root: Path) -> Path`（`data/data_verify_status.json`）
  - `write_quarantine(entries: list[dict], data_root: Path) -> Path`
    （`data/data_verify_quarantine.json`；合并去重，键 `table/day/symbol/reason`）
  - `GET /api/settings/data-verify/status` → `{last_run, stage, findings, repaired, sources}`
  - `POST /api/settings/data-verify/run` → `{job_id}`（`job_store.create(long_running=True)`，
    后台线程跑 `run_verify(stage, apply=True)`，进度写 `job_store.progress`）

- [ ] **Step 1: 写失败测试**

```python
import json
from fastapi.testclient import TestClient
from app.main import app
from app.quant.data_verify.report import write_quarantine


def test_quarantine_merges_and_dedupes(tmp_path):
    entry = {"table": "kline_minute", "day": "2026-09-01",
             "symbol": "600004.SH", "reason": "conflict"}
    write_quarantine([entry], tmp_path)
    write_quarantine([entry, {**entry, "symbol": "600005.SH"}], tmp_path)
    data = json.loads((tmp_path / "data_verify_quarantine.json").read_text())
    assert len(data) == 2


def test_status_endpoint_shape():
    with TestClient(app) as client:
        r = client.get("/api/settings/data-verify/status")
    assert r.status_code in (200, 401)      # 未登录时 401 也算通过鉴权约定
```

> 若鉴权中间件导致 401，另写一条带登录夹具的用例（参考
> `tests/test_data_integrity.py` 的鉴权写法），断言 200 + 四个键。

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_api.py -v`
Expected: FAIL（404）

- [ ] **Step 3: 实现 report + 端点**

`write_status`：原子写 JSON（`tmp` → `replace`），键固定
`{"last_run_at","stage","findings","repaired","quarantined","sources"}`。
端点沿用 `settings.py` 现有鉴权依赖；`POST` 用 `job_store` 单飞。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_api.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/report.py backend/app/api/settings.py \
  backend/tests/quant/test_data_verify_api.py
git commit -m "feat(data-verify): 状态报告与 API"
```

---

### Task 11: 每日增量调度 + 验收脚本

**Files:**
- Modify: `backend/app/jobs/daily_pipeline.py:1074`（`start_scheduler` 内注册新 job）
- Create: `backend/scripts/verify_acceptance.py`
- Test: `backend/tests/quant/test_data_verify_schedule.py`

**Interfaces:**
- Consumes: Task 5 `run_verify`、Task 8 `recompute_enriched`、Task 10 `write_status`。
- Produces:
  - `run_daily_incremental(days: int = 7) -> dict`（最近 N 个交易日，apply 规则见 §6.4）
  - APScheduler job `data_verify_daily`，在盘后管道时间 +60 分钟后触发，工作日；
    若 `job_store` 有活跃任务则跳过本轮。

- [ ] **Step 1: 写失败测试**

```python
from app.jobs.daily_pipeline import build_data_verify_trigger


def test_trigger_is_after_pipeline():
    trig = build_data_verify_trigger({"hour": 15, "minute": 30})
    assert trig.hour == 16 and trig.minute == 30
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_schedule.py -v`
Expected: FAIL（`build_data_verify_trigger` 不存在）

- [ ] **Step 3: 实现调度与验收脚本**

`build_data_verify_trigger(pipeline_sched)`：`CronTrigger(day_of_week="mon-fri",
hour=..., minute=..., timezone="Asia/Shanghai")`，时间 = 管道时间 + 60 分钟（跨日则 +1 天）。
`run_daily_incremental`：取最近 7 个交易日 → 日线族先走
`fetch_daily_package` → `compare_daily_external` → `decide` → `replace_symbols`（只对
`repair_local` 决策），再 `run_verify("all", apply=True, days=...)` → 对实际修过的日线日期调
`recompute_enriched` → `write_status`。
`scripts/verify_acceptance.py`：
1. 打印科创板 ×100 检出/修复数（应 ≥600/日）；
2. 对 2023-04-19..2023-12-20 抽样跑 Task 12 的 qfq 核验，与
   `daily_basis_repair.detect` 的结论对比（两者都判定为异常才算 PASS）；
3. 对 2016-01-04、2018-06-01 断言收盘差异全部为 `soft`。

- [ ] **Step 4: 运行确认通过 + 验收**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_schedule.py -v`
Expected: PASS
Run: `cd backend && uv run --extra dev python scripts/verify_acceptance.py --dry-run`
Expected: 三项全部 `PASS`（第 2 项允许 `SKIP`，如果 baostock 超时）

- [ ] **Step 5: Commit**

```bash
git add backend/app/jobs/daily_pipeline.py backend/scripts/verify_acceptance.py \
  backend/tests/quant/test_data_verify_schedule.py
git commit -m "feat(data-verify): 每日增量调度与验收脚本"
```

---

### Task 12: 复权因子/qfq 口径核验

**Files:**
- Create: `backend/app/quant/data_verify/checks_factors.py`
- Test: `backend/tests/quant/test_data_verify_factors.py`

**Interfaces:**
- Consumes: Task 2 `Finding`；`data/adj_factor*` 与 `kline_daily` 原始价。
- Produces:
  - `apply_local_qfq(raw: pl.DataFrame, factors: pl.DataFrame) -> pl.DataFrame`
    （`raw` 为不复权日线，`factors` 含 `symbol,date,ex_factor`；输出加 `qfq_close`）
  - `compare_qfq(local: pl.DataFrame, external: pl.DataFrame, *, symbol: str,
    rel_tol: float = 0.005) -> list[Finding]`
  - 错误码：`QFQ_STEP`（比例序列出现台阶 → 重复/漏复权）、`QFQ_OFFSET`（整体偏移）。

- [ ] **Step 1: 写失败测试**

```python
from datetime import date
import polars as pl
from app.quant.data_verify.checks_factors import apply_local_qfq, compare_qfq

DATES = [date(2023, 4, 19), date(2023, 6, 1), date(2023, 12, 20)]


def _raw():
    return pl.DataFrame(
        [("600000.SH", d, 10.0) for d in DATES],
        schema={"symbol": pl.String, "date": pl.Date, "close": pl.Float64}, orient="row")


def _factors(factor):
    return pl.DataFrame(
        [("600000.SH", d, factor) for d in DATES],
        schema={"symbol": pl.String, "date": pl.Date, "ex_factor": pl.Float64}, orient="row")


def _external(close_base=7.5):
    return pl.DataFrame(
        [("600000.SH", d, close_base) for d in DATES],
        schema={"symbol": pl.String, "date": pl.Date, "qfq_close": pl.Float64}, orient="row")


def test_double_applied_factor_produces_step():
    local = apply_local_qfq(_raw(), _factors(0.5625))    # 0.75 被用了两次
    findings = compare_qfq(local, _external(7.5), symbol="600000.SH")
    assert [f.code for f in findings] == ["QFQ_OFFSET"]
    local2 = apply_local_qfq(_raw(), _factors(0.5625)).with_columns(
        pl.when(pl.col("date") >= date(2023, 6, 1))
        .then(pl.col("qfq_close") * 0.75).otherwise(pl.col("qfq_close")).alias("qfq_close"))
    findings2 = compare_qfq(local2, _external(7.5), symbol="600000.SH")
    assert "QFQ_STEP" in [f.code for f in findings2]


def test_consistent_factor_has_no_finding():
    local = apply_local_qfq(_raw(), _factors(1.0))
    assert compare_qfq(local, _external(10.0), symbol="600000.SH") == []
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_factors.py -v`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现**

`apply_local_qfq`：`qfq_close = close × ex_factor`（ex_factor 口径以
`DataSources.apply_qfq_daily` 为准，实现前先读它确认乘除方向）。
`compare_qfq`：按日期 join 后算 `ratio = local_qfq / external_qfq`；
`ratio` 方差超阈值 → `QFQ_STEP`；`abs(median(ratio)-1) > rel_tol` → `QFQ_OFFSET`。

- [ ] **Step 4: 运行确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_data_verify_factors.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/data_verify/checks_factors.py \
  backend/tests/quant/test_data_verify_factors.py
git commit -m "feat(data-verify): qfq 口径核验"
```

---

## Execution Notes

- **先跑 M1（Task 1-5）**：这部分不依赖任何外部源，能立刻修掉科创板 ×100。
  跑完先执行一次全量历史修复：
  `cd backend && uv run --extra dev python -m app.quant.data_verify --stage internal --apply`
  预计扫描 2633 个分钟分区（28GB），约 10-30 分钟；建议非交易时段跑。
- Task 6-9 涉及外部源，先把 probe 的离线 fixture 做扎实，再开联网冒烟。
- Task 10-11 接线到主后端，注意 `settings.py` 的鉴权依赖与 `job_store` 单飞语义。
- 全程不要动 `kline_daily_enriched` 的写入逻辑，只通过 `prune + 重跑` 更新。
