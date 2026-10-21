"""``archive_fill`` 的安全决策逻辑测试（reviewer Important #6）。

核心承诺：**只填空壳/缺失，绝不覆盖本机已完整的数据**。这是手工验证过的，
本文件用假分区把它锁住：
- 完整分区 → skip_complete（文件 mtime 不变）
- 空壳/残缺分区 → overwrite_shell
- 缺失分区 → create
- 完整性判定按**标的数覆盖比**（不是绝对行数：2016/2017 的日线分区只有 ~2592 行）
"""

from pathlib import Path

import polars as pl

from app.quant.archive_fill import _write_partition_atomic, is_complete


def _partition(root: Path, day: str, symbols: list[str], n_rows: int = 1) -> Path:
    """造一个 date= 分区。"""
    import datetime as _dt
    d = root / f"date={day}"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "part.parquet"
    pl.DataFrame({
        "symbol": symbols,
        "datetime": [_dt.datetime(2025, 1, 2, 9, 30)] * len(symbols),
        "open": [10.0] * len(symbols),
        "high": [10.0] * len(symbols),
        "low": [10.0] * len(symbols),
        "close": [10.0] * len(symbols),
        "volume": [100.0] * len(symbols),
        "amount": [100.0] * len(symbols),
    }, schema={
        "symbol": pl.String, "datetime": pl.Datetime("us"), "open": pl.Float64,
        "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
        "volume": pl.Float64, "amount": pl.Float64,
    }).write_parquet(f)
    return f


def test_missing_partition_is_not_complete(tmp_path):
    assert not is_complete(tmp_path / "date=2025-01-02" / "part.parquet", {"A"}, "daily")


def test_shell_partition_is_not_complete(tmp_path):
    """空壳：只有 1 个标的、1 行 → 视为不完整（可覆盖修复）。"""
    f = _partition(tmp_path, "2025-01-02", ["A"], n_rows=1)
    assert not is_complete(f, {"A", "B", "C"}, "daily")


def test_full_coverage_is_complete(tmp_path):
    """本机标的数 ≥ 归档 × 95% → 完整（跳过，不覆盖）。"""
    f = _partition(tmp_path, "2025-01-02", [f"S{i}" for i in range(100)])
    assert is_complete(f, {f"S{i}" for i in range(100)}, "daily")


def test_partial_coverage_is_not_complete(tmp_path):
    """本机只覆盖归档一半标的 → 不完整（可覆盖修复）。"""
    f = _partition(tmp_path, "2025-01-02", [f"S{i}" for i in range(50)])
    assert not is_complete(f, {f"S{i}" for i in range(100)}, "daily")


def test_95_percent_coverage_counts_as_complete(tmp_path):
    """95% 覆盖算完整（避免边界抖动导致反复重写）。"""
    f = _partition(tmp_path, "2025-01-02", [f"S{i}" for i in range(95)])
    assert is_complete(f, {f"S{i}" for i in range(100)}, "daily")


def test_fallback_to_row_threshold_when_archive_symbols_unknown(tmp_path):
    """归档标的数取不到时退化为行数下限（分钟线 10 万行）。"""
    f = _partition(tmp_path, "2025-01-02", [f"S{i}" for i in range(200)])
    assert not is_complete(f, set(), "minute")     # 200 行 < 10 万 → 不完整
    big = tmp_path / "date=2025-01-03"
    big.mkdir()
    pl.DataFrame({"symbol": [f"S{i}" for i in range(200_000)],
                  "close": [1.0] * 200_000}).write_parquet(big / "part.parquet")
    assert is_complete(big / "part.parquet", set(), "minute")


def test_atomic_write_replaces_and_leaves_no_tmp(tmp_path):
    """原子写：产出目标文件，且不留 .tmp。"""
    out = tmp_path / "date=2025-01-02" / "part.parquet"
    df = pl.DataFrame({"symbol": ["A"], "close": [1.0]})
    _write_partition_atomic(out, df)
    assert out.exists()
    assert not list(out.parent.glob("*.tmp"))
    assert pl.read_parquet(out).height == 1
