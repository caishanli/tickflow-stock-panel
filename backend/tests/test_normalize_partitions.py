# ruff: noqa: RUF003
"""Tests for data/ partition normalization script (scripts/normalize_partitions.py)."""
from datetime import date, datetime
from pathlib import Path

import polars as pl

from scripts.normalize_partitions import _normalize_partition_files, normalize_partitions


def _write_df(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def test_normalize_no_date_partition_dedup_by_symbol(tmp_path):
    # kline_etf_daily: 文件内无 date/datetime 列，日期在 hive 分区路径里，去重退化为 symbol
    pdir = tmp_path / "kline_etf_daily" / "date=2026-07-31"
    _write_df(pdir / "data_0.parquet", [
        {"symbol": "510300.SH", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 100.0, "amount": 105.0},
        {"symbol": "510500.SH", "open": 2.0, "high": 2.2, "low": 1.9, "close": 2.1, "volume": 200.0, "amount": 420.0},
    ])
    _write_df(pdir / "data_1.parquet", [
        {"symbol": "510300.SH", "open": 1.2, "high": 1.3, "low": 1.1, "close": 1.25, "volume": 150.0, "amount": 187.5},
    ])

    renamed = _normalize_partition_files(pdir)

    assert renamed == 2
    result = pl.read_parquet(pdir / "part.parquet")
    assert result["symbol"].to_list() == ["510300.SH", "510500.SH"]
    # keep="last"：data_1 的 510300 覆盖 data_0
    assert result.filter(pl.col("symbol") == "510300.SH")["close"].item() == 1.25
    assert result.filter(pl.col("symbol") == "510500.SH")["close"].item() == 2.1
    assert list(pdir.glob("data_*.parquet")) == []


def test_normalize_multifile_concat_dedup_keep_last(tmp_path):
    pdir = tmp_path / "kline_daily" / "date=2026-07-31"
    _write_df(pdir / "data_0.parquet", [
        {"symbol": "000001.SZ", "date": date(2026, 7, 31), "open": 10.0, "close": 10.1, "volume": 100.0, "amount": 1010.0},
        {"symbol": "600000.SH", "date": date(2026, 7, 31), "open": 20.0, "close": 20.1, "volume": 200.0, "amount": 4020.0},
    ])
    _write_df(pdir / "data_1.parquet", [
        {"symbol": "000001.SZ", "date": date(2026, 7, 31), "open": 10.5, "close": 10.6, "volume": 110.0, "amount": 1166.0},
        {"symbol": "601888.SH", "date": date(2026, 7, 31), "open": 30.0, "close": 30.5, "volume": 300.0, "amount": 9150.0},
    ])

    renamed = _normalize_partition_files(pdir)

    assert renamed == 2
    result = pl.read_parquet(pdir / "part.parquet")
    assert result.height == 3
    assert result["symbol"].to_list() == ["000001.SZ", "600000.SH", "601888.SH"]
    # 重叠 key 取最后一个文件（data_1）的值
    assert result.filter(pl.col("symbol") == "000001.SZ")["close"].item() == 10.6
    assert result.filter(pl.col("symbol") == "600000.SH")["close"].item() == 20.1


def test_normalize_atomic_part_exists_before_unlink(tmp_path, monkeypatch):
    # 写序必须是：tmp.rename(part.parquet) 在前，unlink 源文件在后（任何失败重跑可恢复）
    pdir = tmp_path / "kline_daily" / "date=2026-07-31"
    _write_df(pdir / "data_0.parquet", [
        {"symbol": "000001.SZ", "date": date(2026, 7, 31), "open": 10.0, "close": 10.1, "volume": 100.0, "amount": 1010.0},
    ])
    real_unlink = Path.unlink

    def _assert_unlink_after_part(instance, *args, **kwargs):
        assert (pdir / "part.parquet").exists(), "part.parquet must be written before source unlink"
        return real_unlink(instance, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", _assert_unlink_after_part)

    _normalize_partition_files(pdir)

    assert (pdir / "part.parquet").exists()
    assert list(pdir.glob("data_*.parquet")) == []


def test_normalize_rerun_recovers_interrupted_run(tmp_path):
    # 模拟上次运行 rename 成功但 unlink 中途失败：part.parquet 与残留源文件并存，
    # 重跑必须产出完整数据并清理源文件
    pdir = tmp_path / "kline_minute" / "date=2026-07-31"
    _write_df(pdir / "data_0.parquet", [
        {"symbol": "000001.SZ", "datetime": datetime(2026, 7, 31, 9, 31), "open": 10.0, "close": 10.1, "volume": 100.0, "amount": 1010.0},
        {"symbol": "000001.SZ", "datetime": datetime(2026, 7, 31, 9, 32), "open": 10.1, "close": 10.2, "volume": 110.0, "amount": 1122.0},
    ])
    _normalize_partition_files(pdir)
    assert list(pdir.glob("data_*.parquet")) == []

    _write_df(pdir / "data_1.parquet", [
        {"symbol": "000001.SZ", "datetime": datetime(2026, 7, 31, 9, 33), "open": 10.2, "close": 10.3, "volume": 120.0, "amount": 1236.0},
    ])

    renamed = _normalize_partition_files(pdir)

    assert renamed == 2
    result = pl.read_parquet(pdir / "part.parquet")
    assert result.height == 3
    assert result.filter(pl.col("datetime") == datetime(2026, 7, 31, 9, 33))["close"].item() == 10.3
    assert list(pdir.glob("data_*.parquet")) == []


def test_normalize_partitions_returns_skipped_count(tmp_path):
    # 损坏分区应记入 skipped，不中断整体迁移
    corrupt = tmp_path / "kline_daily" / "date=2026-07-30"
    corrupt.mkdir(parents=True)
    (corrupt / "data_0.parquet").write_bytes(b"this is not a parquet file")

    good = tmp_path / "kline_daily" / "date=2026-07-31"
    _write_df(good / "data_0.parquet", [
        {"symbol": "000001.SZ", "date": date(2026, 7, 31), "open": 10.0, "close": 10.1, "volume": 100.0, "amount": 1010.0},
    ])

    stats = normalize_partitions(str(tmp_path))

    assert stats["skipped"] == 1
    assert stats["renamed"] == 1
    assert stats["deleted"] == 0


def test_normalize_ignores_non_target_subdirs(tmp_path):
    # 只处理 kline_daily/kline_etf_daily/kline_minute/kline_etf_minute 四个子目录
    other = tmp_path / "kline_daily_enriched" / "date=2026-07-31"
    _write_df(other / "data_0.parquet", [
        {"symbol": "000001.SZ", "date": date(2026, 7, 31), "open": 10.0, "close": 10.1, "volume": 100.0, "amount": 1010.0},
    ])

    stats = normalize_partitions(str(tmp_path))

    assert stats["renamed"] == 0
    assert stats["skipped"] == 0
    assert (other / "data_0.parquet").exists()
