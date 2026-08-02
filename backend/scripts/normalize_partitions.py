#!/usr/bin/env python3
# ruff: noqa: RUF002, RUF003
"""统一 Parquet 分区存储：分区内文件命名归一为 part.parquet。

- 分区内文件命名统一为 part.parquet（现有 data_*.parquet 合并重命名）
- 注意：volume 单位不做盘上换算，保留原始数据（A股日线为手，读取层再 ×100）
- 删除每股票缓存与 stock.duckdb 遗留
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

DATA_ROOT = Path("/home/caisl/tickflow-stock-panel/data")


def _normalize_partition_files(partition_dir: Path) -> int:
    """把分区目录内多个 data_*.parquet 合并为 part.parquet，返回重命名数。"""
    files = sorted(partition_dir.glob("*.parquet"))
    if not files:
        return 0
    if len(files) == 1 and files[0].name == "part.parquet":
        return 0
    frames = [pl.read_parquet(f) for f in files]
    merged = pl.concat(frames)
    # 分区内按业务主键去重：日线 symbol+date，分钟 symbol+datetime；
    # 无 date/datetime 列（如 kline_etf_daily，日期在分区路径 date=* 内）退化为主键 symbol
    if "date" in merged.columns:
        key_cols = ["symbol", "date"]
    elif "datetime" in merged.columns:
        key_cols = ["symbol", "datetime"]
    else:
        key_cols = ["symbol"]
    merged = merged.unique(subset=key_cols, keep="last").sort(key_cols)
    tmp = partition_dir / "part.parquet.tmp"
    merged.write_parquet(tmp)
    # 先原子替换 part.parquet，再删源文件：任何一步失败，重跑都能从
    # 保留的源文件（或完整 part.parquet）恢复，避免先删后写的数据丢失
    tmp.rename(partition_dir / "part.parquet")
    for f in files:
        # part.parquet 已被 rename 原子替换为新内容，不能连同源文件一起删
        if f.name != "part.parquet":
            f.unlink()
    return len(files)


def normalize_partitions(partition_root: str = str(DATA_ROOT)) -> dict:
    root = Path(partition_root)
    stats = {"renamed": 0, "deleted": 0, "skipped": 0}
    for subdir in ("kline_daily", "kline_etf_daily", "kline_minute", "kline_etf_minute"):
        base = root / subdir
        if not base.is_dir():
            continue
        for pdir in base.glob("date=*"):
            try:
                stats["renamed"] += _normalize_partition_files(pdir)
            except Exception as exc:
                # 记录并跳过异常分区，不让整体迁移中断
                print(f"[skip] {pdir}: {exc}", file=sys.stderr)
                stats["skipped"] += 1
    # 删除每股票缓存
    for path in (root / "quant_kline" / "daily").glob("*.parquet"):
        path.unlink()
        stats["deleted"] += 1
    for path in (root / "quant_kline" / "minute").glob("real_*.parquet"):
        path.unlink()
        stats["deleted"] += 1
    return stats


if __name__ == "__main__":
    print(normalize_partitions())
    sys.exit(0)
