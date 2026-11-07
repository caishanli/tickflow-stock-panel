"""确定性修复：备份 / 原子写 / 分钟量额 ×100 还原。

修复目标只有一个 —— 科创板等标的的分钟 ``volume`` / ``amount`` 被按「股」
写成了「手」的 100 倍（见单位契约）。还原就是两列同除一个固定因子，不需要
任何跨源数据，因此结果可复现、可回滚：

1. **备份**：写盘前把原分区复制到 ``backup_root/run_id/<table>/date=.../``；
   同名备份已存在则**跳过**，保证重复执行时留着最早那份原始数据。
2. **原子写**：`.tmp` + :func:`os.replace`，并持有
   :func:`app.raw_partition_lock.daily_partition_lock`——与收盘回源等写入方
   互斥，进程中断也不会留下半截 parquet。
3. **只动两列**：``volume`` / ``amount``，列名、顺序、dtype 与其余列一律不变；
   dtype 为整数时缩放后取整再转回，避免 Polars 除法把整型悄悄升成 Float64。

幂等性**不在本模块**：除以 100 不是幂等操作，是否该调用由 runner 依据 L1
结论（``MINUTE_VOLUME_X100``）决定。
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import polars as pl

from app.raw_partition_lock import daily_partition_lock

#: 需要缩放的列。其余列一律不碰（含 OHLC 与 symbol/datetime）。
SCALED_COLUMNS: tuple[str, ...] = ("volume", "amount")


def _backup_path(part: Path, backup_root: Path, run_id: str, table: str) -> Path:
    """备份落点：``backup_root/run_id/<table>/date=.../part.parquet``。"""
    return Path(backup_root) / run_id / table / part.parent.name / part.name


def _copy_backup(part: Path, target: Path) -> Path:
    """把分区复制到 ``target``；已存在则保留旧备份（可能是更原始的版本）。

    复制本身也走 ``.tmp`` + :func:`os.replace`，中断不会留下半截备份。
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        return target
    tmp = target.with_suffix(target.suffix + ".tmp")
    try:
        shutil.copy2(part, tmp)
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()
    return target


def backup_partition(part: Path, backup_root: Path, run_id: str) -> Path:
    """备份单个分区，返回备份路径。

    表名与日期分区名都从 ``part`` 的路径推断（``<table>/date=.../part.parquet``），
    调用方不用另传，避免备份目录与真实分区对不上号。
    """
    part = Path(part)
    return _copy_backup(part, _backup_path(part, backup_root, run_id, part.parent.parent.name))


def _write_partition_unlocked(df: pl.DataFrame, part: Path) -> None:
    """原子写（调用方**必须**已持有 ``daily_partition_lock``）。"""
    part.parent.mkdir(parents=True, exist_ok=True)
    tmp = part.with_suffix(".tmp")
    try:
        df.write_parquet(tmp)
        os.replace(tmp, part)
    finally:
        if tmp.exists():
            tmp.unlink()


def write_partition_atomic(df: pl.DataFrame, part: Path) -> None:
    """持锁原子写分区（``.tmp`` → :func:`os.replace`）。"""
    part = Path(part)
    with daily_partition_lock(part):
        _write_partition_unlocked(df, part)


def _nonzero_mask() -> pl.Expr:
    """``volume`` 或 ``amount`` 非 0 的行（含 null 按 0 处理）。"""
    return pl.any_horizontal([pl.col(col).fill_null(0) != 0 for col in SCALED_COLUMNS])


def _require_scaled_columns(part: Path, df: pl.DataFrame) -> None:
    """分区必须含 ``volume`` / ``amount``，否则说明表/路径选错了。"""
    missing = [col for col in SCALED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"{part}: missing scaled columns {missing}")


def _scaled_expr(column: str, dtype: pl.DataType, factor: float) -> pl.Expr:
    """``column / factor`` 并保持原 dtype（整型取整，不引入浮点列）。"""
    expr = pl.col(column) / factor
    if dtype.is_integer():
        return expr.round().cast(dtype)
    return expr.cast(dtype)


def scale_minute_volume_amount(
    part: Path,
    *,
    table: str,
    factor: float,
    backup_root: Path,
    run_id: str,
    dry_run: bool,
) -> dict:
    """把分区的 ``volume`` / ``amount`` 同除 ``factor``（``factor=100`` 即还原 ×100）。

    返回 ``{"changed_rows": int, "backup": str | None}``：``changed_rows`` 是
    ``volume``/``amount`` 非 0 的行数，``dry_run=True`` 时不备份不写盘、
    ``backup`` 为 ``None``；全 0 量/额的分区除下来不变，同样不备份不写盘。
    读盘、备份、写盘在同一把锁内完成，避免备份与写入之间被别的写者改掉分区。
    """
    if factor <= 0:
        raise ValueError(f"factor must be > 0, got {factor!r}")
    part = Path(part)
    if dry_run:
        staged = pl.read_parquet(part)
        _require_scaled_columns(part, staged)
        return {"changed_rows": staged.filter(_nonzero_mask()).height, "backup": None}

    with daily_partition_lock(part):
        df = pl.read_parquet(part)
        _require_scaled_columns(part, df)
        changed_rows = df.filter(_nonzero_mask()).height
        if changed_rows == 0:               # 全 0 量/额：除下来还是 0，不写不备份
            return {"changed_rows": 0, "backup": None}
        scaled = df.with_columns([
            _scaled_expr(col, df.schema[col], factor) for col in SCALED_COLUMNS
        ])
        target = _copy_backup(part, _backup_path(part, backup_root, run_id, table))
        _write_partition_unlocked(scaled, part)
    return {"changed_rows": changed_rows, "backup": str(target)}
