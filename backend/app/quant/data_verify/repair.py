"""确定性修复：备份 / 原子写 / 分钟量额 ×100 还原 / 外部整行替换。

两类修复共用同一套「备份 → 持锁 → 原子写」原语，结果可复现、可回滚：

1. **备份**：写盘前把原分区复制到 ``backup_root/run_id/<table>/date=.../``；
   同名备份已存在则**跳过**，保证重复执行时留着最早那份原始数据。
2. **原子写**：`.tmp` + :func:`os.replace`，并持有
   :func:`app.raw_partition_lock.daily_partition_lock`——与收盘回源等写入方
   互斥，进程中断也不会留下半截 parquet。
3. **分钟量额还原**（:func:`scale_minute_volume_amount`）：只动 ``volume`` /
   ``amount``，列名、顺序、dtype 与其余列一律不变；dtype 为整数时缩放后取整
   再转回，避免 Polars 除法把整型悄悄升成 Float64。
4. **外部整行替换**（:func:`replace_symbols`）：按 ``symbol`` 用外部源的行覆盖
   本地行，列结构以本地分区为准（外部多带的列丢弃），未涉及标的一行不动。

幂等性**不在本模块**：除以 100 不是幂等操作，是否该调用由 runner 依据 L1
结论（``MINUTE_VOLUME_X100``）决定；整行替换的幂等性由外部源数据本身保证
（同一天同一 symbol 只有一行）。
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

import polars as pl

from app.raw_partition_lock import daily_partition_lock

logger = logging.getLogger(__name__)

#: 需要缩放的列。其余列一律不碰（含 OHLC 与 symbol/datetime）。
SCALED_COLUMNS: tuple[str, ...] = ("volume", "amount")

PARTITION_BLAST_RADIUS = "PARTITION_BLAST_RADIUS"
PARTITION_BLAST_RADIUS_LIMIT = 0.05


def blast_radius_block(
    affected_rows: int, partition_rows: int, *, force: bool = False
) -> dict | None:
    """超过单分区 5% 的自动写入降级为报告；``force=True`` 显式放行。"""
    ratio = affected_rows / max(partition_rows, 1)
    if force or ratio <= PARTITION_BLAST_RADIUS_LIMIT:
        return None
    logger.warning(
        "%s: affected_rows=%d partition_rows=%d ratio=%.4f，跳过写入",
        PARTITION_BLAST_RADIUS, affected_rows, partition_rows, ratio,
    )
    return {
        "changed_rows": 0,
        "backup": None,
        "affected_rows": affected_rows,
        "partition_rows": partition_rows,
        "blast_radius": ratio,
        "blocked": PARTITION_BLAST_RADIUS,
    }


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
    force: bool = False,
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
        blocked = blast_radius_block(changed_rows, df.height, force=force)
        if blocked is not None:
            return blocked
        scaled = df.with_columns([
            _scaled_expr(col, df.schema[col], factor) for col in SCALED_COLUMNS
        ])
        target = _copy_backup(part, _backup_path(part, backup_root, run_id, table))
        _write_partition_unlocked(scaled, part)
    return {"changed_rows": changed_rows, "backup": str(target)}


#: 整行替换时给外部帧列加的前缀，避免与分区原有列重名。
_REPLACEMENT_PREFIX = "__dv_replace__"


def _validate_replacement(part: Path, replacement: pl.DataFrame) -> None:
    """校验外部帧：必须有 ``symbol``、无 null、同一 symbol 只许一行。

    外部帧可以带分区没有的列（如 ETF 日线历史分区无 ``date``），这类列
    在覆盖时直接丢弃——**分区列结构由本地决定**，不能因为外部源带了什么
    就加一列。
    """
    if "symbol" not in replacement.columns:
        raise ValueError(f"{part}: replacement missing 'symbol' column")
    if replacement.height == 0:
        return
    if replacement.filter(pl.col("symbol").is_null()).height:
        raise ValueError(f"{part}: replacement contains null symbol")
    if replacement["symbol"].n_unique() != replacement.height:
        dupes = (
            replacement.group_by("symbol")
            .len()
            .filter(pl.col("len") > 1)["symbol"]
            .sort()
            .to_list()
        )
        raise ValueError(f"{part}: replacement has duplicate symbols {dupes}")


def _replace_rows(
    part: Path,
    df: pl.DataFrame,
    replacement: pl.DataFrame,
) -> tuple[pl.DataFrame, int]:
    """把 ``replacement`` 中出现的 symbol 的整行按列覆盖，返回 (新帧, 重写行数)。

    ``left`` join + ``coalesce``：外部帧给了值的列覆盖旧值，没给值（null /
    整列缺失）的列保留旧值。行数、行序、其余 symbol 与列结构（名字/顺序/
    dtype）全部保持原样。
    """
    if "symbol" not in df.columns:
        raise ValueError(f"{part}: partition missing 'symbol' column")
    _validate_replacement(part, replacement)
    if replacement.height == 0:
        return df, 0
    shared = [
        col for col in df.columns
        if col != "symbol" and col in replacement.columns
    ]
    if not shared:                       # 只给了 symbol，没给任何可覆盖的列
        return df, 0
    clash = [col for col in df.columns if col.startswith(_REPLACEMENT_PREFIX)]
    if clash:
        raise ValueError(f"{part}: columns {clash} collide with replacement prefix")

    new_names = {col: f"{_REPLACEMENT_PREFIX}{col}" for col in shared}
    marker = f"{_REPLACEMENT_PREFIX}hit"
    staged = (
        replacement.select(["symbol", *shared])
        .with_columns(pl.lit(True).alias(marker))
        .rename(new_names)
    )
    joined = df.join(staged, on="symbol", how="left")
    exprs = [
        pl.coalesce([pl.col(new_names[col]), pl.col(col)]).alias(col)
        if col in new_names
        else pl.col(col)
        for col in df.columns
    ]
    replaced = joined.select(exprs).select(
        [pl.col(col).cast(dtype) for col, dtype in df.schema.items()]
    )
    touched = int(joined[marker].is_not_null().sum())
    return replaced, touched


def replace_symbols(
    part: Path,
    replacement: pl.DataFrame,
    *,
    backup_root: Path,
    run_id: str,
    dry_run: bool,
    force: bool = False,
) -> dict:
    """按 ``symbol`` 整行替换分区内的行（外部覆盖修复）。

    - 只替换 ``replacement.symbol`` 里的标的；其余标的、行序、列结构
      （含 dtype）原样保留，``replacement`` 多出来的列一律丢弃；
    - ``replacement`` 里本地分区没有的 symbol 不会新增行（覆盖度问题由核验
      报告呈现，修复只改已有行）；
    - ``replacement`` 某列为 null / 整列缺失时保留旧值（``coalesce`` 语义），
      因此只带 ``close``/``volume`` 的外部帧也能用；
    - 返回 ``{"changed_rows": int, "backup": str | None}``：``changed_rows`` 是
      **被重写的行数**（被替换 symbol 在分区里的行数，无论值是否真的变了），
      ``dry_run=True`` 时不备份不写盘、``backup`` 为 ``None``；
    - 备份在锁内、写盘前完成（重复执行保留最早那份原始备份），写入走持锁
      原子替换。
    """
    part = Path(part)
    if dry_run:
        df = pl.read_parquet(part)
        _, changed_rows = _replace_rows(part, df, replacement)
        return {"changed_rows": changed_rows, "backup": None}

    with daily_partition_lock(part):
        df = pl.read_parquet(part)
        replaced, changed_rows = _replace_rows(part, df, replacement)
        if changed_rows == 0:            # 没有匹配标的：不备份不写盘
            return {"changed_rows": 0, "backup": None}
        blocked = blast_radius_block(changed_rows, df.height, force=force)
        if blocked is not None:
            return blocked
        target = backup_partition(part, backup_root, run_id)
        _write_partition_unlocked(replaced, part)
    return {"changed_rows": changed_rows, "backup": str(target)}
