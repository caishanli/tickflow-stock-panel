"""核验编排：枚举分区 → L0/L1 → 定向修复 → 断点续跑。

这是 M1 的可交付入口。一次 ``run_verify`` 干的事：

1. **枚举**：扫 ``<data_root>/kline_minute`` 与 ``kline_etf_minute`` 的
   ``date=YYYY-MM-DD`` 分区，取并集排序。只认这两个目录的**直接子目录**，
   所以备份目录（``data_verify_backup``，与它们同级）永远不会被当成待检分区。
2. **逐日编排**：一个分区只读一次，先 L0 结构（:mod:`.checks_structure`），
   再 L1 内部一致性（:mod:`.checks_internal`，分钟聚合 vs 同日日线）。
   日线分区缺失时跳过 L1——没有对照就没有结论，不臆造 finding。L0 用默认
   241 bar 期望、不传停牌名单（缺 bar / 全 0 量只会是 ``soft``，不驱动修复）。
3. **修复**：``apply=True`` 时只处理 ``MINUTE_VOLUME_X100`` 这一种 ``hard``
   结论（``factor=100``，写前备份到 ``data_verify_backup/<run_id>/``）。修复
   **按 symbol 圈定范围**：同一个分区里既有被污染的科创板、也有大量正常标的
   （2026-11-06 实测 616 只 ratio≈100 vs 4602 只 ratio≈1），整分区缩放会把
   正常数据打成 1/100；只有分区内**全部** symbol 都命中结论时才整分区调
   :func:`~app.quant.data_verify.repair.scale_minute_volume_amount`。
   ETF 分钟不在修复范围（它的量本来就是股）；且只修已收盘交易日
   （``day < today``，design §6.4）——当日分区可能还在被收盘链路重写。
4. **断点**：每完成一天原子写 ``data_verify_state.json``；
   ``resume=True`` 从状态里的 ``last_day_done`` 之**后**继续。断点**绑定 stage**：
   状态里的 ``stage`` 与本次不一致时忽略断点并 warning（否则先跑
   ``--stage structure`` 会把日子推进到末尾，再 ``--stage internal --resume``
   就会静默跳过这些日子的 L1 核验）。

口径：

- **dry-run 是默认，且零写入**：分区、备份、状态文件一概不落盘。状态文件只在
  ``apply=True`` 时更新——否则一次干跑就会把 ``last_day_done`` 推到最后一天，
  之后 ``--resume --apply`` 会静默跳过这些只干跑过的日子、漏掉真实修复。
- ``findings`` = 整轮产出的 :class:`~app.quant.data_verify.findings.Finding` 条数；
  ``repaired`` = 因 ``MINUTE_VOLUME_X100`` 被缩放的 **symbol 条数**（与 findings
  同量纲，便于看出「查出的问题修了多少条」），实际写盘动作是每分区一次。
- 单个分区不可读（坏 parquet / schema 损坏）记一条 ``PARTITION_UNREADABLE``
  后继续——全量 2600+ 天的扫描不能因为一个坏分区整轮中断（设计验收 §7「0 崩溃」）；
  其余异常照常抛出，已完成的日期已在状态文件里，``--resume`` 可接着跑。
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final

import polars as pl

from app.quant.data_verify.checks_internal import (
    MINUTE_VOLUME_X100,
    compare_minute_to_daily,
)
from app.quant.data_verify.checks_structure import check_structure
from app.quant.data_verify.findings import (
    SEVERITY_HARD,
    Finding,
)
from app.quant.data_verify.repair import (
    SCALED_COLUMNS,
    backup_partition,
    scale_minute_volume_amount,
)
from app.quant.data_verify.units import MINUTE_TABLE_TO_DAILY
from app.raw_partition_lock import daily_partition_lock

logger = logging.getLogger(__name__)

#: 支持的核验层。``all`` = L0 + L1（后续里程碑再扩展 external / factors）。
STAGE_STRUCTURE: Final = "structure"
STAGE_INTERNAL: Final = "internal"
STAGE_ALL: Final = "all"
STAGES: Final[tuple[str, ...]] = (STAGE_STRUCTURE, STAGE_INTERNAL, STAGE_ALL)

#: 参与枚举的分钟表（也是 L1 比对的主体）。顺序固定便于日志可读。
MINUTE_TABLES: Final[tuple[str, ...]] = ("kline_minute", "kline_etf_minute")

#: 唯一允许的自动修复：单位归一后量比 ≈100 的分钟量额（科创板 ×100 事故）。
REPAIR_CODE: Final = MINUTE_VOLUME_X100
REPAIR_FACTOR: Final = 100.0
#: 只修 ``kline_minute``：ETF 分钟按「股」落盘，不存在这个 100 倍问题。
REPAIR_TABLES: Final[tuple[str, ...]] = ("kline_minute",)

#: 分区路径约定：``<table>/date=YYYY-MM-DD/part.parquet``。
PARTITION_FILENAME: Final = "part.parquet"
DAY_PREFIX: Final = "date="

#: 断点状态与备份的落点（与分区表同级，不进任何 ``date=*`` 目录）。
STATE_FILENAME: Final = "data_verify_state.json"
BACKUP_DIRNAME: Final = "data_verify_backup"

#: 分区读取失败的结论码（只在本模块产生，不影响修复决策）。
PARTITION_UNREADABLE: Final = "PARTITION_UNREADABLE"

#: ``progress(stage, pct, msg)``：``pct`` 为 0..100 的整数。
ProgressCallback = Callable[[str, int, str], None]


def _default_data_root() -> Path:
    """默认数据根 = ``mootdx_service.DATA_ROOT``（惰性导入，避免拉起重依赖）。"""
    from app.services.mootdx_service import DATA_ROOT

    return Path(DATA_ROOT)


def _now_iso() -> str:
    """状态文件里的时间戳（秒级，可被 ``datetime.fromisoformat`` 解析）。"""
    return datetime.now().replace(microsecond=0).isoformat()


def _today() -> date:
    """当天日期（抽成函数便于测试注入「今天」）。"""
    return date.today()


def _run_id() -> str:
    """本次运行的 id，用于区分备份批次（微秒级，手动连跑也不会撞）。"""
    return datetime.now().strftime("%Y%m%dT%H%M%S%f")


def _parse_day_dir(name: str) -> date | None:
    """``date=2026-11-06`` → ``date(2026, 11, 6)``；不合约定返回 ``None``。"""
    if not name.startswith(DAY_PREFIX):
        return None
    try:
        return date.fromisoformat(name[len(DAY_PREFIX):])
    except ValueError:
        return None


def _partition_days(data_root: Path) -> list[date]:
    """枚举已有分钟分区的交易日（升序、去重）。"""
    days: set[date] = set()
    for table in MINUTE_TABLES:
        base = Path(data_root) / table
        if not base.is_dir():
            continue
        for pdir in base.glob(f"{DAY_PREFIX}*"):
            day = _parse_day_dir(pdir.name)
            if day is not None and (pdir / PARTITION_FILENAME).is_file():
                days.add(day)
    return sorted(days)


def _as_day(value: date) -> date:
    """把 ``date`` / ``datetime`` / ISO 字符串统一成 ``date``（截掉时间分量）。

    ``datetime`` 是 ``date`` 的子类，``isinstance`` 判断会让带时间戳的值混进
    分区日集合、与 ``date(2026, 11, 6)`` 比对失败（``datetime != date``），
    所以这里用 ``type(...) is date`` 精确判断。
    """
    if type(value) is date:
        return value
    return date.fromisoformat(str(value)[:10])


def _select_days(data_root: Path, days: Sequence[date] | None) -> list[date]:
    """``days=None``（或空）取全部已有分区，否则只取显式给定且存在的日期。"""
    available = _partition_days(data_root)
    if not days:
        return available
    explicit = {_as_day(day) for day in days}
    missing = sorted(explicit - set(available))
    if missing:
        logger.warning("以下日期没有分钟分区，已跳过: %s",
                       ", ".join(day.isoformat() for day in missing))
    return [day for day in available if day in explicit]


def _read_partition(
    part: Path, *, table: str, day: date
) -> tuple[pl.DataFrame | None, Finding | None]:
    """读一个分区；不存在返回 ``(None, None)``，读坏返回 ``(None, Finding)``。"""
    if not part.is_file():
        return None, None
    try:
        return pl.read_parquet(part), None
    except Exception as exc:  # 坏分区记 finding，不能拖垮整轮扫描
        logger.warning("分区不可读: %s (%s)", part, exc)
        return None, Finding(
            table=table,
            day=day,
            symbol=None,
            code=PARTITION_UNREADABLE,
            severity=SEVERITY_HARD,
            detail={"path": str(part), "error": f"{type(exc).__name__}: {exc}"},
        )


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    """原子写状态文件（``.tmp`` + :func:`os.replace`）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _read_state(state_path: Path) -> dict[str, Any] | None:
    """读状态文件；缺失/损坏/不是 JSON 对象一律当作「没有断点」。"""
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return raw if isinstance(raw, dict) else None


def _state_day(state: dict[str, Any] | None) -> date | None:
    """状态里的 ``last_day_done``；缺失/非法一律当作「没有断点」。"""
    value = state.get("last_day_done") if state else None
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _emit(progress: ProgressCallback | None, stage: str, pct: int, msg: str) -> None:
    if progress is not None:
        progress(stage, pct, msg)


def _scaled_expr(column: str, dtype: pl.DataType, factor: float, mask: pl.Expr) -> pl.Expr:
    """命中 ``mask`` 的行 ``column / factor``，其余原样；保持原 dtype。"""
    expr = pl.when(mask).then(pl.col(column) / factor).otherwise(pl.col(column))
    if dtype.is_integer():
        return expr.round().cast(dtype).alias(column)
    return expr.cast(dtype).alias(column)


def _write_partition_unlocked(df: pl.DataFrame, part: Path) -> None:
    """原子写分区（调用方**必须**已持有 ``daily_partition_lock``）。

    ``daily_partition_lock`` 不可重入，锁内不能再调 ``write_partition_atomic``
    （那会二次取锁自死锁，见 AGENTS.md 2026-08-31 事故纪律）。
    """
    part.parent.mkdir(parents=True, exist_ok=True)
    tmp = part.with_name(part.name + ".tmp")
    try:
        df.write_parquet(tmp)
        os.replace(tmp, part)
    finally:
        if tmp.exists():
            tmp.unlink()


def _scale_symbols(
    part: Path,
    symbols: Sequence[str],
    *,
    factor: float,
    backup_root: Path,
    run_id: str,
) -> int:
    """只把分区内 ``symbols`` 的 ``volume`` / ``amount`` 同除 ``factor``。

    读、备份、写在同一把锁内完成；只改这两列，其余列与 dtype 一律不动。
    """
    with daily_partition_lock(part):
        df = pl.read_parquet(part)
        absent = [col for col in SCALED_COLUMNS if col not in df.columns]
        if absent:
            raise ValueError(f"{part}: missing scaled columns {absent}")
        mask = pl.col("symbol").is_in(list(symbols))
        scaled = df.with_columns([
            _scaled_expr(col, df.schema[col], factor, mask) for col in SCALED_COLUMNS
        ])
        backup_partition(part, backup_root, run_id)
        _write_partition_unlocked(scaled, part)
    return len(symbols)


def _repair_x100(
    part: Path,
    *,
    table: str,
    symbols: Sequence[str],
    partition_symbols: Sequence[str],
    backup_root: Path,
    run_id: str,
) -> int:
    """修一个分区里的 ×100 结论，返回被缩放的 symbol 条数。

    分区里**只有**科创板那部分被放大（2026-11-06 实测：616 只 ×100，另外
    4602 只 ratio≈1），所以默认按 symbol 圈定范围——整分区缩放会把正常标的
    打成 1/100。仅当整个分区的 symbol 全在结论里时才走整分区缩放
    （:func:`scale_minute_volume_amount` 的语义）。
    """
    if not symbols:
        return 0
    if set(symbols) >= set(partition_symbols):
        result = scale_minute_volume_amount(
            part,
            table=table,
            factor=REPAIR_FACTOR,
            backup_root=backup_root,
            run_id=run_id,
            dry_run=False,
        )
        return len(symbols) if result["changed_rows"] else 0
    return _scale_symbols(
        part, symbols, factor=REPAIR_FACTOR, backup_root=backup_root, run_id=run_id
    )


def _verify_table(
    data_root: Path,
    table: str,
    day: date,
    stage: str,
    *,
    apply: bool,
    allow_repair: bool,
    backup_root: Path,
    run_id: str,
) -> tuple[list[Finding], int, bool]:
    """核验单个分区（``<table>/date=<day>``）。

    返回 ``(findings, repaired, touched)``：``repaired`` 是本次被缩放的 symbol
    条数，``touched`` 表示该分区存在（当天算「已处理」）。
    """
    part = data_root / table / f"{DAY_PREFIX}{day.isoformat()}" / PARTITION_FILENAME
    minute, unreadable = _read_partition(part, table=table, day=day)
    findings: list[Finding] = []
    if unreadable is not None:
        findings.append(unreadable)
    if minute is None:
        return findings, 0, False

    if stage in (STAGE_STRUCTURE, STAGE_ALL):
        findings.extend(check_structure(minute, table=table, day=day))

    if stage in (STAGE_INTERNAL, STAGE_ALL):
        daily_table = MINUTE_TABLE_TO_DAILY[table]
        daily_part = data_root / daily_table / f"{DAY_PREFIX}{day.isoformat()}" / PARTITION_FILENAME
        daily, daily_unreadable = _read_partition(daily_part, table=daily_table, day=day)
        if daily_unreadable is not None:
            findings.append(daily_unreadable)
        if daily is not None:
            findings.extend(compare_minute_to_daily(minute, daily, table=table))

    repaired = 0
    x100 = [f for f in findings if f.code == REPAIR_CODE and f.severity == SEVERITY_HARD]
    if apply and table in REPAIR_TABLES and x100:
        if not allow_repair:
            # 安全阀（design §6.4）：只修已收盘交易日——当日分区可能还在被
            # 15:35 链路重写，此时缩量会被后续批次覆盖或写出混合口径。
            logger.warning("%s %s 是当日/未来分区，跳过修复（只修已收盘交易日）", table, day)
        else:
            repaired = _repair_x100(
                part,
                table=table,
                symbols=sorted({f.symbol for f in x100 if f.symbol}),
                partition_symbols=minute["symbol"].unique().to_list(),
                backup_root=backup_root,
                run_id=run_id,
            )
            if repaired:
                logger.info("修复 %s %s: %d 个 symbol ×100", table, day, repaired)
    return findings, repaired, True


def run_verify(
    stage: str,
    *,
    apply: bool = False,
    days: list[date] | None = None,
    data_root: Path | None = None,
    resume: bool = False,
    progress: ProgressCallback | None = None,
) -> dict:
    """跑一遍 ``stage`` 核验层，返回 ``{"findings","repaired","days","state_path"}``。

    ``stage`` ∈ ``{"structure","internal","all"}``；``days=None`` = 全部已有分区；
    ``apply=False``（默认）只读不写，``resume=True`` 从状态文件的
    ``last_day_done`` 之后继续。``progress(stage, pct, msg)`` 可选，用于 UI 进度。
    """
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}; expected one of {STAGES}")
    root = Path(data_root) if data_root is not None else _default_data_root()
    state_path = root / STATE_FILENAME

    selected = _select_days(root, days)
    if resume:
        state = _read_state(state_path)
        last = _state_day(state)
        state_stage = state.get("stage") if state else None
        if last is not None and state_stage != stage:
            # 断点只在同一 stage 内有效：structure 跑出来的 last_day_done 不能让
            # internal 跳过这些日子（否则内部一致性核验被静默漏掉）。
            logger.warning("断点来自 stage=%s，与本次 stage=%s 不匹配，忽略断点从头跑",
                           state_stage, stage)
        elif last is not None:
            selected = [day for day in selected if day > last]

    run_id = _run_id()
    backup_root = root / BACKUP_DIRNAME
    today = _today()
    total = len(selected)
    findings_count = 0
    repaired_count = 0
    days_done = 0

    logger.info("data_verify 开始: stage=%s days=%d apply=%s root=%s",
                stage, total, apply, root)
    _emit(progress, stage, 0, f"start: {total} day(s)")

    for index, day in enumerate(selected, start=1):
        day_findings = 0
        touched = False
        for table in MINUTE_TABLES:
            table_findings, repaired, table_touched = _verify_table(
                root,
                table,
                day,
                stage,
                apply=apply,
                allow_repair=day < today,
                backup_root=backup_root,
                run_id=run_id,
            )
            day_findings += len(table_findings)
            repaired_count += repaired
            touched = touched or table_touched

        findings_count += day_findings
        if touched:
            days_done += 1
            if apply:
                # 只在 apply 时落盘：干跑必须零写入（见模块 docstring）。
                _write_state(state_path, {
                    "last_run_at": _now_iso(),
                    "stage": stage,
                    "last_day_done": day.isoformat(),
                    "applied": repaired_count,
                })
        logger.info("%s: %d findings（累计 %d，已修 %d/%d 天）",
                    day, day_findings, findings_count, repaired_count, days_done)
        _emit(progress, stage, index * 100 // total, f"{day}: {day_findings} finding(s)")

    logger.info("data_verify 完成: findings=%d repaired=%d days=%d",
                findings_count, repaired_count, days_done)
    return {
        "findings": findings_count,
        "repaired": repaired_count,
        "days": days_done,
        "state_path": str(state_path),
    }
