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
5. **分钟外部源**（``stage=external_minute``，``all`` 也包含）：对最近
   ``lookback_days``（默认 94，eltdx 深度）个交易日、每天按 symbol 排序等距抽
   ``sample``（默认 100）只，用 eltdx（可选）/ 腾讯 / 新浪重拉 1m，按
   ``datetime`` 逐 bar 比对（缺 bar / close / volume）。修复严格按 design §5.4
   仲裁：两个外部源一致 → ``confirmed``；唯一外部源且本地无佐证 →
   ``single_source``；外源与本地对峙 → 冲突不修。外源全部不可用时该 stage
   直接跳过并在报告里记 ``{"skipped": "no_source"}``，**绝不因网络失败报错**。

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
from dataclasses import dataclass
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

#: 支持的核验层。``all`` = L0 + L1 + L2 分钟外部源。
STAGE_STRUCTURE: Final = "structure"
STAGE_INTERNAL: Final = "internal"
STAGE_EXTERNAL_MINUTE: Final = "external_minute"
STAGE_ALL: Final = "all"
STAGES: Final[tuple[str, ...]] = (
    STAGE_STRUCTURE,
    STAGE_INTERNAL,
    STAGE_EXTERNAL_MINUTE,
    STAGE_ALL,
)

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

#: 外部小时源比对错误码（只在本模块产生；amount 不参与跨源比对/修复）。
EXTERNAL_BAR_MISSING: Final = "EXTERNAL_BAR_MISSING"
EXTERNAL_CLOSE_MISMATCH: Final = "EXTERNAL_CLOSE_MISMATCH"
EXTERNAL_VOLUME_MISMATCH: Final = "EXTERNAL_VOLUME_MISMATCH"

#: 容差与 L1 一致（design §5.2）：价格 1 tick+0.1%，量相对 0.5%。
EXTERNAL_PRICE_ABS_TOL: Final = 0.011
EXTERNAL_PRICE_REL_TOL: Final = 0.001
EXTERNAL_VOLUME_REL_TOL: Final = 0.005

#: ``external_minute`` 默认每天最多抽 100 只、回看 94 个交易日（eltdx 深度）。
DEFAULT_SAMPLE: Final = 100
DEFAULT_LOOKBACK_DAYS: Final = 94

#: 三源优先级（design §5.4）：eltdx（TDX 协议）> 腾讯 > 新浪。
SOURCE_PRIORITY: Final[dict[str, int]] = {"eltdx": 5, "tencent": 3, "sina": 2}


@dataclass(frozen=True)
class _MinuteSource:
    """一个分钟外部源：``fetch(symbol)`` 返回全量 bar（由 runner 按日切）。

    ``fetch`` 失败应抛异常（网络/未安装/HTTP 错误），runner 捕获后把该源本轮
    视为不可用——空帧表示「源能连上但该标的没有数据」，不算不可用。
    """

    name: str
    fetch: Callable[[str], pl.DataFrame]
    priority: int


def _configured_sources() -> list[_MinuteSource]:
    """探测可用的分钟源；``eltdx`` 未安装时自动缺席（不 import 它）。"""
    from app.quant.data_verify.probes import eltdx_m1, sina_m1, tencent_m1

    modules = {"eltdx": eltdx_m1, "tencent": tencent_m1, "sina": sina_m1}
    fetchers: dict[str, Callable[[str], pl.DataFrame]] = {
        "eltdx": eltdx_m1.fetch_eltdx_m1,
        "tencent": tencent_m1.fetch_tencent_m1,
        "sina": sina_m1.fetch_sina_m1,
    }
    sources = [
        _MinuteSource(name, fetchers[name], SOURCE_PRIORITY[name])
        for name in ("eltdx", "tencent", "sina")
        if modules[name].available_sources()
    ]
    return sorted(sources, key=lambda source: (-source.priority, source.name))


def _sample_symbols(symbols: Sequence[str], sample: int) -> list[str]:
    """按 symbol 排序后等距抽样（确定性：同一分区每次抽到同一批标的）。

    ``sample<=0`` 或标的数不超过 ``sample`` 时返回全部。
    """
    ordered = sorted(set(symbols))
    if sample <= 0 or len(ordered) <= sample:
        return ordered
    stride = len(ordered) / sample
    return [ordered[int(index * stride)] for index in range(sample)]


def _empty_bars() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "symbol": pl.String,
        "datetime": pl.Datetime("us"),
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
        "amount": pl.Float64,
    })


def _fetch_for_day(
    source: _MinuteSource, symbol: str, day: date
) -> pl.DataFrame | None:
    """取一个源某标的的当日 bar；``None`` = 该源本次调用失败（不可用）。

    空帧 = 源可用但该标的当日无数据（停牌、超过源深度等）。
    """
    try:
        frame = source.fetch(symbol)
    except Exception as exc:       # ImportError / 网络 / 解析错误：该源本轮跳过
        logger.debug("分钟源 %s 拉取 %s 失败: %s: %s",
                     source.name, symbol, type(exc).__name__, exc)
        return None
    if frame is None or frame.is_empty():
        return _empty_bars()
    required = {"symbol", "datetime", "close", "volume"}
    absent = sorted(required - set(frame.columns))
    if absent:
        logger.warning("分钟源 %s 返回缺列 %s（%s）", source.name, absent, symbol)
        return _empty_bars()
    return frame.filter(
        (pl.col("symbol") == symbol) & (pl.col("datetime").dt.date() == day)
    )


def _first_symbol(data_root: Path, day: date) -> str | None:
    """取某日任一分钟分区的第一个 symbol（用于「源是否可达」探测）。"""
    for table in MINUTE_TABLES:
        part = data_root / table / f"{DAY_PREFIX}{day.isoformat()}" / PARTITION_FILENAME
        if not part.is_file():
            continue
        try:
            symbols = pl.read_parquet(part, columns=["symbol"])["symbol"].unique().to_list()
        except Exception:
            continue
        if symbols:
            return sorted(str(symbol) for symbol in symbols)[0]
    return None


def _sources_reachable(
    sources: Sequence[_MinuteSource], data_root: Path, day: date
) -> bool:
    """用最新分区的一只标的探一次源，全部失败则整轮跳过（不逐标的硬刷超时）。"""
    symbol = _first_symbol(data_root, day)
    if symbol is None:
        return True
    return any(
        _fetch_for_day(source, symbol, day) is not None for source in sources
    )


def _is_auction_bar(value: datetime) -> bool:
    """09:30/09:31 是竞价撮合+首分钟，跨源分桶口径不同，不参与逐 bar 数值比对。

    本地把 09:25 竞价量记为 09:30 一根、09:31 是首分钟；新浪把竞价并进 09:31。
    设计 §5.3 明确「集合竞价 09:30 bar 不单独校验」，这里把 09:31 也一并放过。
    """
    return value.hour == 9 and value.minute < 32


def _diff_stats(local: pl.DataFrame, external: pl.DataFrame) -> dict[str, Any]:
    """逐 bar 对齐后的差异计数（按 datetime join，不猜单位）。

    跨源 1m 的**分桶边界**并不可比：同一笔成交在 A 源记 11:19、B 源可能记
    11:20，逐 bar 相对差会大面积误报（实测新浪 vs 本地 238 根里 195 根超
    0.5%）。所以量的容差按**当日总量**的 0.5% 计（design §5.2 的相对容差在
    跨源场景只剩「总量」可比）；价格仍按 L1 的每 bar 容差。
    """
    left = local.select([
        pl.col("datetime"),
        pl.col("close").cast(pl.Float64).alias("l_close"),
        pl.col("volume").cast(pl.Float64).alias("l_volume"),
    ])
    right = external.select([
        pl.col("datetime"),
        pl.col("close").cast(pl.Float64).alias("e_close"),
        pl.col("volume").cast(pl.Float64).alias("e_volume"),
    ])
    matched = left.join(right, on="datetime", how="inner")
    external_only = right.join(left.select("datetime"), on="datetime", how="anti")
    local_only = left.join(right.select("datetime"), on="datetime", how="anti")
    daily_volume = float(left["l_volume"].sum() or 0.0)
    volume_tolerance = EXTERNAL_VOLUME_REL_TOL * daily_volume

    close_bad = 0
    volume_bad = 0
    examples: list[dict[str, Any]] = []
    for row in matched.iter_rows(named=True):
        if _is_auction_bar(row["datetime"]):
            continue
        l_close = row["l_close"]
        e_close = row["e_close"]
        l_volume = row["l_volume"]
        e_volume = row["e_volume"]
        if None in (l_close, e_close, l_volume, e_volume):
            continue
        tolerance = max(EXTERNAL_PRICE_ABS_TOL, EXTERNAL_PRICE_REL_TOL * abs(l_close))
        if abs(e_close - l_close) > tolerance:
            close_bad += 1
            if len(examples) < 3:
                examples.append({"datetime": row["datetime"].isoformat(),
                                 "local_close": l_close, "external_close": e_close})
        diff = abs(e_volume - l_volume)
        bad_volume = diff > volume_tolerance
        if not bad_volume and l_volume == 0:
            bad_volume = e_volume != 0
        if bad_volume:
            volume_bad += 1
            if len(examples) < 3:
                examples.append({"datetime": row["datetime"].isoformat(),
                                 "local_volume": l_volume, "external_volume": e_volume})
    return {
        "matched": matched.height,
        "local_only": local_only.height,
        "external_only": external_only.height,
        "close_mismatch": close_bad,
        "volume_mismatch": volume_bad,
        "volume_tolerance": volume_tolerance,
        "examples": examples,
        "external_only_times": [
            value.isoformat()
            for value in external_only["datetime"].sort().head(3).to_list()
        ],
    }


def _disagrees(stats: dict[str, Any]) -> bool:
    """该源是否支持「本地有错」：缺 bar 或价/量不一致。"""
    return bool(
        stats["external_only"] or stats["close_mismatch"] or stats["volume_mismatch"]
    )


def _frames_agree(left: pl.DataFrame, right: pl.DataFrame) -> bool:
    """两个外部源在共同 bar 上是否一致（价格 + 量，容差同 L1）。"""
    stats = _diff_stats(left, right)
    return stats["matched"] > 0 and stats["close_mismatch"] == 0 \
        and stats["volume_mismatch"] == 0


def _source_findings(
    local: pl.DataFrame, external: pl.DataFrame, *, table: str, day: date,
    symbol: str, source: str,
) -> list[Finding]:
    """一个源 vs 本地的逐 bar 结论（每类一条，detail 里带计数与示例）。"""
    stats = _diff_stats(local, external)
    findings: list[Finding] = []
    base = {"source": source, **stats}
    if stats["external_only"]:
        findings.append(Finding(
            table=table, day=day, symbol=symbol, code=EXTERNAL_BAR_MISSING,
            severity=SEVERITY_HARD,
            detail={**base, "side": "local", "missing_bars": stats["external_only"]},
        ))
    if stats["close_mismatch"]:
        findings.append(Finding(
            table=table, day=day, symbol=symbol, code=EXTERNAL_CLOSE_MISMATCH,
            severity=SEVERITY_HARD, detail=dict(base),
        ))
    if stats["volume_mismatch"]:
        findings.append(Finding(
            table=table, day=day, symbol=symbol, code=EXTERNAL_VOLUME_MISMATCH,
            severity=SEVERITY_HARD, detail=dict(base),
        ))
    # 外部源少 bar（腾讯 800 根上限 / 新浪把收盘竞价并进 15:00）只是覆盖度
    # 差异，不产出 finding（design §6.3：不删数据、不因此改本地）。
    return findings


def _merge_replacements(
    external: dict[str, pl.DataFrame], group: Sequence[str], priorities: dict[str, int]
) -> pl.DataFrame:
    """按来源优先级合并多个外部源：高优先级先占位，低优先级只补缺口。"""
    ranked = sorted(group, key=lambda name: (-priorities.get(name, 0), name))
    merged = pl.concat([external[name] for name in ranked], how="vertical_relaxed")
    return (
        merged.unique(subset=["datetime"], keep="first", maintain_order=True)
        .sort("datetime")
    )


def _resolve_replacement(
    local: pl.DataFrame,
    external: dict[str, pl.DataFrame],
    *,
    priorities: dict[str, int] | None = None,
) -> tuple[pl.DataFrame | None, dict[str, Any]]:
    """按 design §5.4 仲裁：外源多数/单源修复，外源与本地对峙则冲突不修。

    返回 ``(replacement | None, decision)``。``replacement`` 只含外部 bar，
    runner 再按 ``(symbol, datetime)`` 覆盖本地分区。
    """
    priorities = priorities or SOURCE_PRIORITY
    usable = {
        name: frame for name, frame in external.items()
        if frame is not None and not frame.is_empty()
    }
    disagree = [name for name, frame in usable.items() if _disagrees(_diff_stats(local, frame))]
    agree_local = [name for name in usable if name not in disagree]

    clusters: list[list[str]] = []
    for name in sorted(disagree, key=lambda item: (-priorities.get(item, 0), item)):
        for cluster in clusters:
            if all(_frames_agree(usable[name], usable[other]) for other in cluster):
                cluster.append(name)
                break
        else:
            clusters.append([name])
    clusters.sort(key=lambda cluster: (-len(cluster), cluster))
    group = clusters[0] if clusters else []

    decision: dict[str, Any] = {
        "action": "keep_local",
        "external_sources": list(group),
        "local_sources": sorted(agree_local),
        "confirmed": False,
        "single_source": False,
        "conflict": False,
    }
    if len(group) >= 2:
        decision["action"] = "repair"
        decision["confirmed"] = True
    elif len(group) == 1 and not agree_local:
        decision["action"] = "repair"
        decision["single_source"] = True
    elif group:
        decision["action"] = "quarantine"
        decision["conflict"] = True
    if decision["action"] != "repair":
        return None, decision
    replacement = _merge_replacements(usable, group, priorities)
    return replacement, decision


def _cast_to_schema(frame: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """按分区 schema 补齐缺列 / 对齐 dtype（外部多带的列丢弃）。"""
    exprs = [
        (pl.col(column).cast(dtype) if column in frame.columns
         else pl.lit(None, dtype=dtype).alias(column))
        for column, dtype in schema.items()
    ]
    return frame.select(exprs)


def _merge_bars(
    part: Path, df: pl.DataFrame, replacement: pl.DataFrame
) -> tuple[pl.DataFrame, int]:
    """按 ``(symbol, datetime)`` 覆盖/补入外部 bar，返回 (新帧, 重写 bar 数)。

    本地独有的 bar **不删除**（外部源可能只是覆盖不全，design §6.3 不删数据）。
    """
    for key in ("symbol", "datetime"):
        if key not in df.columns or key not in replacement.columns:
            raise ValueError(f"{part}: 分钟替换需要 {key} 列")
    staged = _cast_to_schema(replacement, df.schema)
    new_bars = staged.join(
        df.select(["symbol", "datetime"]), on=["symbol", "datetime"], how="anti"
    )
    renames = {
        column: f"__dv_bar__{column}"
        for column in df.columns
        if column not in ("symbol", "datetime")
    }
    lookup = staged.rename(renames)
    joined = df.join(lookup, on=["symbol", "datetime"], how="left")
    exprs = [
        (pl.coalesce([pl.col(renames[column]), pl.col(column)]).alias(column)
         if column in renames else pl.col(column))
        for column in df.columns
    ]
    updated = joined.select(exprs).select(
        [pl.col(column).cast(dtype) for column, dtype in df.schema.items()]
    )
    if new_bars.height:
        updated = pl.concat(
            [updated, _cast_to_schema(new_bars, df.schema)], how="vertical_relaxed"
        ).sort(["symbol", "datetime"])
    return updated, staged.height


def _replace_minute_bars(
    part: Path,
    replacement: pl.DataFrame,
    *,
    backup_root: Path,
    run_id: str,
    dry_run: bool,
) -> int:
    """按 ``(symbol, datetime)`` 覆盖分钟 bar（写前备份、持锁原子写）。

    不复用日线语义的 :func:`~app.quant.data_verify.repair.replace_symbols`：
    它要求「一个 symbol 一行」，而分钟分区是「一个 bar 一行」。
    """
    if replacement.is_empty():
        return 0
    if dry_run:
        df = pl.read_parquet(part)
        _, changed = _merge_bars(part, df, replacement)
        return changed
    with daily_partition_lock(part):
        df = pl.read_parquet(part)
        merged, changed = _merge_bars(part, df, replacement)
        if changed == 0:               # 没有匹配/新增的 bar：不备份不写盘
            return 0
        backup_partition(part, backup_root, run_id)
        _write_partition_unlocked(merged, part)
    return changed


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


def _verify_external_table(
    data_root: Path,
    table: str,
    day: date,
    *,
    apply: bool,
    allow_repair: bool,
    backup_root: Path,
    run_id: str,
    sample: int,
    sources: Sequence[_MinuteSource],
    minute: pl.DataFrame | None = None,
) -> tuple[list[Finding], int, bool, bool, bool]:
    """外部分钟源比对 + 仲裁修复一个分区。

    返回 ``(findings, repaired, touched, attempted, reachable)``：
    ``attempted`` = 是否真的发出过取数请求，``reachable`` = 是否有源正常返回。
    """
    part = data_root / table / f"{DAY_PREFIX}{day.isoformat()}" / PARTITION_FILENAME
    findings: list[Finding] = []
    touched = False
    if minute is None:
        minute, unreadable = _read_partition(part, table=table, day=day)
        if unreadable is not None:
            findings.append(unreadable)
        if minute is None:
            return findings, 0, False, False, False
    touched = True
    if minute.is_empty():
        return findings, 0, touched, False, False

    symbols = _sample_symbols(minute["symbol"].unique().to_list(), sample)
    priorities = {source.name: source.priority for source in sources}
    repaired = 0
    attempted = False
    reachable = False
    for symbol in symbols:
        local = minute.filter(pl.col("symbol") == symbol)
        if local.is_empty():
            continue
        external: dict[str, pl.DataFrame] = {}
        for source in sources:
            attempted = True
            frame = _fetch_for_day(source, symbol, day)
            if frame is None:
                continue
            reachable = True
            if frame.is_empty():
                continue
            external[source.name] = frame
            findings.extend(_source_findings(
                local, frame, table=table, day=day, symbol=symbol, source=source.name
            ))
        replacement, decision = _resolve_replacement(
            local, external, priorities=priorities
        )
        if replacement is None:
            if decision["conflict"]:
                logger.info("%s %s %s 外源冲突（%s），保留本地",
                            table, day, symbol, decision["external_sources"])
            continue
        if apply and allow_repair:
            changed = _replace_minute_bars(
                part, replacement, backup_root=backup_root, run_id=run_id, dry_run=False
            )
            repaired += 1 if changed else 0
            logger.info("%s %s 用 %s 覆盖 %s（%d bar, %s）",
                        table, day, decision["external_sources"], symbol, changed,
                        "confirmed" if decision["confirmed"] else "single_source")
        elif apply:
            logger.warning("%s %s 是当日/未来分区，跳过外部修复", table, day)
        else:
            # dry-run：只报告「本可修复」，不读盘不写盘。
            logger.debug("%s %s %s 本可用 %s 覆盖（dry-run）",
                         table, day, symbol, decision["external_sources"])
    return findings, repaired, touched, attempted, reachable


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
    external_sources: Sequence[_MinuteSource] = (),
    sample: int = DEFAULT_SAMPLE,
) -> tuple[list[Finding], int, bool, bool, bool]:
    """核验单个分区（``<table>/date=<day>``）。

    返回 ``(findings, repaired, touched, attempted, reachable)``：``repaired``
    是本次修复的 symbol 条数，``touched`` 表示该分区存在（当天算「已处理」），
    后两项只服务 ``external_minute`` 的「无源跳过」判定。
    """
    part = data_root / table / f"{DAY_PREFIX}{day.isoformat()}" / PARTITION_FILENAME
    minute, unreadable = _read_partition(part, table=table, day=day)
    findings: list[Finding] = []
    if unreadable is not None:
        findings.append(unreadable)
    if minute is None:
        return findings, 0, False, False, False

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

    attempted = False
    reachable = False
    if external_sources:
        ext_findings, ext_repaired, _, attempted, reachable = _verify_external_table(
            data_root,
            table,
            day,
            apply=apply,
            allow_repair=allow_repair,
            backup_root=backup_root,
            run_id=run_id,
            sample=sample,
            sources=external_sources,
            minute=minute,
        )
        findings.extend(ext_findings)
        repaired += ext_repaired

    return findings, repaired, True, attempted, reachable


def run_verify(
    stage: str,
    *,
    apply: bool = False,
    days: list[date] | None = None,
    data_root: Path | None = None,
    resume: bool = False,
    progress: ProgressCallback | None = None,
    sample: int = DEFAULT_SAMPLE,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> dict:
    """跑一遍 ``stage`` 核验层，返回核验摘要。

    返回 ``{"findings","repaired","days","state_path","sources"}``；外部小时源
    不可用时额外带 ``"skipped": "no_source"``（该 stage 跳过，绝不抛错）。

    ``stage`` ∈ ``{"structure","internal","external_minute","all"}``；
    ``days=None`` = 全部已有分区；``apply=False``（默认）只读不写，
    ``resume=True`` 从状态文件的 ``last_day_done`` 之后继续。
    ``external_minute`` 默认只看最近 ``lookback_days`` 个交易日、每天按
    symbol 等距抽 ``sample`` 只——外部 1m 源最深约 94 个交易日。
    """
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}; expected one of {STAGES}")
    if sample < 0:
        raise ValueError(f"sample 必须为非负数，收到 {sample!r}")
    if lookback_days < 0:
        raise ValueError(f"lookback_days 必须为非负数，收到 {lookback_days!r}")
    root = Path(data_root) if data_root is not None else _default_data_root()
    state_path = root / STATE_FILENAME
    external_stage = stage in (STAGE_EXTERNAL_MINUTE, STAGE_ALL)
    sources = _configured_sources() if external_stage else []

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

    # 外部 1m 只有近 ~94 个交易日；``all`` 只在这个窗口内跑外部环节，
    # structure/internal 仍然覆盖全部（显式 --days 时以调用方为准）。
    if external_stage and days is None and lookback_days > 0:
        external_days = set(selected[-lookback_days:])
    else:
        external_days = set(selected)
    if stage == STAGE_EXTERNAL_MINUTE:
        selected = [day for day in selected if day in external_days]

    external_ready = external_stage and bool(sources)
    if external_ready and selected:
        external_ready = _sources_reachable(sources, root, selected[-1])
    if stage == STAGE_EXTERNAL_MINUTE and not external_ready:
        logger.warning("分钟外部源均不可用，跳过 external_minute")
        return {
            "findings": 0,
            "repaired": 0,
            "days": 0,
            "state_path": str(state_path),
            "sources": [source.name for source in sources],
            "skipped": "no_source",
        }

    run_id = _run_id()
    backup_root = root / BACKUP_DIRNAME
    today = _today()
    total = len(selected)
    findings_count = 0
    repaired_count = 0
    days_done = 0
    external_attempted = False
    external_reachable = False

    logger.info("data_verify 开始: stage=%s days=%d apply=%s root=%s sources=%s",
                stage, total, apply, root, [source.name for source in sources])
    _emit(progress, stage, 0, f"start: {total} day(s)")

    for index, day in enumerate(selected, start=1):
        day_findings = 0
        touched = False
        run_external = external_ready and day in external_days
        for table in MINUTE_TABLES:
            table_findings, repaired, table_touched, attempted, reachable = _verify_table(
                root,
                table,
                day,
                stage,
                apply=apply,
                allow_repair=day < today,
                backup_root=backup_root,
                run_id=run_id,
                external_sources=sources if run_external else (),
                sample=sample,
            )
            day_findings += len(table_findings)
            repaired_count += repaired
            touched = touched or table_touched
            external_attempted = external_attempted or attempted
            external_reachable = external_reachable or reachable

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
    report: dict[str, Any] = {
        "findings": findings_count,
        "repaired": repaired_count,
        "days": days_done,
        "state_path": str(state_path),
        "sources": [source.name for source in sources],
    }
    if (
        external_stage
        and not external_reachable
        and (not external_ready or external_attempted)
    ):
        # 一个源都没连上：该 stage 跳过，由上层报告呈现（绝不因网络失败中断核验）。
        logger.warning("分钟外部源均不可用（attempted=%s），跳过 external_minute",
                       external_attempted)
        report["skipped"] = "no_source"
    return report
