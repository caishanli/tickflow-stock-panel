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
6. **股票日线 / 复权因子**（``daily`` / ``factors``，``all`` 也包含）：分别用
   通达信盘后包、baostock 因子 + 腾讯 qfq 核验；两者都可注入 fetcher 离线测试。
   日线单源写入必须显式 opt-in，因子阶段只报告。

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
import math
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
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
    PARTITION_BLAST_RADIUS,
    SCALED_COLUMNS,
    backup_partition,
    blast_radius_block,
    scale_minute_volume_amount,
)
from app.quant.data_verify.retention import BACKUP_DIRNAME
from app.quant.data_verify.units import MINUTE_TABLE_TO_DAILY
from app.raw_partition_lock import daily_partition_lock

logger = logging.getLogger(__name__)

#: 支持的核验层。``all`` = 结构 + 内部 + 外部分钟 + 股票日线 + 复权因子。
STAGE_STRUCTURE: Final = "structure"
STAGE_INTERNAL: Final = "internal"
STAGE_EXTERNAL_MINUTE: Final = "external_minute"
STAGE_DAILY: Final = "daily"
STAGE_FACTORS: Final = "factors"
STAGE_ALL: Final = "all"
STAGES: Final[tuple[str, ...]] = (
    STAGE_STRUCTURE,
    STAGE_INTERNAL,
    STAGE_EXTERNAL_MINUTE,
    STAGE_DAILY,
    STAGE_FACTORS,
    STAGE_ALL,
)

#: 参与枚举的分钟表（也是 L1 比对的主体）。顺序固定便于日志可读。
MINUTE_TABLES: Final[tuple[str, ...]] = ("kline_minute", "kline_etf_minute")

#: 唯一允许的自动修复：单位归一后量比 ≈100 的分钟量额（科创板 ×100 事故）。
REPAIR_CODE: Final = MINUTE_VOLUME_X100
REPAIR_FACTOR: Final = 100.0
STAR_PREFIXES: Final = ("688", "689")
POSTCONDITION_FAILED: Final = "POSTCONDITION_FAILED"
#: 只修 ``kline_minute``：ETF 分钟按「股」落盘，不存在这个 100 倍问题。
REPAIR_TABLES: Final[tuple[str, ...]] = ("kline_minute",)

#: 分区路径约定：``<table>/date=YYYY-MM-DD/part.parquet``。
PARTITION_FILENAME: Final = "part.parquet"
DAY_PREFIX: Final = "date="

#: 断点状态与备份的落点（与分区表同级，不进任何 ``date=*`` 目录）。
STATE_FILENAME: Final = "data_verify_state.json"

#: 分区读取失败的结论码（只在本模块产生，不影响修复决策）。
PARTITION_UNREADABLE: Final = "PARTITION_UNREADABLE"

#: ``progress(stage, pct, msg)``：``pct`` 为 0..100 的整数。
ProgressCallback = Callable[[str, int, str], None]

#: 外部小时源比对错误码（只在本模块产生；amount 不参与跨源比对/修复）。
EXTERNAL_BAR_MISSING: Final = "EXTERNAL_BAR_MISSING"
EXTERNAL_CLOSE_MISMATCH: Final = "EXTERNAL_CLOSE_MISMATCH"
EXTERNAL_VOLUME_MISMATCH: Final = "EXTERNAL_VOLUME_MISMATCH"
REPLACEMENT_SOURCE_COLUMN: Final = "__data_verify_source"

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


@dataclass
class _PartitionOutcome:
    findings: list[Finding]
    repaired: int = 0
    touched: bool = False
    attempted: bool = False
    reachable: bool = False
    quarantine: list[dict[str, Any]] = field(default_factory=list)
    receipts: list[dict[str, Any]] = field(default_factory=list)
    single_source: int = 0
    repair_failures: list[dict[str, Any]] = field(default_factory=list)


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


def _full_history(
    source: _MinuteSource,
    symbol: str,
    cache: dict[tuple[str, str], pl.DataFrame | None] | None,
) -> pl.DataFrame | None:
    """一个源某标的的全量 bar（按 ``(source, symbol)`` 每轮只拉一次）。

    缓存值是 ``None`` 表示该次拉取失败（后续 day 不再重试）；空帧表示源可用
    但该标的无数据。``cache=None`` 时不缓存（测试/一次性调用）。
    """
    key = (source.name, symbol)
    if cache is not None and key in cache:
        return cache[key]
    try:
        frame = source.fetch(symbol)
    except Exception as exc:       # ImportError / 网络 / 解析错误：该源本轮跳过
        logger.debug("分钟源 %s 拉取 %s 失败: %s: %s",
                     source.name, symbol, type(exc).__name__, exc)
        frame = None
    if cache is not None:
        cache[key] = frame
    return frame


def _fetch_for_day(
    source: _MinuteSource,
    symbol: str,
    day: date,
    cache: dict[tuple[str, str], pl.DataFrame | None] | None = None,
) -> pl.DataFrame | None:
    """取一个源某标的的当日 bar；``None`` = 该源本次调用失败（不可用）。

    全量历史按 ``(source, symbol)`` 缓存，逐日循环只做内存切片——否则默认
    94 天 × 100 只会把同一只标的的完整历史重复拉 94 次。
    空帧 = 源可用但该标的当日无数据（停牌、超过源深度等）。
    """
    frame = _full_history(source, symbol, cache)
    if frame is None:
        return None
    if frame.is_empty():
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
    sources: Sequence[_MinuteSource],
    data_root: Path,
    day: date,
    cache: dict[tuple[str, str], pl.DataFrame | None] | None = None,
) -> bool:
    """用最新分区的一只标的探一次源，全部失败则整轮跳过（不逐标的硬刷超时）。"""
    symbol = _first_symbol(data_root, day)
    if symbol is None:
        return True
    return any(
        _fetch_for_day(source, symbol, day, cache) is not None for source in sources
    )


def _is_auction_bar(value: datetime, *, source: str = "") -> bool:
    """竞价 bar 不参与数值比对（design §5.3）。

    09:30 对所有源都跳过（本地把 09:25 竞价量记在 09:30 一根）。09:31 只有
    新浪需要跳过：它把竞价并进 09:31（腾讯与本地一样是独立的一分钟）。
    把 09:31 对所有源都跳过会漏掉腾讯 09:31 的真实错误。
    """
    if value.hour != 9:
        return False
    if value.minute == 30:
        return True
    return value.minute == 31 and source == "sina"


def _finite(value: Any) -> float | None:
    """``None`` / NaN / ±Inf 一律当「不可比」（不参与比对与求和）。"""
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _nonfinite_bar_fields(frame: pl.DataFrame) -> dict[str, int]:
    """NaN/Inf 计数（null 由结构检查负责），用于拒绝无效外部票。"""
    result: dict[str, int] = {}
    for column in ("close", "volume", "amount"):
        if column not in frame.columns:
            continue
        count = frame.filter(
            (~pl.col(column).cast(pl.Float64).is_finite()).fill_null(False)
        ).height
        if count:
            result[column] = count
    return result


def _diff_stats(
    local: pl.DataFrame, external: pl.DataFrame, *, skip_0931: bool = False
) -> dict[str, Any]:
    """逐 bar 对齐后的差异计数（按 datetime join，不猜单位）。

    跨源 1m 的**分桶边界**并不可比：同一笔成交在 A 源记 11:19、B 源可能记
    11:20，逐 bar 相对差会大面积误报（实测新浪 vs 本地 238 根里 195 根超
    0.5%）。所以量的**逐 bar** 容差按当日总量的 0.5% 计（design §5.2 的相对
    容差在跨源场景只剩「总量」可比）；价格仍按 L1 的每 bar 容差。

    但只看逐 bar 会漏掉「总量守恒的重新分配」：因此额外返回
    ``volume_total_mismatch``——共同 bar 的 Σ量差 > 0.5% × Σ本地量。
    竞价 bar（09:30 全部、09:31 仅新浪）不参与任何数值比对。
    """
    local_nonfinite = _nonfinite_bar_fields(local)
    external_nonfinite = _nonfinite_bar_fields(external)
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
    matched_local_volume = 0.0
    matched_external_volume = 0.0
    examples: list[dict[str, Any]] = []
    for row in matched.iter_rows(named=True):
        source = "sina" if skip_0931 else ""
        if _is_auction_bar(row["datetime"], source=source):
            continue
        l_close = _finite(row["l_close"])
        e_close = _finite(row["e_close"])
        l_volume = _finite(row["l_volume"])
        e_volume = _finite(row["e_volume"])
        if l_close is None or e_close is None or l_volume is None or e_volume is None:
            continue
        matched_local_volume += l_volume
        matched_external_volume += e_volume
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
    total_tolerance = EXTERNAL_VOLUME_REL_TOL * matched_local_volume
    total_diff = abs(matched_external_volume - matched_local_volume)
    return {
        "matched": matched.height,
        "local_only": local_only.height,
        "external_only": external_only.height,
        "close_mismatch": close_bad,
        "volume_mismatch": volume_bad,
        "volume_tolerance": volume_tolerance,
        "volume_total_local": matched_local_volume,
        "volume_total_external": matched_external_volume,
        "volume_total_mismatch": matched_local_volume > 0
        and total_diff > total_tolerance,
        "volume_total_tolerance": total_tolerance,
        "local_nonfinite": local_nonfinite,
        "external_nonfinite": external_nonfinite,
        "examples": examples,
        "external_only_times": [
            value.isoformat()
            for value in external_only["datetime"].sort().head(3).to_list()
        ],
    }


def _disagrees(stats: dict[str, Any]) -> bool:
    """该源是否支持「本地有错」：缺 bar 或价/量（逐 bar 或总量）不一致。"""
    return bool(
        stats["external_only"]
        or stats["close_mismatch"]
        or stats["volume_mismatch"]
        or stats["volume_total_mismatch"]
    )


def _frames_agree(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    left_name: str = "",
    right_name: str = "",
) -> bool:
    """两个外部源在共同 bar 上是否一致（价格 + 量，容差同 L1）。"""
    stats = _diff_stats(
        left, right, skip_0931="sina" in (left_name, right_name)
    )
    return (
        stats["matched"] > 0
        and stats["close_mismatch"] == 0
        and stats["volume_mismatch"] == 0
        and not stats["volume_total_mismatch"]
    )


def _source_findings(
    local: pl.DataFrame, external: pl.DataFrame, *, table: str, day: date,
    symbol: str, source: str,
) -> list[Finding]:
    """一个源 vs 本地的逐 bar 结论（每类一条，detail 里带计数与示例）。"""
    stats = _diff_stats(local, external, skip_0931=source == "sina")
    findings: list[Finding] = []
    base = {"source": source, **stats}
    if stats["local_nonfinite"] or stats["external_nonfinite"]:
        findings.append(Finding(
            table=table, day=day, symbol=symbol, code="NON_FINITE",
            severity=SEVERITY_HARD,
            detail={
                **base,
                "side": "both" if stats["local_nonfinite"] and stats["external_nonfinite"]
                else "local" if stats["local_nonfinite"] else "external",
                "fields": {
                    "local": stats["local_nonfinite"],
                    "external": stats["external_nonfinite"],
                },
            },
        ))
        return findings
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
    if stats["volume_mismatch"] or stats["volume_total_mismatch"]:
        findings.append(Finding(
            table=table, day=day, symbol=symbol, code=EXTERNAL_VOLUME_MISMATCH,
            severity=SEVERITY_HARD,
            detail={**base, "scope": (
                "bars+aggregate" if stats["volume_mismatch"]
                and stats["volume_total_mismatch"]
                else "bars" if stats["volume_mismatch"] else "aggregate"
            )},
        ))
    # 外部源少 bar（腾讯 800 根上限 / 新浪把收盘竞价并进 15:00）只是覆盖度
    # 差异，不产出 finding（design §6.3：不删数据、不因此改本地）。
    return findings


def _merge_replacements(
    external: dict[str, pl.DataFrame], group: Sequence[str], priorities: dict[str, int]
) -> pl.DataFrame:
    """按来源优先级合并多个外部源：高优先级先占位，低优先级只补缺口。"""
    ranked = sorted(group, key=lambda name: (-priorities.get(name, 0), name))
    merged = pl.concat([
        external[name].with_columns(pl.lit(name).alias(REPLACEMENT_SOURCE_COLUMN))
        for name in ranked
    ], how="vertical_relaxed")
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
    local_nonfinite = _nonfinite_bar_fields(local)
    usable = {
        name: frame for name, frame in external.items()
        if frame is not None and not frame.is_empty() and not _nonfinite_bar_fields(frame)
    }
    disagree = [
        name for name, frame in usable.items()
        if _disagrees(_diff_stats(local, frame, skip_0931=name == "sina"))
    ]
    agree_local = [name for name in usable if name not in disagree]

    clusters: list[list[str]] = []
    for name in sorted(disagree, key=lambda item: (-priorities.get(item, 0), item)):
        for cluster in clusters:
            if all(
                _frames_agree(
                    usable[name], usable[other],
                    left_name=name, right_name=other,
                )
                for other in cluster
            ):
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
        "non_finite": bool(local_nonfinite),
    }
    if local_nonfinite:
        decision["action"] = "quarantine"
        return None, decision
    # 多数票要比较「外源簇」与「与本地一致的源数」，不是「有没有源同意本地」：
    # 2 个源确认本地、1 个源不一致（2026-11-08 实测 eltdx+tencent vs sina）
    # 说明本地是对的，应 keep_local，而不是判冲突隔离。
    if len(agree_local) >= 2 and len(agree_local) > len(group):
        decision["action"] = "keep_local"
        decision["confirmed"] = True
        return None, decision
    if len(group) >= 2:
        decision["action"] = "repair"
        decision["confirmed"] = True
    elif len(group) == 1 and not agree_local and len(usable) == 1:
        # 只有「本轮总共只有一个可用外源」才允许单源修复。两个外源互相矛盾
        # 时各成单元素簇，不能靠优先级挑一个当 single_source 硬修（§5.4 冲突）。
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
    part: Path,
    df: pl.DataFrame,
    replacement: pl.DataFrame,
    *,
    source: str = "",
) -> tuple[pl.DataFrame, int]:
    """只覆盖已有 bar 的 ``close``/``volume``，不新增、不删除、不改 OHLC 其余字段。

    缺 bar 只报告：外部数据没有为新增行验证过完整的 OHLC，不能把它写成完整分钟 bar。
    09:30 对所有源跳过，09:31 对新浪跳过（两者都是源特定竞价口径）。
    """
    for key in ("symbol", "datetime"):
        if key not in df.columns or key not in replacement.columns:
            raise ValueError(f"{part}: 分钟替换需要 {key} 列")
    value_columns = [
        column for column in ("close", "volume")
        if column in df.columns and column in replacement.columns
    ]
    if not value_columns:
        return df, 0
    sina_0931 = (
        pl.col(REPLACEMENT_SOURCE_COLUMN) == "sina"
        if REPLACEMENT_SOURCE_COLUMN in replacement.columns
        else pl.lit(source == "sina")
    )
    auction = (pl.col("datetime").dt.hour() == 9) & (
        (pl.col("datetime").dt.minute() == 30)
        | ((pl.col("datetime").dt.minute() == 31) & sina_0931)
    )
    staged = _cast_to_schema(replacement.filter(~auction), df.schema).select(
        ["symbol", "datetime", *value_columns]
    )
    compared = staged.join(
        df.select(["symbol", "datetime", *value_columns]),
        on=["symbol", "datetime"], how="inner", suffix="_local",
    )
    mask = pl.lit(False)
    for column in value_columns:
        tolerance = (
            pl.max_horizontal(
                pl.lit(EXTERNAL_PRICE_ABS_TOL),
                pl.lit(EXTERNAL_PRICE_REL_TOL) * pl.col(f"{column}_local").abs(),
            )
            if column == "close"
            else pl.lit(EXTERNAL_VOLUME_REL_TOL) * pl.max_horizontal(
                pl.col("volume_local").abs(), pl.lit(1.0)
            )
        )
        finite = (
            pl.col(column).is_finite() & pl.col(f"{column}_local").is_finite()
        ).fill_null(False)
        mask = mask | (
            finite & ((pl.col(column) - pl.col(f"{column}_local")).abs() > tolerance)
        )
    keys = compared.filter(mask.fill_null(False)).select(["symbol", "datetime"])
    staged = staged.join(keys, on=["symbol", "datetime"], how="semi")
    if staged.is_empty():
        return df, 0
    renames = {column: f"__dv_bar__{column}" for column in value_columns}
    lookup = staged.rename(renames)
    joined = df.join(lookup, on=["symbol", "datetime"], how="left")
    exprs = [
        pl.coalesce([pl.col(renames[column]), pl.col(column)]).alias(column)
        if column in renames else pl.col(column)
        for column in df.columns
    ]
    updated = joined.select(exprs).select(
        [pl.col(column).cast(dtype) for column, dtype in df.schema.items()]
    )
    return updated, staged.height


def _replace_minute_bars(
    part: Path,
    replacement: pl.DataFrame,
    *,
    backup_root: Path,
    run_id: str,
    source: str = "",
    force: bool = False,
    prior_changed: int = 0,
) -> dict[str, Any]:
    """覆盖已核验字段；``prior_changed`` 保证同分区逐 symbol 写入也共享 5% 上限。"""
    if replacement.is_empty():
        return {"changed_rows": 0, "backup": None}
    with daily_partition_lock(part):
        df = pl.read_parquet(part)
        merged, changed = _merge_bars(part, df, replacement, source=source)
        if changed == 0:
            return {"changed_rows": 0, "backup": None}
        blocked = blast_radius_block(prior_changed + changed, df.height, force=force)
        if blocked is not None:
            return blocked
        backup = backup_partition(part, backup_root, run_id)
        _write_partition_unlocked(merged, part)
    return {"changed_rows": changed, "backup": str(backup), "merged": merged}


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


def _partition_days(
    data_root: Path, *, tables: Sequence[str] = MINUTE_TABLES
) -> list[date]:
    """枚举指定表的已有交易日分区（升序、去重）。"""
    days: set[date] = set()
    for table in tables:
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


def _select_days(
    data_root: Path,
    days: Sequence[date] | None,
    *,
    tables: Sequence[str] = MINUTE_TABLES,
) -> list[date]:
    """``days=None``（或空）取全部已有分区，否则只取显式给定且存在的日期。"""
    available = _partition_days(data_root, tables=tables)
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
    force: bool = False,
) -> dict[str, Any]:
    """只缩放指定 symbol 的量额；所有分区写入共用 5% 防爆阈值。"""
    with daily_partition_lock(part):
        df = pl.read_parquet(part)
        absent = [col for col in SCALED_COLUMNS if col not in df.columns]
        if absent:
            raise ValueError(f"{part}: missing scaled columns {absent}")
        mask = pl.col("symbol").is_in(list(symbols))
        nonzero = pl.any_horizontal([
            pl.col(column).fill_null(0) != 0 for column in SCALED_COLUMNS
        ])
        affected = df.filter(mask & nonzero).height
        if affected == 0:
            return {"changed_rows": 0, "backup": None}
        blocked = blast_radius_block(affected, df.height, force=force)
        if blocked is not None:
            return blocked
        scaled = df.with_columns([
            _scaled_expr(col, df.schema[col], factor, mask) for col in SCALED_COLUMNS
        ])
        backup = backup_partition(part, backup_root, run_id)
        _write_partition_unlocked(scaled, part)
    return {"changed_rows": affected, "backup": str(backup)}


def _repair_x100(
    part: Path,
    *,
    table: str,
    symbols: Sequence[str],
    partition_symbols: Sequence[str],
    backup_root: Path,
    run_id: str,
    force: bool = False,
) -> dict[str, Any]:
    """只修 688/689 的 ×100 结论，并返回备份/影响行数等审计信息。"""
    symbols = sorted({
        symbol for symbol in symbols
        if str(symbol).startswith(STAR_PREFIXES)
    })
    if not symbols:
        return {"changed_rows": 0, "backup": None, "symbols": []}
    if set(symbols) >= set(partition_symbols):
        result = scale_minute_volume_amount(
            part,
            table=table,
            factor=REPAIR_FACTOR,
            backup_root=backup_root,
            run_id=run_id,
            dry_run=False,
            force=force,
        )
    else:
        result = _scale_symbols(
            part, symbols, factor=REPAIR_FACTOR, backup_root=backup_root,
            run_id=run_id, force=force,
        )
    result["symbols"] = symbols
    return result


def _blast_finding(table: str, day: date, result: dict[str, Any]) -> Finding:
    return Finding(
        table=table, day=day, symbol=None, code=PARTITION_BLAST_RADIUS,
        severity=SEVERITY_HARD, detail=dict(result),
    )


def _audit_bars(frame: pl.DataFrame, keys: pl.DataFrame) -> list[dict[str, Any]]:
    """receipt 中保留最多 50 根对应 bar 的旧/新值，避免审计文件无限膨胀。"""
    columns = [name for name in ("datetime", "close", "volume") if name in frame.columns]
    rows = frame.join(keys, on=["symbol", "datetime"], how="semi").select(columns).head(50)
    output: list[dict[str, Any]] = []
    for row in rows.iter_rows(named=True):
        output.append({
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in row.items()
        })
    return output


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
    cache: dict[tuple[str, str], pl.DataFrame | None] | None = None,
    allow_single_source: bool = False,
    force: bool = False,
) -> _PartitionOutcome:
    """外部分钟源比对 + 仲裁；定时/默认路径绝不采用 single-source 写入。"""
    part = data_root / table / f"{DAY_PREFIX}{day.isoformat()}" / PARTITION_FILENAME
    outcome = _PartitionOutcome([])
    if minute is None:
        minute, unreadable = _read_partition(part, table=table, day=day)
        if unreadable is not None:
            outcome.findings.append(unreadable)
        if minute is None:
            return outcome
    outcome.touched = True
    if minute.is_empty():
        return outcome

    symbols = _sample_symbols(minute["symbol"].unique().to_list(), sample)
    priorities = {source.name: source.priority for source in sources}
    partition_changed = 0
    for symbol in symbols:
        local = minute.filter(pl.col("symbol") == symbol)
        if local.is_empty():
            continue
        external: dict[str, pl.DataFrame] = {}
        for source in sources:
            outcome.attempted = True
            frame = _fetch_for_day(source, symbol, day, cache)
            if frame is None:
                continue
            outcome.reachable = True
            if frame.is_empty():
                continue
            external[source.name] = frame
            outcome.findings.extend(_source_findings(
                local, frame, table=table, day=day, symbol=symbol, source=source.name
            ))
        replacement, decision = _resolve_replacement(
            local, external, priorities=priorities
        )
        if decision["single_source"]:
            outcome.single_source += 1
        if replacement is None:
            if decision["conflict"] or decision.get("non_finite"):
                reason = "non_finite" if decision.get("non_finite") else "conflict"
                outcome.quarantine.append({
                    "table": table, "day": day.isoformat(), "symbol": symbol,
                    "reason": reason, "detail": dict(decision),
                })
                logger.info("%s %s %s %s，保留本地", table, day, symbol, reason)
            continue
        if not decision["confirmed"] and not allow_single_source:
            continue
        if apply and allow_repair:
            source_names = decision["external_sources"]
            result = _replace_minute_bars(
                part, replacement, backup_root=backup_root, run_id=run_id,
                source="sina" if source_names == ["sina"] else "", force=force,
                prior_changed=partition_changed,
            )
            if result.get("blocked"):
                outcome.findings.append(_blast_finding(table, day, result))
                continue
            changed = int(result.get("changed_rows") or 0)
            if changed:
                partition_changed += changed
                outcome.repaired += 1
                after = pl.read_parquet(part).filter(pl.col("symbol") == symbol)
                keys = replacement.select(["symbol", "datetime"])
                outcome.receipts.append({
                    "table": table, "day": day.isoformat(), "symbol": symbol,
                    "field": "close,volume",
                    "old": _audit_bars(local, keys), "new": _audit_bars(after, keys),
                    "source": ",".join(source_names), "run_id": run_id,
                    "status": "applied", "backup": result.get("backup"),
                    "changed_bars": changed,
                })
                logger.info("%s %s 用 %s 覆盖 %s（%d bar, %s）",
                            table, day, source_names, symbol, changed,
                            "confirmed" if decision["confirmed"] else "single_source")
        elif apply:
            logger.warning("%s %s 是当日/未来分区，跳过外部修复", table, day)
        else:
            logger.debug("%s %s %s 本可用 %s 覆盖（dry-run）",
                         table, day, symbol, decision["external_sources"])
    return outcome


def _volume_audit(frame: pl.DataFrame, symbol: str) -> dict[str, float | None]:
    row = frame.filter(pl.col("symbol") == symbol).select([
        pl.col("volume").cast(pl.Float64).sum().alias("volume"),
        pl.col("amount").cast(pl.Float64).sum().alias("amount"),
    ])
    return row.to_dicts()[0] if row.height else {"volume": None, "amount": None}


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
    external_cache: dict[tuple[str, str], pl.DataFrame | None] | None = None,
    allow_single_source: bool = False,
    force: bool = False,
) -> _PartitionOutcome:
    """核验单个分钟分区；X100 写后必须用 L1 复核，再把新帧交给外部检查。"""
    part = data_root / table / f"{DAY_PREFIX}{day.isoformat()}" / PARTITION_FILENAME
    minute, unreadable = _read_partition(part, table=table, day=day)
    outcome = _PartitionOutcome([])
    if unreadable is not None:
        outcome.findings.append(unreadable)
    if minute is None:
        return outcome
    outcome.touched = True

    if stage in (STAGE_STRUCTURE, STAGE_ALL):
        outcome.findings.extend(check_structure(minute, table=table, day=day))

    daily: pl.DataFrame | None = None
    if stage in (STAGE_INTERNAL, STAGE_ALL):
        daily_table = MINUTE_TABLE_TO_DAILY[table]
        daily_part = data_root / daily_table / f"{DAY_PREFIX}{day.isoformat()}" / PARTITION_FILENAME
        daily, daily_unreadable = _read_partition(daily_part, table=daily_table, day=day)
        if daily_unreadable is not None:
            outcome.findings.append(daily_unreadable)
        if daily is not None:
            outcome.findings.extend(compare_minute_to_daily(minute, daily, table=table))

    x100 = [
        finding for finding in outcome.findings
        if finding.code == REPAIR_CODE and finding.severity == SEVERITY_HARD
        and finding.symbol and finding.symbol.startswith(STAR_PREFIXES)
    ]
    if apply and table in REPAIR_TABLES and x100:
        if not allow_repair:
            logger.warning("%s %s 是当日/未来分区，跳过修复（只修已收盘交易日）", table, day)
        else:
            before = minute
            result = _repair_x100(
                part, table=table,
                symbols=sorted({finding.symbol for finding in x100 if finding.symbol}),
                partition_symbols=minute["symbol"].unique().to_list(),
                backup_root=backup_root, run_id=run_id, force=force,
            )
            if result.get("blocked"):
                outcome.findings.append(_blast_finding(table, day, result))
            elif result.get("changed_rows"):
                # 无论复核成功与否，外部环节都必须使用写后的帧，不能拿旧帧继续仲裁。
                minute = pl.read_parquet(part)
                symbols = result.get("symbols") or []
                post_findings = (
                    compare_minute_to_daily(minute, daily, table=table)
                    if daily is not None else []
                )
                failed = [
                    finding for finding in post_findings
                    if finding.symbol in symbols
                    and finding.code in (REPAIR_CODE, "MINUTE_VOLUME_MISMATCH")
                ]
                if failed:
                    failure = {
                        "table": table, "day": day.isoformat(), "symbols": symbols,
                        "code": POSTCONDITION_FAILED, "backup": result.get("backup"),
                        "findings": [finding.to_dict() for finding in failed],
                    }
                    outcome.repair_failures.append(failure)
                    outcome.findings.append(Finding(
                        table=table, day=day, symbol=None, code=POSTCONDITION_FAILED,
                        severity=SEVERITY_HARD, detail=dict(failure),
                    ))
                    for symbol in symbols:
                        outcome.receipts.append({
                            "table": table, "day": day.isoformat(), "symbol": symbol,
                            "field": "volume,amount", "old": _volume_audit(before, symbol),
                            "new": _volume_audit(minute, symbol), "source": "L1",
                            "run_id": run_id, "status": "postcondition_failed",
                            "backup": result.get("backup"), "code": POSTCONDITION_FAILED,
                        })
                else:
                    outcome.repaired += len(symbols)
                    for symbol in symbols:
                        outcome.receipts.append({
                            "table": table, "day": day.isoformat(), "symbol": symbol,
                            "field": "volume,amount", "old": _volume_audit(before, symbol),
                            "new": _volume_audit(minute, symbol), "source": "L1",
                            "run_id": run_id, "status": "applied",
                            "backup": result.get("backup"), "code": REPAIR_CODE,
                        })
                    logger.info("修复 %s %s: %d 个 symbol ×100", table, day, len(symbols))

    if external_sources:
        external = _verify_external_table(
            data_root, table, day, apply=apply, allow_repair=allow_repair,
            backup_root=backup_root, run_id=run_id, sample=sample,
            sources=external_sources, minute=minute, cache=external_cache,
            allow_single_source=allow_single_source, force=force,
        )
        outcome.findings.extend(external.findings)
        outcome.repaired += external.repaired
        outcome.attempted = external.attempted
        outcome.reachable = external.reachable
        outcome.quarantine.extend(external.quarantine)
        outcome.receipts.extend(external.receipts)
        outcome.single_source += external.single_source
        outcome.repair_failures.extend(external.repair_failures)
    return outcome


def _default_daily_fetcher(day: date) -> pl.DataFrame:
    from app.quant.data_verify.probes.tdx_package import fetch_daily_package

    return fetch_daily_package(day)


def _verify_daily(
    data_root: Path,
    day: date,
    *,
    fetcher: Callable[[date], pl.DataFrame],
    apply: bool,
    allow_repair: bool,
    allow_single_source: bool,
    force: bool,
    backup_root: Path,
    run_id: str,
) -> _PartitionOutcome:
    """股票日线 vs 通达信盘后包；单源写入必须显式 opt-in。"""
    from app.quant.data_verify.checks_external import (
        SOURCE_VOLUME_FACTOR,
        compare_daily_external,
    )
    from app.quant.data_verify.policy import (
        ACTION_QUARANTINE,
        ACTION_REPAIR_LOCAL,
        decide,
    )
    from app.quant.data_verify.repair import replace_symbols

    part = data_root / "kline_daily" / f"{DAY_PREFIX}{day}" / PARTITION_FILENAME
    local, unreadable = _read_partition(part, table="kline_daily", day=day)
    outcome = _PartitionOutcome([])
    if unreadable is not None:
        outcome.findings.append(unreadable)
    if local is None:
        return outcome
    outcome.touched = True
    try:
        external = fetcher(day)
        outcome.attempted = True
    except Exception as exc:
        logger.warning("data_verify daily 外部拉取失败 %s: %s", day, exc)
        return outcome
    if external is None or external.is_empty():
        return outcome
    outcome.reachable = True
    volume_factor = SOURCE_VOLUME_FACTOR["tdx"]
    outcome.findings.extend(compare_daily_external(
        local, external, source="tdx", volume_factor=volume_factor
    ))
    if not outcome.findings:
        return outcome
    normalized = external.with_columns(
        (pl.col("volume").cast(pl.Float64) * volume_factor).alias("volume")
    ) if "volume" in external.columns else external
    decisions = decide(outcome.findings, {"tdx": "agree_external"})
    repair_symbols: set[str] = set()
    for decision in decisions:
        finding = decision.finding
        if decision.action == ACTION_QUARANTINE:
            outcome.quarantine.append({
                "table": finding.table, "day": finding.day.isoformat(),
                "symbol": finding.symbol, "reason": "conflict",
                "detail": dict(finding.detail),
            })
        elif (
            decision.action == ACTION_REPAIR_LOCAL
            and finding.code != "NON_FINITE" and finding.symbol
        ):
            if finding.detail.get("single_source"):
                outcome.single_source += 1
                if not allow_single_source:
                    continue
            repair_symbols.add(finding.symbol)
    if not repair_symbols or not apply:
        return outcome
    if not allow_repair:
        logger.warning("kline_daily %s 是当日/未来分区，跳过外部修复", day)
        return outcome
    replacement = normalized.filter(pl.col("symbol").is_in(sorted(repair_symbols)))
    result = replace_symbols(
        part, replacement, backup_root=backup_root, run_id=run_id,
        dry_run=False, force=force,
    )
    if result.get("blocked"):
        outcome.findings.append(_blast_finding("kline_daily", day, result))
        return outcome
    if not result.get("changed_rows"):
        return outcome
    outcome.repaired = len(repair_symbols)
    for decision in decisions:
        finding = decision.finding
        symbol = finding.symbol
        if (
            decision.action != ACTION_REPAIR_LOCAL or symbol not in repair_symbols
            or finding.code == "NON_FINITE"
        ):
            continue
        field = finding.code.removeprefix("DAILY_").removesuffix("_MISMATCH").lower()
        old_row = local.filter(pl.col("symbol") == symbol)
        new_row = normalized.filter(pl.col("symbol") == symbol)
        outcome.receipts.append({
            "table": "kline_daily", "day": day.isoformat(), "symbol": symbol,
            "field": field,
            "old": old_row[field][0] if field in old_row.columns and old_row.height else None,
            "new": new_row[field][0] if field in new_row.columns and new_row.height else None,
            "source": decision.source, "run_id": run_id, "status": "applied",
            "backup": result.get("backup"), "code": finding.code,
        })
    return outcome


def _default_factor_fetcher(symbol: str) -> pl.DataFrame:
    """腾讯 qfq 日线探针；复用已有 15 秒超时与 vendor 代码归一。"""
    import requests

    from app.services.alt_daily import fetch_tencent_day, tf_to_vendor

    vendor = tf_to_vendor(symbol)
    schema = {"symbol": pl.String, "date": pl.Date, "qfq_close": pl.Float64}
    if vendor is None:
        return pl.DataFrame(schema=schema)
    with requests.Session() as session:
        rows = fetch_tencent_day(session, vendor, count=10000) or []
    values = []
    for row in rows:
        try:
            values.append((symbol, date.fromisoformat(str(row[0])[:10]), float(row[2])))
        except (TypeError, ValueError, IndexError):
            continue
    return pl.DataFrame(values, schema=schema, orient="row")


def _verify_factors(
    data_root: Path,
    days: Sequence[date],
    *,
    fetcher: Callable[[str], pl.DataFrame],
    sample: int,
) -> list[Finding]:
    """本地 raw × baostock 因子 vs 腾讯 qfq；本 stage 只报告，不自动修。"""
    from app.quant.data_verify.checks_factors import apply_local_qfq, compare_qfq

    factor_path = data_root / "adj_factor_baostock" / "all.parquet"
    if not factor_path.is_file():
        logger.warning("data_verify factors 跳过：缺少 %s", factor_path)
        return []
    frames: list[pl.DataFrame] = []
    for day in days:
        part = data_root / "kline_daily" / f"{DAY_PREFIX}{day}" / PARTITION_FILENAME
        if not part.is_file():
            continue
        try:
            frames.append(pl.read_parquet(part, columns=["symbol", "date", "close"]))
        except Exception as exc:
            logger.warning("data_verify factors 日线不可读 %s: %s", part, exc)
    if not frames:
        return []
    raw = pl.concat(frames, how="vertical_relaxed")
    factors = pl.read_parquet(factor_path).rename({"trade_date": "date"})
    findings: list[Finding] = []
    for symbol in _sample_symbols(raw["symbol"].unique().to_list(), sample):
        try:
            external = fetcher(symbol)
        except Exception as exc:
            logger.warning("data_verify factors 腾讯拉取失败 %s: %s", symbol, exc)
            continue
        if external is None or external.is_empty():
            continue
        local_raw = raw.filter(pl.col("symbol") == symbol)
        local = apply_local_qfq(
            local_raw, factors.filter(pl.col("symbol") == symbol)
        )
        findings.extend(compare_qfq(local, external, symbol=symbol))
    return findings


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
    force: bool = False,
    allow_single_source: bool = False,
    daily_fetcher: Callable[[date], pl.DataFrame] | None = None,
    factor_fetcher: Callable[[str], pl.DataFrame] | None = None,
) -> dict[str, Any]:
    """运行核验层；只有 ``days is None`` 的全量运行推进 ``last_day_done``。

    ``daily_fetcher(day)`` 与 ``factor_fetcher(symbol)`` 可注入离线 fixture。
    外部修复默认只接受多源 confirmed；``allow_single_source=True`` 是手动单源
    写入的显式开关，``force=True`` 仅覆盖单分区 5% 防爆阈值。
    """
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}; expected one of {STAGES}")
    if sample < 0:
        raise ValueError(f"sample 必须为非负数，收到 {sample!r}")
    if lookback_days < 0:
        raise ValueError(f"lookback_days 必须为非负数，收到 {lookback_days!r}")
    root = Path(data_root) if data_root is not None else _default_data_root()
    state_path = root / STATE_FILENAME
    run_id = _run_id()
    external_stage = stage in (STAGE_EXTERNAL_MINUTE, STAGE_ALL)
    daily_stage = stage in (STAGE_DAILY, STAGE_ALL)
    factor_stage = stage in (STAGE_FACTORS, STAGE_ALL)
    minute_stage = stage in (
        STAGE_STRUCTURE, STAGE_INTERNAL, STAGE_EXTERNAL_MINUTE, STAGE_ALL
    )
    sources = _configured_sources() if external_stage else []
    source_names = [source.name for source in sources]
    if daily_stage:
        source_names.append("tdx")
    if factor_stage:
        source_names.append("tencent")
    source_names = sorted(set(source_names))

    selection_tables: Sequence[str]
    if stage in (STAGE_DAILY, STAGE_FACTORS):
        selection_tables = ("kline_daily",)
    elif stage == STAGE_ALL:
        selection_tables = (*MINUTE_TABLES, "kline_daily")
    else:
        selection_tables = MINUTE_TABLES
    selected = _select_days(root, days, tables=selection_tables)
    if resume:
        state = _read_state(state_path)
        last = _state_day(state)
        state_stage = state.get("stage") if state else None
        if last is not None and state_stage != stage:
            logger.warning("断点来自 stage=%s，与本次 stage=%s 不匹配，忽略断点从头跑",
                           state_stage, stage)
        elif last is not None:
            selected = [day for day in selected if day > last]

    external_days = (
        set(selected[-lookback_days:])
        if external_stage and days is None and lookback_days > 0
        else set(selected)
    )
    if stage == STAGE_EXTERNAL_MINUTE:
        selected = [day for day in selected if day in external_days]
    external_cache: dict[tuple[str, str], pl.DataFrame | None] = {}
    external_ready = external_stage and bool(sources)
    if external_ready and selected:
        external_ready = _sources_reachable(sources, root, selected[-1], external_cache)
    if stage == STAGE_EXTERNAL_MINUTE and not external_ready:
        logger.warning("分钟外部源均不可用，跳过 external_minute")
        return {
            "stage": stage, "run_id": run_id, "findings": 0, "finding_details": [],
            "repaired": 0, "days": 0, "state_path": str(state_path),
            "sources": source_names, "skipped": "no_source", "quarantine": [],
            "quarantined": 0, "receipts": [], "single_source": 0,
            "repair_failures": [],
        }

    backup_root = root / BACKUP_DIRNAME
    today = _today()
    total = len(selected)
    all_findings: list[Finding] = []
    all_quarantine: list[dict[str, Any]] = []
    all_receipts: list[dict[str, Any]] = []
    repair_failures: list[dict[str, Any]] = []
    repaired_count = 0
    single_source_count = 0
    days_done = 0
    external_attempted = False
    external_reachable = False

    logger.info("data_verify 开始: stage=%s days=%d apply=%s root=%s sources=%s",
                stage, total, apply, root, source_names)
    _emit(progress, stage, 0, f"start: {total} day(s)")
    if not selected:
        _emit(progress, stage, 100, "no partitions")

    for index, day in enumerate(selected, start=1):
        day_findings = 0
        touched = stage == STAGE_FACTORS
        run_external = external_ready and day in external_days
        if minute_stage:
            for table in MINUTE_TABLES:
                outcome = _verify_table(
                    root, table, day, stage, apply=apply, allow_repair=day < today,
                    backup_root=backup_root, run_id=run_id,
                    external_sources=sources if run_external else (), sample=sample,
                    external_cache=external_cache,
                    allow_single_source=allow_single_source, force=force,
                )
                all_findings.extend(outcome.findings)
                all_quarantine.extend(outcome.quarantine)
                all_receipts.extend(outcome.receipts)
                repair_failures.extend(outcome.repair_failures)
                repaired_count += outcome.repaired
                single_source_count += outcome.single_source
                touched = touched or outcome.touched
                external_attempted = external_attempted or outcome.attempted
                external_reachable = external_reachable or outcome.reachable
        if daily_stage:
            outcome = _verify_daily(
                root, day, fetcher=daily_fetcher or _default_daily_fetcher,
                apply=apply, allow_repair=day < today,
                allow_single_source=allow_single_source, force=force,
                backup_root=backup_root, run_id=run_id,
            )
            all_findings.extend(outcome.findings)
            all_quarantine.extend(outcome.quarantine)
            all_receipts.extend(outcome.receipts)
            repair_failures.extend(outcome.repair_failures)
            repaired_count += outcome.repaired
            single_source_count += outcome.single_source
            touched = touched or outcome.touched
        day_findings = len(all_findings)  # 日志与进度均展示累计值
        if touched:
            days_done += 1
            if apply:
                previous = _read_state(state_path) if days is not None else None
                keep_full_cursor = bool(previous and _state_day(previous) is not None)
                state_stage = (
                    str(previous.get("stage") or stage)
                    if keep_full_cursor and previous is not None else stage
                )
                payload: dict[str, Any] = {
                    **(previous or {}), "last_run_at": _now_iso(), "stage": state_stage,
                    "applied": repaired_count,
                }
                if days is None:
                    payload["last_day_done"] = day.isoformat()
                    payload.pop("completed_days", None)
                else:
                    completed = set((previous or {}).get("completed_days") or [])
                    completed.add(day.isoformat())
                    payload["completed_days"] = sorted(completed)
                _write_state(state_path, payload)
        logger.info("%s: 累计 %d findings（已修 %d/%d 天）",
                    day, len(all_findings), repaired_count, days_done)
        _emit(progress, stage, index * 100 // total,
              f"{day}: {day_findings} cumulative finding(s)")

    if factor_stage and selected:
        all_findings.extend(_verify_factors(
            root, selected, fetcher=factor_fetcher or _default_factor_fetcher, sample=sample
        ))

    retention: dict[str, Any] | None = None
    if apply:
        from app.quant.data_verify.report import write_receipts

        write_receipts(all_receipts, root, run_id)
        # 备份/receipt 按轮次滚动清理：定时任务每天跑，不清理就是净增长。
        # 清理失败只告警——它是卫生，不该影响核验结论。
        try:
            from app.quant.data_verify.retention import prune_old_runs

            retention = prune_old_runs(root)
        except Exception as exc:
            logger.warning("data_verify 旧产物清理失败（不影响本轮结果）: %s", exc)
    if all_receipts:
        try:
            from app.api.data import invalidate_storage_cache

            invalidate_storage_cache()
        except Exception as exc:
            logger.warning("data_verify 缓存失效失败（修复已落盘）: %s", exc)
    report: dict[str, Any] = {
        "stage": stage, "run_id": run_id, "findings": len(all_findings),
        "finding_details": [finding.to_dict() for finding in all_findings],
        "repaired": repaired_count, "days": days_done, "state_path": str(state_path),
        "sources": source_names, "quarantine": all_quarantine,
        "quarantined": len(all_quarantine), "receipts": all_receipts,
        "single_source": single_source_count, "repair_failures": repair_failures,
        "retention": retention,
    }
    if external_stage and not external_reachable and (not external_ready or external_attempted):
        logger.warning("分钟外部源均不可用（attempted=%s），跳过 external_minute",
                       external_attempted)
        report["skipped"] = "no_source"
    logger.info("data_verify 完成: findings=%d repaired=%d days=%d",
                len(all_findings), repaired_count, days_done)
    return report
