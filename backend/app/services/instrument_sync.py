"""标的维表同步服务。

盘前 9:10 调用 tf.exchanges.get_instruments("SH"/"SZ"/"BJ", type="stock")
获取全量标的元数据，flatten ext 字段，写入 instruments.parquet。

Starter+ 盘后可用 quotes.get(universes) 顺便补充 name。
"""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import date
from pathlib import Path

import polars as pl

from app.tickflow.client import get_client

logger = logging.getLogger(__name__)

_EXCHANGES = ["SH", "SZ", "BJ"]


def _flatten_instruments(items: list[dict]) -> list[dict]:
    """把 SDK 返回的 Instrument 列表 flatten 成扁平行。"""
    rows = []
    for item in items:
        row = {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "code": item.get("code"),
            "exchange": item.get("exchange"),
            "region": item.get("region"),
            "type": item.get("type"),
        }
        ext = item.get("ext") or {}
        row["listing_date"] = ext.get("listing_date")
        row["total_shares"] = ext.get("total_shares")
        row["float_shares"] = ext.get("float_shares")
        row["tick_size"] = ext.get("tick_size")
        row["limit_up"] = ext.get("limit_up")
        row["limit_down"] = ext.get("limit_down")
        rows.append(row)
    return rows


def _fetch_instruments_via_provider() -> list[dict] | None:
    """若当前日K数据源不是 tickflow 且该 provider 提供 get_instruments, 用它拉标的维表。

    返回 flatten 行列表; 未命中(仍应走 tickflow)时返回 None。
    标的维表跟随日K数据源(二者天然耦合, 无独立偏好项)。
    """
    from app.services import preferences

    provider_name = preferences.get_daily_data_provider()
    if provider_name == "tickflow":
        return None
    from app.data_providers import custom as custom_sources

    if not custom_sources.is_custom_provider(provider_name):
        return None
    provider = custom_sources.get_provider(provider_name)
    if not hasattr(provider, "get_instruments"):
        return None
    try:
        items = provider.get_instruments("stock") or []
    except Exception as e:
        logger.warning("provider %s get_instruments 失败: %s", provider_name, e)
        return None
    rows = _flatten_instruments(items)
    logger.info("instruments via %s: %d stocks", provider_name, len(rows))
    return rows


def sync_instruments(data_dir: Path, *, require_complete: bool = False) -> int:
    """全量同步标的维表 → data/instruments/instruments.parquet。

    返回写入的行数。
    """
    all_rows = _fetch_instruments_via_provider()
    retained = None
    out = data_dir / "instruments" / "instruments.parquet"
    if all_rows is None:
        # 未命中非 tickflow provider → 走 tickflow 直连
        tf = get_client()
        all_rows = []
        failed = []
        for ex in _EXCHANGES:
            try:
                items = tf.exchanges.get_instruments(ex, instrument_type="stock")
                if items:
                    all_rows.extend(_flatten_instruments(items))
                    logger.info("instruments %s: %d stocks", ex, len(items))
                else:
                    failed.append(ex)
                    logger.warning("get_instruments(%s) returned no stocks", ex)
            except Exception as e:
                failed.append(ex)
                logger.warning("get_instruments(%s) failed: %s", ex, e)

        if failed:
            if require_complete:
                raise RuntimeError(
                    f"instruments {','.join(failed)} 同步失败，保留原文件，等待下一轮重试")
            old = pl.read_parquet(out) if out.exists() else pl.DataFrame()
            parts = []
            for ex in failed:
                cached = (old.filter(pl.col("exchange") == ex)
                          if "exchange" in old.columns else pl.DataFrame())
                if cached.is_empty():
                    raise RuntimeError(
                        f"instruments {ex} 同步失败且无旧快照，保留原文件，拒绝发布残缺名录")
                parts.append(cached)
                logger.warning("instruments %s 同步失败，保留旧快照 %d 只", ex, cached.height)
            retained = pl.concat(parts, how="diagonal_relaxed")

    if not all_rows:
        if require_complete:
            raise RuntimeError("instruments 返回空名录，保留原文件，等待下一轮重试")
        return 0

    df = pl.DataFrame(all_rows)
    df = df.with_columns(pl.lit(date.today()).alias("as_of"))
    if retained is not None:
        # 旧交易所行保留原 as_of，不能把旧股本标成今日新数据。
        df = pl.concat([df, retained], how="diagonal_relaxed")

    out.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=out.parent, suffix=".tmp")
    os.close(fd)
    try:
        df.write_parquet(temporary)
        os.replace(temporary, out)
    finally:
        Path(temporary).unlink(missing_ok=True)

    logger.info("instruments synced: %d rows → %s", df.height, out)
    return df.height


def enrich_names_from_quotes(
    data_dir: Path,
    quotes_data: list[dict],
) -> int:
    """从 quotes 响应中提取 name，更新 instruments 维表（兜底补充）。

    盘后 quotes.get(universes) 返回的数据中包含 ext.name，
    用来补充 instruments 中可能缺失的 name。
    """
    if not quotes_data:
        return 0

    # 构建 symbol → name 映射
    name_map: dict[str, str] = {}
    for q in quotes_data:
        symbol = q.get("symbol", "")
        ext = q.get("ext") or {}
        name = ext.get("name") or q.get("name", "")
        if symbol and name:
            name_map[symbol] = name

    if not name_map:
        return 0

    inst_path = data_dir / "instruments" / "instruments.parquet"
    if not inst_path.exists():
        return 0

    df = pl.read_parquet(inst_path)

    # 只更新空 name 的行
    updates = pl.DataFrame({
        "symbol": list(name_map.keys()),
        "_new_name": list(name_map.values()),
    })
    df = df.join(updates, on="symbol", how="left")
    df = df.with_columns(
        pl.when(pl.col("name").is_null() | (pl.col("name") == ""))
        .then(pl.col("_new_name"))
        .otherwise(pl.col("name"))
        .alias("name"),
    ).drop("_new_name")

    df.write_parquet(inst_path)
    logger.info("instruments name enriched from quotes: %d names", len(name_map))
    return len(name_map)
