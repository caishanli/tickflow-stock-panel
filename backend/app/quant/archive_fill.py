"""归档 → 本机分区的批量导入驱动（安全填充策略）。

安全规则（关键）：
  1. 目标分区已存在且**内容完整** → **跳过**（绝不覆盖本机更新的数据）
  2. 目标分区已存在但**空壳/残缺** → 用归档覆盖（修复）
  3. 目标分区不存在 → 写入（补洞）
  4. 归档里没有该日期 → 跳过

"完整"的判定用**标的数覆盖比**（不是绝对行数）：绝对行数门槛不可靠 ——
2016/2017 年的日线分区只有 ~2592 行（当年上市数少），若用 3000 行门槛，
这 307 个完整分区每次重跑都会被覆盖（本机数据可能已被更新）。
判定改为「本机该日标的数 ≥ 归档该日标的数 × 95%」→ 视为完整、跳过。

写入走**原子写**（.tmp + rename）并持有 **daily_partition_lock**：
与收盘回源等其它写入方互斥，且进程中断不会留下损坏的 parquet。
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import polars as pl

from app.quant.archive_import import (
    convert_daily,
    convert_minute,
    day_to_partition,
    iter_zip_days,
    read_day_from_zip,
)
from app.raw_partition_lock import daily_partition_lock

KIND_MAP = {"minute": convert_minute, "daily": convert_daily}
# 标的数覆盖比：本机该日标的数 ≥ 归档该日标的数 × 该比例 → 视为"完整"、跳过
SYMBOL_COVERAGE = 0.95
# 兜底：归档该日标的数取不到时，用行数下限判断（分钟线一天 ~80~130 万行）
MIN_ROWS_FALLBACK = {"minute": 100_000, "daily": 500}


def _existing_rows(path: Path) -> int:
    """读现有分区的行数；读不了按 0（视为空壳）。"""
    try:
        return pl.scan_parquet(path).select(pl.len()).collect().item()
    except Exception:  # noqa: BLE001
        return 0


def _existing_symbols(path: Path) -> set[str]:
    """读现有分区的标的集合；读不了返回空集。"""
    try:
        df = pl.scan_parquet(path).select("symbol").collect()
        return {str(s) for s in df["symbol"].to_list()}
    except Exception:  # noqa: BLE001
        return set()


def is_complete(existing: Path, archived_symbols: set[str], kind: str) -> bool:
    """本机分区是否已完整覆盖该交易日的数据。

    判定：标的数覆盖比 ≥ SYMBOL_COVERAGE → 完整（跳过）。
    归档该日标的数取不到时退化为行数下限（MIN_ROWS_FALLBACK）。
    """
    if not existing.exists():
        return False
    if archived_symbols:
        have = _existing_symbols(existing)
        if have:
            return len(have) >= len(archived_symbols) * SYMBOL_COVERAGE
    return _existing_rows(existing) >= MIN_ROWS_FALLBACK[kind]


def _write_partition_atomic(out_file: Path, df: pl.DataFrame) -> None:
    """原子写分区（.tmp + rename），并持有 daily_partition_lock 与其它写入方互斥。"""
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with daily_partition_lock(out_file):
        tmp = out_file.with_name(out_file.name + ".tmp")
        df.write_parquet(tmp)
        tmp.replace(out_file)


def plan_target(years: list[int], kind: str) -> list[tuple[Path, str]]:
    """列出 (zip 路径, 成员名) 序列，按年份过滤。

    归档目录结构（/home/caisl/A股分钟数据/4.21更新/A股分钟数据/）：
      A股分钟/1m_price/2019.zip   ← 1 分钟
      A股日线/1d_price/2019.zip   ← 日线
    """
    root = Path("/home/caisl/A股分钟数据/4.21更新/A股分钟数据")
    sub = "1m_price" if kind == "minute" else "1d_price"
    parent = "A股分钟" if kind == "minute" else "A股日线"
    out: list[tuple[Path, str]] = []
    for y in sorted(years):
        z = root / parent / sub / f"{y}.zip"
        if not z.exists():
            continue
        for member in iter_zip_days(z):
            out.append((z, member))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-root", required=True, help="本机分区根，如 data/kline_minute")
    ap.add_argument("--kind", choices=["minute", "daily"], required=True)
    ap.add_argument("--from-year", type=int, required=True)
    ap.add_argument("--to-year", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report", default="", help="把逐日决策写到 JSON 文件")
    args = ap.parse_args()

    years = list(range(args.from_year, args.to_year + 1))
    target_root = Path(args.target_root)
    conv = KIND_MAP[args.kind]

    items = plan_target(years, args.kind)
    stats = {
        "kind": args.kind,
        "years": years,
        "target_root": str(target_root),
        "candidates": len(items),
        "written": 0,
        "skipped_complete": 0,
        "rows_written": 0,
        "bytes_written": 0,
        "elapsed_s": 0.0,
        "errors": [],
    }
    decisions: list[dict] = []
    t0 = time.time()

    for zip_path, member in items:
        part = day_to_partition(member)
        out_file = target_root / f"date={part}" / "part.parquet"
        if out_file.exists():
            # 完整性判定：需要归档该日的标的集合（先读归档再判，避免无谓解压）
            try:
                archived = conv(read_day_from_zip(zip_path, member))
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"{part}: {exc}")
                continue
            archived_symbols = {str(s) for s in archived["symbol"].unique().to_list()}
            if is_complete(out_file, archived_symbols, args.kind):
                stats["skipped_complete"] += 1
                decisions.append({"date": part, "action": "skip_complete",
                                  "existing_rows": _existing_rows(out_file)})
                continue
            action = "overwrite_shell"
        else:
            action = "create"
        if args.dry_run:
            stats["written"] += 1
            decisions.append({"date": part, "action": action + "_dryrun"})
            continue
        try:
            df = conv(read_day_from_zip(zip_path, member))
        except Exception as exc:
            stats["errors"].append(f"{part}: {exc}")
            continue
        out_file.parent.mkdir(parents=True, exist_ok=True)
        _write_partition_atomic(out_file, df)
        stats["written"] += 1
        stats["rows_written"] += df.height
        stats["bytes_written"] += out_file.stat().st_size
        decisions.append({"date": part, "action": action, "rows": df.height})

    stats["elapsed_s"] = round(time.time() - t0, 1)
    if args.report:
        Path(args.report).write_text(json.dumps({"stats": stats, "decisions": decisions},
                                                ensure_ascii=False, indent=1))
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0 if not stats["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
