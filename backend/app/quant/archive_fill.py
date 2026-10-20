"""归档 → 本机分区的批量导入驱动（安全填充策略）。

安全规则（关键）：
  1. 目标分区已存在且**内容完整** → **跳过**（绝不覆盖本机更新的数据）
  2. 目标分区已存在但**空壳/残缺**（行数 < threshold）→ 用归档覆盖（修复）
  3. 目标分区不存在 → 写入（补洞）
  4. 归档里没有该日期 → 跳过

"完整"的判定用行数（比文件大小可靠）：分钟线一天约 80~130 万行；
取 threshold=100000 行作为"完整"门槛（远低于任何正常交易日，又高于所有空壳/残缺）。
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

KIND_MAP = {"minute": convert_minute, "daily": convert_daily}
# 各 kind 的"完整"行数门槛
COMPLETE_ROWS = {"minute": 100_000, "daily": 3_000}


def _existing_rows(path: Path) -> int:
    """读现有分区的行数；读不了按 0（视为空壳）。"""
    try:
        return pl.scan_parquet(path).select(pl.len()).collect().item()
    except Exception:
        return 0


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
    complete_rows = COMPLETE_ROWS[args.kind]

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
            rows = _existing_rows(out_file)
            if rows >= complete_rows:
                stats["skipped_complete"] += 1
                decisions.append({"date": part, "action": "skip_complete", "existing_rows": rows})
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
        df.write_parquet(out_file, compression="zstd")
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
