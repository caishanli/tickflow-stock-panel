"""归档导入 CLI：把归档 zip 里的历史数据导成本机分区格式。

用法:
    uv run python -m app.quant.archive_import_cli \
        --source-zip /home/caisl/A股分钟数据/4.21更新/A股分钟数据/A股分钟/1m_price/2019.zip \
        --target-root /tmp/opencode/import_probe \
        --kind minute \
        --months 2019-01 \
        --dry-run
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from app.quant.archive_fill import _write_partition_atomic, is_complete
from app.quant.archive_import import (
    convert_daily,
    convert_minute,
    day_to_partition,
    iter_zip_days,
    read_day_from_zip,
)

KIND_MAP = {"minute": convert_minute, "daily": convert_daily}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-zip", required=True)
    ap.add_argument("--target-root", required=True)
    ap.add_argument("--kind", choices=["minute", "daily"], required=True)
    ap.add_argument("--months", default="", help="只导这些月，逗号分隔 YYYY-MM；空=全部")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--overwrite", action="store_true",
                    help="目标分区已存在时覆盖（仍会做完整性判定：完整的分区不覆盖）")
    ap.add_argument("--limit", type=int, default=0, help="最多处理多少个交易日（0=不限）")
    ap.add_argument("--force-overwrite-complete", action="store_true",
                    help="连**完整**分区也覆盖（危险）")
    ap.add_argument("--allow-live-root", action="store_true",
                    help="允许把目标指向**线上数据根**（data/kline_*）——默认拒绝，"
                         "避免误覆盖线上完整分区")
    args = ap.parse_args()

    src = Path(args.source_zip)
    root = Path(args.target_root).resolve()
    live_root = Path(__file__).resolve().parents[3] / "data"
    if ((live_root / "kline_daily").exists() and str(root).startswith(str(live_root))
            and not args.allow_live_root):
            print(json.dumps({"error": "目标指向线上数据根，拒绝执行",
                              "target_root": str(root),
                              "hint": "确认要覆盖线上分区请加 --allow-live-root"},
                             ensure_ascii=False, indent=2))
            return 2
    months = {m.strip() for m in args.months.split(",") if m.strip()}
    conv = KIND_MAP[args.kind]

    days = iter_zip_days(src)
    if months:
        days = [d for d in days if day_to_partition(d)[:7] in months]
    if args.limit:
        days = days[: args.limit]

    stats = {
        "source_zip": str(src),
        "kind": args.kind,
        "target_root": str(root),
        "days_total": len(days),
        "days_written": 0,
        "days_skipped": 0,
        "rows_total": 0,
        "bytes_total": 0,
        "elapsed_s": 0.0,
        "errors": [],
    }
    t0 = time.time()

    for member in days:
        part = day_to_partition(member)
        out_dir = root / f"date={part}"
        out_file = out_dir / "part.parquet"
        try:
            df = conv(read_day_from_zip(src, member))
        except Exception as exc:
            stats["errors"].append(f"{part}: {exc}")
            continue
        if out_file.exists():
            archived_symbols = {str(s) for s in df["symbol"].unique().to_list()}
            # 完整性判定：**完整的分区不覆盖**（--overwrite 也不例外，除非显式
            # --force-overwrite-complete）——旧实现直接写，任何完整分区都会被替换。
            if is_complete(out_file, archived_symbols, args.kind) and not args.force_overwrite_complete:
                stats["days_skipped"] += 1
                continue
            if not args.overwrite:
                stats["days_skipped"] += 1
                continue
        if args.dry_run:
            stats["days_written"] += 1
            stats["rows_total"] += df.height
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        # 原子写 + 分区锁（与其它写入方互斥）
        _write_partition_atomic(out_file, df)
        stats["days_written"] += 1
        stats["rows_total"] += df.height
        stats["bytes_total"] += out_file.stat().st_size

    stats["elapsed_s"] = round(time.time() - t0, 1)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0 if not stats["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
