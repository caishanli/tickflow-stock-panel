"""日线口径修复：把被"前复权"污染的分区重建成**原始价**（不复权）。

背景（2026-10-22 体检定位）
--------------------------
``mootdx_src.MootdxSource.get_daily`` 取到通达信原始价后，会用 xdxr 除权事件
换算前复权（``adj=“qfq”``），换算**依赖帧内前一收盘价**。``sync_daily`` 是
单日帧，所以：

* **当日增量写入**（收盘 cron，帧内无未来事件）→ 落盘 = 原始价 ✓
* **事后批量回补**（帧内该日之后有除权事件）→ 落盘 = 前复权 ✗

读侧 ``DataSources.apply_qfq_daily`` 会再乘一次 ``ex_factor`` —— 对"前复权"
分区就是**重复复权**（实测 688032.SH 2023-05-04 偏 58%、000001.SZ 偏 18%）。

实测污染范围（本机 2026-10-22 抽样 250 只 × 860 日）：``2023-04-19 → 2023-12-20``
共 165 个交易日，约 14% 的 (标的,日) 单元格；2024 起 100% 原始价。

本工具
------
``--detect``  抽样比对 baostock 不复权价，报告哪些日期段是前复权（无副作用）。
``--repair``  用归档（baostock 原始价，实测逐行精确）重写指定日期段的分区。

安全策略
--------
* 重写前**备份**原分区（默认 ``data/.daily_basis_backup/<日期段>/``）
* **不丢标的**：本机有、归档没有的标的**保留原行**（不用归档覆盖掉）
* 原子写 + ``daily_partition_lock``（与收盘回源互斥）
* ``--dry-run`` 只报告不落盘
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import sys
import zipfile
from pathlib import Path

import polars as pl

from app.quant.archive_import import convert_daily

DATA = Path("/home/caisl/tickflow-stock-panel/data")
ARCHIVE_DAILY = Path("/home/caisl/A股分钟数据/4.21更新/A股分钟数据/A股日线/1d_price")
DEFAULT_BACKUP = DATA / ".daily_basis_backup"

# 判定阈值：|store/raw - 1| 超过此值即认为不是原始价
RAW_TOL = 0.005


def _archive_day(day: str) -> pl.DataFrame:
    """读归档该交易日的原始价日线（已转本机 schema，volume 已 ÷100）。"""
    with zipfile.ZipFile(ARCHIVE_DAILY / f"{day[:4]}.zip") as zf:
        return convert_daily(pl.read_parquet(zf.open(day.replace("-", "") + ".parquet")))


def _baostock_raw(symbols: list[str], start: str, end: str) -> pl.DataFrame:
    """baostock 不复权日线（adjustflag="3"），逐标的取整段。"""
    import baostock as bs

    bs.login()
    frames: list[pl.DataFrame] = []
    try:
        for i, sym in enumerate(symbols, 1):
            code, mk = sym.split(".")
            rs = bs.query_history_k_data_plus(
                f"{'sh' if mk == 'SH' else 'sz'}.{code}", "date,close",
                start_date=start, end_date=end, frequency="d", adjustflag="3")
            if rs.error_code != "0":
                continue
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            if rows:
                frames.append(pl.DataFrame({
                    "symbol": [sym] * len(rows),
                    "date": [r[0] for r in rows],
                    "raw_close": [float(r[1]) if r[1] else None for r in rows],
                }))
            if i % 50 == 0:
                print(f"  baostock {i}/{len(symbols)}", file=sys.stderr, flush=True)
    finally:
        bs.logout()
    if not frames:
        return pl.DataFrame(schema={"symbol": pl.String, "date": pl.String,
                                    "raw_close": pl.Float64})
    return pl.concat(frames)


def _store_stock_daily(start: str, end: str, symbols: list[str] | None = None) -> pl.DataFrame:
    """读本机日线（可限定标的集合）。"""
    frames = []
    for f in sorted(DATA.glob("kline_daily/date=*/part.parquet")):
        day = f.parent.name.split("=")[1]
        if day < start or day > end:
            continue
        df = pl.read_parquet(f, columns=["symbol", "date", "close"])
        if symbols is not None:
            df = df.filter(pl.col("symbol").is_in(symbols))
        if not df.is_empty():
            frames.append(df)
    return pl.concat(frames) if frames else pl.DataFrame()


def detect(start: str, end: str, n_sym: int, seed: int = 42) -> dict:
    """抽样检测：哪些日期是前复权、哪些是原始价。"""
    inst = pl.read_parquet(DATA / "instruments/instruments.parquet", columns=["symbol"])
    pool = sorted(s for s in inst["symbol"].to_list() if not s.endswith(".BJ"))
    random.seed(seed)
    picked = sorted(random.sample(pool, min(n_sym, len(pool))))
    store = _store_stock_daily(start, end, picked)
    raw = _baostock_raw(picked, start, end).with_columns(pl.col("date").str.to_date())
    j = store.join(raw, on=["symbol", "date"], how="inner").filter(
        pl.col("raw_close").is_not_null() & (pl.col("raw_close") > 0))
    j = j.with_columns((pl.col("close") / pl.col("raw_close")).alias("ratio"),
                       ((pl.col("close") / pl.col("raw_close") - 1).abs() < RAW_TOL).alias("is_raw"))
    by_day = j.group_by("date").agg(
        pl.len().alias("n"), pl.col("is_raw").sum().alias("raw"),
    ).sort("date").with_columns((1 - pl.col("raw") / pl.col("n")).alias("qfq_frac"))
    bad = by_day.filter(pl.col("qfq_frac") > 0.05)
    return {
        "cells": j.height,
        "raw_frac": round(float(j["is_raw"].mean()), 4) if j.height else None,
        "qfq_days": bad.height,
        "qfq_range": [str(bad["date"][0]), str(bad["date"][-1])] if bad.height else None,
        "worst": [
            {"date": str(r["date"]), "rows": r["n"], "qfq_frac": round(r["qfq_frac"], 3)}
            for r in bad.sort("qfq_frac", descending=True).head(5).iter_rows(named=True)
        ],
    }


def repair(start: str, end: str, *, apply: bool, backup_dir: Path) -> dict:
    """用归档原始价重写 [start, end] 区间的日线分区。"""
    days = sorted(f.parent.name.split("=")[1]
                  for f in DATA.glob("kline_daily/date=*/part.parquet"))
    days = [d for d in days if start <= d <= end]
    stats: dict = {"days": len(days), "written": 0, "skipped_no_archive": 0,
                   "kept_local_only_rows": 0, "local_only_samples": [],
                   "backup": str(backup_dir) if apply else None, "errors": []}
    if not days:
        stats["errors"].append("区间内无本机分区")
        return stats
    for day in days:
        out_file = DATA / "kline_daily" / f"date={day}" / "part.parquet"
        try:
            arch = _archive_day(day)
        except (FileNotFoundError, KeyError):
            stats["skipped_no_archive"] += 1
            continue
        loc = pl.read_parquet(out_file)
        only_local = sorted(set(loc["symbol"].to_list()) - set(arch["symbol"].to_list()))
        merged = arch
        if only_local:
            stats["kept_local_only_rows"] += len(only_local)
            if len(stats["local_only_samples"]) < 5:
                stats["local_only_samples"].append({"date": day, "symbols": only_local[:5]})
            merged = pl.concat([arch, loc.filter(pl.col("symbol").is_in(only_local))],
                               how="vertical")
        merged = merged.sort("symbol")
        if not apply:
            stats["written"] += 1
            continue
        bak = backup_dir / f"date={day}" / "part.parquet"
        try:
            if not bak.exists():
                bak.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(out_file, bak)
            from app.quant.archive_fill import _write_partition_atomic

            _write_partition_atomic(out_file, merged)
            stats["written"] += 1
        except Exception as exc:
            stats["errors"].append(f"{day}: {exc}")
    return stats


def _baostock_raw_etf(symbols: list[str], start: str, end: str) -> pl.DataFrame:
    """baostock 不复权 ETF 日线（OHLC），逐标的取整段。"""
    import baostock as bs

    bs.login()
    frames: list[pl.DataFrame] = []
    try:
        for i, sym in enumerate(symbols, 1):
            code, mk = sym.split(".")
            rs = bs.query_history_k_data_plus(
                f"{'sh' if mk == 'SH' else 'sz'}.{code}", "date,open,high,low,close",
                start_date=start, end_date=end, frequency="d", adjustflag="3")
            if rs.error_code != "0":
                continue
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            rows = [r for r in rows if all(r[1:])]
            if rows:
                frames.append(pl.DataFrame({
                    "symbol": [sym] * len(rows),
                    "date": [r[0] for r in rows],
                    "o_raw": [float(r[1]) for r in rows],
                    "h_raw": [float(r[2]) for r in rows],
                    "l_raw": [float(r[3]) for r in rows],
                    "c_raw": [float(r[4]) for r in rows],
                }))
            if i % 50 == 0:
                print(f"  baostock {i}/{len(symbols)}", file=sys.stderr, flush=True)
    finally:
        bs.logout()
    if not frames:
        return pl.DataFrame(schema={"symbol": pl.String, "date": pl.String,
                                    "o_raw": pl.Float64, "h_raw": pl.Float64,
                                    "l_raw": pl.Float64, "c_raw": pl.Float64})
    return pl.concat(frames)


def _etf_day_frame(day: str, columns: list[str]) -> pl.DataFrame | None:
    """读 ETF 日线分区，补上 ``date`` 列（ETF 分区按目录分日、无 date 列）。"""
    f = DATA / "kline_etf_daily" / f"date={day}" / "part.parquet"
    if not f.exists():
        return None
    df = pl.read_parquet(f, columns=columns)
    return df.with_columns(pl.lit(day).str.to_date().alias("date"))


def detect_etf(start: str, end: str, n_sym: int, seed: int = 42) -> dict:
    """抽样检测 ETF 日线口径（本机 vs baostock 不复权）。"""
    import random

    days = sorted(f.parent.name.split("=")[1] for f in DATA.glob("kline_etf_daily/date=*/part.parquet"))
    days = [d for d in days if start <= d <= end]
    frames = [fr for fr in (_etf_day_frame(d, ["symbol", "close"]) for d in days) if fr is not None]
    store = pl.concat(frames)
    syms = sorted(set(store["symbol"].to_list()))
    random.seed(seed)
    picked = sorted(random.sample(syms, min(n_sym, len(syms))))
    raw = _baostock_raw_etf(picked, start, end).with_columns(pl.col("date").str.to_date())
    j = store.filter(pl.col("symbol").is_in(picked)).join(raw, on=["symbol", "date"], how="inner")
    j = j.filter(pl.col("c_raw") > 0).with_columns(
        (pl.col("close") / pl.col("c_raw") - 1).abs().alias("dev"))
    bad = j.filter(pl.col("dev") > RAW_TOL)
    per_sym = bad.group_by("symbol").len().sort("len", descending=True)
    return {
        "symbols_sampled": len(picked), "cells": j.height,
        "bad_cells": bad.height,
        "bad_frac": round(bad.height / j.height, 4) if j.height else None,
        "bad_symbols": per_sym.height,
        "worst_symbols": [
            {"symbol": r["symbol"], "cells": r["len"]}
            for r in per_sym.head(8).iter_rows(named=True)
        ],
    }


def repair_etf(start: str, end: str, *, apply: bool, backup_dir: Path) -> dict:
    """用 baostock 不复权 ETF 日线修正被前复权污染的**价格列**（不删不改行）。"""
    days = sorted(f.parent.name.split("=")[1]
                  for f in DATA.glob("kline_etf_daily/date=*/part.parquet"))
    days = [d for d in days if start <= d <= end]
    out: dict = {"days": len(days), "written": 0, "cells_fixed": 0,
                 "symbols_fixed": 0, "errors": []}
    if not days:
        return out
    frames = [fr for fr in (_etf_day_frame(d, ["symbol", "close"]) for d in days) if fr is not None]
    store = pl.concat(frames)
    syms = sorted(set(store["symbol"].to_list()))
    print(f"[etf] {len(days)} 天, {len(syms)} 只标的 → baostock 回源", file=sys.stderr, flush=True)
    raw = _baostock_raw_etf(syms, start, end).with_columns(pl.col("date").str.to_date())
    # 只保留"本机与原始价不一致"的单元格（前复权污染），避免无谓改写
    cmp = store.join(raw, on=["symbol", "date"], how="inner").filter(pl.col("c_raw") > 0)
    cmp = cmp.with_columns((pl.col("close") / pl.col("c_raw") - 1).abs().alias("dev"))
    fix = cmp.filter(pl.col("dev") > RAW_TOL)
    out["cells_fixed"] = fix.height
    out["symbols_fixed"] = fix["symbol"].n_unique() if fix.height else 0
    if not fix.height:
        return out
    by_day = {d: fix.filter(pl.col("date") == d) for d in fix["date"].unique().to_list()}
    for day_date, patch in by_day.items():
        day_key = str(day_date)
        out_file = DATA / "kline_etf_daily" / f"date={day_key}" / "part.parquet"
        df = pl.read_parquet(out_file)
        merged = (df.join(patch.select("symbol", "o_raw", "h_raw", "l_raw", "c_raw"),
                          on="symbol", how="left")
                  .with_columns([
                      pl.when(pl.col("c_raw").is_not_null()).then(pl.col("o_raw")).otherwise(pl.col("open")).alias("open"),
                      pl.when(pl.col("c_raw").is_not_null()).then(pl.col("h_raw")).otherwise(pl.col("high")).alias("high"),
                      pl.when(pl.col("c_raw").is_not_null()).then(pl.col("l_raw")).otherwise(pl.col("low")).alias("low"),
                      pl.when(pl.col("c_raw").is_not_null()).then(pl.col("c_raw")).otherwise(pl.col("close")).alias("close"),
                  ])
                  .select(df.columns))
        if not apply:
            out["written"] += 1
            continue
        bak = backup_dir / f"date={day_key}" / "part.parquet"
        try:
            if not bak.exists():
                bak.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(out_file, bak)
            from app.quant.archive_fill import _write_partition_atomic

            _write_partition_atomic(out_file, merged)
            out["written"] += 1
        except Exception as exc:
            out["errors"].append(f"{day_key}: {exc}")
    return out


def _minute_suspects(days: list[str], tol: float = 0.005) -> set[str]:
    """本地预筛：日线 close 与同日分钟最后一根 close 差 > tol 的标的（分钟线是原始价）。"""
    suspects: set[str] = set()
    for day in days:
        mp = DATA / "kline_minute" / f"date={day}" / "part.parquet"
        dp = DATA / "kline_daily" / f"date={day}" / "part.parquet"
        if not mp.exists() or not dp.exists():
            continue
        d = pl.read_parquet(dp, columns=["symbol", "close"])
        m = (pl.read_parquet(mp, columns=["symbol", "close"])
             .group_by("symbol").agg(pl.col("close").last().alias("mc")))
        j = d.join(m, on="symbol", how="inner")
        bad = j.filter((pl.col("close") - pl.col("mc")).abs() > tol * pl.col("mc").abs())
        suspects.update(bad["symbol"].to_list())
    return suspects


def repair_stock_baostock(start: str, end: str, *, apply: bool, backup_dir: Path,
                          prefilter: bool = True) -> dict:
    """用 baostock 不复权日线修正**残留**的前复权污染（只改价格列、不删行）。

    与 ``repair``（整分区用归档重建）互补：适用于归档覆盖不到的日期段 / 由其它
    qfq 链路（如 ``alt_daily`` 腾讯 qfq 补洞）写进来的零散单元格。
    """
    days = sorted(p.name.split("=")[1]
                  for p in DATA.glob("kline_daily/date=*")
                  if start <= p.name.split("=")[1] <= end)
    out: dict = {"days": len(days), "written": 0, "cells_fixed": 0,
                 "symbols_fixed": 0, "symbols_checked": 0, "errors": []}
    if not days:
        return out
    syms = sorted({s for d in days
                   for s in pl.read_parquet(DATA / "kline_daily" / f"date={d}" / "part.parquet",
                                            columns=["symbol"])["symbol"].to_list()})
    if prefilter:
        suspects = _minute_suspects(days)
        print(f"[stock] 分钟线预筛出可疑标的 {len(suspects)}/{len(syms)}",
              file=sys.stderr, flush=True)
        if suspects:
            syms = sorted(suspects)
    out["symbols_checked"] = len(syms)
    raw = _baostock_raw_etf(syms, start, end).with_columns(pl.col("date").str.to_date())
    store = pl.concat([
        pl.read_parquet(DATA / "kline_daily" / f"date={d}" / "part.parquet",
                        columns=["symbol", "date", "close"])
        for d in days
    ], how="vertical")
    cmp = store.join(raw, on=["symbol", "date"], how="inner").filter(pl.col("c_raw") > 0)
    cmp = cmp.with_columns((pl.col("close") / pl.col("c_raw") - 1).abs().alias("dev"))
    fix = cmp.filter(pl.col("dev") > RAW_TOL)
    out["cells_fixed"] = fix.height
    out["symbols_fixed"] = fix["symbol"].n_unique() if fix.height else 0
    if not fix.height:
        return out
    by_day = {d: fix.filter(pl.col("date") == d) for d in fix["date"].unique().to_list()}
    for day_date, patch in by_day.items():
        day_key = str(day_date)
        out_file = DATA / "kline_daily" / f"date={day_key}" / "part.parquet"
        df = pl.read_parquet(out_file)
        merged = (df.join(patch.select("symbol", "o_raw", "h_raw", "l_raw", "c_raw"),
                          on="symbol", how="left")
                  .with_columns([
                      pl.when(pl.col("c_raw").is_not_null()).then(pl.col("o_raw")).otherwise(pl.col("open")).alias("open"),
                      pl.when(pl.col("c_raw").is_not_null()).then(pl.col("h_raw")).otherwise(pl.col("high")).alias("high"),
                      pl.when(pl.col("c_raw").is_not_null()).then(pl.col("l_raw")).otherwise(pl.col("low")).alias("low"),
                      pl.when(pl.col("c_raw").is_not_null()).then(pl.col("c_raw")).otherwise(pl.col("close")).alias("close"),
                  ])
                  .select(df.columns))
        if not apply:
            out["written"] += 1
            continue
        bak = backup_dir / f"date={day_key}" / "part.parquet"
        try:
            if not bak.exists():
                bak.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(out_file, bak)
            from app.quant.archive_fill import _write_partition_atomic

            _write_partition_atomic(out_file, merged)
            out["written"] += 1
        except Exception as exc:
            out["errors"].append(f"{day_key}: {exc}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="日线复权口径检测 / 修复")
    ap.add_argument("--detect", action="store_true", help="抽样检测口径（无副作用）")
    ap.add_argument("--repair", action="store_true", help="用归档重写为原始价")
    ap.add_argument("--etf-detect", action="store_true", help="抽样检测 ETF 日线口径")
    ap.add_argument("--etf-repair", action="store_true", help="用 baostock 修正 ETF 价格列")
    ap.add_argument("--baostock-repair", action="store_true",
                    help="用 baostock 修正股票日线残留污染（先分钟线预筛）")
    ap.add_argument("--from", dest="start", default="2023-04-19")
    ap.add_argument("--to", dest="end", default="2023-12-20")
    ap.add_argument("--samples", type=int, default=250, help="检测抽样标的数")
    ap.add_argument("--apply", action="store_true", help="真正落盘（否则 dry-run）")
    ap.add_argument("--backup-dir", default=str(DEFAULT_BACKUP))
    args = ap.parse_args()

    out: dict = {}
    if args.detect:
        out["detect"] = detect(args.start, args.end, args.samples)
    if args.repair:
        out["repair"] = repair(args.start, args.end, apply=args.apply,
                               backup_dir=Path(args.backup_dir))
    if args.etf_detect:
        out["etf_detect"] = detect_etf(args.start, args.end, args.samples)
    if args.etf_repair:
        out["etf_repair"] = repair_etf(args.start, args.end, apply=args.apply,
                                       backup_dir=Path(args.backup_dir) / "etf")
    if args.baostock_repair:
        out["baostock_repair"] = repair_stock_baostock(
            args.start, args.end, apply=args.apply,
            backup_dir=Path(args.backup_dir) / "residual")
    if not any((args.detect, args.repair, args.etf_detect, args.etf_repair,
                args.baostock_repair)):
        ap.print_help()
        return 2
    print(json.dumps(out, ensure_ascii=False, indent=2))
    errs = (out.get("repair", {}).get("errors")
            or out.get("etf_repair", {}).get("errors")
            or out.get("baostock_repair", {}).get("errors") or [])
    return 1 if errs else 0


if __name__ == "__main__":
    raise SystemExit(main())
