"""全标的本地数据体检（数据质量审计）。

检查维度：
1. **分区结构**：逐分区行数/标的数 → 残缺分区、空壳分区
2. **价格合理性**：<=0、NaN/inf、high<low、close 越界
3. **成交一致性**：amount>0 但 volume=0、volume<0
4. **涨跌幅合理性**：超过该板块涨跌停上限（复权污染/数据错误的信号）
5. **日线 ↔ 分钟线一致性**：抽样交易日的分钟聚合 close 是否等于日线 close
6. **覆盖完整性**：instruments 里没有数据的标的、复权因子缺失的标的、
   交易日历与实际分区的日期空洞
7. **重复行**：同一 (symbol, date) 重复

用法:
    uv run python -m app.quant.data_audit                # 全量
    uv run python -m app.quant.data_audit --quick        # 跳过分钟线深扫
    uv run python -m app.quant.data_audit --out /tmp/opencode/data_audit.json
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

import polars as pl

DATA = Path("/home/caisl/tickflow-stock-panel/data")


def limit_pct(symbol: str) -> float:
    """该标的的日涨跌幅上限（%）。"""
    pure = symbol.split(".")[0]
    if pure.startswith(("300", "301", "688", "689")):
        return 20.0
    if pure.startswith(("4", "8", "92")):
        return 30.0
    return 10.0


def _daily_files(subdir: str) -> list[Path]:
    root = DATA / subdir
    if not root.is_dir():
        return []
    return sorted(root.glob("date=*/part.parquet"))


def audit_daily(subdir: str = "kline_daily") -> dict:
    """日线全量审计。"""
    files = _daily_files(subdir)
    out: dict = {"partitions": len(files), "days": []}
    if not files:
        return out
    frames = []
    per_day = []
    t0 = time.time()
    for f in files:
        day = f.parent.name.split("=")[1]
        try:
            df = pl.read_parquet(f)
        except Exception as exc:  # noqa: BLE001
            out.setdefault("read_errors", []).append(f"{day}: {exc}")
            continue
        per_day.append((day, df.height, df["symbol"].n_unique()))
        frames.append(df)
    if not frames:
        return out
    allrows = pl.concat(frames)
    out["rows"] = allrows.height
    out["symbols"] = allrows["symbol"].n_unique()
    out["elapsed_s"] = round(time.time() - t0, 1)

    # --- 逐日标的数：找残缺日（相对中位数骤降）---
    counts = sorted(n for _, _, n in per_day)
    median = counts[len(counts) // 2] if counts else 0
    thin = [(d, n) for d, _, n in per_day if median and n < median * 0.9]
    out["median_symbols_per_day"] = median
    out["thin_days"] = {"count": len(thin), "worst": sorted(thin, key=lambda x: x[1])[:10]}

    # --- 逐日 bar 数：日线每天每标的 1 行 ---
    dup = (allrows.group_by(["symbol", "date"]).len().filter(pl.col("len") > 1))
    out["duplicate_symbol_date"] = dup.height

    # --- 价格合理性 ---
    bad = allrows.filter(
        (pl.col("open") <= 0) | (pl.col("high") <= 0) | (pl.col("low") <= 0)
        | (pl.col("close") <= 0)
        | pl.col("open").is_nan() | pl.col("close").is_nan()
        | (pl.col("high") < pl.col("low"))
        | (pl.col("close") > pl.col("high") * 1.001) | (pl.col("close") < pl.col("low") * 0.999)
        | (pl.col("open") > pl.col("high") * 1.001) | (pl.col("open") < pl.col("low") * 0.999))
    out["bad_price_rows"] = bad.height
    out["bad_price_samples"] = [
        {"symbol": r["symbol"], "date": str(r["date"]), "o": r["open"], "h": r["high"],
         "l": r["low"], "c": r["close"]}
        for r in bad.head(8).iter_rows(named=True)
    ]

    # --- 成交一致性 ---
    badv = allrows.filter(
        ((pl.col("volume") < 0) | (pl.col("amount") < 0)
         | ((pl.col("amount") > 0) & (pl.col("volume") == 0))))
    out["bad_volume_rows"] = badv.height
    out["bad_volume_samples"] = [
        {"symbol": r["symbol"], "date": str(r["date"]), "vol": r["volume"], "amt": r["amount"]}
        for r in badv.head(5).iter_rows(named=True)
    ]

    # --- 涨跌幅超限（复权污染/数据错误）---
    srt = allrows.sort(["symbol", "date"]).with_columns(
        ((pl.col("close") / pl.col("close").shift(1).over("symbol")) - 1.0).alias("ret"))
    srt = srt.with_columns(
        pl.col("symbol").map_elements(limit_pct, return_dtype=pl.Float64).alias("lim"))
    over = srt.filter(pl.col("ret").is_not_null() & (pl.col("ret").abs() * 100 > pl.col("lim") + 0.5))
    out["limit_breach_rows"] = over.height
    out["limit_breach_symbols"] = over["symbol"].n_unique() if over.height else 0
    out["limit_breach_worst"] = [
        {"symbol": r["symbol"], "date": str(r["date"]),
         "ret%": round(r["ret"] * 100, 2), "limit%": r["lim"]}
        for r in over.sort(pl.col("ret").abs(), descending=True).head(8).iter_rows(named=True)
    ]
    return out


def audit_minute(subdir: str = "kline_minute") -> dict:
    """分钟线结构审计：逐分区行数/标的数 + 每标的 bar 数分布。"""
    files = _daily_files(subdir)
    out: dict = {"partitions": len(files), "thin": [], "bars_hist": {}}
    if not files:
        return out
    t0 = time.time()
    counts = []
    for f in files:
        day = f.parent.name.split("=")[1]
        try:
            df = pl.read_parquet(f, columns=["symbol", "datetime"])
        except Exception as exc:  # noqa: BLE001
            out.setdefault("read_errors", []).append(f"{day}: {exc}")
            continue
        n_sym = df["symbol"].n_unique()
        counts.append((day, df.height, n_sym))
    out["elapsed_s"] = round(time.time() - t0, 1)
    if not counts:
        return out
    rows = sorted(r for _, r, _ in counts)
    syms = sorted(n for _, _, n in counts)
    out["rows_total"] = sum(r for _, r, _ in counts)
    out["rows_min"] = rows[0]
    out["rows_median"] = rows[len(rows) // 2]
    out["symbols_min"] = syms[0]
    out["symbols_median"] = syms[len(syms) // 2]
    med_rows = out["rows_median"]
    out["thin_partitions"] = [
        {"day": d, "rows": r, "symbols": s}
        for d, r, s in sorted(counts, key=lambda x: x[1])[:10]
        if r < med_rows * 0.5
    ]
    out["thin_partition_count"] = sum(1 for _, r, _ in counts if r < med_rows * 0.5)
    return out


def audit_minute_vs_daily(sample_days: int = 5) -> dict:
    """日线 ↔ 分钟线一致性：抽样交易日，分钟聚合 close 是否等于日线 close。"""
    daily_files = _daily_files("kline_daily")
    minute_files = {f.parent.name.split("=")[1]: f for f in _daily_files("kline_minute")}
    if not daily_files or not minute_files:
        return {"skipped": "缺分区"}
    common = sorted(set(minute_files) & {f.parent.name.split("=")[1] for f in daily_files})
    if not common:
        return {"skipped": "无重叠日期"}
    step = max(1, len(common) // sample_days)
    picked = common[::step][:sample_days]
    out = {"days": [], "mismatch": 0, "compared": 0}
    for day in picked:
        d = pl.read_parquet(dict((f.parent.name.split("=")[1], f) for f in daily_files)[day])
        m = pl.read_parquet(minute_files[day])
        agg = (m.group_by("symbol").agg(pl.col("close").last().alias("m_close"),
                                       pl.len().alias("bars"))
               .rename({"symbol": "symbol"}))
        j = d.join(agg, on="symbol", how="inner")
        if j.height == 0:
            out["days"].append({"day": day, "note": "无重叠标的"})
            continue
        diff = (j["close"] - j["m_close"]).abs()
        n_bad = int((diff > 0.011).sum())
        out["compared"] += j.height
        out["mismatch"] += n_bad
        out["days"].append({
            "day": day, "symbols": j.height, "mismatch": n_bad,
            "max_diff": round(float(diff.max()), 4),
            "bars_min": int(j["bars"].min()), "bars_max": int(j["bars"].max()),
            "bars_median": int(j["bars"].median()),
        })
    return out


def audit_coverage() -> dict:
    """覆盖完整性：标的→数据、因子；交易日历 vs 实际分区。"""
    out: dict = {}
    inst = pl.read_parquet(DATA / "instruments" / "instruments.parquet")
    universe = set(inst["symbol"].to_list())

    daily = pl.read_parquet(DATA / "kline_daily" / "date=2026-10-21" / "part.parquet",
                            columns=["symbol"]) if (DATA / "kline_daily" / "date=2026-10-21").exists() else None
    if daily is not None:
        have = set(daily["symbol"].to_list())
        out["daily_missing_symbols"] = {
            "count": len(universe - have),
            "sample": sorted(universe - have)[:10],
        }
        out["daily_extra_symbols"] = {
            "count": len(have - universe),
            "sample": sorted(have - universe)[:10],
        }
    # 因子覆盖
    fac = pl.read_parquet(DATA / "adj_factor_baostock" / "all.parquet", columns=["symbol"])
    covered = set(fac["symbol"].unique().to_list())
    out["factor_missing_stocks"] = {
        "count": len(universe - covered),
        "sample": sorted(universe - covered)[:10],
        "by_market": {m: sum(1 for s in (universe - covered) if s.endswith(m))
                      for m in (".SZ", ".SH", ".BJ")},
    }
    # 交易日历 vs 实际分区
    cal_path = DATA / "trade_calendar.json"
    if cal_path.exists():
        cal = json.loads(cal_path.read_text())
        days = cal.get("dates") or cal.get("trade_dates") or []
        if isinstance(days, list) and days and isinstance(days[0], str):
            cal_days = {d[:10] for d in days}
        else:
            cal_days = {str(d)[:10] for d in days}
        parts = {f.parent.name.split("=")[1] for f in _daily_files("kline_daily")}
        missing = sorted(cal_days - parts)
        out["calendar"] = {
            "trade_days": len(cal_days), "partitions": len(parts),
            "missing_partitions": len(missing),
            "missing_sample": missing[:10],
            "extra_partitions": len(parts - cal_days),
        }
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="跳过分钟线深扫")
    ap.add_argument("--out", default="/tmp/opencode/data_audit.json")
    args = ap.parse_args()

    report: dict = {"daily": audit_daily()}
    if not args.quick:
        report["minute"] = audit_minute()
    report["minute_vs_daily"] = audit_minute_vs_daily()
    report["coverage"] = audit_coverage()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
