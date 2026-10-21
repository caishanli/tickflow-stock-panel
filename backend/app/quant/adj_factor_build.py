"""从归档日线重建股票前复权因子表（归档版，**非权威源**）。

用途：归档（2026-04-18 快照）里的 `adj_factor` 列也能重建因子表，锚点是
归档末日（2026-04-17）。**生产加载不用它** —— 权威源是
`data/adj_factor_baostock/`（baostock 回源，锚点更新、无舍入噪声）。
本脚本产物落 `data/adj_factor_archive/`（该目录**不被**服务端因子加载扫描），
避免与权威源混源（不同锚点混用会产生伪跳变，见 sources._load_adj_factors）。

## 口径（2026-10-20 实测确认，见 docs/2026-10-20-archive-import-feasibility.md）

- **归档日线的 close 是原始价**（未复权）。证据：平安银行 2025-06-12 分红除权，
  原始价 11.85 → 11.68 跳空 -1.43%；归档自带 `pre_close` = 前日 close（同为原始口径）。
- **归档的 `adj_factor` 列是累计因子**，随时间递增（000001.SZ：2019 年 108.03 →
  2026-04 的 134.58），在除权日跳变（+4.0037 / +2.7916）。
- 本机契约（`adj_factor_etf/all.parquet` + `sources.apply_qfq_daily`）要求
  **最新锚定**：`ex_factor(最新日) = 1.0`，越往前越小，读取时 `price × ex_factor`。
  转换即：`ex_factor = adj_factor / max(adj_factor)`。
- 数值验证：11.85 × 0.949507 = 11.2517（06-11）→ 11.68 × 0.979257 = 11.4377（06-12），
  复权后序列连续且涨幅 +1.63%（真实涨幅），而非原始价的 -1.43% 跳空。

## 产物

`data/adj_factor_archive/all.parquet`，schema 与 ETF 表一致：`symbol, trade_date(date), ex_factor(f64)`。
symbol 用 tushare 口径（`000001.SZ`）；读取侧 `apply_qfq_daily` 会把因子表的聚宽码
（`.XSHG/.XSHE`）归一成 tushare 码再 join，故本表用 tushare 码亦可（join key 归一后一致）。
"""

from __future__ import annotations

import argparse
import json
import os
import zipfile
from pathlib import Path

import polars as pl

ARCHIVE_ROOT = Path("/home/caisl/A股分钟数据/4.21更新/A股分钟数据/A股日线/1d_price")


def collect_raw_factors(years: list[int], archive_root: Path = ARCHIVE_ROOT) -> pl.DataFrame:
    """从归档年度 zip 抽取 (symbol, trade_date, adj_factor) 长表。"""
    parts: list[pl.DataFrame] = []
    for y in sorted(years):
        z = archive_root / f"{y}.zip"
        if not z.exists():
            continue
        with zipfile.ZipFile(z) as zf:
            for member in sorted(n for n in zf.namelist() if n.endswith(".parquet")):
                df = pl.read_parquet(zf.open(member), columns=["code", "date", "adj_factor"])
                parts.append(
                    df.select(
                        pl.col("code").alias("symbol"),
                        pl.col("date").str.to_date("%Y%m%d").alias("trade_date"),
                        pl.col("adj_factor").cast(pl.Float64),
                    )
                )
    if not parts:
        return pl.DataFrame(schema={"symbol": pl.String, "trade_date": pl.Date, "adj_factor": pl.Float64})
    return pl.concat(parts)


def build_factor_table(raw: pl.DataFrame) -> pl.DataFrame:
    """归档累计因子 → 最新锚定 ex_factor。

    1. 归一化：ex_factor = adj_factor / 该标的的 max(adj_factor)
       （max 即该标的归档快照末日，因子最新=1.0）
    2. 逐日展开成 (symbol, trade_date, ex_factor)：因子在两个事件日之间是常数，
       所以对每个标的按日期区间填充（前值填充到下一个跳变日）
    """
    # 归一化
    norm = raw.with_columns(
        (pl.col("adj_factor") / pl.col("adj_factor").max().over("symbol")).alias("ex_factor")
    )
    # 每个标的的因子按日期递增；对每个交易日取「当日及之前最近一次记录」的因子
    # —— 归档本身每个交易日都有记录，直接用即可；缺失的交易日用前值填充
    out = norm.sort(["symbol", "trade_date"]).with_columns(
        pl.col("ex_factor").forward_fill().over("symbol")
    )
    return out.select(["symbol", "trade_date", "ex_factor"])


def _audit_breakpoints(daily_dir: Path, table: pl.DataFrame, threshold: float = 0.25) -> dict:
    """因子表质量审计：调整后序列仍有 >threshold 单日跳变的标的 = 缺口。

    口径说明（2026-10-20 实测校准）：
    - mootdx_service 用 0.2，那是给 ETF 用的（ETF 无 ±20% 涨跌幅）。
      **股票**创业板/科创板有 ±20% 涨跌幅，0.2 会把大量合法涨停误报为缺口
      （实测 0.2 → 543 个 flagged，0.25 → 136 个，0.2 那批几乎全是 ±20% 涨停）。
    - 故股票用 0.25，且把「北交所」与「上市首日」单列，不计入缺口：
      北交所（.BJ）归档无数据；次新股首日涨幅可 >100%，属合法行情。
    """
    import glob as _glob

    fmap: dict[str, dict] = {}
    for r in table.iter_rows(named=True):
        fmap.setdefault(r["symbol"], {})[r["trade_date"]] = r["ex_factor"]

    # 跨分区聚成 (symbol, date, close) 长表
    frames = []
    for path in sorted(_glob.glob(os.path.join(str(daily_dir), "date=*", "part.parquet"))):
        frames.append(pl.read_parquet(path, columns=["symbol", "date", "close"]))
    if not frames:
        return {"checked_symbol_days": 0, "flagged": [], "threshold": threshold,
                "note": "日线分区为空"}
    allrows = pl.concat(frames)
    # 只审计因子表覆盖到的日期区间（区间外无因子是数据边界，不算缺口）
    if not table.is_empty():
        lo, hi = table["trade_date"].min(), table["trade_date"].max()
        allrows = allrows.filter((pl.col("date") >= lo) & (pl.col("date") <= hi))

    flagged: list[str] = []
    bj: set[str] = set()
    first_day: set[str] = set()
    checked = 0
    for key, grp in allrows.group_by("symbol"):
        sym = key[0] if isinstance(key, tuple) else key
        s = grp.sort("date")
        closes = s["close"].drop_nulls()
        if len(closes) < 2:
            continue
        checked += 1
        frows = fmap.get(sym) or {}
        fac = pl.Series([frows.get(d, 1.0) for d in s["date"].to_list()])
        adj = closes.to_numpy() * fac.to_numpy()
        ret = (adj[1:] / adj[:-1]) - 1.0
        if not (abs(ret) > threshold).any():
            continue
        if ".BJ" in sym:
            bj.add(sym)
        elif frows and s["date"][0] == min(frows):
            # 该标的在因子表里的首个交易日 = 上市首日附近，上市初期无涨跌幅限制
            # （创业板次新首日涨幅可 >90%），属合法行情而非因子缺口
            first_day.add(sym)
        else:
            flagged.append(sym)
    return {
        "checked_symbol_days": checked,
        "flagged": sorted(set(flagged)),
        "flagged_count": len(set(flagged)),
        "excluded_bj": len(bj),
        "excluded_first_day": len(first_day),
        "threshold": threshold,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-year", type=int, default=2016)
    ap.add_argument("--to-year", type=int, default=2026)
    ap.add_argument("--out", default="/home/caisl/tickflow-stock-panel/data/adj_factor_archive/all.parquet",
                    help="输出路径。**不要**用 data/adj_factor/ —— 该目录会被服务端"
                         "因子加载扫描到，与 adj_factor_baostock 混源（不同锚点）会产生伪跳变")
    ap.add_argument("--daily-dir", default="/home/caisl/tickflow-stock-panel/data/kline_daily")
    ap.add_argument("--audit", action="store_true", help="用 20%% 跳变门槛审计因子质量")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    years = list(range(args.from_year, args.to_year + 1))
    raw = collect_raw_factors(years)
    if raw.is_empty():
        print(json.dumps({"error": "归档无数据", "years": years}, ensure_ascii=False))
        return 1

    table = build_factor_table(raw)
    stats = {
        "years": years,
        "raw_rows": raw.height,
        "symbols": table["symbol"].n_unique(),
        "table_rows": table.height,
        "date_min": str(table["trade_date"].min()),
        "date_max": str(table["trade_date"].max()),
        "out": args.out,
    }
    # 抽查
    sample = table.filter(pl.col("symbol") == "000001.SZ").sort("trade_date")
    if sample.height:
        stats["sample_000001_SZ"] = {
            "first": {"date": str(sample["trade_date"][0]), "ex_factor": round(sample["ex_factor"][0], 6)},
            "last": {"date": str(sample["trade_date"][-1]), "ex_factor": round(sample["ex_factor"][-1], 6)},
        }
    if args.audit:
        stats["audit"] = _audit_breakpoints(Path(args.daily_dir), table)
    if not args.dry_run:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        table.write_parquet(out, compression="zstd")
        stats["bytes"] = out.stat().st_size
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
