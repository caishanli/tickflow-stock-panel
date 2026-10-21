"""因子表质量与复权价伪跳变审计（独立于因子表自身口径的校验）。

reviewer 指出：as-of backward 展开在**多来源混用**时会在两个归一化锚点之间
来回跳 → 复权价出现 ±20%~134% 伪跳变（实测 300795.SZ 单日 +72.89%）。

本脚本做两件事（都用**独立判据**，不依赖因子表自身的口径）：
1. 因子表自检：每个标的的因子序列必须是**单调不减**且 ≤ 1.0（最新锚定的不变量）。
2. 复权价自检：复权后的单日涨跌幅不得超过该板块涨跌停上限
   （主板 10%、创业板/科创板 20%、北交所 30%），超出即伪跳变。

用法: uv run python -m app.quant.adj_factor_audit [--days 400]
"""

from __future__ import annotations

import argparse
import json

import polars as pl

from app.services.stockdata.sources import DataSources


def limit_pct(symbol: str) -> float:
    """该标的的日涨跌幅上限（%）。"""
    pure = symbol.split(".")[0]
    if pure.startswith(("300", "301", "688", "689")):
        return 20.0
    if pure.startswith("8") or pure.startswith("4"):
        return 30.0
    if pure.startswith("92"):
        return 30.0
    return 10.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=400, help="回看多少个交易日")
    ap.add_argument("--out", default="/tmp/opencode/adj_factor_audit.json")
    args = ap.parse_args()

    ds = DataSources()
    fac = ds.get_adj_factors()
    report: dict = {"factor_rows": fac.height, "symbols": fac["symbol"].n_unique()}

    # ---- 1. 因子表自检：单调不减 + ≤ 1.0 ----
    srt = fac.sort(["symbol", "trade_date"]).with_columns(
        pl.col("ex_factor").shift(1).over("symbol").alias("prev"))
    drops = srt.filter(pl.col("prev").is_not_null() & (pl.col("ex_factor") < pl.col("prev") - 1e-9))
    over1 = srt.filter(pl.col("ex_factor") > 1.0 + 1e-9)
    report["factor_selfcheck"] = {
        "drop_steps": drops.height,
        "drop_symbols": drops["symbol"].n_unique() if drops.height else 0,
        "over_one_symbols": over1["symbol"].n_unique() if over1.height else 0,
        "drop_samples": sorted({str(x) for x in drops["symbol"].unique().to_list()})[:10] if drops.height else [],
    }

    # ---- 2. 复权价自检：因子发生变化的当日，复权后涨跌幅应落在涨跌停内 ----
    # 判据要精确：真实暴跌（退市/风险股）也会超限，但那时因子没变、复权价=原始价，
    # 不是复权伪影。故只在「因子当日发生变化」的日期上检查。
    universe = pl.read_parquet("/home/caisl/tickflow-stock-panel/data/instruments/instruments.parquet",
                               columns=["symbol"])
    syms = universe["symbol"].unique().to_list()[:400]
    daily = ds.get_daily(syms, "2024-01-01", "2026-10-20")
    if not daily.is_empty():
        raw = daily.sort(["symbol", "date"]).with_columns(
            ((pl.col("close") / pl.col("close").shift(1).over("symbol")) - 1.0)
            .alias("raw_ret"))
        qfq = ds.apply_qfq_daily(daily).sort(["symbol", "date"]).with_columns(
            ((pl.col("close") / pl.col("close").shift(1).over("symbol")) - 1.0)
            .alias("qfq_ret"))
        merged = raw.join(qfq.select(["symbol", "date", "qfq_ret"]), on=["symbol", "date"],
                          how="left")
        # 因子变化日：与前一交易日的因子不同
        fac = ds.get_adj_factors().with_columns(
            pl.col("trade_date").cast(pl.Date).alias("date"),
            pl.col("symbol").str.replace(".XSHG", ".SH", literal=True)
            .str.replace(".XSHE", ".SZ", literal=True).alias("symbol"))
        fac = fac.sort(["symbol", "date"]).with_columns(
            pl.col("ex_factor").shift(1).over("symbol").alias("prev"))
        changed = fac.filter(pl.col("prev").is_not_null()
                             & ((pl.col("ex_factor") - pl.col("prev")).abs() > 1e-9))
        merged = merged.join(changed.select(["symbol", "date"]), on=["symbol", "date"], how="inner")
        merged = merged.with_columns(
            pl.col("symbol").map_elements(limit_pct, return_dtype=pl.Float64).alias("lim"))
        bad = merged.filter(pl.col("qfq_ret").abs() * 100 > pl.col("lim") + 0.5)
        report["qfq_jump_check"] = {
            "rows_checked": merged.height,
            "violations": bad.height,
            "violation_symbols": bad["symbol"].n_unique() if bad.height else 0,
            "worst": [
                {"symbol": r["symbol"], "date": str(r["date"]),
                 "raw_ret": round(r["raw_ret"] * 100, 2) if r["raw_ret"] is not None else None,
                 "qfq_ret": round(r["qfq_ret"] * 100, 2), "limit": r["lim"]}
                for r in bad.sort(pl.col("qfq_ret").abs(), descending=True).head(5).iter_rows(named=True)
            ],
        }
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
