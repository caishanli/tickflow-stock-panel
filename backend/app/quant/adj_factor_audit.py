"""因子表质量与复权价伪跳变审计（独立于因子表自身口径的校验）。

reviewer 指出：as-of backward 展开在**多来源混用**时会在两个归一化锚点之间
来回跳 → 复权价出现 ±20%~134% 伪跳变（实测 300795.SZ 单日 +72.89%）。

本脚本做两件事（都用**独立判据**，不依赖因子表自身的口径）：
1. 因子表自检：每个标的的因子序列必须是**单调不减**且 ≤ 1.0（最新锚定的不变量）。
2. 复权价自检：复权后的单日涨跌幅不得超过该板块涨跌停上限
   （主板 10%、创业板/科创板 20%、北交所 30%），超出即伪跳变。

**检查的日期集合**（关键）：除「因子相对上一行发生变化」的日期外，**必须包含
每个标的的首个因子行**——因子表覆盖首日是伪跳变最容易出现的位置（覆盖前的
bar 与覆盖后的 bar 用的是两套因子；早期实现给覆盖前填 1.0，实测 ETF
159971.SZ 在 2025-06-30 单日 −49.2%）。只查「变化日」会漏掉这一类。

**抽样**：股票 universe 按步长均匀取样（不取前 N 只——那会集中在同一板块），
再叠加因子表里**非股票**的标的（ETF），因为 ETF 走的是 mootdx 低质量源、
是伪跳变的高发区。

用法: uv run python -m app.quant.adj_factor_audit [--days 400]
"""

from __future__ import annotations

import argparse
import json
import os

import polars as pl

from app.services.stockdata import adj_factor_quality as _adjq
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


def _raw_factor_table(ds: DataSources) -> pl.DataFrame:
    """**未隔离**的候选因子表（同样的来源分离/优先级规则，但不做质量隔离）。

    审计必须看候选数据：被隔离的标的正是要审计的对象，用隔离后的表审计会
    「自己盖自己的章」（永远 0 违规）。
    """
    return ds._load_adj_factors(quarantine=False)


def _sample_symbols(ds: DataSources, fac: pl.DataFrame, limit: int) -> list[str]:
    """股票 universe 均匀抽样 + 因子表里的非股票标的（ETF）。"""
    inst = os.path.join(ds.data_root, "instruments", "instruments.parquet")
    stocks: list[str] = []
    if os.path.exists(inst):
        stocks = sorted({str(s) for s in
                         pl.read_parquet(inst, columns=["symbol"])["symbol"].to_list()})
    picks: list[str] = []
    if stocks:
        step = max(1, len(stocks) // max(limit // 2, 1))
        picks = stocks[::step][: limit // 2]
    if not fac.is_empty():
        etf = sorted(set(fac["symbol"].unique().to_list()) - set(stocks))
        picks += etf[: limit - len(picks)]
    return picks


def _violation_symbols(report: dict) -> list[str]:
    """报告里的违规标的集合（写隔离名单用）。"""
    return list(report.get("qfq_jump_check", {}).get("violation_symbol_list", []))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=400, help="回看多少个交易日")
    ap.add_argument("--limit", type=int, default=400, help="抽样标的数")
    ap.add_argument("--out", default="", help="报告输出路径（默认 <data_root>/adj_factor_audit.json）")
    ap.add_argument("--write-quarantine", action="store_true",
                    help="把「复权后单日涨跌超涨跌停」的标的写入隔离名单"
                         "（<data_root>/adj_factor_quarantine.txt），加载时按原始价计算")
    ap.add_argument("--full", action="store_true",
                    help="全量扫描（不抽样）——写隔离名单时建议加")
    args = ap.parse_args()

    ds = DataSources()
    fac = ds.get_adj_factors()          # 线上生效的表（已隔离）
    raw_fac = _raw_factor_table(ds)      # 原始表（未隔离）——审计用
    report: dict = {
        "factor_rows": fac.height,
        "symbols": fac["symbol"].n_unique(),
        "raw_factor_rows": raw_fac.height,
        "raw_symbols": raw_fac["symbol"].n_unique() if not raw_fac.is_empty() else 0,
        "quarantined_by_list": sorted(_adjq.load_quarantine(ds.data_root)),
    }

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

    # ---- 2. 复权价自检：因子变化日 / 覆盖首日，复权后涨跌幅应落在涨跌停内 ----
    # 判据要精确：真实暴跌（退市/风险股）也会超限，但那时因子没变、复权价=原始价，
    # 不是复权伪影。故只在「因子发生变化」或「因子覆盖首日」这些日期上检查。
    if args.full:
        # 用**候选表**的标的（隔离名单里的也在内——它们正是要审计的对象）
        syms = sorted(set(raw_fac["symbol"].unique().to_list()))
    else:
        syms = _sample_symbols(ds, raw_fac, args.limit)
    daily = ds.get_daily(syms, "2024-01-01", "2026-10-20")
    if not daily.is_empty():
        raw = daily.sort(["symbol", "date"]).with_columns(
            ((pl.col("close") / pl.col("close").shift(1).over("symbol")) - 1.0)
            .alias("raw_ret"),
            # bar_idx = 该标的的第几根 K 线（1 起）：上市初期无涨跌幅限制
            pl.col("symbol").cum_count().over("symbol").alias("bar_idx"))
        # 用**原始**因子表算复权价（隔离后的表里这些标的已是原始价 → 测不出问题）
        qfq = _adjq.apply_qfq(raw_fac, daily).sort(["symbol", "date"]).with_columns(
            ((pl.col("close") / pl.col("close").shift(1).over("symbol")) - 1.0)
            .alias("qfq_ret"))
        merged = raw.join(qfq.select(["symbol", "date", "qfq_ret"]), on=["symbol", "date"],
                          how="left")
        # 因子变化日：与前一交易日/前一行的因子不同；**外加每标的的首个因子行**
        # （覆盖首日 = 伪跳变高发位）
        fac2 = raw_fac.with_columns(
            pl.col("trade_date").cast(pl.Date).alias("date"),
            pl.col("symbol").str.replace(".XSHG", ".SH", literal=True)
            .str.replace(".XSHE", ".SZ", literal=True).alias("symbol"))
        fac2 = fac2.sort(["symbol", "date"]).with_columns(
            pl.col("ex_factor").shift(1).over("symbol").alias("prev"))
        changed = fac2.filter(
            (pl.col("prev").is_null() & pl.col("ex_factor").is_not_null())
            | ((pl.col("ex_factor") - pl.col("prev")).abs() > 1e-9))
        merged = merged.join(changed.select(["symbol", "date"]), on=["symbol", "date"], how="inner")
        # 排除上市初期：次新股上市前 5 个交易日无涨跌幅限制（首日可 +100%），
        # 那些天的复权价变动是**真实行情**，不是因子伪影。
        merged = merged.filter(pl.col("bar_idx") >= 5)
        merged = merged.with_columns(
            pl.col("symbol").map_elements(limit_pct, return_dtype=pl.Float64).alias("lim"))
        bad = merged.filter(pl.col("qfq_ret").abs() * 100 > pl.col("lim") + 0.5)
        report["qfq_jump_check"] = {
            "violation_symbol_list": sorted({str(x) for x in bad["symbol"].unique().to_list()}),
            "sampled_symbols": len(syms),
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
    if args.write_quarantine:
        qpath = os.path.join(ds.data_root, "adj_factor_quarantine.txt")
        with open(qpath, "w", encoding="utf-8") as fh:
            fh.write("# 前复权因子隔离名单（adj_factor_audit --write-quarantine 生成）\n")
            fh.write("# 名单内标的按**原始价**计算：其因子序列会让复权价出现\n")
            fh.write("# 超涨跌停的假跳变（源数据质量问题）。删除本行即可恢复。\n")
            for sym in _violation_symbols(report):
                fh.write(sym + "\n")
        print(f"隔离名单已写入: {qpath}（{len(_violation_symbols(report))} 只标的）")
    out = args.out or os.path.join(ds.data_root, "adj_factor_audit.json")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n报告已写入: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
