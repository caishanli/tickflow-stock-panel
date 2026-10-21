"""前复权因子表的**来源优先级 / 口径 / 质量闸门**（服务端与客户端共用）。

服务端（``app/services/stockdata/sources.py``）与客户端（``app/quant/
jqengine/datasource/manager.py``）都要加载同一张因子表。若两侧的规则各写
一份，很容易漂移成"服务端认为权威源是 A、客户端认为是 B"——口径一旦不同，
同一天的复权价就会差一个常数（实测 0.5 vs 0.9 = 44%），回测与实盘对不上。

本模块是**唯一规则源**，两侧都调用它：

1. :func:`normalize_symbol` —— 代码归一（聚宽码 .XSHG/.XSHE → tushare 码
   .SH/.SZ；北交所 .BJ 保持 .BJ）。不同来源的 symbol 口径不同，不归一则
   来源比较/去重全都不中（分离规则静默失效）。
2. :data:`SOURCE_PRIORITY` —— 来源优先级（**靠前 = 权威**）。同一
   (symbol, date) 多源共存时取靠前者。
3. :func:`merge_sources` —— 按优先级合并多来源（同 key 取权威源）。
4. :func:`quarantine_bad_series` —— 质量闸门：因子是**累计**量，出现明显
   下降 = 源数据有伪跳变（mootdx cliff 合成），复权价会在那天凭空出现一次
   涨跌。下降幅度超阈值的标的整段隔离（按原始价算），并记录告警。
"""

from __future__ import annotations

import itertools
import os

import polars as pl

# 因子来源优先级（**靠前 = 权威**）：
# - adj_factor_baostock：baostock foreAdjustFactor 回源，股票因子的唯一权威源
#   （最新锚定、单调递增、已审计）。
# - adj_factor：旧部署遗留表，优先级最低（只在前者缺失时兜底）。
# - adj_factor_etf：mootdx xdxr 重建（已 quarantine），只保留其非股票标的行。
SOURCE_PRIORITY = ("adj_factor_baostock", "adj_factor", "adj_factor_etf")

# 因子序列「假跳变」隔离阈值：累计因子的单次下降幅度超过它 = 源数据有伪跳变。
JUMP_QUARANTINE = 0.10


def normalize_symbol(code: str) -> str:
    """平台代码(.XSHG/.XSHE/.SH/.SZ/.BJ) → 分区符号(.SH/.SZ/.BJ)。

    北交所保持 ``.BJ``——早先的实现把它映射成 .SZ，会让北交所码与深市码
    混在同一集合里比较（因子来源去重、股票 universe 匹配都可能误判）。
    """
    pure, _, suf = str(code).rpartition(".")
    if not pure:
        return str(code)
    up = suf.upper()
    if up in ("XSHG", "SH", "SS"):
        return pure + ".SH"
    if up in ("XSHE", "SZ"):
        return pure + ".SZ"
    if up in ("BJ", "XBSE"):
        return pure + ".BJ"
    return str(code)


def normalized_symbols(df: pl.DataFrame) -> set[str]:
    """帧内标的集合（**归一化**后）。"""
    if df is None or df.is_empty():
        return set()
    return {normalize_symbol(str(s)) for s in df["symbol"].unique().to_list()}


def merge_sources(frames: list[tuple[str, pl.DataFrame]]) -> pl.DataFrame:
    """按来源优先级合并多份因子表（同一 (symbol, date) 取**权威源**）。

    ``frames`` = [(来源名, 帧), ...]（顺序无关，优先级由 :data:`SOURCE_PRIORITY`
    决定）。返回 [symbol, trade_date, ex_factor]（symbol 已归一）。
    """
    usable = [(name, df) for name, df in frames
              if df is not None and not df.is_empty()]
    if not usable:
        return pl.DataFrame()
    ordered = sorted(
        usable,
        key=lambda item: (SOURCE_PRIORITY.index(item[0])
                          if item[0] in SOURCE_PRIORITY else len(SOURCE_PRIORITY)),
    )
    parts = []
    for prio, (_name, df) in enumerate(ordered):
        parts.append(df.with_columns(pl.lit(prio).alias("__prio")))
    out = pl.concat(parts, how="vertical_relaxed")
    # 同 key 取优先级最高（__prio 最小）的那行；maintain_order 保证排序稳定
    out = (out.sort(["symbol", "trade_date", "__prio"], maintain_order=True)
              .unique(subset=["symbol", "trade_date"], keep="first")
              .drop("__prio"))
    return out


def bad_series_symbols(series: dict, threshold: float = JUMP_QUARANTINE) -> bool:
    """单个标的的因子序列是否存在「假跳变」（下降幅度超阈值）。

    ``series`` = {date: factor}（任意可排序的日期键）。
    """
    vals = [float(series[d]) for d in sorted(series)]
    return any(prev - cur > threshold
               for prev, cur in itertools.pairwise(vals))


QUARANTINE_FILE = "adj_factor_quarantine.txt"


def load_quarantine(data_root: str) -> set[str]:
    """读取**离线审计产出的隔离名单**（``<data_root>/adj_factor_quarantine.txt``）。

    名单里的标的整段不参与复权（按原始价计算）。用于隔离**价格口径才能判定**
    的伪跳变：因子跳变与价格不匹配（复权后单日涨跌超涨跌停）——这类问题只有
    拿日线比价才知道，没法在因子表内部判定。

    名单文件是**显式产物**（``python -m app.quant.adj_factor_audit
    --write-quarantine`` 生成），不存在 = 不隔离任何标的。
    """
    path = os.path.join(data_root, QUARANTINE_FILE)
    if not os.path.exists(path):
        return set()
    out: set[str] = set()
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                out.add(normalize_symbol(s))
    except OSError:
        return set()
    return out


def apply_quarantine(fac: pl.DataFrame, banned: set[str]) -> pl.DataFrame:
    """把隔离名单里的标的从因子表里摘掉（该标的按原始价计算）。"""
    if fac is None or fac.is_empty() or not banned:
        return fac
    return fac.filter(~pl.col("symbol").map_elements(
        normalize_symbol, return_dtype=pl.String).is_in(banned))


def quarantine_bad_series(fac: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """因子表质量闸门：隔离「假跳变」序列，返回 (过滤后的表, 报告)。

    前复权因子是**累计**量：最新锚定时应单调不减、末值 = 1.0。序列出现明显
    下降 = 源数据有伪跳变（mootdx cliff 合成），复权价会在那天凭空出现一次
    涨跌（实测 159971.SZ 2026-06-15 因子 1.20 → 1.00 = 复权价 −16.7% 假跌）。

    处置：下降幅度 > :data:`JUMP_QUARANTINE` 的标的整段隔离（该标的按**原始价**
    计算——宁可保留除权当日的真实跳空，也不注入伪跳变），并把标的数/样本写进
    报告（**不做静默处理**）。
    末值 != 1.0（未最新锚定）只统计不隔离：口径差异由回测桥的锚点对齐
    （``DataManager._apply_qfq``）吸收，不影响同一口径内的收益率。
    """
    report: dict = {"rows": fac.height if fac is not None else 0,
                    "symbols": 0, "quarantined": [], "not_anchored": 0}
    if fac is None or fac.is_empty():
        return (fac if fac is not None else pl.DataFrame()), report
    report["symbols"] = fac["symbol"].n_unique()
    srt = fac.sort(["symbol", "trade_date"]).with_columns(
        pl.col("ex_factor").shift(1).over("symbol").alias("prev"))
    bad = srt.filter(pl.col("prev").is_not_null()
                     & ((pl.col("prev") - pl.col("ex_factor")) > JUMP_QUARANTINE))
    if bad.height:
        bad_syms = sorted({str(x) for x in bad["symbol"].unique().to_list()})
        report["quarantined"] = bad_syms
        fac = fac.filter(~pl.col("symbol").is_in(bad_syms))
    last = (fac.sort(["symbol", "trade_date"]).group_by("symbol")
            .agg(pl.col("ex_factor").last().alias("f_last")))
    report["not_anchored"] = last.filter(
        (pl.col("f_last") - 1.0).abs() > 1e-6).height
    return fac, report


def apply_qfq(fac: pl.DataFrame, df: pl.DataFrame) -> pl.DataFrame:
    """按因子表给日线帧应用**最新锚定前复权**（price × ex_factor(date)）。

    服务端 ``DataSources.apply_qfq_daily`` 与离线审计共用本实现（口径唯一源）。

    - **as-of backward join**（而非精确 join）：因子表的两种表示都能用 ——
      逐日表与**事件行表**（step function 的稀疏表示：基准行 + 变点）。
    - **因子表起始日之前**：回填该标的**首个**因子值（不是 1.0）。因子是累计
      量，首行值已含全部历史事件；填 1.0 会在覆盖首日凭空出现一次跳变
      （实测 ETF 159971.SZ 2025-06-30 单日 −49.2%）。
    """
    if df is None or df.is_empty() or "symbol" not in df.columns or "date" not in df.columns:
        return df
    if fac is None or fac.is_empty():
        return df
    fdf = fac.select(
        pl.col("symbol"),
        pl.col("trade_date").cast(pl.Date).alias("date"),
        pl.col("ex_factor"),
    ).unique(subset=["symbol", "date"], keep="first").sort(["symbol", "date"])
    out = df.with_columns(pl.col("date").cast(pl.Date).alias("date"))
    # join_asof 要求 asof 键有序；用行号占位，join 后恢复原序
    out = out.with_row_index("__ord").sort("date")
    out = out.join_asof(fdf.sort("date"), on="date", by="symbol", strategy="backward")
    firsts = fdf.group_by("symbol").agg(pl.col("ex_factor").first().alias("__first"))
    out = out.join(firsts, on="symbol", how="left").with_columns(
        pl.coalesce([pl.col("ex_factor"), pl.col("__first")])
        .fill_null(1.0).alias("ex_factor")
    )
    for col in ("open", "high", "low", "close"):
        if col in out.columns and out.schema[col] in (pl.Float64, pl.Float32,
                                                      pl.Int64, pl.Int32):
            out = out.with_columns(
                (pl.col(col).cast(pl.Float64) * pl.col("ex_factor")).alias(col))
    return out.drop("ex_factor", "__first").sort("__ord").drop("__ord")
