"""回测桥 vs 服务端 复权口径一致性实测（真实数据，**离线诊断脚本**）。

注：本脚本用 pandas 只为做逐点对比（离线诊断，非生产路径）；生产链路的
pandas 边界仍限定在回测（vectorbt）内。

对比两条路径：
  服务端   : apply_qfq_daily  = close × ex_factor(date)      （直查因子表，无阈值）
  回测桥   : _apply_qfq       = close × prod(事件乘数, t < 事件日 <= cutoff)
             事件由 _adj_events() 从同一张因子表重建（带噪声阈值）

用法: uv run python -m app.quant.qfq_consistency_check [--symbols A B ...]
"""

from __future__ import annotations

import argparse

import pandas as pd

from app.quant.jqengine.datasource.manager import DataManager
from app.services.stockdata.sources import DataSources


def _bridge_factor(dm: DataManager, jq: str, dates: list[pd.Timestamp],
                   cutoff: pd.Timestamp | None = None) -> list[float]:
    """回测桥的隐含复权因子（与 _apply_qfq 内部同构）。"""
    events = dm._adj_events().get(jq) or []
    if cutoff is not None:
        events = [e for e in events if e[0] <= cutoff]
    out = []
    for d in dates:
        f = 1.0
        for ex_dt, m in events:
            if d < ex_dt:
                f *= m
        out.append(f)
    return out


def check(sym: str, ds: DataSources, dm: DataManager,
          start: str, end: str) -> tuple[str, float, int, str]:
    df = ds.get_daily([sym], start, end)
    if df.is_empty():
        return sym, 0.0, 0, "无数据"
    srv = ds.apply_qfq_daily(df).sort("date")
    # 回测桥路径：_apply_qfq 要求 DatetimeIndex（或 trade_dt 列）才能定位日期
    pdf = df.to_pandas().copy()
    pdf["trade_dt"] = pdf["date"].astype(str)
    bt = dm._apply_qfq(pdf, dm._to_jq_code(sym)).copy()
    bt["d"] = bt["date"].astype(str)
    s = {str(r["date"]): r["close"] for r in srv.iter_rows(named=True)}
    b = {str(r["d"]): float(r["close"]) for r in bt.to_dict("records")}
    common = [d for d in s if d in b and s[d]]
    if not common:
        return sym, 0.0, 0, "无重叠"
    rel = max(abs(s[d] - b[d]) / s[d] for d in common)
    return sym, rel, len(common), ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=[
        # 股票（权威源 baostock）
        "000001.SZ", "600000.SH", "000002.SZ", "300750.SZ", "601318.SH",
        "000651.SZ", "600036.SH",
        # ETF（mootdx 源）——历史伪跳变高发区，不能只抽样股票
        "159934.SZ", "159971.SZ", "159207.SZ", "510300.SH", "588000.SH",
    ])
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2026-10-20")
    args = ap.parse_args()

    ds = DataSources()
    dm = DataManager()
    print(f"{'标的':<14}{'最大相对差':>13}   n")
    worst = 0.0
    for sym in args.symbols:
        try:
            sym, rel, n, err = check(sym, ds, dm, args.start, args.end)
            worst = max(worst, rel)
            print(f"{sym:<14}{rel:>12.6%}   {n}" + (f"  ({err})" if err else ""))
        except Exception as exc:
            print(f"{sym:<14} ERROR {type(exc).__name__}: {str(exc)[:70]}")
    print(f"\n最大相对差合计: {worst:.6%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
