"""真实回测路径（jqcompat._apply_qfq_bars）下的复权口径一致性实测。

回测实际走的是 ``jqcompat._apply_qfq_bars``（numpy recarray，datetime 为
YYYYMMDDHHMMSS int64），它经 ``DataManager._adj_events()`` 从因子表重建事件。
本脚本对比：
  服务端 : apply_qfq_daily = close × ex_factor(date)   （直查因子表，最新锚定）
  回测桥 : _apply_qfq_bars  = close × prod(事件乘数, bar日 < 事件日 <= 决策日 dt)

口径一致要求：dt 取最新交易日时两者逐点相等。

用法: uv run python -m app.quant.qfq_bars_consistency_check
"""

from __future__ import annotations

import argparse
import json

import numpy as np

from app.quant.jqengine.datasource.manager import DataManager
from app.services.stockdata.sources import DataSources


def bars_from_daily(symbol: str, rows: list[tuple[str, float]]) -> np.ndarray:
    """构造 jqcompat 风格的 recarray：datetime=YYYYMMDDHHMMSS int64 + OHLC。"""
    dt = np.array([int(d.replace("-", "") + "150000") for d, _ in rows], dtype="int64")
    close = np.array([c for _, c in rows], dtype="float64")
    return np.rec.fromarrays(
        [dt, close, close, close, close],
        names="datetime,open,high,low,close",
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=[
        "000001.SZ", "600000.SH", "000002.SZ", "300750.SZ", "601318.SH", "000651.SZ",
    ])
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2026-10-20")
    args = ap.parse_args()

    ds = DataSources()
    dm = DataManager()
    out = []
    worst = 0.0
    for sym in args.symbols:
        df = ds.get_daily([sym], args.start, args.end)
        if df.is_empty():
            out.append({"symbol": sym, "error": "无数据"})
            continue
        srv = ds.apply_qfq_daily(df).sort("date")
        s = {str(r["date"]): r["close"] for r in srv.iter_rows(named=True)}

        rows = [(str(r["date"]), r["close"]) for r in df.sort("date").iter_rows(named=True)]
        bars = bars_from_daily(sym, rows)
        jq = dm._to_jq_code(sym)
        # 决策日取最后一天 → 无未来事件参与
        dt = df["date"].max()
        adj_bars = dm._apply_qfq_bars_probe(bars, jq, dt) if hasattr(dm, "_apply_qfq_bars_probe") \
            else _apply_bars(dm, bars, jq, dt)
        b = {str(int(x["datetime"]) // 1000000): float(x["close"])
             for x in adj_bars}
        diffs = []
        for d, sv in s.items():
            key = str(int(d.replace("-", "")))
            if key in b and sv:
                diffs.append((abs(sv - b[key]) / sv, d, sv, b[key]))
        if not diffs:
            out.append({"symbol": sym, "error": "无重叠"})
            continue
        rel = max(diffs)[0]
        worst = max(worst, rel)
        worst_row = max(diffs)
        out.append({
            "symbol": sym, "n": len(diffs), "max_rel_diff": round(rel, 10),
            "worst_date": worst_row[1],
            "server": round(worst_row[2], 4), "bridge": round(worst_row[3], 4),
        })
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n最大相对差: {worst:.8%}")
    return 0


def _apply_bars(dm: DataManager, bars: np.ndarray, jq: str, dt) -> np.ndarray:
    """复刻 jqcompat._apply_qfq_bars 的逻辑（事件法 + 决策日过滤）。"""
    events = dm._adj_events().get(jq) or []
    if not events:
        return bars
    import pandas as pd
    dt_ts = pd.Timestamp(dt)
    events = [e for e in events if e[0] <= dt_ts]
    if not events:
        return bars
    dates = bars["datetime"] // 1000000
    factors = np.ones(len(dates), dtype=float)
    for ex_ts, f in events:
        ex_int = ex_ts.year * 10000 + ex_ts.month * 100 + ex_ts.day
        factors[dates < ex_int] *= f
    if np.allclose(factors, 1.0):
        return bars
    bars = bars.copy()
    for col in ("open", "high", "low", "close"):
        bars[col] = bars[col] * factors
    return bars


if __name__ == "__main__":
    raise SystemExit(main())
