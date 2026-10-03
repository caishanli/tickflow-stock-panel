"""LB 变体批量回测：python scripts/lb_opt_backtest.py <策略文件> <out_dir> [start] [end]。

打印一行 metrics JSON：total_return / max_dd / ratio / n_trades / days。
"""
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

load_dotenv()

import pandas as pd  # noqa: E402

from app.quant.rqalpha_bridge import run_jq_backtest  # noqa: E402


def main() -> None:
    strat = sys.argv[1]
    out = sys.argv[2]
    start = sys.argv[3] if len(sys.argv) > 3 else "2026-04-01"
    end = sys.argv[4] if len(sys.argv) > 4 else "2026-09-28"
    os.makedirs(out, exist_ok=True)
    t0 = datetime.datetime.now()
    res = run_jq_backtest(
        strat,
        {"start": start, "end": end, "capital": 100000.0,
         "benchmark": "510300.XSHG", "out_dir": out,
         "universe": "all_stocks"},
    )
    el = (datetime.datetime.now() - t0).total_seconds()
    if isinstance(res, dict) and res.get("error"):
        print(json.dumps({"file": strat, "error": str(res.get("error"))[:300],
                          "elapsed_s": round(el, 1)}))
        return
    eq = pd.read_csv(os.path.join(out, "equity.csv"))
    val_col = next((c for c in ("total_value", "value", "total")
                    if c in eq.columns), eq.columns[-1])
    v = eq[val_col].astype(float)
    total_return = float(v.iloc[-1] / v.iloc[0] - 1)
    max_dd = float(((v / v.cummax()) - 1).min()) * -1
    ratio = (total_return / max_dd) if max_dd > 0 else 0.0
    n_trades = 0
    for cand in ("trades.csv",):
        p = os.path.join(out, cand)
        if os.path.exists(p):
            try:
                n_trades = len(pd.read_csv(p))
            except Exception:
                pass
    print(json.dumps({"file": os.path.basename(strat), "start": start, "end": end,
                      "return": round(total_return, 4), "max_dd": round(max_dd, 4),
                      "ratio": round(ratio, 3), "n_trades": n_trades,
                      "days": len(eq), "elapsed_s": round(el, 1)}))


if __name__ == "__main__":
    main()
