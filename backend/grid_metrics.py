"""网格搜索指标汇总：python grid_metrics.py [run_id ...]"""
import sqlite3
import sys

import pandas as pd

con = sqlite3.connect("/home/caisl/tickflow-stock-panel/data/quant.db")

def metrics(run_id):
    eq = pd.read_sql(f"SELECT dt,value FROM backtest_equity WHERE run_id='{run_id}' ORDER BY dt", con)
    tr = pd.read_sql(f"SELECT ts,code,action,pnl,commission FROM backtest_trades "
                     f"WHERE run_id='{run_id}' ORDER BY ts", con)
    if eq.empty:
        return None
    cap = 100000.0
    final = eq["value"].iloc[-1]
    ret = final / cap - 1
    peak = eq["value"].cummax()
    mdd = ((eq["value"] - peak) / peak).min()
    sells = tr[tr["action"] == "SIDE.SELL"]
    wins = sells[sells["pnl"] > 0]
    losses = sells[sells["pnl"] <= 0]
    winrate = len(wins) / len(sells) if len(sells) else float("nan")
    avg_win = wins["pnl"].mean() if len(wins) else 0.0
    avg_loss = losses["pnl"].mean() if len(losses) else -1.0
    pl_ratio = abs(avg_win / avg_loss) if avg_loss else float("inf")
    days, open_pos = [], {}
    for _, r in tr.iterrows():
        if r["action"] == "SIDE.BUY":
            open_pos[r["code"]] = r["ts"]
        else:
            t0 = open_pos.pop(r["code"], None)
            if t0:
                days.append((pd.Timestamp(r["ts"]) - pd.Timestamp(t0)).days)
    return {"收益%": round(ret*100, 2), "回撤%": round(mdd*100, 2),
            "收益/回撤": round(ret/abs(mdd), 3) if mdd else 0,
            "笔数": len(sells), "胜率%": round(winrate*100, 1),
            "盈亏比": round(pl_ratio, 2),
            "均持天": round(sum(days)/len(days), 2) if days else 0}

if __name__ == "__main__":
    rows = {}
    for rid in sys.argv[1:]:
        name_row = con.execute("SELECT name FROM backtest_runs WHERE id=?", (rid,)).fetchone()
        name = name_row[0] if name_row else rid
        m = metrics(rid)
        if m:
            rows[f"{name}|{rid}"] = m
    df = pd.DataFrame(rows).T
    print(df.to_string())
