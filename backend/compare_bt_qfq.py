import json, sys, time
from pathlib import Path
import polars as pl
sys.path.insert(0,'/home/y/.hermes/workspace/stock_rules')
from qq_kline_helper import qq_kline_get
P=Path('/home/y/.hermes/workspace/stock_rules/reports/paper5_v7_backtest_20260401_20260907.json')
d=json.loads(P.read_text())['strict']['trades']
syms=sorted({t['symbol'] for t in d})
rows=[]
for sym in syms:
 code=('sh' if sym.endswith('.SH') else 'sz' if sym.endswith('.SZ') else 'bj')+sym[:6]
 qq=qq_kline_get(code,'2026-04-01','2026-09-07',250,'qfq')
 q={str(x[0]):float(x[2]) for x in qq if len(x)>=3}
 diffs=[]
 for dt,qp in q.items():
  p=Path(f'/home/y/tickflow-stock-panel/data/kline_daily/date={dt}/part.parquet')
  if not p.exists(): continue
  z=pl.read_parquet(p).filter(pl.col('symbol')==sym)
  if not z.height: continue
  lp=float(z['close'][0]); diffs.append(abs(lp/qp-1) if qp else 0)
 rows.append({'symbol':sym,'qq_days':len(q),'matched':len(diffs),'max_pct_diff':max(diffs)*100 if diffs else None,'median_pct_diff':sorted(diffs)[len(diffs)//2]*100 if diffs else None})
 time.sleep(.15)
print(json.dumps({'symbols':len(syms),'compared':sum(r['matched']>0 for r in rows),'material_symbols':sum((r['max_pct_diff'] or 0)>0.05 for r in rows),'max_diff_pct':max((r['max_pct_diff'] or 0) for r in rows),'worst':sorted(rows,key=lambda x:-(x['max_pct_diff'] or 0))[:10]},ensure_ascii=False,indent=2))
