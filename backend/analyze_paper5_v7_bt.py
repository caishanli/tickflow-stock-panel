import json
from collections import defaultdict
from pathlib import Path
p=Path('/home/y/.hermes/workspace/stock_rules/reports/paper5_v7_backtest_QFQ_20260401_20260907.json')
d=json.loads(p.read_text(encoding='utf-8'))
r=d['strict']; ts=r['trades']
g=json.loads(Path('/home/y/.hermes/workspace/stock_rules/data/gua_database.json').read_text(encoding='utf-8'))['records']
names={}
for x in g:
 c=str(x.get('stock_code') or '')
 if c.startswith('sh'): s=c[2:]+'.SH'
 elif c.startswith('sz'): s=c[2:]+'.SZ'
 elif c.startswith('bj'): s=c[2:]+'.BJ'
 else: continue
 names[s]=x.get('stock_name') or s
by_reason=defaultdict(list)
for t in ts: by_reason[t['reason']].append(t)
reason_rows=[]
for k,v in by_reason.items():
 reason_rows.append({'reason':k,'n':len(v),'win_rate':sum(x['ret']>0 for x in v)/len(v),'avg_ret':sum(x['ret'] for x in v)/len(v),'pnl':sum(x['pnl'] for x in v)})
wins=[x['ret'] for x in ts if x['ret']>0]; losses=[x['ret'] for x in ts if x['ret']<0]
top=sorted(ts,key=lambda x:x['pnl'],reverse=True)[:5]
bottom=sorted(ts,key=lambda x:x['pnl'])[:5]
p5_keys={(x['symbol'],x['buy_date']) for x in ts}
p1_ts=d['paper1_threshold_control']['trades']
p1_keys={(x['symbol'],x['buy_date']) for x in p1_ts}
extra=[x for x in ts if (x['symbol'],x['buy_date']) not in p1_keys]
removed=[x for x in p1_ts if (x['symbol'],x['buy_date']) not in p5_keys]
out={'closed':len(ts),'wins':len(wins),'losses':len(losses),'avg_win':sum(wins)/len(wins),'avg_loss':sum(losses)/len(losses),'payoff':(sum(wins)/len(wins))/abs(sum(losses)/len(losses)),'reason_rows':reason_rows,'top':[dict(name=names.get(x['symbol'],x['symbol']),**x) for x in top],'bottom':[dict(name=names.get(x['symbol'],x['symbol']),**x) for x in bottom],'excess_vs_index':r['total_return']-d['benchmark_sh000001'],'excess_vs_random_mean':r['total_return']-d['random_control_200']['mean'],'improvement_vs_p1':r['total_return']-d['paper1_threshold_control']['total_return'],'market_proxy_value':r['total_return']-d['without_market_proxy']['total_return'],'v7_extra_vs_p1':{'n':len(extra),'wins':sum(x['ret']>0 for x in extra),'pnl':sum(x['pnl'] for x in extra),'avg_ret':sum(x['ret'] for x in extra)/len(extra) if extra else 0},'p1_only_due_to_path_dependency':{'n':len(removed),'pnl':sum(x['pnl'] for x in removed)}}
print(json.dumps(out,ensure_ascii=False,indent=2))
