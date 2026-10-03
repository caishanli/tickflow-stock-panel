import json
import math
from pathlib import Path
p=Path('/home/y/.hermes/workspace/stock_rules/reports/paper5_v7_backtest_QFQ_20260401_20260907.json')
d=json.loads(p.read_text(encoding='utf-8'))
r=d['strict']; trades=r['trades']; eq=r['equity_curve']
keys=[(t['symbol'],t['buy_date'],t['sell_date'],round(t['entry'],4),round(t['exit'],4)) for t in trades]
monthly_prod=math.prod(1+x for x in r['monthly'].values())-1
reasons={}
for t in trades:
    reasons[t['reason']]=reasons.get(t['reason'],0)+1
out={
 'trades':len(trades),
 'duplicate_trade_keys':len(keys)-len(set(keys)),
 'first_buy':min((t['buy_date'] for t in trades),default=None),
 'last_sell':max((t['sell_date'] for t in trades),default=None),
 'monthly_product':monthly_prod,
 'total_return':r['total_return'],
 'monthly_diff':monthly_prod-r['total_return'],
 'equity_dates':len(eq),
 'duplicate_equity_dates':len(eq)-len({x['date'] for x in eq}),
 'exit_reasons':reasons,
 'open_positions':r['open_positions'],
 'same_day_roundtrips':sum(t['buy_date']==t['sell_date'] for t in trades),
}
print(json.dumps(out,ensure_ascii=False,indent=2))
assert out['duplicate_trade_keys']==0
assert out['duplicate_equity_dates']==0
assert out['same_day_roundtrips']==0
assert abs(out['monthly_diff'])<1e-12
assert out['equity_dates']==d['coverage']['trade_days']
print('PASS: 回测账本完整性与月度复合收益一致')
