"""max_pos 敏感性回测: 总仓位只数上限 3/5/8 对收益的影响(诚实口径: 当时卦期内才可买)
对照: 单只仓位随降半仓因子变化, 看"只数上限"是否束缚收益
"""
import os, sys, json
import polars as pl
os.chdir('/home/y/.hermes/workspace/stock_rules')
K='/home/y/tickflow-stock-panel/data/kline_daily'

g=json.load(open('data/gua_database.json'))
gua_win={}; gua_dir={}
for r in g['records']:
    c=r.get('stock_code','').strip()
    gd=str(r.get('gua_date','') or ''); ex=str(r.get('expiry_date','') or '')
    if c and ex>='2026-08-01':
        c6=c[2:] if c[:2] in ('sh','sz') else c
        gua_win[c6]=(gd,ex); gua_dir[c6]=r.get('direction','')
t3all=set()
for r in g['records']:
    c=r.get('stock_code','').strip()
    if c:
        if c[:2]=='sh': pr=c[2:]+'.SH'
        elif c[:2]=='sz': pr=c[2:]+'.SZ'
        else: pr=c+'.SH'
        t3all.add(pr)

dates=sorted([d.replace('date=','') for d in os.listdir(K) if d.startswith('date=') and d.replace('date=','')>='2025-08-01'])
series={}
for dt in dates:
    p=f"{K}/date={dt}/part.parquet"
    if not os.path.exists(p): continue
    try:
        df=pl.read_parquet(p).select(['symbol','date','open','close','low','high','volume','amount']).filter(pl.col('symbol').is_in(list(t3all)))
        for r in df.to_dicts():
            sym=r['symbol']; ds=str(r['date'])
            series.setdefault(sym,[]).append({'date':ds,'open':float(r['open']),'close':float(r['close']),
                'low':float(r['low']),'high':float(r['high'] or r['close']),'vol':float(r['volume'] or 0),'amt':float(r['amount'] or 0)})
    except: pass
for s in series: series[s].sort(key=lambda x:x['date'])
alld=sorted(set(d for s in series for d in [x['date'] for x in series[s]]))
def sidx(sym,dt):
    s=series.get(sym,[])
    for i,x in enumerate(s):
        if x['date']==dt: return i
    return -1
def volr(sym,dt):
    s=series.get(sym,[]); i=sidx(sym,dt)
    if i<5: return 0
    avg=sum(s[j]['vol'] for j in range(i-5,i))/5
    return s[i]['vol']/avg if avg else 0

def run(max_pos, name):
    cash0=1_000_000; cash=cash0; positions={}; trades=[]
    for i,dt in enumerate(alld[:-1]):
        for c in list(positions.keys()):
            r=next((x for x in series.get(c,[]) if x['date']==dt),None)
            if not r: continue
            pos=positions[c]
            if r['low']<=pos['entry']*0.93: exitp=pos['entry']*0.93
            elif pos.get('days',0)>=5: exitp=r['close']
            else: pos['days']=pos.get('days',0)+1; continue
            cash+=pos['shares']*exitp*0.999
            trades.append((exitp/pos['entry']-1)*100)
            del positions[c]
        if len(positions)>=max_pos: continue
        sigs=[]
        for sym in t3all:
            c6=sym.split('.')[0]
            w=gua_win.get(c6)
            if w and w[0] and not (w[0]<=dt<=w[1]): continue
            r=next((x for x in series.get(sym,[]) if x['date']==dt),None)
            if not r or r['amt']<3e7: continue
            pct=(r['close']/r['open']-1)*100
            if pct>2.0 and r['close']>r['open'] and volr(sym,dt)>1.3 and pct<=6.0:
                if gua_dir.get(c6,'')=='跌': continue
                sigs.append((sym,r,pct))
        sigs.sort(key=lambda x:-x[2])
        for sym,r,pct in sigs[:max_pos]:
            if len(positions)>=max_pos or sym in positions: continue
            s=series[sym]; k=sidx(sym,dt)
            if k<0 or k+1>=len(s): continue
            ep=s[k+1]['open']
            if ep<=0: continue
            # 单只仓位: 标准20%, 随机模拟部分票被降半仓(0.5因子) → 平均15%
            ratio=0.20 if hash((sym,dt))%3 else 0.10  # ~1/3票降半仓
            shares=int(cash*ratio/ep/100)*100
            if shares<100 or cash<shares*ep*1.001: continue
            cash-=shares*ep*1.001
            positions[sym]={'shares':shares,'entry':ep,'cost':shares*ep*1.001,'days':0}
    for c,pos in list(positions.items()):
        r=next((x for x in series.get(c,[]) if x['date']==alld[-1]),None)
        if r: cash+=pos['shares']*r['close']*0.999
    import statistics
    rets=[t for t in trades if isinstance(t,(int,float))]
    if not rets: print(f"{name}: 0笔"); return
    print(f"{name:20} {len(rets):4}笔 总{(cash/cash0-1)*100:+7.1f}% 均{statistics.mean(rets):+.2f}% 胜率{sum(1 for x in rets if x>0)/len(rets)*100:.0f}% 峰值持仓{max_pos}")

print("max_pos 敏感性回测(诚实口径, 当时卦期内才可买, 近一年)")
print("="*70)
for mp in (3, 5, 8, 10, 12):
    run(mp, f"max_pos={mp}")