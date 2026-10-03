"""回测: 赚钱效应总闸该不该当"硬开关"锁死买入?
口径: 表3放量阳线, 当时卦期内才可买, 近一年
对比: A=赚钱效应<3硬禁(现逻辑)  B=赚钱效应<3降半仓  C=完全不设总闸
用"当日涨停家数"近似赚钱效应(涨停少=效应差), 东财涨停池历史存档
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

# 涨停家数存档(近似赚钱效应): data/zt_pool_archive/*.json
import glob
zt={}
for f in glob.glob('data/zt_pool_archive/*.json'):
    d=os.path.basename(f).replace('.json','')
    try:
        z=json.load(open(f)); zt[d]=len(z) if isinstance(z,list) else len(z.get('data',[]))
    except: pass
print(f"涨停存档: {len(zt)}天 (用作赚钱效应近似)")

dates=sorted([d.replace('date=','') for d in os.listdir(K) if d.startswith('date=') and d.replace('date=','')>='2025-08-01'])
series={}
for dt in dates:
    p=f"{K}/date={dt}/part.parquet"
    if not os.path.exists(p): continue
    try:
        df=pl.read_parquet(p).select(['symbol','date','open','close','low','high','volume','amount']).filter(pl.col('symbol').is_in(list(t3all)))
        for r in df.to_dicts():
            sym=r['symbol']; ds=str(r['date'])
            series.setdefault(sym,[]).append({'date':ds,'open':float(r['open']),'close':float(r['close']),'low':float(r['low']),'high':float(r['high'] or r['close']),'vol':float(r['volume'] or 0),'amt':float(r['amount'] or 0)})
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

def run(me_mode, name):
    """me_mode: 'hard'=涨停<40硬禁 / 'half'=涨停<40半仓 / 'none'=不限"""
    cash0=1_000_000; cash=cash0; positions={}
    for i,dt in enumerate(alld[:-1]):
        for c in list(positions.keys()):
            r=next((x for x in series.get(c,[]) if x['date']==dt),None)
            if not r: continue
            pos=positions[c]
            if r['low']<=pos['entry']*0.93: exitp=pos['entry']*0.93
            elif pos.get('days',0)>=5: exitp=r['close']
            else: pos['days']=pos.get('days',0)+1; continue
            cash+=pos['shares']*exitp*0.999
            del positions[c]
        if len(positions)>=5: continue
        # 赚钱效应近似: 涨停<40=效应差
        zt_today=zt.get(dt,50)
        weak_me = zt_today < 40
        if me_mode=='hard' and weak_me: continue
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
        for sym,r,pct in sigs[:3]:
            if len(positions)>=5 or sym in positions: continue
            s=series[sym]; k=sidx(sym,dt)
            if k<0 or k+1>=len(s): continue
            ep=s[k+1]['open']
            if ep<=0: continue
            ratio=1.0
            if me_mode=='half' and weak_me: ratio*=0.5
            shares=int(cash*0.2*ratio/ep/100)*100
            if shares<100 or cash<shares*ep*1.001: continue
            cash-=shares*ep*1.001
            positions[sym]={'shares':shares,'entry':ep,'cost':shares*ep*1.001,'days':0}
    for c,pos in list(positions.items()):
        r=next((x for x in series.get(c,[]) if x['date']==alld[-1]),None)
        if r: cash+=pos['shares']*r['close']*0.999
    print(f"{name:32} 总{(cash/cash0-1)*100:+7.1f}%")

print("赚钱效应总闸 诚实回测(涨停家数近似, 近一年)")
print("="*60)
run('hard', "A/涨停<40硬禁(现逻辑)")
run('half', "B/涨停<40降半仓")
run('none', "C/不限总闸")