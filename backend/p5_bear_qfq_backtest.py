#!/usr/bin/env python3
"""全市场熊市超跌反包：候选级腾讯QFQ复核与公平随机对照。"""
from __future__ import annotations
import json, random, statistics, sys, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl

sys.path.insert(0,"/home/y/.hermes/workspace/stock_rules")
from qq_kline_helper import qq_kline_get
sys.path.insert(0,str(Path(__file__).parent))
from p5_strategy_optimizer import bear_reversal_signal, entry_is_tradeable, stop_fill_price, take_fill_price

TF=Path('/home/y/tickflow-stock-panel'); K=TF/'data/kline_daily'; CACHE=TF/'data/cache/p5_bear_candidates_qfq.json'
OUT=Path('/home/y/.hermes/workspace/stock_rules/reports/p5_bear_regime_qfq.json')
MD=Path('/home/y/.hermes/workspace/stock_rules/reports/模拟盘5_熊市反包通道_QFQ验证.md')
START='2025-11-01'; END='2026-09-07'; FEE_B=0.00025; FEE_S=0.00125

def code_of(sym):
    c,ex=sym.split('.'); return {'SH':'sh','SZ':'sz','BJ':'bj'}[ex]+c

def load_local():
    parts=[p for p in sorted(K.glob('date=*/part.parquet')) if 'date=2025-08-01'<=p.parent.name<=f'date={END}']
    df=pl.scan_parquet([str(p) for p in parts]).select('symbol','date','open','high','low','close','volume','amount').collect().sort(['symbol','date'])
    inst=pl.read_parquet(TF/'data/instruments/instruments.parquet').select('symbol','name','exchange')
    bad=set(inst.filter(pl.col('name').str.contains('ST|退')|(pl.col('exchange')=='BJ')).get_column('symbol').to_list())
    df=df.filter(~pl.col('symbol').is_in(bad))
    # 先计算再做当日流动性过滤，禁止把缺失日拼成假连续收益。
    f=df.with_columns([
      (pl.col('close')/pl.col('close').shift(1).over('symbol')-1).alias('pct'),
      pl.col('volume').rolling_mean(5).shift(1).over('symbol').alias('vma5_prev'),
      pl.col('high').rolling_max(60).shift(1).over('symbol').alias('hi60'),
      pl.col('low').rolling_min(60).shift(1).over('symbol').alias('lo60'),
      (pl.col('close')/pl.col('close').shift(3).over('symbol')-1).alias('ret3'),
    ]).with_columns([
      (pl.col('volume')/pl.col('vma5_prev')).alias('volr'),
      ((pl.col('close')-pl.col('lo60'))/(pl.col('hi60')-pl.col('lo60')+1e-9)).alias('pos60')])
    broad=f.filter((pl.col('date')>=pl.datetime(2025,11,1))&(pl.col('date')<=pl.datetime(2026,9,7))&
      (pl.col('ret3') < -0.05)&(pl.col('pct')>0.005)&(pl.col('pos60')<0.35)&
      (pl.col('amount')>=3e7)&(pl.col('close')>=2))
    return df, sorted(set(broad.get_column('symbol').to_list())), inst

def fetch(symbols):
    old=json.loads(CACHE.read_text(encoding='utf-8')) if CACHE.exists() else {}
    todo=[s for s in symbols if code_of(s) not in old or not old[code_of(s)].get('rows')]
    def one(sym):
      c=code_of(sym); rows=qq_kline_get(c,'2025-08-01',END,350,'qfq')
      if not rows: time.sleep(.4); rows=qq_kline_get(c,'2025-08-01',END,350,'qfq')
      clean=[]
      for x in rows or []:
       try: clean.append([str(x[0]),*[float(x[i]) for i in range(1,6)]])
       except Exception: pass
      return c,clean
    with ThreadPoolExecutor(max_workers=6) as ex:
      futs=[ex.submit(one,s) for s in todo]
      for i,f in enumerate(as_completed(futs),1):
       c,r=f.result(); old[c]={'rows':r}
       if i%50==0: print(f'qfq {i}/{len(futs)}',file=sys.stderr)
    CACHE.parent.mkdir(parents=True,exist_ok=True)
    tmp=CACHE.with_suffix('.tmp'); tmp.write_text(json.dumps(old,ensure_ascii=False),encoding='utf-8'); tmp.replace(CACHE)
    return old

def build_qfq(local, symbols, cache):
    amount={(str(r['symbol']),str(r['date'])):float(r['amount'] or 0) for r in local.filter(pl.col('symbol').is_in(symbols)).select('symbol','date','amount').to_dicts()}
    rows=[]
    for sym in symbols:
      for x in cache.get(code_of(sym),{}).get('rows',[]):
       dt=x[0]
       if '2025-08-01'<=dt<=END:
        rows.append({'symbol':sym,'date':dt,'open':x[1],'close':x[2],'high':x[3],'low':x[4],'volume':x[5], 'amount':amount.get((sym,dt),0)})
    if not rows: raise RuntimeError('无QFQ')
    df=pl.DataFrame(rows).sort(['symbol','date'])
    return df.with_columns([
      (pl.col('close')/pl.col('close').shift(1).over('symbol')-1).alias('pct'),
      pl.col('volume').rolling_mean(5).shift(1).over('symbol').alias('vma5_prev'),
      pl.col('high').rolling_max(60).shift(1).over('symbol').alias('hi60'),
      pl.col('low').rolling_min(60).shift(1).over('symbol').alias('lo60'),
      (pl.col('close')/pl.col('close').shift(3).over('symbol')-1).alias('ret3'),
    ]).with_columns([(pl.col('volume')/pl.col('vma5_prev')).alias('volr'),((pl.col('close')-pl.col('lo60'))/(pl.col('hi60')-pl.col('lo60')+1e-9)).alias('pos60')])

def market_momentum(local):
    f=local.with_columns((pl.col('close')/pl.col('close').shift(1).over('symbol')-1).alias('ret'))
    m=f.filter((pl.col('amount')>=3e7)&pl.col('ret').is_not_null()).group_by('date').agg(pl.col('ret').mean().alias('r')).sort('date')
    m=m.with_columns(((1+pl.col('r')).log().rolling_sum(20).exp()-1).alias('mom'))
    return {str(r['date']):float(r['mom'] or 0) for r in m.to_dicts()}

def make_pools(qdf,mm):
    signal=defaultdict(list); control=defaultdict(list)
    for r in qdf.filter((pl.col('date')>=START)&(pl.col('date')<=END)&(pl.col('amount')>=5e7)&(pl.col('close')>=2)).to_dicts():
      dt=str(r['date']); mom=mm.get(dt,0)
      base_pool=mom<=0 and float(r.get('ret3') or 0)<-.08 and float(r.get('pos60') or 1)<.25
      if base_pool: control[dt].append(r)
      if bear_reversal_signal(r,mom): signal[dt].append(r)
    return signal,control

def prepare_bt(qdf):
    idx={(r['symbol'],str(r['date'])):r for r in qdf.to_dicts()}
    dates=sorted({d for _,d in idx if START<=d<=END})
    return idx,dates,{d:i for i,d in enumerate(dates)}

def bt(qdf, pools, hold=3, max_pos=5, max_day=3, seed=None, start=START, end=END, prepared=None,
       gap_min=-.05, gap_max=.05):
    idx,dates,di=prepared or prepare_bt(qdf)
    by_entry=defaultdict(list); rng=random.Random(seed)
    for d,xs in pools.items():
      if not start<=d<=end or d not in di or di[d]+1>=len(dates): continue
      chosen=list(xs); rng.shuffle(chosen) if seed is not None else chosen.sort(key=lambda x:(-float(x.get('volr') or 0),x['symbol']))
      by_entry[dates[di[d]+1]].extend(chosen[:max_day])
    cash=500000.; pos=[]; done=[]; curve=[]
    for d in dates:
      for p in list(pos):
       b=idx.get((p['symbol'],d))
       if not b: continue
       held=di[d]-di[p['buy_date']]
       stop_px=stop_fill_price(p['entry'],b['open'],b['low'],.07) if held>=1 else None
       take_px=take_fill_price(p['entry'],b['open'],b['high'],.15) if held>=1 and stop_px is None else None
       if stop_px is not None or take_px is not None or held>=hold:
        exit_px=stop_px if stop_px is not None else take_px if take_px is not None else b['close']
        proceeds=p['shares']*exit_px*(1-FEE_S); cash+=proceeds
        done.append(proceeds/(p['shares']*p['entry']*(1+FEE_B))-1); pos.remove(p)
      nav=cash+sum(p['shares']*(idx.get((p['symbol'],d)) or {'close':p['entry']})['close'] for p in pos)
      for s in by_entry.get(d,[]):
       if len(pos)>=max_pos or any(p['symbol']==s['symbol'] for p in pos): continue
       b=idx.get((s['symbol'],d)); prev=float(s['close'])
       if not b or not entry_is_tradeable(s['symbol'],prev,float(b['open']),gap_min,gap_max): continue
       budget=min(nav*.15,nav*.95-sum(p['shares']*p['entry'] for p in pos),cash/(1+FEE_B)); sh=int(budget/b['open']/100)*100
       if sh<100: continue
       cash-=sh*b['open']*(1+FEE_B); pos.append({'symbol':s['symbol'],'buy_date':d,'entry':b['open'],'shares':sh})
      curve.append(cash+sum(p['shares']*(idx.get((p['symbol'],d)) or {'close':p['entry']})['close'] for p in pos))
    pk=curve[0]; dd=0
    for v in curve: pk=max(pk,v); dd=min(dd,v/pk-1)
    return {'return':curve[-1]/500000-1,'trades':len(done),'win_rate':sum(x>0 for x in done)/len(done) if done else 0,'max_dd':dd,'avg':statistics.mean(done) if done else 0}

def main():
    local,syms,inst=load_local(); cache=fetch(syms); ok=[s for s in syms if cache.get(code_of(s),{}).get('rows')]; q=build_qfq(local,ok,cache); mm=market_momentum(local); sig,ctrl=make_pools(q,mm); prepared=prepare_bt(q)
    variants=[]
    for hold in (2,3,4,5):
     for mp in (5,8,10):
      full=bt(q,sig,hold,mp,min(3,mp),prepared=prepared); train=bt(q,sig,hold,mp,min(3,mp),start=START,end='2026-04-30',prepared=prepared); valid=bt(q,sig,hold,mp,min(3,mp),start='2026-05-01',end=END,prepared=prepared)
      variants.append({'hold':hold,'max_pos':mp,'train':train,'valid':valid,'full':full})
    gap_sensitivity=[]
    for gm in (-.05,-.03,-.01,0.0,.01):
      full=bt(q,sig,2,5,3,prepared=prepared,gap_min=gm); train=bt(q,sig,2,5,3,start=START,end='2026-04-30',prepared=prepared,gap_min=gm); valid=bt(q,sig,2,5,3,start='2026-05-01',end=END,prepared=prepared,gap_min=gm)
      gap_sensitivity.append({'gap_min':gm,'train':train,'valid':valid,'full':full})
    randoms=[bt(q,ctrl,3,5,3,seed=i,prepared=prepared) for i in range(200)]
    robust=[x for x in variants if x['train']['return']>0 and x['valid']['return']>0]; robust.sort(key=lambda x:x['full']['return'],reverse=True)
    payload={'candidate_symbols':len(syms),'qfq_ok':len(ok),'signal_days':len(sig),'signals':sum(map(len,sig.values())),'variants':variants,'gap_sensitivity':gap_sensitivity,'robust':robust,'random':{'mean':statistics.mean(x['return'] for x in randoms),'median':statistics.median(x['return'] for x in randoms)}}
    OUT.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
    lines=['# 模拟盘5熊市超跌反包独立通道QFQ验证','', '| 项目 | 结果 |','|:---|---:|',f'| 原始宽筛候选股票 | {len(syms)} |',f'| 腾讯QFQ成功 | {len(ok)} |',f'| 精确QFQ信号 | {sum(map(len,sig.values()))} |',f'| 双段均盈利参数 | {len(robust)} |','', '| 持有 | 最大持仓 | 发现收益 | 验证收益 | 全段收益 | 回撤 | 交易数 |','|---:|---:|---:|---:|---:|---:|---:|']
    for x in robust[:10]: lines.append(f"| {x['hold']} | {x['max_pos']} | {x['train']['return']:+.2%} | {x['valid']['return']:+.2%} | {x['full']['return']:+.2%} | {x['full']['max_dd']:+.2%} | {x['full']['trades']} |")
    if not robust: lines.append('| — | — | — | — | — | — | — |')
    lines += ['', '## T+1开盘缺口敏感性', '', '| 最低缺口 | 发现收益 | 验证收益 | 全段收益 | 回撤 | 交易数 |', '|---:|---:|---:|---:|---:|---:|']
    for x in gap_sensitivity:
      lines.append(f"| {x['gap_min']:+.0%} | {x['train']['return']:+.2%} | {x['valid']['return']:+.2%} | {x['full']['return']:+.2%} | {x['full']['max_dd']:+.2%} | {x['full']['trades']} |")
    lines += ['', '> 旧V7适合测试拒绝低开，但V8属于超跌反包均值回归；拒绝负缺口会明显削弱V8，故生产保持-5%～+5%。', '', f"> 同日同低位超跌池随机对照200次：均值{payload['random']['mean']:+.2%}，中位{payload['random']['median']:+.2%}。"]
    MD.write_text('\n'.join(lines)+'\n',encoding='utf-8'); print(json.dumps({'qfq':f'{len(ok)}/{len(syms)}','signals':payload['signals'],'robust':len(robust),'top':robust[:3],'random':payload['random']},ensure_ascii=False,indent=2))
if __name__=='__main__': main()
