#!/usr/bin/env python3
import json, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
sys.path.insert(0,'/home/y/.hermes/workspace/stock_rules')
from qq_kline_helper import qq_kline_get
G=Path('/home/y/.hermes/workspace/stock_rules/data/gua_database.json')
O=Path('/home/y/tickflow-stock-panel/data/cache/paper5_v7_qfq_20251101_20260907.json')
O.parent.mkdir(parents=True,exist_ok=True)
records=json.loads(G.read_text(encoding='utf-8'))['records']
codes={}
for r in records:
 c=str(r.get('stock_code') or '')
 if not (r.get('direction') and r.get('gua_date') and r.get('expiry_date')): continue
 if c.startswith(('sh','sz','bj')): codes[c]=r.get('stock_name') or c

def one(code):
 rows=qq_kline_get(code,'2025-11-01','2026-09-07',250,'qfq')
 if not rows:
  time.sleep(.5); rows=qq_kline_get(code,'2025-11-01','2026-09-07',250,'qfq')
 clean=[]
 for x in rows:
  if len(x)<6: continue
  try: clean.append([str(x[0]),float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5])])
  except: pass
 return code,clean
out={}
with ThreadPoolExecutor(max_workers=6) as ex:
 futs=[ex.submit(one,c) for c in sorted(codes)]
 for i,f in enumerate(as_completed(futs),1):
  c,rows=f.result(); out[c]={'name':codes[c],'rows':rows}
  if i%25==0: print(f'{i}/{len(futs)}',file=sys.stderr)
tmp=O.with_suffix('.tmp')
tmp.write_text(json.dumps(out,ensure_ascii=False),encoding='utf-8'); tmp.replace(O)
ok=sum(bool(v['rows']) for v in out.values())
print(json.dumps({'path':str(O),'codes':len(codes),'ok':ok,'empty':len(codes)-ok,'coverage':ok/len(codes) if codes else 0,'min_rows':min((len(v['rows']) for v in out.values() if v['rows']),default=0),'max_rows':max((len(v['rows']) for v in out.values()),default=0)},ensure_ascii=False,indent=2))
if ok < len(codes)*.95: raise SystemExit('QFQ覆盖率不足95%')
