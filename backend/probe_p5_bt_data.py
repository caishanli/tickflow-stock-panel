from pathlib import Path
import json
import polars as pl
R=Path('/home/y/tickflow-stock-panel/data')
parts=sorted((R/'kline_daily').glob('date=*/part.parquet'))
print('daily_parts', len(parts), parts[0].parent.name if parts else None, parts[-1].parent.name if parts else None)
for sub in ('kline_index_daily','kline_daily_enriched','instruments'):
 p=R/sub
 fs=sorted(p.glob('**/*.parquet'))
 print(sub, len(fs), str(fs[-1]) if fs else None)
 if fs:
  try:
   print(pl.read_parquet(fs[-1]).schema)
  except Exception as e: print(type(e).__name__,e)
g=json.loads(Path('/home/y/.hermes/workspace/stock_rules/data/gua_database.json').read_text())['records']
valid=[x for x in g if x.get('direction') and x.get('gua_date') and x.get('expiry_date')]
print('gua_directional',len(valid),'min',min(x['gua_date'] for x in valid),'max',max(x['gua_date'] for x in valid),'codes',len({x.get('stock_code') for x in valid}))
