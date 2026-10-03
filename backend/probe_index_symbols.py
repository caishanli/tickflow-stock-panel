from pathlib import Path
import polars as pl
p=Path('/home/y/tickflow-stock-panel/data/kline_index_daily/date=2026-09-07/part.parquet')
df=pl.read_parquet(p)
print(df.select('symbol').unique().sort('symbol'))
