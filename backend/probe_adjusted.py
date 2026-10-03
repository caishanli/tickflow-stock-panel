from pathlib import Path
import polars as pl
p=Path('/home/y/tickflow-stock-panel/data/kline_daily_enriched/date=2026-09-04/part.parquet')
d=pl.read_parquet(p)
g=d.select(((pl.col('close')/pl.col('raw_close')-1).abs()).alias('gap'))
print(g.select(pl.col('gap').max().alias('max'),pl.col('gap').median().alias('median'),(pl.col('gap')>1e-8).mean().alias('changed')))
print(d.filter((pl.col('close')-pl.col('raw_close')).abs()>1e-8).select('symbol','close','raw_close').head(5))
