#!/usr/bin/env python3
"""Fetch all ETF daily K from mootdx and upsert into DuckDB kline_daily."""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import polars as pl
import pandas as pd
from datetime import date, datetime
from mootdx.quotes import Quotes
from app.tickflow.repository import DataStore, KlineRepository

START_DATE = date(2026, 1, 1)

store = DataStore()
repo = KlineRepository(store)
db = store.db

etfs = [r[0] for r in db.execute('SELECT symbol FROM instruments_etf ORDER BY symbol').fetchall()]
print(f'Total ETFs: {len(etfs)}', flush=True)

c = Quotes.factory(market='std')

batch_rows = []
total_inserted = 0
errors = 0

for i, sym in enumerate(etfs):
    code = sym.split('.')[0]
    try:
        frames = []
        start = 0
        offset = 800
        for _ in range(10):
            df = c.bars(symbol=code, frequency=9, start=start, offset=offset)
            if df is None or df.empty:
                break
            frames.append(df)
            earliest = pd.Timestamp(df.index.min()).date()
            if earliest <= START_DATE:
                break
            if len(df) < offset:
                break
            start += offset

        if not frames:
            continue

        all_df = pd.concat(frames)
        all_df = all_df[~all_df.index.duplicated(keep='last')].sort_index()
        all_df = all_df[all_df.index >= pd.Timestamp(START_DATE)]
        if all_df.empty:
            continue

        for dt_idx, row in all_df.iterrows():
            dt = pd.Timestamp(dt_idx).date()
            vol = float(row.get('vol', row.get('volume', 0)))
            if 'vol' in all_df.columns and 'volume' not in all_df.columns:
                vol = float(row['vol']) * 100
            elif 'volume' in all_df.columns:
                vol = float(row['volume'])
            amt = float(row.get('amount', row.get('money', 0)))
            ts = int(datetime.combine(dt, datetime.min.time()).timestamp())
            batch_rows.append((sym, dt, float(row['open']), float(row['high']),
                              float(row['low']), float(row['close']), vol, amt, ts))

        if len(batch_rows) >= 50000:
            df_pl = pl.DataFrame(
                [{'symbol': r[0], 'date': r[1], 'open': r[2], 'high': r[3],
                  'low': r[4], 'close': r[5], 'volume': r[6], 'amount': r[7],
                  'quote_ts': r[8]} for r in batch_rows]
            )
            repo._upsert_daily(df_pl, 'kline_daily')
            total_inserted += len(batch_rows)
            print(f'[{i+1}/{len(etfs)}] Upserted {len(batch_rows)} rows, total={total_inserted}', flush=True)
            batch_rows = []

    except Exception as e:
        errors += 1
        if errors <= 10:
            print(f'Error {sym}: {e}', flush=True)

if batch_rows:
    df_pl = pl.DataFrame(
        [{'symbol': r[0], 'date': r[1], 'open': r[2], 'high': r[3],
          'low': r[4], 'close': r[5], 'volume': r[6], 'amount': r[7],
          'quote_ts': r[8]} for r in batch_rows]
    )
    repo._upsert_daily(df_pl, 'kline_daily')
    total_inserted += len(batch_rows)
    print(f'Final batch: {len(batch_rows)} rows', flush=True)

print(f'Done. Inserted: {total_inserted} rows, Errors: {errors}', flush=True)
cnt = db.execute('SELECT count(*) FROM kline_daily').fetchone()[0]
syms = db.execute('SELECT count(DISTINCT symbol) FROM kline_daily').fetchone()[0]
print(f'kline_daily now: {cnt:,} rows, {syms} symbols', flush=True)
store.db.close()
