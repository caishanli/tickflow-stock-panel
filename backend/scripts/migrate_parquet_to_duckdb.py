#!/usr/bin/env python3
"""将本地 parquet 缓存迁移至 DuckDB 并删除原文件。

支持三种来源的 parquet:
- tushare_*: ts_code, trade_date(YYYYMMDD), vol, amount
- mootdx_*:  datetime(YYYY-MM-DD HH:MM), vol/volume, amount/money
- astock_*:  time(YYYY-MM-DD), 全 VARCHAR 列

用法:
    cd backend
    python -m scripts.migrate_parquet_to_duckdb [--dry-run]
"""
from __future__ import annotations

import argparse
import contextlib
import glob
import logging
import os
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
MINUTE_DIR = DATA_DIR / "quant_kline" / "minute"
DAILY_DIR = DATA_DIR / "quant_kline" / "daily"
DB_PATH = DATA_DIR / "stock.duckdb"

KLINE_DAILY_COLS = "symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, amount DOUBLE, quote_ts BIGINT"


def migrate_minute(con, dry_run: bool = False) -> int:
    pattern = str(MINUTE_DIR / "*.parquet")
    files = glob.glob(pattern)
    if not files:
        logger.info("minute: no parquet files")
        return 0
    logger.info("minute: %d parquet files", len(files))

    if dry_run:
        total = con.execute(f"SELECT count(*) FROM read_parquet('{pattern}')").fetchone()[0]
        logger.info("minute dry-run: %d rows", total)
        return total

    con.execute("""
        CREATE TABLE IF NOT EXISTS kline_minute AS
        SELECT
            regexp_replace(filename, '.*/real_', '') AS symbol_raw,
            regexp_replace(symbol_raw, '\\.parquet$', '') AS symbol,
            datetime,
            open, high, low, close, volume, amount
        FROM read_parquet(?)
    """, [pattern])

    total = con.execute("SELECT count(*) FROM kline_minute").fetchone()[0]
    logger.info("minute done: %d rows in DuckDB", total)

    for f in files:
        os.remove(f)
    with contextlib.suppress(OSError):
        os.rmdir(MINUTE_DIR)
    return total


def _migrate_tushare_daily(con, dry_run: bool) -> int:
    files = glob.glob(str(DAILY_DIR / "tushare_*.parquet"))
    if not files:
        return 0
    logger.info("daily tushare: %d files", len(files))
    if dry_run:
        total = con.execute(
            f"SELECT count(*) FROM read_parquet('{DAILY_DIR / 'tushare_*.parquet'}')"
        ).fetchone()[0]
        logger.info("daily tushare dry-run: %d rows", total)
        return total

    con.execute(f"""
        INSERT INTO kline_daily
        SELECT
            ts_code AS symbol,
            strptime(trade_date, '%Y%m%d')::DATE AS date,
            open, high, low, close,
            vol AS volume, amount,
            0::BIGINT AS quote_ts
        FROM read_parquet('{DAILY_DIR / 'tushare_*.parquet'}')
    """)
    count = con.execute("SELECT count(*) FROM kline_daily").fetchone()[0]
    logger.info("daily tushare inserted: %d rows (total %d)", len(files), count)

    for f in files:
        os.remove(f)
    return len(files)


def _mootdx_daily_symbol(filename: str) -> str:
    """Extract symbol from mootdx filename like mootdx_159007.XSHE.parquet"""
    basename = os.path.basename(filename)
    return basename.removeprefix("mootdx_").removesuffix(".parquet")


def _migrate_mootdx_daily(con, dry_run: bool) -> int:
    files = glob.glob(str(DAILY_DIR / "mootdx_*.parquet"))
    if not files:
        return 0
    logger.info("daily mootdx: %d files", len(files))
    if dry_run:
        total = con.execute(
            f"SELECT count(*) FROM read_parquet('{DAILY_DIR / 'mootdx_*.parquet'}')"
        ).fetchone()[0]
        logger.info("daily mootdx dry-run: %d rows", total)
        return total

    con.execute(f"""
        INSERT INTO kline_daily
        SELECT
            regexp_replace(
                regexp_replace(filename, '.*/mootdx_', ''),
                '\\.parquet$', ''
            ) AS symbol,
            strptime(
                regexp_extract(datetime, '(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1),
                '%Y-%m-%d'
            )::DATE AS date,
            open, high, low, close,
            COALESCE(vol, volume) AS volume,
            COALESCE(amount, money) AS amount,
            0::BIGINT AS quote_ts
        FROM read_parquet('{DAILY_DIR / 'mootdx_*.parquet'}')
    """)
    count = con.execute("SELECT count(*) FROM kline_daily").fetchone()[0]
    logger.info("daily mootdx inserted: %d rows (total %d)", len(files), count)

    for f in files:
        os.remove(f)
    return len(files)


def _migrate_astock_daily(con, dry_run: bool) -> int:
    files = glob.glob(str(DAILY_DIR / "astock_*.parquet"))
    if not files:
        return 0
    logger.info("daily astock: %d files", len(files))
    if dry_run:
        total = con.execute(
            f"SELECT count(*) FROM read_parquet('{DAILY_DIR / 'astock_*.parquet'}')"
        ).fetchone()[0]
        logger.info("daily astock dry-run: %d rows", total)
        return total

    con.execute(f"""
        INSERT INTO kline_daily
        SELECT
            regexp_replace(
                regexp_replace(filename, '.*/astock_', ''),
                '\\.parquet$', ''
            ) AS symbol,
            strptime(time, '%Y-%m-%d')::DATE AS date,
            open::DOUBLE, high::DOUBLE, low::DOUBLE, close::DOUBLE,
            volume::DOUBLE, amount::DOUBLE,
            0::BIGINT AS quote_ts
        FROM read_parquet('{DAILY_DIR / 'astock_*.parquet'}')
    """)
    count = con.execute("SELECT count(*) FROM kline_daily").fetchone()[0]
    logger.info("daily astock inserted: %d rows (total %d)", len(files), count)

    for f in files:
        os.remove(f)
    return len(files)


def migrate_daily(con, dry_run: bool = False) -> int:
    files = glob.glob(str(DAILY_DIR / "*.parquet"))
    if not files:
        logger.info("daily: no parquet files")
        return 0
    logger.info("daily: %d total parquet files", len(files))

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS kline_daily (
            {KLINE_DAILY_COLS},
            PRIMARY KEY (symbol, date)
        )
    """)

    n = 0
    n += _migrate_tushare_daily(con, dry_run)
    n += _migrate_mootdx_daily(con, dry_run)
    n += _migrate_astock_daily(con, dry_run)

    if not dry_run and n > 0:
        try:
            os.rmdir(DAILY_DIR)
        except OSError:
            remaining = glob.glob(str(DAILY_DIR / "*.parquet"))
            logger.warning("daily: %d parquet files remain", len(remaining))

    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="parquet -> DuckDB migration")
    parser.add_argument("--dry-run", action="store_true", help="只统计不写入")
    args = parser.parse_args()

    import duckdb
    con = duckdb.connect(str(DB_PATH))

    t0 = time.perf_counter()
    m_rows = migrate_minute(con, args.dry_run)
    d_rows = migrate_daily(con, args.dry_run)
    elapsed = time.perf_counter() - t0

    duckdb_size = os.path.getsize(DB_PATH) if DB_PATH.exists() else 0
    logger.info("migration complete: %d minute + %d daily rows, %.1fs, duckdb=%.1fMB",
                m_rows, d_rows, elapsed, duckdb_size / 1024 / 1024)

    con.close()


if __name__ == "__main__":
    main()
