"""One-time migration from Parquet files to DuckDB.

Usage: python -m scripts.migrate_parquet_to_duckdb [--data-dir DATA_DIR] [--db-path DB_PATH]
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
import polars as pl

logger = logging.getLogger(__name__)

# Table definitions: (duckdb_table, parquet_subdir, primary_keys)
TABLES = [
    ("instruments", "instruments", ["symbol"]),
    ("instruments_index", "instruments_index", ["symbol"]),
    ("instruments_etf", "instruments_etf", ["symbol"]),
    ("instruments_ext", "instruments_ext", ["symbol"]),
    ("adj_factor", "adj_factor", ["symbol", "trade_date"]),
    ("adj_factor_etf", "adj_factor_etf", ["symbol", "trade_date"]),
    ("kline_daily", "kline_daily", ["symbol", "date"]),
    ("kline_daily_enriched", "kline_daily_enriched", ["symbol", "date"]),
    ("kline_index_daily", "kline_index_daily", ["symbol", "date"]),
    ("kline_index_enriched", "kline_index_enriched", ["symbol", "date"]),
    ("kline_etf_daily", "kline_etf_daily", ["symbol", "date"]),
    ("kline_etf_enriched", "kline_etf_enriched", ["symbol", "date"]),
    ("kline_minute", "kline_minute", ["symbol", "datetime"]),
    ("kline_etf_minute", "kline_etf_minute", ["symbol", "datetime"]),
    ("kline_ext", "kline_ext", ["symbol", "date"]),
    ("depth5", "depth5", ["symbol", "date"]),
    ("financials_metrics", "financials/metrics", ["symbol", "report_date"]),
    ("financials_income", "financials/income", ["symbol", "report_date"]),
    ("financials_balance_sheet", "financials/balance_sheet", ["symbol", "report_date"]),
    ("financials_cash_flow", "financials/cash_flow", ["symbol", "report_date"]),
    ("financials_shares", "financials/shares", ["symbol", "report_date"]),
    ("pools", "pools", ["pool_name", "symbol"]),
]


def find_parquet_files(data_dir: Path, subdir: str) -> list[Path]:
    """Find all Parquet files in a subdirectory (recursive)."""
    target = data_dir / subdir
    if not target.exists():
        return []
    return sorted(target.rglob("*.parquet"))


def read_parquet_files(data_dir: Path, subdir: str) -> pl.DataFrame | None:
    """Read and concatenate all Parquet files in a subdirectory."""
    files = find_parquet_files(data_dir, subdir)
    if not files:
        return None

    dfs = []
    for f in files:
        try:
            df = pl.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            logger.warning("Failed to read %s: %s", f, e)

    if not dfs:
        return None

    return pl.concat(dfs, how="diagonal_relaxed")


def create_table_schema(
    conn: duckdb.DuckDBPyConnection, table_name: str, df: pl.DataFrame
) -> None:
    """Create DuckDB table with appropriate schema from Polars DataFrame."""
    col_defs = []
    for col, dtype in df.schema.items():
        duckdb_type = {
            pl.Utf8: "VARCHAR",
            pl.Float64: "DOUBLE",
            pl.Float32: "FLOAT",
            pl.Int64: "BIGINT",
            pl.Int32: "INTEGER",
            pl.Int16: "SMALLINT",
            pl.Int8: "TINYINT",
            pl.UInt32: "UINTEGER",
            pl.UInt64: "UBIGINT",
            pl.Date: "DATE",
            pl.Datetime: "TIMESTAMP",
            pl.Boolean: "BOOLEAN",
        }.get(dtype, "VARCHAR")
        col_defs.append(f'"{col}" {duckdb_type}')

    cols = ", ".join(col_defs)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})")


def migrate_parquet_to_duckdb(data_dir: Path, db_path: Path) -> None:
    """Migrate all Parquet files to DuckDB tables."""
    logger.info("Starting migration: %s -> %s", data_dir, db_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    for table_name, subdir, primary_keys in TABLES:
        logger.info("Migrating %s from %s/", table_name, subdir)

        df = read_parquet_files(data_dir, subdir)
        if df is None or df.is_empty():
            logger.info("  No data found, skipping")
            continue

        create_table_schema(conn, table_name, df)

        conn.register("_tmp_df", df)

        if primary_keys:
            key_cols = ", ".join(primary_keys)

            conn.execute(f"CREATE TEMPORARY TABLE tmp_{table_name} AS SELECT * FROM _tmp_df")

            conn.execute(
                f"DELETE FROM {table_name} "
                f"WHERE ({key_cols}) IN (SELECT {key_cols} FROM tmp_{table_name})"
            )

            conn.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_{table_name}")
            conn.execute(f"DROP TABLE tmp_{table_name}")
        else:
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM _tmp_df")

        count = conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        logger.info("  Migrated %d rows", count)

    conn.close()
    logger.info("Migration complete: %s", db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Parquet to DuckDB")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--db-path", type=Path, default=Path("data/stock.duckdb"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    migrate_parquet_to_duckdb(args.data_dir, args.db_path)


if __name__ == "__main__":
    main()
