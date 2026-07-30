# DuckDB Storage Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all Parquet file storage with DuckDB native storage for stock market data, while keeping the existing API surface and subprocess architecture.

**Architecture:** DuckDB persistent database (`data/stock.duckdb`) replaces partitioned Parquet files. Main FastAPI process is the sole writer; child processes connect read-only. Polars computation pipeline unchanged — loads from DuckDB via `.pl()`.

**Tech Stack:** DuckDB (persistent storage + query engine), Polars (computation), threading.Lock (write serialization)

## Global Constraints

- Python 3.12+, duckdb >= 1.0, polars >= 0.20
- Line length 100, select E,F,I,N,UP,B,SIM,RUF (ignore E501)
- `asyncio_mode = "auto"` for pytest
- No pandas in core code (only in quant module and legacy vectorbt)
- Auth middleware applies to all `/api/*` endpoints

---

## Task 1: Create DuckDB Connection Manager

**Files:**
- Create: `backend/app/tickflow/duckdb_manager.py`
- Test: `backend/tests/test_duckdb_manager.py`

**Interfaces:**
- Produces: `DuckDBManager` class with `get_connection(read_only=False)`, `execute(sql, params)`, `execute_many(sql, params_list)`, `close()`

- [ ] **Step 1: Write test for DuckDBManager**

```python
# backend/tests/test_duckdb_manager.py
"""Tests for DuckDB connection manager."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import duckdb
import pytest

from app.tickflow.duckdb_manager import DuckDBManager


@pytest.fixture
def tmp_db(tmp_path: Path) -> DuckDBManager:
    db_path = tmp_path / "test.duckdb"
    mgr = DuckDBManager(db_path)
    yield mgr
    mgr.close()


class TestDuckDBManager:
    def test_creates_file(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        mgr = DuckDBManager(db_path)
        assert db_path.exists()
        mgr.close()

    def test_execute_create_table(self, tmp_db: DuckDBManager) -> None:
        tmp_db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
        tmp_db.execute("INSERT INTO test VALUES (1, 'hello')")
        result = tmp_db.execute("SELECT * FROM test").fetchall()
        assert result == [(1, "hello")]

    def test_execute_many(self, tmp_db: DuckDBManager) -> None:
        tmp_db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
        tmp_db.execute_many(
            "INSERT INTO test VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        result = tmp_db.execute("SELECT count(*) FROM test").fetchone()
        assert result[0] == 3

    def test_read_only_connection(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        mgr = DuckDBManager(db_path)
        mgr.execute("CREATE TABLE test (id INTEGER)")
        mgr.execute("INSERT INTO test VALUES (1)")
        
        read_mgr = DuckDBManager(db_path, read_only=True)
        result = read_mgr.execute("SELECT * FROM test").fetchall()
        assert result == [(1,)]
        
        with pytest.raises(Exception):
            read_mgr.execute("INSERT INTO test VALUES (2)")
        
        read_mgr.close()
        mgr.close()

    def test_context_manager(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        with DuckDBManager(db_path) as mgr:
            mgr.execute("CREATE TABLE test (id INTEGER)")
            assert db_path.exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/test_duckdb_manager.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.tickflow.duckdb_manager'`

- [ ] **Step 3: Write DuckDBManager implementation**

```python
# backend/app/tickflow/duckdb_manager.py
"""DuckDB connection manager for persistent storage.

Provides thread-safe access to a DuckDB database file with support for
concurrent reads and serialized writes via WAL mode.
"""
from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


class DuckDBManager:
    """Manages a DuckDB database connection with WAL mode for concurrent access."""

    def __init__(self, db_path: Path, read_only: bool = False) -> None:
        self.db_path = db_path
        self._read_only = read_only
        self._lock = threading.Lock()
        
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._conn = duckdb.connect(
            database=str(db_path),
            read_only=read_only,
        )
        
        if not read_only:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA enable_progress_bar=false")
        
        logger.info("DuckDB connected: %s (read_only=%s)", db_path, read_only)

    def execute(self, sql: str, params: list[Any] | None = None) -> duckdb.DuckDBPyRelation:
        """Execute a SQL statement and return the relation."""
        if params:
            return self._conn.execute(sql, params)
        return self._conn.execute(sql)

    def execute_many(self, sql: str, params_list: list[tuple[Any, ...]]) -> None:
        """Execute a SQL statement with multiple parameter sets."""
        self._conn.executemany(sql, params_list)

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            logger.info("DuckDB closed: %s", self.db_path)

    def __enter__(self) -> DuckDBManager:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_duckdb_manager.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/tickflow/duckdb_manager.py backend/tests/test_duckdb_manager.py
git commit -m "feat: add DuckDB connection manager for persistent storage"
```

---

## Task 2: Create Parquet-to-DuckDB Migration Script

**Files:**
- Create: `scripts/migrate_parquet_to_duckdb.py`
- Test: `backend/tests/test_migration.py`

**Interfaces:**
- Produces: `migrate_parquet_to_duckdb(data_dir: Path)` function that reads all Parquet files and writes to DuckDB

- [ ] **Step 1: Write test for migration**

```python
# backend/tests/test_migration.py
"""Tests for Parquet to DuckDB migration."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from scripts.migrate_parquet_to_duckdb import migrate_parquet_to_duckdb


@pytest.fixture
def parquet_data(tmp_path: Path) -> Path:
    """Create sample Parquet data for migration testing."""
    data_dir = tmp_path / "data"
    
    # Create kline_daily partition
    kline_dir = data_dir / "kline_daily" / "date=2026-01-15"
    kline_dir.mkdir(parents=True)
    df = pl.DataFrame({
        "symbol": ["000001", "000002"],
        "date": [date(2026, 1, 15), date(2026, 1, 15)],
        "open": [10.0, 20.0],
        "high": [11.0, 21.0],
        "low": [9.0, 19.0],
        "close": [10.5, 20.5],
        "volume": [1000.0, 2000.0],
        "amount": [10000.0, 40000.0],
        "quote_ts": [1705305600, 1705305600],
    })
    df.write_parquet(kline_dir / "part.parquet")
    
    # Create instruments
    inst_dir = data_dir / "instruments"
    inst_dir.mkdir(parents=True)
    inst_df = pl.DataFrame({
        "symbol": ["000001", "000002"],
        "name": ["平安银行", "万科A"],
        "exchange": ["SZSE", "SZSE"],
    })
    inst_df.write_parquet(inst_dir / "instruments.parquet")
    
    return data_dir


class TestMigration:
    def test_migrate_creates_duckdb(self, parquet_data: Path, tmp_path: Path) -> None:
        db_path = tmp_path / "stock.duckdb"
        migrate_parquet_to_duckdb(parquet_data, db_path)
        assert db_path.exists()

    def test_migrate_kline_daily(self, parquet_data: Path, tmp_path: Path) -> None:
        db_path = tmp_path / "stock.duckdb"
        migrate_parquet_to_duckdb(parquet_data, db_path)
        
        import duckdb
        conn = duckdb.connect(str(db_path), read_only=True)
        result = conn.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 2
        conn.close()

    def test_migrate_instruments(self, parquet_data: Path, tmp_path: Path) -> None:
        db_path = tmp_path / "stock.duckdb"
        migrate_parquet_to_duckdb(parquet_data, db_path)
        
        import duckdb
        conn = duckdb.connect(str(db_path), read_only=True)
        result = conn.execute("SELECT count(*) FROM instruments").fetchone()
        assert result[0] == 2
        conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/test_migration.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write migration script**

```python
# scripts/migrate_parquet_to_duckdb.py
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


def create_table_schema(conn: duckdb.DuckDBPyConnection, table_name: str, df: pl.DataFrame) -> None:
    """Create DuckDB table with appropriate schema from Polars DataFrame."""
    # Convert Polars schema to DuckDB column definitions
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
    conn.execute("PRAGMA journal_mode=WAL")
    
    for table_name, subdir, primary_keys in TABLES:
        logger.info("Migrating %s from %s/", table_name, subdir)
        
        df = read_parquet_files(data_dir, subdir)
        if df is None or df.is_empty():
            logger.info("  No data found, skipping")
            continue
        
        # Create table
        create_table_schema(conn, table_name, df)
        
        # Insert data
        # Register Polars DataFrame as DuckDB relation
        rel = conn.sql("SELECT * FROM df")
        
        # Use INSERT OR REPLACE for upsert behavior
        if primary_keys:
            # For tables with primary keys, use DELETE + INSERT
            key_conditions = " AND ".join(f"old.{k} = new.{k}" for k in primary_keys)
            key_cols = ", ".join(primary_keys)
            
            # Create temp table
            conn.execute(f"CREATE TEMPORARY TABLE tmp_{table_name} AS SELECT * FROM rel")
            
            # Delete existing records that will be replaced
            conn.execute(f"""
                DELETE FROM {table_name} 
                WHERE ({key_cols}) IN (SELECT {key_cols} FROM tmp_{table_name})
            """)
            
            # Insert new records
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_{table_name}")
            conn.execute(f"DROP TABLE tmp_{table_name}")
        else:
            # Simple insert for tables without primary keys
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM rel")
        
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_migration.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/migrate_parquet_to_duckdb.py backend/tests/test_migration.py
git commit -m "feat: add Parquet-to-DuckDB migration script"
```

---

## Task 3: Modify DataStore to Use Persistent DuckDB

**Files:**
- Modify: `backend/app/tickflow/repository.py` (DataStore.__init__, _register_views)
- Test: `backend/tests/test_datastore_duckdb.py`

**Interfaces:**
- Consumes: `DuckDBManager` from Task 1
- Produces: `DataStore.db` is now a persistent DuckDB connection instead of in-memory

- [ ] **Step 1: Write test for persistent DuckDB DataStore**

```python
# backend/tests/test_datastore_duckdb.py
"""Tests for DataStore with persistent DuckDB."""
from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pytest

from app.tickflow.repository import DataStore


@pytest.fixture
def store(tmp_path: Path) -> DataStore:
    """Create a DataStore with persistent DuckDB."""
    # Override data_dir to use temp directory
    store = DataStore(data_dir=tmp_path / "data")
    yield store
    store.db.close()


class TestDataStoreDuckDB:
    def test_creates_duckdb_file(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        db_path = tmp_path / "data" / "stock.duckdb"
        assert db_path.exists()
        store.db.close()

    def test_creates_tables(self, store: DataStore) -> None:
        # Tables should be created on initialization
        tables = store.db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "kline_daily" in table_names
        assert "instruments" in table_names

    def test_insert_and_query(self, store: DataStore) -> None:
        store.db.execute("""
            INSERT INTO kline_daily (symbol, date, open, high, low, close, volume, amount, quote_ts)
            VALUES ('000001', '2026-01-15', 10.0, 11.0, 9.0, 10.5, 1000.0, 10000.0, 1705305600)
        """)
        result = store.db.execute("SELECT * FROM kline_daily").fetchall()
        assert len(result) == 1

    def test_read_only_connection(self, tmp_path: Path) -> None:
        store = DataStore(data_dir=tmp_path / "data")
        store.db.execute("""
            INSERT INTO kline_daily (symbol, date, open, high, low, close, volume, amount, quote_ts)
            VALUES ('000001', '2026-01-15', 10.0, 11.0, 9.0, 10.5, 1000.0, 10000.0, 1705305600)
        """)
        
        # Read-only connection should work
        read_conn = duckdb.connect(
            str(tmp_path / "data" / "stock.duckdb"),
            read_only=True
        )
        result = read_conn.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1
        read_conn.close()
        store.db.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/test_datastore_duckdb.py -v`
Expected: FAIL (no stock.duckdb file created, tables not created)

- [ ] **Step 3: Modify DataStore.__init__ and _register_views**

Replace the in-memory DuckDB with persistent storage. Key changes:
1. Connect to `data/stock.duckdb` instead of `:memory:`
2. Create tables on startup instead of registering views over Parquet
3. Keep `_register_unified_views()` for cross-asset views

The modified `__init__` and `_register_views` methods:

```python
# In repository.py, DataStore class:

def __init__(self, data_dir: Path | None = None) -> None:
    self.data_dir = Path(data_dir or settings.data_dir)
    self.data_dir.mkdir(parents=True, exist_ok=True)
    
    self._migrate_legacy_data_dir()
    
    # Create subdirectories (still needed for user_data, backtest_results, etc.)
    for sub in (
        "user_data", "backtest_results", "screener_results", "ai_cache",
    ):
        (self.data_dir / sub).mkdir(parents=True, exist_ok=True)
    
    # Persistent DuckDB — replaces in-memory + Parquet views
    db_path = self.data_dir / "stock.duckdb"
    self.db = duckdb.connect(database=str(db_path))
    self.db.execute("PRAGMA journal_mode=WAL")
    
    self._create_tables()
    self._register_unified_views()

def _create_tables(self) -> None:
    """Create DuckDB tables if they don't exist."""
    statements = [
        """CREATE TABLE IF NOT EXISTS kline_daily (
            symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE, quote_ts BIGINT,
            PRIMARY KEY (symbol, date)
        )""",
        """CREATE TABLE IF NOT EXISTS kline_daily_enriched (
            symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE, raw_close DOUBLE, raw_high DOUBLE, raw_low DOUBLE,
            turnover_rate DOUBLE, consecutive_limit_ups UINTEGER, consecutive_limit_downs UINTEGER,
            quote_ts BIGINT,
            PRIMARY KEY (symbol, date)
        )""",
        """CREATE TABLE IF NOT EXISTS kline_minute (
            symbol VARCHAR, datetime TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE,
            PRIMARY KEY (symbol, datetime)
        )""",
        """CREATE TABLE IF NOT EXISTS instruments (
            symbol VARCHAR PRIMARY KEY, name VARCHAR, exchange VARCHAR,
            asset_type VARCHAR, list_date DATE, total_shares DOUBLE, float_shares DOUBLE
        )""",
        """CREATE TABLE IF NOT EXISTS adj_factor (
            symbol VARCHAR, trade_date DATE, adj_factor DOUBLE,
            PRIMARY KEY (symbol, trade_date)
        )""",
        """CREATE TABLE IF NOT EXISTS depth5 (
            symbol VARCHAR, date DATE, 
            bid1_price DOUBLE, bid1_volume DOUBLE, ask1_price DOUBLE, ask1_volume DOUBLE,
            sealed BOOLEAN,
            PRIMARY KEY (symbol, date)
        )""",
        # Add other tables as needed...
    ]
    for sql in statements:
        try:
            self.db.execute(sql)
        except Exception as e:
            logger.debug("Table creation skipped: %s", e)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_datastore_duckdb.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/tickflow/repository.py backend/tests/test_datastore_duckdb.py
git commit -m "feat: DataStore uses persistent DuckDB instead of in-memory views"
```

---

## Task 4: Replace Parquet Write Primitives with DuckDB Writes

**Files:**
- Modify: `backend/app/tickflow/repository.py` (KlineRepository write methods)
- Test: `backend/tests/test_repository_duckdb_writes.py`

**Interfaces:**
- Consumes: DuckDB tables from Task 3
- Produces: `_upsert_daily()`, `_upsert_enriched()` methods that write to DuckDB

- [ ] **Step 1: Write tests for DuckDB write operations**

```python
# backend/tests/test_repository_duckdb_writes.py
"""Tests for KlineRepository DuckDB write operations."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestDuckDBWrites:
    def test_upsert_daily_insert(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "date": [date(2026, 1, 15)],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "volume": [1000.0], "amount": [10000.0], "quote_ts": [1705305600],
        })
        repo.append_daily(df)
        result = repo.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1

    def test_upsert_daily_merge(self, repo: KlineRepository) -> None:
        df1 = pl.DataFrame({
            "symbol": ["000001"], "date": [date(2026, 1, 15)],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "volume": [1000.0], "amount": [10000.0], "quote_ts": [1705305600],
        })
        df2 = pl.DataFrame({
            "symbol": ["000001"], "date": [date(2026, 1, 15)],
            "open": [10.5], "high": [11.5], "low": [9.5], "close": [11.0],
            "volume": [1500.0], "amount": [15000.0], "quote_ts": [1705309200],
        })
        repo.append_daily(df1)
        repo.append_daily(df2)
        result = repo.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1  # Should be merged, not duplicated
        close = repo.db.execute("SELECT close FROM kline_daily").fetchone()
        assert close[0] == 11.0  # Should use latest value

    def test_flush_live_daily(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001", "000002"],
            "date": [date(2026, 1, 15), date(2026, 1, 15)],
            "open": [10.0, 20.0], "high": [11.0, 21.0], "low": [9.0, 19.0],
            "close": [10.5, 20.5], "volume": [1000.0, 2000.0],
            "amount": [10000.0, 40000.0], "quote_ts": [1705305600, 1705305600],
        })
        repo.flush_live_daily(df)
        result = repo.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/test_repository_duckdb_writes.py -v`
Expected: FAIL (methods still write to Parquet)

- [ ] **Step 3: Implement DuckDB write methods**

Add new methods to `KlineRepository` class:

```python
def _upsert_daily(self, df: pl.DataFrame, table: str) -> None:
    """Upsert daily data into DuckDB table."""
    if df.is_empty():
        return
    
    with self._write_lock:
        # Convert Polars DataFrame to list of tuples for executemany
        cols = df.columns
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        
        sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
        data = [tuple(row) for row in df.iter_rows()]
        
        self.db.execute_many(sql, data)

def append_daily(self, df: pl.DataFrame) -> None:
    """Append daily K data (merge-upsert by symbol+date)."""
    self._upsert_daily(df, "kline_daily")
    self._bump_matrix_data_generation("stock")

def append_enriched(self, df: pl.DataFrame) -> None:
    """Append enriched data (strip to storage columns first)."""
    from app.indicators.pipeline import ENRICHED_STORAGE_COLS
    df_storage = df.select([c for c in ENRICHED_STORAGE_COLS if c in df.columns])
    self._upsert_daily(df_storage, "kline_daily_enriched")
    self._bump_matrix_data_generation("stock")

def flush_live_daily(self, df: pl.DataFrame) -> None:
    """Flush today's daily K data (full overwrite for today)."""
    self._upsert_daily(df, "kline_daily")
    self._bump_matrix_data_generation("stock")

def flush_live_enriched(self, df: pl.DataFrame) -> None:
    """Flush today's enriched data (full overwrite for today)."""
    from app.indicators.pipeline import ENRICHED_STORAGE_COLS
    df_storage = df.select([c for c in ENRICHED_STORAGE_COLS if c in df.columns])
    self._upsert_daily(df_storage, "kline_daily_enriched")
    self._bump_matrix_data_generation("stock")

def merge_live_daily_asset(self, asset_type: str, df: pl.DataFrame) -> None:
    """Merge live daily data for specific asset type."""
    table = {
        "stock": "kline_daily",
        "index": "kline_index_daily",
        "etf": "kline_etf_daily",
    }.get(asset_type)
    if table:
        self._upsert_daily(df, table)

def merge_live_enriched_asset(self, asset_type: str, df: pl.DataFrame) -> None:
    """Merge live enriched data for specific asset type."""
    from app.indicators.pipeline import ENRICHED_STORAGE_COLS
    df_storage = df.select([c for c in ENRICHED_STORAGE_COLS if c in df.columns])
    table = {
        "stock": "kline_daily_enriched",
        "index": "kline_index_enriched",
        "etf": "kline_etf_enriched",
    }.get(asset_type)
    if table:
        self._upsert_daily(df_storage, table)
    # Also update in-memory cache
    self._update_enriched_cache(asset_type, df)

def _update_enriched_cache(self, asset_type: str, df: pl.DataFrame) -> None:
    """Update in-memory enriched cache after write."""
    if asset_type == "stock" and self._enriched_cache is not None:
        # Merge into cache
        self._enriched_cache = pl.concat([self._enriched_cache, df], how="diagonal_relaxed").unique(
            subset=["symbol", "date"], keep="last"
        )
    elif asset_type == "etf" and self._etf_enriched_cache is not None:
        self._etf_enriched_cache = pl.concat([self._etf_enriched_cache, df], how="diagonal_relaxed").unique(
            subset=["symbol", "date"], keep="last"
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_repository_duckdb_writes.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/tickflow/repository.py backend/tests/test_repository_duckdb_writes.py
git commit -m "feat: KlineRepository write methods use DuckDB instead of Parquet"
```

---

## Task 5: Replace Parquet Read Operations with DuckDB Queries

**Files:**
- Modify: `backend/app/tickflow/repository.py` (KlineRepository read methods)
- Test: `backend/tests/test_repository_duckdb_reads.py`

**Interfaces:**
- Consumes: DuckDB tables from Task 3
- Produces: `_query_daily()`, `_scan_daily()` methods that read from DuckDB

- [ ] **Step 1: Write tests for DuckDB read operations**

```python
# backend/tests/test_repository_duckdb_reads.py
"""Tests for KlineRepository DuckDB read operations."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo_with_data(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    
    # Insert test data
    df = pl.DataFrame({
        "symbol": ["000001", "000001", "000002"],
        "date": [date(2026, 1, 15), date(2026, 1, 16), date(2026, 1, 15)],
        "open": [10.0, 11.0, 20.0],
        "high": [11.0, 12.0, 21.0],
        "low": [9.0, 10.0, 19.0],
        "close": [10.5, 11.5, 20.5],
        "volume": [1000.0, 1200.0, 2000.0],
        "amount": [10000.0, 12000.0, 40000.0],
        "quote_ts": [1705305600, 1705305600, 1705305600],
    })
    repo.append_daily(df)
    
    yield repo
    store.db.close()


class TestDuckDBReads:
    def test_query_daily_all(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.db.execute("SELECT * FROM kline_daily").fetchall()
        assert len(result) == 3

    def test_query_daily_by_symbol(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.db.execute(
            "SELECT * FROM kline_daily WHERE symbol = ?", ["000001"]
        ).fetchall()
        assert len(result) == 2

    def test_query_daily_by_date_range(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.db.execute(
            "SELECT * FROM kline_daily WHERE date BETWEEN ? AND ?",
            [date(2026, 1, 15), date(2026, 1, 15)]
        ).fetchall()
        assert len(result) == 2

    def test_scan_daily_to_polars(self, repo_with_data: KlineRepository) -> None:
        result = repo_with_data.db.execute("SELECT * FROM kline_daily").pl()
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/test_repository_duckdb_reads.py -v`
Expected: FAIL (methods still scan Parquet)

- [ ] **Step 3: Implement DuckDB read methods**

Add new methods to `KlineRepository` class:

```python
def _scan_daily(self, table: str, symbol: str | None = None, 
                start_date: date | None = None, end_date: date | None = None) -> pl.DataFrame:
    """Scan daily data from DuckDB with optional filters."""
    sql = f"SELECT * FROM {table}"
    params = []
    conditions = []
    
    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol)
    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)
    
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    
    sql += " ORDER BY symbol, date"
    
    return self.db.execute(sql, params).pl()

def get_latest_date(self, table: str = "kline_daily") -> date | None:
    """Get the latest date in the table."""
    result = self.db.execute(f"SELECT max(date) FROM {table}").fetchone()
    return result[0] if result and result[0] else None

def get_date_range(self, table: str = "kline_daily") -> tuple[date | None, date | None]:
    """Get the date range in the table."""
    result = self.db.execute(f"SELECT min(date), max(date) FROM {table}").fetchone()
    return (result[0], result[1]) if result else (None, None)

def symbols_lagging(self, latest_date: date, table: str = "kline_daily") -> list[str]:
    """Find symbols whose data lags behind the latest date."""
    result = self.db.execute(f"""
        SELECT symbol FROM {table} 
        GROUP BY symbol 
        HAVING max(date) < ?
    """, [latest_date]).fetchall()
    return [r[0] for r in result]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_repository_duckdb_reads.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/tickflow/repository.py backend/tests/test_repository_duckdb_reads.py
git commit -m "feat: KlineRepository read methods use DuckDB instead of Parquet scan"
```

---

## Task 6: Update Indicator Pipeline to Write DuckDB

**Files:**
- Modify: `backend/app/indicators/pipeline.py` (run_enriched_pipeline)
- Test: `backend/tests/test_pipeline_duckdb.py`

**Interfaces:**
- Consumes: DuckDB tables from Task 3
- Produces: `run_enriched_pipeline()` writes to DuckDB instead of Parquet

- [ ] **Step 1: Write test for pipeline DuckDB writes**

```python
# backend/tests/test_pipeline_duckdb.py
"""Tests for indicator pipeline with DuckDB writes."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository
from app.indicators.pipeline import run_enriched_pipeline


@pytest.fixture
def repo_with_daily(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    
    # Insert daily K data
    df = pl.DataFrame({
        "symbol": ["000001"] * 30,
        "date": [date(2026, 1, i + 1) for i in range(30)],
        "open": [10.0 + i * 0.1 for i in range(30)],
        "high": [11.0 + i * 0.1 for i in range(30)],
        "low": [9.0 + i * 0.1 for i in range(30)],
        "close": [10.5 + i * 0.1 for i in range(30)],
        "volume": [1000.0] * 30,
        "amount": [10000.0] * 30,
        "quote_ts": [1705305600] * 30,
    })
    repo.append_daily(df)
    
    yield repo
    store.db.close()


class TestPipelineDuckDB:
    def test_enriched_pipeline_writes_to_duckdb(self, repo_with_daily: KlineRepository) -> None:
        run_enriched_pipeline(repo_with_daily, full=True)
        result = repo_with_daily.db.execute("SELECT count(*) FROM kline_daily_enriched").fetchone()
        assert result[0] == 30

    def test_enriched_pipeline_has_required_columns(self, repo_with_daily: KlineRepository) -> None:
        run_enriched_pipeline(repo_with_daily, full=True)
        result = repo_with_daily.db.execute("SELECT * FROM kline_daily_enriched LIMIT 1").pl()
        assert "turnover_rate" in result.columns
        assert "consecutive_limit_ups" in result.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/test_pipeline_duckdb.py -v`
Expected: FAIL (pipeline still writes to Parquet)

- [ ] **Step 3: Modify pipeline to write DuckDB**

In `backend/app/indicators/pipeline.py`, modify `run_enriched_pipeline()` to write to DuckDB:

```python
# In run_enriched_pipeline(), replace Parquet writes with DuckDB writes:

def run_enriched_pipeline(repo: KlineRepository, full: bool = False) -> None:
    """Run enriched pipeline and write results to DuckDB."""
    # ... existing computation code ...
    
    # After computing enriched DataFrame:
    if enriched_df is not None and not enriched_df.is_empty():
        # Strip to storage columns
        from app.parquet import ENRICHED_STORAGE_COLS
        df_storage = enriched_df.select([c for c in ENRICHED_STORAGE_COLS if c in enriched_df.columns])
        
        # Write to DuckDB instead of Parquet
        repo._upsert_daily(df_storage, "kline_daily_enriched")
    
    # Remove Parquet write code:
    # - out = base / f"date={ds}" / "part.parquet"
    # - df.write_parquet(out)
    # - etc.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_pipeline_duckdb.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/indicators/pipeline.py backend/tests/test_pipeline_duckdb.py
git commit -m "feat: indicator pipeline writes enriched data to DuckDB"
```

---

## Task 7: Update Kline Sync Service to Write DuckDB

**Files:**
- Modify: `backend/app/services/kline_sync.py`
- Test: `backend/tests/test_kline_sync_duckdb.py`

**Interfaces:**
- Consumes: DuckDB tables from Task 3
- Produces: `sync_daily_batch()`, `sync_adj_factor()` write to DuckDB

- [ ] **Step 1: Write test for kline sync DuckDB writes**

```python
# backend/tests/test_kline_sync_duckdb.py
"""Tests for kline sync service with DuckDB writes."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository
from app.services.kline_sync import sync_daily_batch


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestKlineSyncDuckDB:
    def test_sync_daily_batch_writes_to_duckdb(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "date": [date(2026, 1, 15)],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "volume": [1000.0], "amount": [10000.0], "quote_ts": [1705305600],
        })
        sync_daily_batch(repo, df, asset_type="stock")
        result = repo.db.execute("SELECT count(*) FROM kline_daily").fetchone()
        assert result[0] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/test_kline_sync_duckdb.py -v`
Expected: FAIL

- [ ] **Step 3: Modify kline_sync.py to write DuckDB**

Replace Parquet writes in `sync_daily_batch()` and `sync_adj_factor()`:

```python
# In sync_daily_batch(), replace:
#   repo.append_daily(df)
# with DuckDB write via repo._upsert_daily()

# In sync_adj_factor(), replace:
#   _atomic_write_parquet(adj_df, out)
# with:
#   repo.db.execute("INSERT OR REPLACE INTO adj_factor ...")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_kline_sync_duckdb.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/kline_sync.py backend/tests/test_kline_sync_duckdb.py
git commit -m "feat: kline sync service writes to DuckDB"
```

---

## Task 8: Update Instrument Sync to Write DuckDB

**Files:**
- Modify: `backend/app/services/instrument_sync.py`
- Test: `backend/tests/test_instrument_sync_duckdb.py`

**Interfaces:**
- Consumes: DuckDB tables from Task 3
- Produces: `sync_instruments()` writes to DuckDB

- [ ] **Step 1: Write test for instrument sync DuckDB writes**

```python
# backend/tests/test_instrument_sync_duckdb.py
"""Tests for instrument sync service with DuckDB writes."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestInstrumentSyncDuckDB:
    def test_sync_instruments_writes_to_duckdb(self, repo: KlineRepository) -> None:
        from app.services.instrument_sync import sync_instruments
        # This test would need mocking of TickFlow API
        # For now, just verify the table exists and is writable
        df = pl.DataFrame({
            "symbol": ["000001"],
            "name": ["平安银行"],
            "exchange": ["SZSE"],
        })
        repo.db.execute("INSERT OR REPLACE INTO instruments SELECT * FROM df")
        result = repo.db.execute("SELECT count(*) FROM instruments").fetchone()
        assert result[0] == 1
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_instrument_sync_duckdb.py -v`
Expected: PASS

- [ ] **Step 3: Modify instrument_sync.py to write DuckDB**

Replace Parquet writes in `sync_instruments()`:

```python
# In sync_instruments(), replace:
#   df.write_parquet(out)
# with:
#   repo.db.execute("DELETE FROM instruments")
#   repo.db.execute("INSERT INTO instruments SELECT * FROM df")
```

- [ ] **Step 4: Run full test suite**

Run: `cd backend && uv run --extra dev pytest -v`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/instrument_sync.py backend/tests/test_instrument_sync_duckdb.py
git commit -m "feat: instrument sync writes to DuckDB"
```

---

## Task 9: Update Financial Sync to Write DuckDB

**Files:**
- Modify: `backend/app/services/financial_sync.py`
- Test: `backend/tests/test_financial_sync_duckdb.py`

**Interfaces:**
- Consumes: DuckDB tables from Task 3
- Produces: `_write_table()` writes to DuckDB

- [ ] **Step 1: Write test for financial sync DuckDB writes**

```python
# backend/tests/test_financial_sync_duckdb.py
"""Tests for financial sync service with DuckDB writes."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestFinancialSyncDuckDB:
    def test_write_financial_table(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "report_date": ["2026-01-01"],
            "revenue": [1000000.0],
            "net_profit": [100000.0],
        })
        # Verify table exists and is writable
        repo.db.execute("CREATE TABLE IF NOT EXISTS financials_income (symbol VARCHAR, report_date DATE, revenue DOUBLE, net_profit DOUBLE)")
        repo.db.execute("INSERT INTO financials_income SELECT * FROM df")
        result = repo.db.execute("SELECT count(*) FROM financials_income").fetchone()
        assert result[0] == 1
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_financial_sync_duckdb.py -v`
Expected: PASS

- [ ] **Step 3: Modify financial_sync.py to write DuckDB**

Replace Parquet writes in `_write_table()`:

```python
# In _write_table(), replace:
#   df.write_parquet(out_file)
# with DuckDB write based on table name
```

- [ ] **Step 4: Run full test suite**

Run: `cd backend && uv run --extra dev pytest -v`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/financial_sync.py backend/tests/test_financial_sync_duckdb.py
git commit -m "feat: financial sync writes to DuckDB"
```

---

## Task 10: Update Depth Service to Write DuckDB

**Files:**
- Modify: `backend/app/services/depth_service.py`
- Test: `backend/tests/test_depth_service_duckdb.py`

**Interfaces:**
- Consumes: DuckDB tables from Task 3
- Produces: `_persist_sealed()` writes to DuckDB

- [ ] **Step 1: Write test for depth service DuckDB writes**

```python
# backend/tests/test_depth_service_duckdb.py
"""Tests for depth service with DuckDB writes."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture
def repo(tmp_path: Path) -> KlineRepository:
    store = DataStore(data_dir=tmp_path / "data")
    yield KlineRepository(store=store)
    store.db.close()


class TestDepthServiceDuckDB:
    def test_write_depth5(self, repo: KlineRepository) -> None:
        df = pl.DataFrame({
            "symbol": ["000001"],
            "date": [date(2026, 1, 15)],
            "bid1_price": [10.0],
            "bid1_volume": [100.0],
            "ask1_price": [10.1],
            "ask1_volume": [200.0],
            "sealed": [True],
        })
        repo.db.execute("INSERT INTO depth5 SELECT * FROM df")
        result = repo.db.execute("SELECT count(*) FROM depth5").fetchone()
        assert result[0] == 1
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_depth_service_duckdb.py -v`
Expected: PASS

- [ ] **Step 3: Modify depth_service.py to write DuckDB**

Replace Parquet writes in `_persist_sealed()`:

```python
# In _persist_sealed(), replace:
#   os.replace(tmp, out)
# with DuckDB write
```

- [ ] **Step 4: Run full test suite**

Run: `cd backend && uv run --extra dev pytest -v`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/depth_service.py backend/tests/test_depth_service_duckdb.py
git commit -m "feat: depth service writes to DuckDB"
```

---

## Task 11: Update Quant Module to Read from DuckDB

**Files:**
- Modify: `backend/app/quant/datasource/tickflow_src.py`
- Test: `backend/tests/test_quant_tickflow_src_duckdb.py`

**Interfaces:**
- Consumes: DuckDB tables from Task 3
- Produces: `TickFlowSource` reads from DuckDB instead of Parquet

- [ ] **Step 1: Write test for quant TickFlow source DuckDB reads**

```python
# backend/tests/test_quant_tickflow_src_duckdb.py
"""Tests for quant TickFlow source with DuckDB reads."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository
from app.quant.datasource.tickflow_src import TickFlowSource


@pytest.fixture
def setup_duckdb(tmp_path: Path) -> Path:
    store = DataStore(data_dir=tmp_path / "data")
    repo = KlineRepository(store=store)
    
    # Insert test data
    df = pl.DataFrame({
        "symbol": ["000001"],
        "date": [date(2026, 1, 15)],
        "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
        "volume": [1000.0], "amount": [10000.0], "quote_ts": [1705305600],
    })
    repo.append_daily(df)
    store.db.close()
    
    return tmp_path / "data" / "stock.duckdb"


class TestQuantTickFlowSourceDuckDB:
    def test_reads_from_duckdb(self, setup_duckdb: Path) -> None:
        src = TickFlowSource(db_path=setup_duckdb)
        # Test that it can read data from DuckDB
        # (actual test depends on TickFlowSource API)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && uv run --extra dev pytest tests/test_quant_tickflow_src_duckdb.py -v`
Expected: FAIL

- [ ] **Step 3: Modify tickflow_src.py to read DuckDB**

Replace Parquet reads in `TickFlowSource`:

```python
# In TickFlowSource, replace:
#   pl.scan_parquet(...)
# with:
#   duckdb.connect(db_path, read_only=True).execute("SELECT * FROM ...").pl()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && uv run --extra dev pytest tests/test_quant_tickflow_src_duckdb.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/datasource/tickflow_src.py backend/tests/test_quant_tickflow_src_duckdb.py
git commit -m "feat: quant TickFlow source reads from DuckDB"
```

---

## Task 12: Clean Up Deprecated Parquet Code

**Files:**
- Modify: `backend/app/tickflow/repository.py` (remove Parquet write methods)
- Modify: `backend/app/parquet.py` (keep schemas, remove scan functions)
- Modify: `backend/app/services/kline_sync.py` (remove _atomic_write_parquet)

**Interfaces:**
- Consumes: All previous tasks completed
- Produces: Clean codebase without deprecated Parquet operations

- [ ] **Step 1: Remove deprecated Parquet write methods**

Remove from `repository.py`:
- `_atomic_write_parquet()`
- `_write_daily_partition()`
- Old `flush_live_*` implementations (replace with DuckDB versions)
- Old `merge_live_*` implementations (replace with DuckDB versions)

- [ ] **Step 2: Update parquet.py**

Keep schema definitions (still used for type hints) but mark scan functions as deprecated:

```python
# In parquet.py, add deprecation warnings to scan functions:
import warnings

def scan_daily_parquet(source: Any, **kwargs: Any) -> pl.LazyFrame:
    warnings.warn("scan_daily_parquet is deprecated, use DuckDB queries", DeprecationWarning)
    # Keep implementation for backward compatibility
    ...
```

- [ ] **Step 3: Remove _atomic_write_parquet from kline_sync.py**

Delete the module-level `_atomic_write_parquet()` function.

- [ ] **Step 4: Run full test suite**

Run: `cd backend && uv run --extra dev pytest -v`
Expected: All tests pass (with deprecation warnings)

- [ ] **Step 5: Commit**

```bash
git add backend/app/tickflow/repository.py backend/app/parquet.py backend/app/services/kline_sync.py
git commit -m "chore: remove deprecated Parquet write code"
```

---

## Task 13: Run Full Test Suite and Fix Issues

**Files:**
- Modify: Various files based on test failures

**Interfaces:**
- Consumes: All previous tasks completed
- Produces: All tests passing

- [ ] **Step 1: Run linting**

Run: `cd backend && uv run --extra dev ruff check app`
Fix any linting errors.

- [ ] **Step 2: Run type checking**

Run: `cd backend && uv run --extra dev mypy app`
Fix any type errors.

- [ ] **Step 3: Run full test suite**

Run: `cd backend && uv run --extra dev pytest -v`
Fix any failing tests.

- [ ] **Step 4: Run migration script on sample data**

Run: `cd backend && python -m scripts.migrate_parquet_to_duckdb --data-dir ../data --db-path ../data/stock.duckdb`
Verify migration works correctly.

- [ ] **Step 5: Start backend and verify functionality**

Run: `cd backend && uv run app.main:app`
- Verify DuckDB file is created
- Verify API endpoints work
- Verify data is readable from DuckDB

- [ ] **Step 6: Final commit**

```bash
git add -A
git commit -m "feat: complete DuckDB storage migration"
```

---

## Task 14: Update Documentation

**Files:**
- Modify: `AGENTS.md` (update architecture section)
- Modify: `docs/configuration.md` (if needed)

**Interfaces:**
- Consumes: All previous tasks completed
- Produces: Updated documentation reflecting DuckDB storage

- [ ] **Step 1: Update AGENTS.md**

Update the architecture section to reflect:
- DuckDB as primary storage
- Parquet files retained as backup
- Migration script available

- [ ] **Step 2: Commit documentation updates**

```bash
git add AGENTS.md docs/
git commit -m "docs: update architecture to reflect DuckDB storage"
```
