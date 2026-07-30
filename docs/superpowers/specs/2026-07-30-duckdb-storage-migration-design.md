# DuckDB Storage Migration Design

**Date**: 2026-07-30
**Author**: opencode (AI)
**Status**: Approved

## 1. Overview

Migrate the stock market data storage from partitioned Parquet files to DuckDB native storage. The quant module's metadata (backtest runs, sim accounts, trades) remains in SQLite.

### Goals
- Replace Parquet with DuckDB as the primary storage format for all stock market data
- Maintain the existing subprocess architecture (backtesting/paper trading in read-only child processes)
- Single writer process (FastAPI main) with concurrent read-only child processes

### Non-Goals
- Migrate quant module data (backtest runs, sim accounts) from SQLite to DuckDB
- Change the subprocess model for backtesting or paper trading
- Introduce new features beyond storage migration

## 2. Data Model

### File Location
`data/stock.duckdb`

### Table Schema

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `kline_daily` | (symbol, date) | Raw daily OHLCV |
| `kline_daily_enriched` | (symbol, date) | 14-column enriched data |
| `kline_minute` | (symbol, date, minute) | 1-minute bars |
| `instruments` | symbol | Stock universe metadata |
| `adj_factor` | (symbol, date) | Adjustment factors |
| `depth5` | (symbol, date) | 5-level order book |
| `financials_metrics` | (symbol, report_date) | Financial metrics |
| `financials_income` | (symbol, report_date) | Income statements |
| `financials_balance_sheet` | (symbol, report_date) | Balance sheets |
| `financials_cash_flow` | (symbol, report_date) | Cash flow statements |
| `financials_shares` | (symbol, report_date) | Share information |
| `pools` | (pool_name, symbol) | Symbol universe pools |

### DuckDB Configuration
- WAL mode enabled for concurrent read/write
- Primary key constraints on (symbol, date) columns
- No explicit partitioning (DuckDB handles this internally)

## 3. Write Path

### Architecture
```
FastAPI Main Process (唯一写进程)
├── QuoteService (实时行情线程)
│   ├── 轮询TickFlow API
│   ├── 写入 kline_daily (当日数据)
│   └── 写入 kline_daily_enriched (增量更新)
├── DailyPipeline (定时checkpoint)
│   ├── 批量写入历史日线
│   ├── 计算enriched指标
│   └── 写入 kline_daily_enriched
└── _write_lock (threading.Lock)
    └── 序列化所有DuckDB写入操作
```

### Write Operations
- **Real-time**: `INSERT OR REPLACE` after each quote polling cycle
- **Batch sync**: `executemany` for historical data ingestion
- **Enriched computation**: Direct write after indicator calculation

### Concurrency Control
- `_write_lock` (threading.Lock) serializes all writes
- DuckDB WAL mode allows concurrent reads during writes
- Atomic transactions via `BEGIN/COMMIT`

## 4. Read Path

### Architecture
```
Read-Only Child Processes (回测/模拟盘)
├── duckdb.connect('stock.duckdb', read_only=True)
├── SELECT queries only (no writes)
└── Multiple concurrent readers via WAL

FastAPI Main Process
├── Direct DuckDB queries (read-write connection)
├── In-memory caches (保留现有缓存机制)
│   ├── _enriched_cache
│   ├── _enriched_history_cache
│   └── _instruments_cache
└── Polars computation pipeline (unchanged)
```

### Read Patterns
- **Metadata queries**: Direct SQL (min/max dates, counts)
- **Full data load**: `conn.execute("SELECT * FROM ...").pl()` → Polars DataFrame
- **Conditional filtering**: SQL WHERE clauses with DuckDB query optimization
- **Real-time cache**: Existing in-memory caches, data source changed from Parquet to DuckDB

### Polars Integration
- DuckDB result → Polars DataFrame via `.pl()`
- Existing `compute_indicators()` pipeline unchanged
- Data flow: DuckDB → Polars → Compute → DuckDB

## 5. Data Migration

### Strategy
One-time full migration from existing Parquet files to DuckDB.

### Migration Script
`scripts/migrate_parquet_to_duckdb.py`

### Migration Order
```
instruments → adj_factor → kline_daily → kline_daily_enriched → kline_minute → depth5 → financials_* → pools
```

### Post-Migration
- Parquet files retained as backup (not deleted)
- Subsequent data writes go to DuckDB only
- No incremental sync mechanism needed (Parquet no longer updated)

## 6. Code Changes

### Files to Modify
- `backend/app/tickflow/repository.py` — Replace Parquet read/write with DuckDB
- `backend/app/parquet.py` — Remove or adapt Parquet schema definitions
- `backend/app/indicators/pipeline.py` — Change write targets to DuckDB
- `backend/app/services/quote_service.py` — Change write targets to DuckDB
- `backend/app/jobs/daily_pipeline.py` — Change write targets to DuckDB
- `backend/app/quant/datasource/tickflow_src.py` — Read from DuckDB instead of Parquet

### Files to Add
- `scripts/migrate_parquet_to_duckdb.py` — Migration script

### Files to Remove/Deprecate
- Parquet write functions: `_atomic_write_parquet()`, `_merge_upsert_parquet()`
- Parquet scan helpers (replaced by DuckDB queries)

## 7. Testing

### Unit Tests
- Adapt existing tests that involve data read/write
- Add DuckDB-specific tests for write operations

### Integration Verification
1. Start backend, verify DuckDB tables created correctly
2. Execute full daily sync → enriched computation → write cycle
3. Verify backtesting reads from DuckDB correctly
4. Verify paper trading subprocess connects read-only

### Performance Validation
- Compare Parquet vs DuckDB query latency
- Verify concurrent read/write stability

### Commands
```bash
cd backend
uv run --extra dev pytest
uv run --extra dev ruff check app
uv run --extra dev mypy app
```

## 8. Risk Assessment

### Risks
- **Data loss during migration**: Mitigated by retaining Parquet files as backup
- **DuckDB file size growth**: DuckDB handles compression internally, should be comparable to Parquet
- **Concurrent write conflicts**: Mitigated by _write_lock serialization
- **Subprocess read-only enforcement**: DuckDB read_only mode enforces this at connection level

### Rollback Plan
If issues arise, revert to Parquet-based storage by:
1. Restoring Parquet read/write code from git history
2. Data remains available in Parquet files (never deleted)
