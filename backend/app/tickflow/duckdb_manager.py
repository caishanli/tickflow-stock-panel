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

        self._conn: duckdb.DuckDBPyConnection | None = duckdb.connect(
            database=str(db_path),
            read_only=read_only,
        )

        if not read_only:
            self._conn.execute("PRAGMA enable_progress_bar=false")

        logger.info("DuckDB connected: %s (read_only=%s)", db_path, read_only)

    def get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Return a DuckDB connection.

        Parameters
        ----------
        read_only:
            If True, return a read-only connection. Ignored; the manager
            always returns its own connection whose access level is set at
            construction time.
        """
        with self._lock:
            if self._conn is None:
                raise RuntimeError("DuckDBManager is closed")
            return self._conn

    def execute(self, sql: str, params: list[Any] | None = None) -> duckdb.DuckDBPyConnection:
        """Execute a SQL statement and return the relation."""
        with self._lock:
            if self._conn is None:
                raise RuntimeError("DuckDBManager is closed")
            if params:
                return self._conn.execute(sql, params)
            return self._conn.execute(sql)

    def execute_many(self, sql: str, params_list: list[tuple[Any, ...]]) -> None:
        """Execute a SQL statement with multiple parameter sets."""
        with self._lock:
            if self._conn is None:
                raise RuntimeError("DuckDBManager is closed")
            self._conn.executemany(sql, params_list)

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
                logger.info("DuckDB closed: %s", self.db_path)

    def __enter__(self) -> DuckDBManager:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
