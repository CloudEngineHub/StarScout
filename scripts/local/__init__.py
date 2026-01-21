"""
Local execution layer using DuckDB.

Provides a drop-in replacement for scripts/gcp.py functionality,
reading from local Parquet files instead of BigQuery.
"""

import os
from pathlib import Path

import duckdb

# Default paths
DEFAULT_DATA_PATH = Path("data/gharchive")
DEFAULT_CHECKPOINT_PATH = Path("data/_checkpoints")

# DuckDB configuration
# Memory limit - set higher than physical RAM, let OS handle swap if needed
# This prevents DuckDB OOM errors on complex queries like CopyCatch reduce_centers
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "32GB")

# Temp directory for spill-to-disk
# Default to local temp directory; override with DUCKDB_TEMP_DIRECTORY env var
DUCKDB_TEMP_DIRECTORY = os.environ.get("DUCKDB_TEMP_DIRECTORY", "data/_duckdb_temp")

# Number of threads (reduce to limit memory pressure during complex JOINs)
DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS", "2"))

# Connection pool for reuse
_connection: duckdb.DuckDBPyConnection | None = None


def get_connection(
    memory_limit: str = DUCKDB_MEMORY_LIMIT,
    temp_directory: str = DUCKDB_TEMP_DIRECTORY,
    threads: int = DUCKDB_THREADS,
) -> duckdb.DuckDBPyConnection:
    """
    Get a configured DuckDB connection.

    Uses a singleton pattern for connection reuse within a process.
    """
    global _connection

    if _connection is None:
        _connection = duckdb.connect()

        # Configure memory
        _connection.execute(f"SET memory_limit = '{memory_limit}'")

        # Configure temp directory for large queries
        if temp_directory:
            _connection.execute(f"SET temp_directory = '{temp_directory}'")

        # Configure threads
        if threads > 0:
            _connection.execute(f"SET threads = {threads}")

        # Enable progress bar for long queries
        _connection.execute("SET enable_progress_bar = true")

        # Disable insertion order preservation - saves significant memory on large queries
        _connection.execute("SET preserve_insertion_order = false")

        # Allow unlimited temp directory size for large aggregations
        _connection.execute("SET max_temp_directory_size = '2TB'")

        # Optimize for lower temp disk usage
        _connection.execute("SET force_compression = 'zstd'")  # Compress intermediate results
        _connection.execute("SET checkpoint_threshold = '256MB'")  # More frequent checkpoints
        _connection.execute("SET wal_autocheckpoint = '256MB'")  # Smaller WAL before checkpoint

    return _connection


def close_connection():
    """Close the global connection."""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None


def new_connection(**kwargs) -> duckdb.DuckDBPyConnection:
    """
    Create a new independent connection.

    Use this when you need isolation from the global connection,
    e.g., for parallel query execution.
    """
    con = duckdb.connect()

    memory_limit = kwargs.get("memory_limit", DUCKDB_MEMORY_LIMIT)
    temp_directory = kwargs.get("temp_directory", DUCKDB_TEMP_DIRECTORY)
    threads = kwargs.get("threads", DUCKDB_THREADS)

    con.execute(f"SET memory_limit = '{memory_limit}'")
    if temp_directory:
        con.execute(f"SET temp_directory = '{temp_directory}'")
    if threads > 0:
        con.execute(f"SET threads = {threads}")

    # Apply same optimizations as get_connection
    con.execute("SET enable_progress_bar = true")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET max_temp_directory_size = '2TB'")

    # Optimize for lower temp disk usage
    con.execute("SET force_compression = 'zstd'")
    con.execute("SET checkpoint_threshold = '256MB'")
    con.execute("SET wal_autocheckpoint = '256MB'")

    return con
