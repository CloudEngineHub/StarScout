"""
Query execution utilities for local DuckDB processing.

Provides helpers for:
- Reading from partitioned Parquet files
- Executing SQL with parameter substitution
- Writing results to Parquet/CSV
- Managing intermediate checkpoint tables
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import get_connection, DEFAULT_DATA_PATH, DEFAULT_CHECKPOINT_PATH


def date_to_yymmdd(dt: datetime) -> str:
    """Convert datetime to YYMMDD format used in StarScout."""
    return dt.strftime("%y%m%d")


def yymmdd_to_date(s: str) -> datetime:
    """Convert YYMMDD string to datetime."""
    return datetime.strptime(s, "%y%m%d")


def get_parquet_glob(
    data_path: Path = DEFAULT_DATA_PATH,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """
    Get a glob pattern for Parquet files in a date range.

    For DuckDB, we use the read_parquet function with hive_partitioning=true,
    which automatically handles partition pruning based on WHERE clauses.

    Returns a glob pattern like: 'data/gharchive/**/*.parquet'
    """
    return str(data_path / "**" / "*.parquet")


def build_date_filter(
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """
    Build a SQL WHERE clause for date filtering.

    Args:
        start_date: Start date in YYMMDD format (inclusive)
        end_date: End date in YYMMDD format (exclusive)

    Returns:
        SQL fragment like "created_at >= '2024-01-01' AND created_at < '2025-01-01'"
    """
    conditions = []

    if start_date:
        dt = yymmdd_to_date(start_date)
        conditions.append(f"created_at >= '{dt.strftime('%Y-%m-%d')}'")

    if end_date:
        dt = yymmdd_to_date(end_date)
        conditions.append(f"created_at < '{dt.strftime('%Y-%m-%d')}'")

    if conditions:
        return " AND ".join(conditions)
    return "1=1"


def execute_query(
    sql: str,
    params: dict[str, Any] | None = None,
    data_path: Path = DEFAULT_DATA_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyRelation:
    """
    Execute a SQL query with parameter substitution.

    Parameters in the SQL should be formatted as {param_name}.

    Args:
        sql: SQL query with optional {param} placeholders
        params: Dictionary of parameter values
        data_path: Base path for Parquet files
        con: Optional DuckDB connection (uses global if not provided)

    Returns:
        DuckDB relation (lazy evaluation)
    """
    if con is None:
        con = get_connection()

    # Default parameters
    all_params = {
        "parquet_glob": get_parquet_glob(data_path),
        "data_path": str(data_path),
    }
    if params:
        all_params.update(params)

    # Substitute parameters
    formatted_sql = sql.format(**all_params)

    return con.sql(formatted_sql)


def query_to_df(
    sql: str,
    params: dict[str, Any] | None = None,
    data_path: Path = DEFAULT_DATA_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Execute a query and return results as a pandas DataFrame."""
    result = execute_query(sql, params, data_path, con)
    return result.df()


def query_to_arrow(
    sql: str,
    params: dict[str, Any] | None = None,
    data_path: Path = DEFAULT_DATA_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """Execute a query and return results as a PyArrow table."""
    result = execute_query(sql, params, data_path, con)
    return result.arrow()


def query_to_parquet(
    sql: str,
    output_path: Path,
    params: dict[str, Any] | None = None,
    data_path: Path = DEFAULT_DATA_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
    compression: str = "zstd",
) -> Path:
    """
    Execute a query and write results to a Parquet file.

    Uses DuckDB's native COPY TO for efficiency.
    """
    if con is None:
        con = get_connection()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build the full query with COPY TO
    all_params = {
        "parquet_glob": get_parquet_glob(data_path),
        "data_path": str(data_path),
    }
    if params:
        all_params.update(params)

    formatted_sql = sql.format(**all_params)

    copy_sql = f"""
        COPY (
            {formatted_sql}
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION {compression})
    """

    con.execute(copy_sql)
    return output_path


def query_to_csv(
    sql: str,
    output_path: Path,
    params: dict[str, Any] | None = None,
    data_path: Path = DEFAULT_DATA_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """Execute a query and write results to a CSV file."""
    if con is None:
        con = get_connection()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_params = {
        "parquet_glob": get_parquet_glob(data_path),
        "data_path": str(data_path),
    }
    if params:
        all_params.update(params)

    formatted_sql = sql.format(**all_params)

    copy_sql = f"""
        COPY (
            {formatted_sql}
        ) TO '{output_path}' (FORMAT CSV, HEADER)
    """

    con.execute(copy_sql)
    return output_path


def create_checkpoint_table(
    name: str,
    sql: str,
    params: dict[str, Any] | None = None,
    checkpoint_path: Path = DEFAULT_CHECKPOINT_PATH,
    data_path: Path = DEFAULT_DATA_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """
    Execute a query and save results as a checkpoint Parquet file.

    Checkpoints are used for intermediate results in multi-step pipelines.
    """
    output_path = checkpoint_path / f"{name}.parquet"
    return query_to_parquet(sql, output_path, params, data_path, con)


def load_checkpoint_table(
    name: str,
    checkpoint_path: Path = DEFAULT_CHECKPOINT_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyRelation:
    """Load a checkpoint table as a DuckDB relation."""
    if con is None:
        con = get_connection()

    parquet_path = checkpoint_path / f"{name}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Checkpoint not found: {parquet_path}")

    return con.read_parquet(str(parquet_path))


def checkpoint_exists(
    name: str,
    checkpoint_path: Path = DEFAULT_CHECKPOINT_PATH,
) -> bool:
    """Check if a checkpoint file exists."""
    return (checkpoint_path / f"{name}.parquet").exists()


def register_parquet_view(
    view_name: str,
    data_path: Path = DEFAULT_DATA_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
) -> None:
    """
    Register the Parquet files as a view for easier querying.

    After calling this, you can query using:
        SELECT * FROM {view_name} WHERE ...
    """
    if con is None:
        con = get_connection()

    glob_pattern = get_parquet_glob(data_path)
    con.execute(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    """)


def get_date_range_from_parquet(
    data_path: Path = DEFAULT_DATA_PATH,
    con: duckdb.DuckDBPyConnection | None = None,
) -> tuple[datetime, datetime]:
    """Get the min and max dates available in the Parquet files."""
    if con is None:
        con = get_connection()

    glob_pattern = get_parquet_glob(data_path)
    result = con.execute(f"""
        SELECT
            MIN(created_at) as min_date,
            MAX(created_at) as max_date
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    """).fetchone()

    return result[0], result[1]


def count_events(
    data_path: Path = DEFAULT_DATA_PATH,
    start_date: str | None = None,
    end_date: str | None = None,
    event_type: str | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Count events matching the criteria."""
    if con is None:
        con = get_connection()

    glob_pattern = get_parquet_glob(data_path)
    date_filter = build_date_filter(start_date, end_date)

    type_filter = f"AND type = '{event_type}'" if event_type else ""

    result = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
        WHERE {date_filter} {type_filter}
    """).fetchone()

    return result[0]


def count_stars(
    data_path: Path = DEFAULT_DATA_PATH,
    start_date: str | None = None,
    end_date: str | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Count star events in the date range."""
    if con is None:
        con = get_connection()

    glob_pattern = get_parquet_glob(data_path)
    date_filter = build_date_filter(start_date, end_date)

    result = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
        WHERE is_star = true AND {date_filter}
    """).fetchone()

    return result[0]
