"""
Write PyArrow tables to Parquet files with Hive partitioning.

Output structure:
    {base_path}/year=YYYY/month=MM/day=DD/HH.parquet

Uses Zstd compression for optimal size/speed balance.
"""

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
)


def get_partition_path(base_path: Path, dt: datetime) -> Path:
    """
    Get the Hive-partitioned directory path for a given hour.

    Returns:
        Path like: {base_path}/year=2024/month=01/day=15/
    """
    return (
        base_path
        / f"year={dt.year}"
        / f"month={dt.month:02d}"
        / f"day={dt.day:02d}"
    )


def get_parquet_path(base_path: Path, dt: datetime) -> Path:
    """
    Get the full Parquet file path for a given hour.

    Returns:
        Path like: {base_path}/year=2024/month=01/day=15/14.parquet
    """
    partition_dir = get_partition_path(base_path, dt)
    return partition_dir / f"{dt.hour:02d}.parquet"


def write_table(
    table: pa.Table,
    base_path: Path,
    dt: datetime,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
) -> Path:
    """
    Write a PyArrow table to a Parquet file.

    Args:
        table: The PyArrow table to write
        base_path: Base directory for partitioned data
        dt: The hour this data represents (used for path)
        compression: Compression codec (default: zstd)
        compression_level: Compression level (default: 3)
        row_group_size: Rows per row group (default: 100K)

    Returns:
        Path to the written Parquet file
    """
    output_path = get_parquet_path(base_path, dt)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove partition columns from the table data
    # (they're encoded in the path, not needed in the file)
    columns_to_drop = ["year", "month", "day", "hour"]
    columns_to_keep = [
        col for col in table.column_names if col not in columns_to_drop
    ]
    table_to_write = table.select(columns_to_keep)

    pq.write_table(
        table_to_write,
        output_path,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        # Write statistics for all columns (helps query optimization)
        write_statistics=True,
        # Use data page v2 for better compression
        data_page_version="2.0",
    )

    return output_path


def write_dataset(
    tables: dict[datetime, pa.Table],
    base_path: Path,
    **kwargs,
) -> list[Path]:
    """
    Write multiple tables as a partitioned dataset.

    Args:
        tables: Dictionary mapping datetime -> PyArrow table
        base_path: Base directory for output
        **kwargs: Additional arguments passed to write_table

    Returns:
        List of paths to written Parquet files
    """
    paths = []
    for dt, table in tables.items():
        path = write_table(table, base_path, dt, **kwargs)
        paths.append(path)
    return paths


def parquet_exists(base_path: Path, dt: datetime) -> bool:
    """Check if a Parquet file already exists for the given hour."""
    return get_parquet_path(base_path, dt).exists()


def read_parquet(base_path: Path, dt: datetime) -> pa.Table:
    """Read a Parquet file for the given hour."""
    path = get_parquet_path(base_path, dt)
    if not path.exists():
        raise FileNotFoundError(f"No Parquet file for {dt}: {path}")
    return pq.read_table(path)


def get_parquet_stats(base_path: Path, dt: datetime) -> dict:
    """Get metadata and statistics for a Parquet file."""
    path = get_parquet_path(base_path, dt)
    if not path.exists():
        return {"exists": False}

    metadata = pq.read_metadata(path)
    return {
        "exists": True,
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "num_rows": metadata.num_rows,
        "num_row_groups": metadata.num_row_groups,
        "num_columns": metadata.num_columns,
        "created_by": metadata.created_by,
    }
