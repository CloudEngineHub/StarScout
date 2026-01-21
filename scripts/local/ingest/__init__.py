"""
GitHub Archive ingestion pipeline configuration.

Downloads hourly event data from gharchive.org and converts to
partitioned Parquet files with Zstd compression.
"""

from pathlib import Path

# Base URL for GitHub Archive hourly dumps
GHARCHIVE_BASE_URL = "https://data.gharchive.org"

# Default data directory (can be overridden via CLI or environment)
DEFAULT_DATA_PATH = Path("data/gharchive")

# Parquet compression settings
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
PARQUET_ROW_GROUP_SIZE = 100_000

# Download settings
DOWNLOAD_TIMEOUT = 60  # seconds
DOWNLOAD_RETRIES = 3
DOWNLOAD_RETRY_DELAY = 5  # seconds

# Parallel processing
DEFAULT_WORKERS = 4

# State file for tracking ingestion progress
STATE_FILE = "ingestion_state.json"
