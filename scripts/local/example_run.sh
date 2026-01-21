#!/bin/bash
#
# StarScout Local Execution Example
#
# This script demonstrates how to run StarScout entirely on local storage
# without requiring NAS, BigQuery, or GCS.
#
# The methodology mirrors the original ICSE '26 paper:
#   1. Low-activity heuristic: Identifies accounts with minimal GitHub activity
#   2. CopyCatch/Lockstep heuristic: Detects coordinated starring in time windows
#
# Original research used July 2019 - January 2025 (5.5 years, ~200 GB).
# This script provides smaller date ranges for testing and validation.
#
# Prerequisites:
#   - Python 3.12 (dependencies auto-installed from scripts/local/requirements.txt)
#   - Disk space varies by mode (see below)
#   - Temp space for DuckDB: 10-500 GB depending on mode (see Temp Space below)
#
# Temp Space:
#   DuckDB spills intermediate results to disk during large aggregations.
#   If your primary disk lacks space, symlink data/_duckdb_temp to external storage:
#
#     ln -sf /Volumes/MyExternalDrive/duckdb_temp data/_duckdb_temp
#
#   An external M.2/SSD is faster than NAS for this purpose due to random I/O.
#
# Usage:
#   ./scripts/local/example_run.sh [quick|month|year|research|full]
#
#   quick    - 1 week of data (~1 GB, ~10 GB temp), good for testing
#   month    - 1 month of data (~5 GB, ~40 GB temp), good for validation
#   year     - 1 year of data (~47 GB, ~250 GB temp), Jan 2025 - Jan 2026
#   new      - New data since last run (auto-detects from data/YYMMDD/)
#   research - 5.5 years (~200 GB, ~500 GB temp), replicates original paper
#   full     - All data from Jul 2019 to today (~250+ GB, ~500+ GB temp)
#

set -e

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Data paths (override with environment variables if needed)
DATA_PATH="${STARSCOUT_DATA_PATH:-data/gharchive}"
CHECKPOINT_PATH="${STARSCOUT_CHECKPOINT_PATH:-data/_checkpoints}"
TEMP_PATH="${STARSCOUT_TEMP_PATH:-data/_duckdb_temp}"

# DuckDB settings - adjust based on your system
# Memory: Set higher than physical RAM - macOS will swap if needed
# This prevents DuckDB OOM errors on complex queries
export DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-32GB}"
export DUCKDB_TEMP_DIRECTORY="$TEMP_PATH"
export DUCKDB_THREADS="${DUCKDB_THREADS:-2}"

# Run mode
MODE="${1:-quick}"

# Date ranges for each mode
case "$MODE" in
    quick)
        # 1 week - good for testing (~1 GB)
        BACKFILL_START="2025-01-08"
        BACKFILL_END="2025-01-15"
        DETECT_START="250108"
        DETECT_END="250115"
        DISK_REQUIRED=5
        ;;
    month)
        # 1 month - good for validation (~5 GB)
        BACKFILL_START="2025-01-01"
        BACKFILL_END="2025-02-01"
        DETECT_START="250101"
        DETECT_END="250201"
        DISK_REQUIRED=15
        ;;
    year)
        # 1 year - production run (~47 GB)
        BACKFILL_START="2025-01-01"
        BACKFILL_END="2026-01-01"
        DETECT_START="250101"
        DETECT_END="260101"
        DISK_REQUIRED=80
        ;;
    new)
        # New data since last run (auto-detects from existing output directories)
        # Looks for data/YYMMDD/ directories with actual CSV results
        LAST_RUN=""
        for dir in $(ls -d data/[0-9][0-9][0-9][0-9][0-9][0-9]/ 2>/dev/null | sort -r); do
            if ls "$dir"/*.csv >/dev/null 2>&1; then
                LAST_RUN=$(echo "$dir" | grep -oE '[0-9]{6}')
                break
            fi
        done
        if [ -z "$LAST_RUN" ]; then
            # No previous runs found, fall back to END_DATE from config
            LAST_RUN=$(python3 -c "from scripts import END_DATE; print(END_DATE)" 2>/dev/null || echo "250101")
            echo "No previous runs found, starting from config END_DATE: $LAST_RUN"
        else
            echo "Found last successful run: $LAST_RUN"
        fi

        # Find the last available data date (not today, which may not have data yet)
        LAST_DATA=$(ls -d data/gharchive/year=*/month=*/day=*/ 2>/dev/null | sort -r | head -1 | grep -oE 'year=([0-9]+)/month=([0-9]+)/day=([0-9]+)' | sed 's/year=\([0-9]\{4\}\)\/month=\([0-9]\{2\}\)\/day=\([0-9]\{2\}\)/\1-\2-\3/' || echo "")
        if [ -n "$LAST_DATA" ]; then
            # Convert YYYY-MM-DD to YYMMDD
            DETECT_END="${LAST_DATA:2:2}${LAST_DATA:5:2}${LAST_DATA:8:2}"
            BACKFILL_END="$LAST_DATA"
            echo "Latest data available: $LAST_DATA"
        else
            BACKFILL_END=$(date +%Y-%m-%d)
            DETECT_END=$(date +%y%m%d)
        fi

        # Convert YYMMDD to YYYY-MM-DD for backfill
        BACKFILL_START="20${LAST_RUN:0:2}-${LAST_RUN:2:2}-${LAST_RUN:4:2}"
        DETECT_START="$LAST_RUN"
        DISK_REQUIRED=60

        # Check if there's actually new data to process
        if [ "$DETECT_START" = "$DETECT_END" ]; then
            echo ""
            echo "ERROR: No new data to process."
            echo "  Last successful run: $DETECT_START"
            echo "  Latest available data: $DETECT_END"
            echo ""
            echo "Either:"
            echo "  1. Run ingestion to get newer data: $PYTHON -m scripts.local.ingest.pipeline --incremental"
            echo "  2. Use a different mode: ./scripts/local/example_run.sh year"
            exit 0
        fi
        ;;
    research)
        # Original paper date range: July 2019 - January 2025 (~200 GB)
        # This replicates the methodology from the ICSE '26 paper
        BACKFILL_START="2019-07-01"
        BACKFILL_END="2025-01-01"
        DETECT_START="190701"
        DETECT_END="250101"
        DISK_REQUIRED=250
        ;;
    full)
        # Full dataset: July 2019 to current day
        # Extends the original research to include all available data
        BACKFILL_START="2019-07-01"
        BACKFILL_END=$(date +%Y-%m-%d)
        DETECT_START="190701"
        DETECT_END=$(date +%y%m%d)
        # Estimate: ~37 GB/year, ~200 GB for 2019-2025, plus ~4 GB/month after
        DISK_REQUIRED=300
        ;;
    *)
        echo "Usage: $0 [quick|month|year|new|research|full]"
        echo ""
        echo "Modes:                        Data      Temp Space"
        echo "  quick    - 1 week test       ~1 GB     ~10 GB"
        echo "  month    - 1 month validate  ~5 GB     ~40 GB"
        echo "  year     - Jan 2025-2026     ~47 GB    ~250 GB"
        echo "  new      - Since last run    varies    varies"
        echo "  research - Jul 2019-Jan 2025 ~200 GB   ~500 GB"
        echo "  full     - Jul 2019-today    ~250+ GB  ~500+ GB"
        echo ""
        echo "The 'research' mode replicates the original ICSE '26 paper methodology."
        echo "The 'new' mode analyzes data since your last run (incremental)."
        echo "The 'full' mode includes all data from Jul 2019 to today."
        exit 1
        ;;
esac

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

check_disk_space() {
    local required_gb=$1
    local available_gb=$(df -g . | awk 'NR==2 {print $4}')

    if [ "$available_gb" -lt "$required_gb" ]; then
        echo "ERROR: Insufficient disk space."
        echo "  Required: ${required_gb} GB"
        echo "  Available: ${available_gb} GB"
        exit 1
    fi
    log "Disk space OK: ${available_gb} GB available (need ${required_gb} GB)"
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

echo "=============================================="
echo "StarScout Local Execution"
echo "=============================================="
echo ""
TEMP_REAL_PATH=$(readlink "$TEMP_PATH" 2>/dev/null || echo "$TEMP_PATH")
TEMP_AVAIL=$(df -h "$TEMP_REAL_PATH" 2>/dev/null | awk 'NR==2 {print $4}' || echo "unknown")

echo "Mode:           $MODE"
echo "Data path:      $DATA_PATH"
echo "Checkpoint:     $CHECKPOINT_PATH"
echo "Temp path:      $TEMP_PATH -> $TEMP_REAL_PATH ($TEMP_AVAIL available)"
echo "Memory limit:   $DUCKDB_MEMORY_LIMIT"
echo "Threads:        $DUCKDB_THREADS"
echo "Date range:     $BACKFILL_START to $BACKFILL_END"
echo ""

echo "=============================================="
echo "Methodology Notes"
echo "=============================================="
echo ""
echo "This local execution mirrors the original ICSE '26 paper methodology:"
echo ""
echo "  1. LOW-ACTIVITY HEURISTIC: Identifies GitHub accounts with minimal"
echo "     activity (single-day activity, ≤2 actions, ≤1 repo, ≤1 org)."
echo "     These accounts are likely created solely for starring."
echo ""
echo "  2. COPYCATCH/LOCKSTEP HEURISTIC: Detects coordinated starring by"
echo "     finding clusters of users who star the same repos in similar"
echo "     time windows. Uses overlapping 6-month chunks."
echo ""
echo "Key differences from original paper:"
echo "  - Original used BigQuery on GitHub Archive (expensive, rate-limited)"
echo "  - This version uses local Parquet + DuckDB (free, unlimited)"
echo "  - GitHub API enrichment runs if secrets.yaml has tokens (silent skip otherwise)"
echo "  - MongoDB storage is optional (skipped by default)"
echo ""
echo "Storage tip: DuckDB temp files can exceed 200-500 GB for large runs."
echo "  Symlink data/_duckdb_temp to external storage if needed:"
echo "    ln -sf /Volumes/MyExternalDrive/duckdb_temp data/_duckdb_temp"
echo ""

# Check disk space
check_disk_space $DISK_REQUIRED

# Create directories
log "Creating directories..."
mkdir -p "$DATA_PATH" "$CHECKPOINT_PATH" "$TEMP_PATH"

# Clean up any leftover temp files from previous failed runs
log "Cleaning up any leftover temp files..."
rm -rf "$TEMP_PATH"/* 2>/dev/null || true

# -----------------------------------------------------------------------------
# Step 0: Setup virtual environment and install dependencies
# -----------------------------------------------------------------------------

log "Step 0: Setting up Python environment..."

# Create venv if it doesn't exist
if [ ! -d ".venv" ]; then
    log "  Creating virtual environment (.venv)..."
    python3 -m venv .venv
fi

# Use venv's Python directly (more reliable than source activate in scripts)
PYTHON=".venv/bin/python3"
PIP=".venv/bin/pip"

# Install dependencies if needed (stscraper = strudel.scraper for GitHub API)
if ! "$PYTHON" -c "import yaml, pandas, duckdb, pyarrow, tqdm, requests, stscraper" 2>/dev/null; then
    log "  Installing dependencies from scripts/local/requirements.txt..."
    "$PIP" install -q -r scripts/local/requirements.txt
fi
log "  Dependencies OK (using $($PYTHON --version))"

# -----------------------------------------------------------------------------
# Step 1: Ingest data from GitHub Archive
# -----------------------------------------------------------------------------

log "Step 1: Ingesting GitHub Archive data..."
log "  This downloads from gharchive.org and converts to Parquet"
log "  Range: $BACKFILL_START to $BACKFILL_END"
echo ""

"$PYTHON" -m scripts.local.ingest.pipeline \
    --data-path "$DATA_PATH" \
    --backfill "$BACKFILL_START" "$BACKFILL_END"

# Retry any failed hours (network glitches, temporary gharchive.org issues)
log "Retrying any failed hours..."
"$PYTHON" -m scripts.local.ingest.pipeline --data-path "$DATA_PATH" --retry-failed

echo ""
log "Ingestion complete. Checking stats..."
"$PYTHON" -m scripts.local.ingest.pipeline --data-path "$DATA_PATH" --stats

# -----------------------------------------------------------------------------
# Step 2: Run low-activity detector
# -----------------------------------------------------------------------------

echo ""
log "Step 2: Running low-activity fake star detector..."
log "  This identifies accounts with minimal GitHub activity"
echo ""

# Build detector arguments (always try GitHub enrichment - silently skips if no tokens)
DETECTOR_ARGS="--data-path $DATA_PATH --checkpoint-path $CHECKPOINT_PATH --start-date $DETECT_START --end-date $DETECT_END --skip-mongodb --enrich-github"

"$PYTHON" -m scripts.local.simple_detector $DETECTOR_ARGS

# -----------------------------------------------------------------------------
# Step 3: Run CopyCatch / Lockstep detector (optional for quick mode)
# -----------------------------------------------------------------------------

if [ "$MODE" != "quick" ]; then
    echo ""
    log "Step 3: Running CopyCatch / Lockstep detector..."
    log "  This identifies coordinated starring behavior using overlapping"
    log "  6-month time windows (chunks) as defined in scripts/__init__.py"
    log "  WARNING: This can take hours/days for large date ranges"
    echo ""

    # The CopyCatch algorithm processes data in overlapping 6-month chunks
    # to detect coordinated behavior. The chunks are defined in:
    #   scripts/__init__.py -> COPYCATCH_DATE_CHUNKS
    #
    # For the 'research' mode, this matches the original paper's methodology.
    # For shorter modes, only relevant chunks within the date range are used.

    "$PYTHON" -m scripts.local.copycatch \
        --data-path "$DATA_PATH" \
        --checkpoint-path "$CHECKPOINT_PATH" \
        --run

    "$PYTHON" -m scripts.local.copycatch \
        --data-path "$DATA_PATH" \
        --checkpoint-path "$CHECKPOINT_PATH" \
        --export
else
    echo ""
    log "Step 3: Skipping CopyCatch (use 'month', 'year', or 'research' mode)"
fi

# -----------------------------------------------------------------------------
# Results
# -----------------------------------------------------------------------------

echo ""
echo "=============================================="
echo "Run Complete"
echo "=============================================="
echo ""
log "Mode: $MODE"
log "Results saved to: data/$DETECT_END/"
echo ""

if [ -f "data/$DETECT_END/fake_stars_low_activity_repos.csv" ]; then
    REPO_COUNT=$(wc -l < "data/$DETECT_END/fake_stars_low_activity_repos.csv")
    log "Found $((REPO_COUNT - 1)) repos with suspicious low-activity stars"
    echo ""
    echo "Top 10 repos by percentage of low-activity stars:"
    head -11 "data/$DETECT_END/fake_stars_low_activity_repos.csv" | column -t -s,
fi

echo ""
log "Cleaning up DuckDB temp files..."
rm -rf "$TEMP_PATH"/* 2>/dev/null || true

echo ""
log "Storage usage:"
du -sh "$DATA_PATH" "$CHECKPOINT_PATH" "$TEMP_PATH" 2>/dev/null || true

