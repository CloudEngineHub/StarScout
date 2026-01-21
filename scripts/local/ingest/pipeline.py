"""
Main ingestion pipeline for GitHub Archive data.

Usage:
    # Backfill historical data
    python -m scripts.local.ingest.pipeline --backfill 2024-01-01 2025-01-17

    # Incremental update (since last ingested hour)
    python -m scripts.local.ingest.pipeline --incremental

    # Validate existing Parquet files
    python -m scripts.local.ingest.pipeline --validate

    # Retry failed hours
    python -m scripts.local.ingest.pipeline --retry-failed
"""

import argparse
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tqdm import tqdm

from . import DEFAULT_DATA_PATH, DEFAULT_WORKERS
from .download import iter_hours, stream_hour_events, check_hour_exists
from .state import (
    get_last_ingested,
    set_last_ingested,
    add_failed_hour,
    clear_failed_hour,
    get_failed_hours,
    update_stats,
    get_stats,
    find_gaps,
)
from .transform import events_to_arrow
from .write import write_table, parquet_exists, get_parquet_stats


def ingest_hour(
    dt: datetime,
    base_path: Path,
    minimal: bool = False,
    skip_existing: bool = True,
) -> dict:
    """
    Ingest a single hour of GitHub Archive data.

    Args:
        dt: The hour to ingest
        base_path: Output directory for Parquet files
        minimal: Use minimal schema (smaller files)
        skip_existing: Skip if Parquet file already exists

    Returns:
        Dictionary with ingestion results:
        - success: bool
        - hour: ISO datetime string
        - events: number of events processed
        - bytes: size of output file
        - error: error message if failed
    """
    result = {
        "success": False,
        "hour": dt.isoformat(),
        "events": 0,
        "bytes": 0,
        "error": None,
    }

    try:
        # Check if already ingested
        if skip_existing and parquet_exists(base_path, dt):
            stats = get_parquet_stats(base_path, dt)
            result["success"] = True
            result["events"] = stats.get("num_rows", 0)
            result["bytes"] = stats.get("size_bytes", 0)
            result["skipped"] = True
            return result

        # Check if source exists
        if not check_hour_exists(dt):
            result["error"] = f"No GitHub Archive data for {dt}"
            return result

        # Stream, transform, and write
        events = stream_hour_events(dt)
        table = events_to_arrow(events, dt, minimal=minimal)

        if table.num_rows == 0:
            result["error"] = f"No valid events for {dt}"
            return result

        output_path = write_table(table, base_path, dt)

        result["success"] = True
        result["events"] = table.num_rows
        result["bytes"] = output_path.stat().st_size

    except Exception as e:
        result["error"] = str(e)

    return result


def backfill(
    start: datetime,
    end: datetime,
    base_path: Path,
    workers: int = DEFAULT_WORKERS,
    minimal: bool = False,
    skip_existing: bool = True,
) -> dict:
    """
    Backfill historical data for a date range.

    Args:
        start: Start datetime (inclusive)
        end: End datetime (exclusive)
        base_path: Output directory
        workers: Number of parallel workers
        minimal: Use minimal schema
        skip_existing: Skip hours that already have Parquet files

    Returns:
        Summary statistics
    """
    hours = list(iter_hours(start, end))
    total = len(hours)

    if total == 0:
        print("No hours to process in the given range.")
        return {"total": 0, "success": 0, "failed": 0}

    print(f"Backfilling {total} hours from {start} to {end}")
    print(f"Output: {base_path}")
    print(f"Workers: {workers}")

    success_count = 0
    failed_count = 0
    skipped_count = 0
    total_events = 0
    total_bytes = 0

    # Use single worker for debugging, parallel for production
    if workers == 1:
        for hour in tqdm(hours, desc="Ingesting"):
            result = ingest_hour(hour, base_path, minimal, skip_existing)
            if result["success"]:
                success_count += 1
                total_events += result["events"]
                total_bytes += result["bytes"]
                if result.get("skipped"):
                    skipped_count += 1
                else:
                    set_last_ingested(base_path, hour)
            else:
                failed_count += 1
                if result["error"]:
                    add_failed_hour(base_path, hour, result["error"])
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    ingest_hour, hour, base_path, minimal, skip_existing
                ): hour
                for hour in hours
            }

            with tqdm(total=total, desc="Ingesting") as pbar:
                for future in as_completed(futures):
                    hour = futures[future]
                    try:
                        result = future.result()
                        if result["success"]:
                            success_count += 1
                            total_events += result["events"]
                            total_bytes += result["bytes"]
                            if result.get("skipped"):
                                skipped_count += 1
                        else:
                            failed_count += 1
                            if result["error"]:
                                add_failed_hour(base_path, hour, result["error"])
                    except Exception as e:
                        failed_count += 1
                        add_failed_hour(base_path, hour, str(e))
                    pbar.update(1)

    # Update state with latest successful hour
    if success_count > 0:
        latest = max(
            h for h in hours
            if parquet_exists(base_path, h)
        )
        set_last_ingested(base_path, latest)

    # Update cumulative stats
    update_stats(
        base_path,
        hours=success_count - skipped_count,
        events=total_events,
        bytes_written=total_bytes,
    )

    summary = {
        "total": total,
        "success": success_count,
        "skipped": skipped_count,
        "failed": failed_count,
        "events": total_events,
        "bytes": total_bytes,
    }

    print(f"\nCompleted: {success_count}/{total} hours")
    print(f"  Skipped (existing): {skipped_count}")
    print(f"  Failed: {failed_count}")
    print(f"  Events processed: {total_events:,}")
    print(f"  Data written: {total_bytes / (1024**3):.2f} GB")

    return summary


def incremental(
    base_path: Path,
    workers: int = DEFAULT_WORKERS,
    minimal: bool = False,
) -> dict:
    """
    Ingest new data since the last successful ingestion.

    Automatically determines the start point from saved state.
    """
    last = get_last_ingested(base_path)

    if last is None:
        # No previous ingestion - start from a reasonable default
        # GitHub Archive starts in 2011, but we'll default to 2019 for StarScout
        print("No previous ingestion found. Use --backfill to set initial range.")
        return {"total": 0}

    # Start from the hour after the last ingested
    start = last + timedelta(hours=1)

    # End at the current hour (GitHub Archive has ~1 hour delay)
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)

    if start >= end:
        print(f"Already up to date. Last ingested: {last}")
        return {"total": 0}

    return backfill(start, end, base_path, workers, minimal)


def retry_failed(
    base_path: Path,
    workers: int = DEFAULT_WORKERS,
    minimal: bool = False,
) -> dict:
    """Retry all previously failed hours."""
    failed = get_failed_hours(base_path)

    if not failed:
        print("No failed hours to retry.")
        return {"total": 0}

    print(f"Retrying {len(failed)} failed hours")

    success_count = 0
    still_failed = 0

    for hour in tqdm(failed, desc="Retrying"):
        result = ingest_hour(hour, base_path, minimal, skip_existing=False)
        if result["success"]:
            success_count += 1
            clear_failed_hour(base_path, hour)
        else:
            still_failed += 1

    print(f"Retry complete: {success_count} succeeded, {still_failed} still failing")
    return {"success": success_count, "failed": still_failed}


def validate(base_path: Path, start: datetime, end: datetime) -> dict:
    """
    Validate Parquet files in a date range.

    Checks:
    - Files exist for each hour
    - Files are readable
    - Files have expected schema
    """
    hours = list(iter_hours(start, end))
    missing = []
    invalid = []
    valid = []

    for hour in tqdm(hours, desc="Validating"):
        if not parquet_exists(base_path, hour):
            missing.append(hour)
            continue

        try:
            stats = get_parquet_stats(base_path, hour)
            if stats.get("num_rows", 0) == 0:
                invalid.append((hour, "Empty file"))
            else:
                valid.append(hour)
        except Exception as e:
            invalid.append((hour, str(e)))

    print(f"\nValidation complete:")
    print(f"  Valid: {len(valid)}")
    print(f"  Missing: {len(missing)}")
    print(f"  Invalid: {len(invalid)}")

    if missing:
        print(f"\nFirst 10 missing hours:")
        for h in missing[:10]:
            print(f"    {h}")

    if invalid:
        print(f"\nFirst 10 invalid files:")
        for h, err in invalid[:10]:
            print(f"    {h}: {err}")

    return {
        "valid": len(valid),
        "missing": len(missing),
        "invalid": len(invalid),
    }


def parse_date(s: str) -> datetime:
    """Parse a date string in YYYY-MM-DD format."""
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main():
    parser = argparse.ArgumentParser(
        description="Ingest GitHub Archive data to Parquet"
    )

    parser.add_argument(
        "--data-path",
        type=Path,
        default=DEFAULT_DATA_PATH,
        help=f"Base path for Parquet output (default: {DEFAULT_DATA_PATH})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--minimal",
        action="store_true",
        help="Use minimal schema (smaller files, fewer fields)",
    )

    # Mutually exclusive operations
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--backfill",
        nargs=2,
        metavar=("START", "END"),
        help="Backfill date range (YYYY-MM-DD YYYY-MM-DD)",
    )
    group.add_argument(
        "--incremental",
        action="store_true",
        help="Ingest new data since last successful run",
    )
    group.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry previously failed hours",
    )
    group.add_argument(
        "--validate",
        nargs=2,
        metavar=("START", "END"),
        help="Validate Parquet files in date range",
    )
    group.add_argument(
        "--stats",
        action="store_true",
        help="Show ingestion statistics",
    )
    group.add_argument(
        "--gaps",
        nargs=2,
        metavar=("START", "END"),
        help="Find missing hours in date range",
    )

    args = parser.parse_args()
    base_path = args.data_path

    if args.backfill:
        start = parse_date(args.backfill[0])
        end = parse_date(args.backfill[1])
        backfill(start, end, base_path, args.workers, args.minimal)

    elif args.incremental:
        incremental(base_path, args.workers, args.minimal)

    elif args.retry_failed:
        retry_failed(base_path, args.workers, args.minimal)

    elif args.validate:
        start = parse_date(args.validate[0])
        end = parse_date(args.validate[1])
        validate(base_path, start, end)

    elif args.stats:
        stats = get_stats(base_path)
        last = get_last_ingested(base_path)
        failed = get_failed_hours(base_path)

        print(f"Ingestion Statistics for {base_path}")
        print(f"  Last ingested: {last or 'None'}")
        print(f"  Total hours: {stats.get('total_hours_ingested', 0):,}")
        print(f"  Total events: {stats.get('total_events_processed', 0):,}")
        print(f"  Total bytes: {stats.get('total_bytes_written', 0) / (1024**3):.2f} GB")
        print(f"  Failed hours pending: {len(failed)}")

    elif args.gaps:
        start = parse_date(args.gaps[0])
        end = parse_date(args.gaps[1])
        gaps = find_gaps(base_path, start, end)
        print(f"Found {len(gaps)} missing hours between {start.date()} and {end.date()}")
        if gaps and len(gaps) <= 20:
            for g in gaps:
                print(f"  {g}")


if __name__ == "__main__":
    main()
