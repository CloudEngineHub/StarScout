"""
State management for incremental ingestion.

Tracks:
- Last successfully ingested hour
- Failed hours for retry
- Ingestion statistics
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from . import STATE_FILE


def get_state_path(base_path: Path) -> Path:
    """Get the path to the state file."""
    return base_path / STATE_FILE


def load_state(base_path: Path) -> dict:
    """
    Load ingestion state from disk.

    Returns:
        State dictionary with keys:
        - last_ingested: ISO format datetime string of last successful hour
        - failed_hours: List of ISO format datetime strings that failed
        - stats: Ingestion statistics
    """
    state_path = get_state_path(base_path)

    if not state_path.exists():
        return {
            "last_ingested": None,
            "failed_hours": [],
            "stats": {
                "total_hours_ingested": 0,
                "total_events_processed": 0,
                "total_bytes_written": 0,
            },
        }

    with open(state_path, "r") as f:
        return json.load(f)


def save_state(base_path: Path, state: dict) -> None:
    """Save ingestion state to disk."""
    state_path = get_state_path(base_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    with open(state_path, "w") as f:
        json.dump(state, f, indent=2, default=str)


def get_last_ingested(base_path: Path) -> Optional[datetime]:
    """Get the last successfully ingested hour."""
    state = load_state(base_path)
    last = state.get("last_ingested")
    if last:
        return datetime.fromisoformat(last)
    return None


def set_last_ingested(base_path: Path, dt: datetime) -> None:
    """Update the last successfully ingested hour."""
    state = load_state(base_path)
    state["last_ingested"] = dt.isoformat()
    save_state(base_path, state)


def add_failed_hour(base_path: Path, dt: datetime, error: str) -> None:
    """Record a failed hour for later retry."""
    state = load_state(base_path)
    failed = state.get("failed_hours", [])

    # Add with error info
    failed.append({
        "hour": dt.isoformat(),
        "error": str(error),
        "attempts": 1,
    })

    state["failed_hours"] = failed
    save_state(base_path, state)


def get_failed_hours(base_path: Path) -> list[datetime]:
    """Get list of hours that failed ingestion."""
    state = load_state(base_path)
    failed = state.get("failed_hours", [])
    return [datetime.fromisoformat(f["hour"]) for f in failed]


def clear_failed_hour(base_path: Path, dt: datetime) -> None:
    """Remove an hour from the failed list (after successful retry)."""
    state = load_state(base_path)
    failed = state.get("failed_hours", [])
    state["failed_hours"] = [
        f for f in failed if f["hour"] != dt.isoformat()
    ]
    save_state(base_path, state)


def update_stats(
    base_path: Path,
    hours: int = 0,
    events: int = 0,
    bytes_written: int = 0,
) -> None:
    """Update cumulative ingestion statistics."""
    state = load_state(base_path)
    stats = state.get("stats", {})

    stats["total_hours_ingested"] = stats.get("total_hours_ingested", 0) + hours
    stats["total_events_processed"] = stats.get("total_events_processed", 0) + events
    stats["total_bytes_written"] = stats.get("total_bytes_written", 0) + bytes_written

    state["stats"] = stats
    save_state(base_path, state)


def get_stats(base_path: Path) -> dict:
    """Get ingestion statistics."""
    state = load_state(base_path)
    return state.get("stats", {})


def find_gaps(base_path: Path, start: datetime, end: datetime) -> list[datetime]:
    """
    Find hours in the range that are missing Parquet files.

    Useful for identifying incomplete ingestions.
    """
    from .download import iter_hours
    from .write import parquet_exists

    gaps = []
    for hour in iter_hours(start, end):
        if not parquet_exists(base_path, hour):
            gaps.append(hour)
    return gaps
