"""
Download hourly GitHub Archive files.

GitHub Archive provides hourly dumps at:
https://data.gharchive.org/{YYYY}-{MM}-{DD}-{H}.json.gz

Files are gzipped JSON with one event per line (NDJSON format).
"""

import gzip
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

import requests

from . import (
    GHARCHIVE_BASE_URL,
    DOWNLOAD_TIMEOUT,
    DOWNLOAD_RETRIES,
    DOWNLOAD_RETRY_DELAY,
)


def get_archive_url(dt: datetime) -> str:
    """Get the GitHub Archive URL for a specific hour."""
    # Format: YYYY-MM-DD-H (hour without leading zero)
    return f"{GHARCHIVE_BASE_URL}/{dt.year}-{dt.month:02d}-{dt.day:02d}-{dt.hour}.json.gz"


def iter_hours(start: datetime, end: datetime) -> Iterator[datetime]:
    """Iterate over each hour in the date range (inclusive start, exclusive end)."""
    current = start.replace(minute=0, second=0, microsecond=0)
    end = end.replace(minute=0, second=0, microsecond=0)
    while current < end:
        yield current
        current += timedelta(hours=1)


def download_hour(dt: datetime, dest_dir: Path | None = None) -> bytes | Path:
    """
    Download a single hour's GitHub Archive data.

    Args:
        dt: The datetime for the hour to download
        dest_dir: If provided, save to file and return path. Otherwise return bytes.

    Returns:
        Either the raw gzipped bytes or the path to the saved file.

    Raises:
        requests.HTTPError: If download fails after retries
    """
    url = get_archive_url(dt)

    for attempt in range(DOWNLOAD_RETRIES):
        try:
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            response.raise_for_status()

            if dest_dir:
                # Save to file
                dest_dir.mkdir(parents=True, exist_ok=True)
                filename = f"{dt.year}-{dt.month:02d}-{dt.day:02d}-{dt.hour}.json.gz"
                dest_path = dest_dir / filename
                with open(dest_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return dest_path
            else:
                # Return bytes
                return response.content

        except requests.RequestException as e:
            if attempt < DOWNLOAD_RETRIES - 1:
                time.sleep(DOWNLOAD_RETRY_DELAY * (attempt + 1))
            else:
                raise


def download_hour_to_memory(dt: datetime) -> list[dict]:
    """
    Download and decompress a single hour's data into memory.

    Returns:
        List of event dictionaries parsed from the NDJSON.
    """
    import json

    content = download_hour(dt)
    if isinstance(content, Path):
        with gzip.open(content, "rt", encoding="utf-8") as f:
            lines = f.readlines()
    else:
        decompressed = gzip.decompress(content).decode("utf-8")
        lines = decompressed.strip().split("\n")

    events = []
    for line in lines:
        if line.strip():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                # Skip malformed lines (rare but happens)
                continue
    return events


def stream_hour_events(dt: datetime) -> Iterator[dict]:
    """
    Stream events from a GitHub Archive hour without loading all into memory.

    Yields:
        Event dictionaries one at a time.
    """
    import json

    url = get_archive_url(dt)

    for attempt in range(DOWNLOAD_RETRIES):
        try:
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            response.raise_for_status()

            # Decompress and parse line by line
            decompressor = gzip.GzipFile(fileobj=response.raw)
            buffer = b""

            for chunk in iter(lambda: decompressor.read(8192), b""):
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if line.strip():
                        try:
                            yield json.loads(line.decode("utf-8"))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            continue

            # Handle any remaining data in buffer
            if buffer.strip():
                try:
                    yield json.loads(buffer.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
            return

        except requests.RequestException as e:
            if attempt < DOWNLOAD_RETRIES - 1:
                time.sleep(DOWNLOAD_RETRY_DELAY * (attempt + 1))
            else:
                raise


def check_hour_exists(dt: datetime) -> bool:
    """Check if a GitHub Archive file exists for the given hour."""
    url = get_archive_url(dt)
    try:
        response = requests.head(url, timeout=10)
        return response.status_code == 200
    except requests.RequestException:
        return False
