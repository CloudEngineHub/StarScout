"""
Low-activity fake star detector using local DuckDB execution.

This is the local version of simple_detector_bigquery.py that reads
from Parquet files instead of BigQuery.

Usage:
    python -m scripts.local.simple_detector
    python -m scripts.local.simple_detector --start-date 240701 --end-date 250101
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import pymongo

from scripts import (
    GITHUB_TOKENS,
    MONGO_URL,
    START_DATE,
    END_DATE,
    MIN_STARS_LOW_ACTIVITY,
)
from . import DEFAULT_DATA_PATH, DEFAULT_CHECKPOINT_PATH
from .queries import (
    query_to_df,
    query_to_parquet,
    yymmdd_to_date,
)


def enrich_repos_with_github_api(repos: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich repos DataFrame with current GitHub API data.

    Adds columns:
    - repo_id: GitHub GraphQL node ID
    - n_stars_latest: Current star count from GitHub API

    Requires GITHUB_TOKENS to be configured in secrets.yaml.
    Requires strudel.scraper package: pip install strudel.scraper
    """
    if not GITHUB_TOKENS:
        logging.info("Skipping GitHub API enrichment (no tokens in secrets.yaml)")
        return repos

    # Lazy import - only load when actually using enrichment
    try:
        from scripts.github import get_repo_id, get_repo_n_stars_latest
    except ImportError:
        logging.info("Skipping GitHub API enrichment (strudel.scraper not installed)")
        return repos

    logging.info(f"Enriching {len(repos)} repos with GitHub API data...")
    logging.info(f"Using {len(GITHUB_TOKENS)} GitHub token(s) for rate limit rotation")

    # Add repo_id column
    from tqdm import tqdm
    tqdm.pandas(desc="Fetching repo IDs")
    repos = repos.copy()
    repos.insert(0, "repo_id", repos.repo_name.progress_apply(get_repo_id))

    # Add current star count
    tqdm.pandas(desc="Fetching current stars")
    repos["n_stars_latest"] = repos.repo_name.progress_apply(get_repo_n_stars_latest)

    # Log summary
    n_found = repos.repo_id.notna().sum()
    n_missing = repos.repo_id.isna().sum()
    logging.info(f"GitHub API enrichment complete: {n_found} repos found, {n_missing} deleted/private")

    return repos


def get_repos_with_low_activity_stars(
    start_date: str,
    end_date: str,
    min_stars: int,
    data_path: Path,
    checkpoint_path: Path,
) -> pd.DataFrame:
    """
    Find repositories with low-activity stargazers.

    Returns DataFrame with columns: repo_name, n_stars, low_activity_actors
    """
    logging.info(f"Finding repos with low-activity stars ({start_date} to {end_date})")

    sql = Path("scripts/local/sql/dagster/stg_all_repos_with_low_activity_stars.sql").read_text()

    # Convert YYMMDD to YYYY-MM-DD for DuckDB
    start_dt = yymmdd_to_date(start_date)
    end_dt = yymmdd_to_date(end_date)

    result = query_to_df(
        sql,
        params={
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date": end_dt.strftime("%Y-%m-%d"),
            "min_stars_low_activity": min_stars,
        },
        data_path=data_path,
    )

    logging.info(f"Found {len(result)} repositories with >= {min_stars} low-activity stars")

    # Save checkpoint
    checkpoint_file = checkpoint_path / f"repos_with_low_activity_stars_{start_date}_{end_date}.parquet"
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    result.to_parquet(checkpoint_file, compression="zstd")
    logging.info(f"Saved checkpoint: {checkpoint_file}")

    return result


def get_low_activity_stars(
    start_date: str,
    end_date: str,
    data_path: Path,
    checkpoint_path: Path,
) -> pd.DataFrame:
    """
    Get individual low-activity star records.

    Returns DataFrame with columns: actor, repo, starred_at, low_activity
    """
    logging.info(f"Extracting low-activity star records ({start_date} to {end_date})")

    sql = Path("scripts/local/sql/dagster/stg_low_activity_stargazers.sql").read_text()

    # Convert YYMMDD to YYYY-MM-DD for DuckDB
    start_dt = yymmdd_to_date(start_date)
    end_dt = yymmdd_to_date(end_date)

    repos_checkpoint = checkpoint_path / f"repos_with_low_activity_stars_{start_date}_{end_date}.parquet"

    result = query_to_df(
        sql,
        params={
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date": end_dt.strftime("%Y-%m-%d"),
            "repos_checkpoint": str(repos_checkpoint),
        },
        data_path=data_path,
    )

    logging.info(f"Found {len(result)} low-activity star records")

    return result


def dump_repos_csv(
    repos: pd.DataFrame,
    output_dir: Path,
) -> Path:
    """
    Save repositories to CSV file.

    Note: Unlike the BigQuery version, we skip GitHub API calls for repo_id
    and n_stars_latest since those are rate-limited and slow.
    """
    logging.info("Saving repositories to CSV")

    # Calculate statistics
    repos = repos.copy()
    repos["n_stars_low_activity"] = repos["low_activity_actors"].apply(
        lambda x: len(set(x)) if x is not None else 0
    )
    repos["p_stars_low_activity"] = repos["n_stars_low_activity"] / repos["n_stars"]

    # Sort by percentage of low-activity stars
    repos.sort_values(by="p_stars_low_activity", ascending=False, inplace=True)

    # Drop the actors list (too large for CSV)
    repos_out = repos.drop(columns=["low_activity_actors"])

    output_path = output_dir / "fake_stars_low_activity_repos.csv"
    repos_out.to_csv(output_path, index=False)
    logging.info(f"Saved: {output_path}")

    return output_path


def dump_stars_mongodb(
    stars: pd.DataFrame,
    mongo_url: str,
) -> int:
    """
    Save star records to MongoDB.

    Returns number of records inserted.
    """
    logging.info("Saving star records to MongoDB")

    client = pymongo.MongoClient(mongo_url)
    db = client.get_database("fake_stars")

    # Drop existing collection and recreate with index
    db.low_activity_stars.drop()
    db.low_activity_stars.create_index(
        ["repo", "actor", "starred_at"],
        unique=True,
    )

    # Convert DataFrame to records
    records = []
    seen = set()
    for _, row in stars.iterrows():
        key = f"{row['repo']}|{row['actor']}|{row['starred_at']}"
        if key in seen:
            continue
        seen.add(key)
        records.append({
            "repo": row["repo"],
            "actor": row["actor"],
            "starred_at": str(row["starred_at"]),
            "low_activity": bool(row["low_activity"]),
        })

    if records:
        db.low_activity_stars.insert_many(records)

    client.close()
    logging.info(f"Inserted {len(records)} records to MongoDB")

    return len(records)


def main():
    parser = argparse.ArgumentParser(
        description="Detect low-activity fake stars using local DuckDB"
    )
    parser.add_argument(
        "--start-date",
        default=START_DATE,
        help=f"Start date in YYMMDD format (default: {START_DATE})",
    )
    parser.add_argument(
        "--end-date",
        default=END_DATE,
        help=f"End date in YYMMDD format (default: {END_DATE})",
    )
    parser.add_argument(
        "--min-stars",
        type=int,
        default=MIN_STARS_LOW_ACTIVITY,
        help=f"Minimum low-activity stars per repo (default: {MIN_STARS_LOW_ACTIVITY})",
    )
    parser.add_argument(
        "--data-path",
        type=Path,
        default=DEFAULT_DATA_PATH,
        help=f"Path to Parquet data (default: {DEFAULT_DATA_PATH})",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=Path,
        default=DEFAULT_CHECKPOINT_PATH,
        help=f"Path for checkpoints (default: {DEFAULT_CHECKPOINT_PATH})",
    )
    parser.add_argument(
        "--skip-mongodb",
        action="store_true",
        help="Skip MongoDB export (useful if MongoDB is not available)",
    )
    parser.add_argument(
        "--enrich-github",
        action="store_true",
        help="Enrich results with GitHub API data (repo_id, current stars). Requires secrets.yaml with github_tokens.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s (PID %(process)d) [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Create output directory
    output_dir = Path(f"data/{args.end_date}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Find repos with low-activity stars
    repos = get_repos_with_low_activity_stars(
        args.start_date,
        args.end_date,
        args.min_stars,
        args.data_path,
        args.checkpoint_path,
    )

    # Step 1b: Optionally enrich with GitHub API data
    if args.enrich_github:
        repos = enrich_repos_with_github_api(repos)

    # Step 2: Get individual star records
    stars = get_low_activity_stars(
        args.start_date,
        args.end_date,
        args.data_path,
        args.checkpoint_path,
    )

    # Step 3: Save to CSV
    dump_repos_csv(repos, output_dir)

    # Step 4: Save to MongoDB (optional)
    if not args.skip_mongodb:
        try:
            dump_stars_mongodb(stars, MONGO_URL)
        except Exception as e:
            logging.warning(f"MongoDB export failed: {e}")
            logging.warning("Use --skip-mongodb to skip MongoDB export")

    logging.info("Done!")


if __name__ == "__main__":
    main()
