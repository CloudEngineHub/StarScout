"""
CopyCatch lockstep fake star detector using local DuckDB execution.

This is the local version of bigquery.py that reads from Parquet files
instead of BigQuery.

Usage:
    python -m scripts.local.copycatch --run
    python -m scripts.local.copycatch --run-one 240701-250101
    python -m scripts.local.copycatch --export
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pymongo

from scripts import (
    END_DATE,
    MONGO_URL,
    MIN_STARS_COPYCATCH_SEED,
    COPYCATCH_NUM_ITERATIONS,
    COPYCATCH_PARAMS,
    COPYCATCH_DATE_CHUNKS,
)
from . import DEFAULT_DATA_PATH, DEFAULT_CHECKPOINT_PATH
from .queries import (
    query_to_df,
    query_to_parquet,
    yymmdd_to_date,
)


def get_chunk_checkpoint_dir(checkpoint_path: Path, start_date: str, end_date: str) -> Path:
    """Get the checkpoint directory for a date chunk."""
    return checkpoint_path / "copycatch" / f"{start_date}_{end_date}"


def get_stargazer_data(
    start_date: str,
    end_date: str,
    data_path: Path,
    checkpoint_path: Path,
) -> Path:
    """
    Extract all star events for a date chunk.

    Returns path to the stargazers checkpoint file.
    """
    chunk_dir = get_chunk_checkpoint_dir(checkpoint_path, start_date, end_date)
    chunk_dir.mkdir(parents=True, exist_ok=True)

    output_path = chunk_dir / "stargazers.parquet"
    if output_path.exists():
        logging.info("Stargazers checkpoint exists: %s", output_path)
        return output_path

    logging.info("Extracting stargazer data (%s to %s)", start_date, end_date)

    sql = Path("scripts/local/sql/copycatch/stg_stargazers_all.sql").read_text()
    start_dt = yymmdd_to_date(start_date)
    end_dt = yymmdd_to_date(end_date)

    query_to_parquet(
        sql,
        output_path,
        params={
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date": end_dt.strftime("%Y-%m-%d"),
        },
        data_path=data_path,
    )

    logging.info("Created stargazers checkpoint: %s", output_path)
    return output_path


def get_initial_centers(
    start_date: str,
    end_date: str,
    checkpoint_path: Path,
) -> Path:
    """
    Initialize cluster centers from repositories with enough stars.

    Returns path to the centers checkpoint file.
    """
    chunk_dir = get_chunk_checkpoint_dir(checkpoint_path, start_date, end_date)
    stargazers_path = chunk_dir / "stargazers.parquet"
    output_path = chunk_dir / "centers.parquet"

    if not stargazers_path.exists():
        raise FileNotFoundError(f"Stargazers checkpoint not found: {stargazers_path}")

    logging.info("Initializing cluster centers (%s to %s)", start_date, end_date)

    sql = Path("scripts/local/sql/copycatch/stg_initial_centers.sql").read_text()

    query_to_parquet(
        sql,
        output_path,
        params={
            "stargazers_checkpoint": str(stargazers_path),
            "min_stars_copycatch_seed": MIN_STARS_COPYCATCH_SEED,
        },
    )

    n_centers = pq.read_metadata(output_path).num_rows
    logging.info("Created %d initial centers: %s", n_centers, output_path)
    return output_path


def map_users(
    start_date: str,
    end_date: str,
    checkpoint_path: Path,
) -> tuple[Path, int]:
    """
    Map users to clusters based on temporal proximity.

    Returns (path to users checkpoint, number of user-cluster pairs).
    """
    chunk_dir = get_chunk_checkpoint_dir(checkpoint_path, start_date, end_date)
    centers_path = chunk_dir / "centers.parquet"
    stargazers_path = chunk_dir / "stargazers.parquet"
    output_path = chunk_dir / "users.parquet"

    sql = Path("scripts/local/sql/copycatch/map_users.sql").read_text()

    query_to_parquet(
        sql,
        output_path,
        params={
            "centers_checkpoint": str(centers_path),
            "stargazers_checkpoint": str(stargazers_path),
            "delta_t": COPYCATCH_PARAMS.delta_t,
            "rho": COPYCATCH_PARAMS.rho,
        },
    )

    n_users = pq.read_metadata(output_path).num_rows
    logging.info("Mapped %d (repo, actor) pairs", n_users)
    return output_path, n_users


def reduce_centers(
    start_date: str,
    end_date: str,
    checkpoint_path: Path,
) -> tuple[Path, int]:
    """
    Update cluster centers based on user assignments.

    Returns (path to updated centers, number of centers).
    """
    chunk_dir = get_chunk_checkpoint_dir(checkpoint_path, start_date, end_date)
    centers_path = chunk_dir / "centers.parquet"
    users_path = chunk_dir / "users.parquet"
    stargazers_path = chunk_dir / "stargazers.parquet"

    sql = Path("scripts/local/sql/copycatch/reduce_centers.sql").read_text()

    # Write to temp file, then replace
    temp_path = chunk_dir / "centers_new.parquet"

    query_to_parquet(
        sql,
        temp_path,
        params={
            "centers_checkpoint": str(centers_path),
            "users_checkpoint": str(users_path),
            "stargazers_checkpoint": str(stargazers_path),
            "delta_t": COPYCATCH_PARAMS.delta_t,
            "relaxed_m": 20 * COPYCATCH_PARAMS.m,
            "m": COPYCATCH_PARAMS.m,
        },
    )

    # Replace old centers with new
    temp_path.replace(centers_path)

    n_centers = pq.read_metadata(centers_path).num_rows
    logging.info("Updated to %d centers", n_centers)
    return centers_path, n_centers


def run_chunk(
    start_date: str,
    end_date: str,
    data_path: Path,
    checkpoint_path: Path,
    max_iterations: int = COPYCATCH_NUM_ITERATIONS,
) -> None:
    """
    Run CopyCatch algorithm on a single date chunk.

    Unlike BigQuery, we can run until true convergence (no 6-hour limit).
    """
    logging.info("Processing chunk %s to %s", start_date, end_date)

    chunk_dir = get_chunk_checkpoint_dir(checkpoint_path, start_date, end_date)
    done_marker = chunk_dir / ".done"

    if done_marker.exists():
        logging.info("Chunk already completed: %s", chunk_dir)
        return

    # Step 1: Extract stargazer data
    get_stargazer_data(start_date, end_date, data_path, checkpoint_path)

    # Step 2: Initialize centers
    get_initial_centers(start_date, end_date, checkpoint_path)

    # Step 3: Iterative clustering
    n_prev_users = -1
    for i in range(max_iterations):
        users_path, n_users = map_users(start_date, end_date, checkpoint_path)

        if n_users == n_prev_users:
            logging.info("Converged after %d iterations (no new users)", i + 1)
            break
        n_prev_users = n_users

        centers_path, n_centers = reduce_centers(start_date, end_date, checkpoint_path)
        logging.info("Iteration %d: %d users, %d clusters", i + 1, n_users, n_centers)

    # Mark as complete
    done_marker.touch()
    logging.info("Finished chunk %s_%s", start_date, end_date)


def agg_results(
    start_date: str,
    end_date: str,
    checkpoint_path: Path,
) -> Path:
    """
    Aggregate final CopyCatch results for a chunk.

    Returns path to clusters checkpoint.
    """
    chunk_dir = get_chunk_checkpoint_dir(checkpoint_path, start_date, end_date)
    centers_path = chunk_dir / "centers.parquet"
    users_path = chunk_dir / "users.parquet"
    output_path = chunk_dir / "clusters.parquet"

    if output_path.exists():
        logging.info("Clusters checkpoint exists: %s", output_path)
        return output_path

    sql = Path("scripts/local/sql/copycatch/agg_results.sql").read_text()

    query_to_parquet(
        sql,
        output_path,
        params={
            "centers_checkpoint": str(centers_path),
            "users_checkpoint": str(users_path),
            "n": COPYCATCH_PARAMS.n,
            "m": COPYCATCH_PARAMS.m,
        },
    )

    n_clusters = pq.read_metadata(output_path).num_rows
    logging.info("Aggregated %d clusters: %s", n_clusters, output_path)
    return output_path


def summarize_all_chunks(
    checkpoint_path: Path,
    date_chunks: list[tuple[str, str]],
) -> dict[str, set[str]]:
    """
    Summarize results from all chunks.

    Returns dict mapping repo_name -> set of fake actor logins.
    """
    repo_to_fakes = defaultdict(set)

    for start_date, end_date in date_chunks:
        chunk_dir = get_chunk_checkpoint_dir(checkpoint_path, start_date, end_date)
        clusters_path = chunk_dir / "clusters.parquet"
        stargazers_path = chunk_dir / "stargazers.parquet"

        if not clusters_path.exists():
            logging.warning("Clusters not found for %s_%s, skipping", start_date, end_date)
            continue

        # Load stargazers mapping
        stargazers_df = pd.read_parquet(stargazers_path)
        repo_user_to_time = {
            (row["repo_name"], row["actor"]): row["starred_at"]
            for _, row in stargazers_df.iterrows()
        }
        logging.info("Loaded %d stargazers for %s_%s", len(repo_user_to_time), start_date, end_date)

        # Load clusters
        clusters_df = pd.read_parquet(clusters_path)

        for _, row in clusters_df.iterrows():
            cluster_repos = row["cluster"]
            actors = row["actors"]

            if len(cluster_repos) < COPYCATCH_PARAMS.m * COPYCATCH_PARAMS.rho:
                continue

            # Validate cluster
            valid_repos = 0
            for repo in cluster_repos:
                star_count = sum(
                    1 for actor in actors if (repo, actor) in repo_user_to_time
                )
                if star_count >= COPYCATCH_PARAMS.n:
                    valid_repos += 1

            if valid_repos < COPYCATCH_PARAMS.m * COPYCATCH_PARAMS.rho:
                continue

            # Add to results
            for repo in cluster_repos:
                star_count = sum(
                    1 for actor in actors if (repo, actor) in repo_user_to_time
                )
                if star_count >= COPYCATCH_PARAMS.n:
                    repo_to_fakes[repo].update(actors)

    logging.info(
        "Total: %d repos with fake stars, %d unique fake actors",
        len(repo_to_fakes),
        len(set().union(*repo_to_fakes.values())) if repo_to_fakes else 0,
    )

    return repo_to_fakes


def export_csv(
    repo_to_fakes: dict[str, set[str]],
    output_dir: Path,
) -> Path:
    """Export results to CSV file."""
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for repo, fakes in repo_to_fakes.items():
        results.append({
            "repo_name": repo,
            "n_stars_clustered": len(fakes),
        })

    df = pd.DataFrame(results)
    df.sort_values(by="n_stars_clustered", ascending=False, inplace=True)

    output_path = output_dir / "fake_stars_clustered_repos.csv"
    df.to_csv(output_path, index=False)
    logging.info("Saved: %s", output_path)

    return output_path


def export_mongodb(
    repo_to_fakes: dict[str, set[str]],
    checkpoint_path: Path,
    date_chunks: list[tuple[str, str]],
    mongo_url: str,
) -> int:
    """Export results to MongoDB."""
    client = pymongo.MongoClient(mongo_url)
    collection = client.fake_stars.clustered_stars
    collection.drop()
    collection.create_index(["repo", "actor", "starred_at"], unique=True)

    total_inserted = 0

    for start_date, end_date in date_chunks:
        chunk_dir = get_chunk_checkpoint_dir(checkpoint_path, start_date, end_date)
        stargazers_path = chunk_dir / "stargazers.parquet"

        if not stargazers_path.exists():
            continue

        stargazers_df = pd.read_parquet(stargazers_path)

        records = []
        for _, row in stargazers_df.iterrows():
            repo = row["repo_name"]
            actor = row["actor"]

            if repo not in repo_to_fakes:
                continue

            is_clustered = actor in repo_to_fakes[repo]
            records.append({
                "repo": repo,
                "actor": actor,
                "starred_at": str(row["starred_at"]),
                "clustered": is_clustered,
            })

            if len(records) >= 10000:
                collection.insert_many(records)
                total_inserted += len(records)
                records.clear()

        if records:
            collection.insert_many(records)
            total_inserted += len(records)

    client.close()
    logging.info("Inserted %d records to MongoDB", total_inserted)
    return total_inserted


def main():
    parser = argparse.ArgumentParser(
        description="Run CopyCatch lockstep detection locally"
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run CopyCatch on all date chunks",
    )
    parser.add_argument(
        "--run-one",
        type=str,
        default=None,
        help="Run CopyCatch on one date range (format: YYMMDD-YYMMDD)",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Aggregate and export results",
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
        help="Skip MongoDB export",
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s (PID %(process)d) [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    if args.run:
        for start_date, end_date in COPYCATCH_DATE_CHUNKS:
            run_chunk(start_date, end_date, args.data_path, args.checkpoint_path)

    if args.run_one:
        start_date, end_date = args.run_one.split("-")
        run_chunk(start_date, end_date, args.data_path, args.checkpoint_path)

    if args.export:
        # Aggregate results from all chunks
        for start_date, end_date in COPYCATCH_DATE_CHUNKS:
            chunk_dir = get_chunk_checkpoint_dir(args.checkpoint_path, start_date, end_date)
            if (chunk_dir / ".done").exists():
                agg_results(start_date, end_date, args.checkpoint_path)

        # Summarize all
        repo_to_fakes = summarize_all_chunks(args.checkpoint_path, COPYCATCH_DATE_CHUNKS)

        # Export to CSV
        output_dir = Path(f"data/{END_DATE}")
        export_csv(repo_to_fakes, output_dir)

        # Export to MongoDB
        if not args.skip_mongodb:
            try:
                export_mongodb(
                    repo_to_fakes,
                    args.checkpoint_path,
                    COPYCATCH_DATE_CHUNKS,
                    MONGO_URL,
                )
            except Exception as e:
                logging.warning("MongoDB export failed: %s", e)

    logging.info("Done!")


if __name__ == "__main__":
    main()
