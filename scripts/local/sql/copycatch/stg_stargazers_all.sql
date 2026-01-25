-- Extract all star events for a date range
-- DuckDB version of scripts/copycatch/queries/stg_stargazers_all.sql
--
-- Parameters:
--   {parquet_glob} - Glob pattern for Parquet files
--   {start_date} - Start date in YYYY-MM-DD format
--   {end_date} - End date in YYYY-MM-DD format

SELECT
  actor_login AS actor,
  repo_name,
  created_at AS starred_at,
FROM read_parquet('{parquet_glob}', hive_partitioning=true)
WHERE
  created_at >= '{start_date}' AND created_at < '{end_date}'
  AND is_star = true
