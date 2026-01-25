-- Extract individual low-activity star records
-- DuckDB version of scripts/dagster/queries/stg_low_activity_stargazers.sql
--
-- Parameters:
--   {parquet_glob} - Glob pattern for Parquet files
--   {start_date} - Start date in YYYY-MM-DD format
--   {end_date} - End date in YYYY-MM-DD format
--   {repos_checkpoint} - Path to repos_with_low_activity_stars checkpoint

WITH
  repo_low_activity_actors AS (
    SELECT
      repo_name,
      UNNEST(low_activity_actors) AS low_activity_actor
    FROM read_parquet('{repos_checkpoint}')
  ),
  events AS (
    SELECT *
    FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    WHERE created_at >= '{start_date}' AND created_at < '{end_date}'
  )
SELECT DISTINCT
  actor_login AS actor,
  repo_name AS repo,
  created_at AS starred_at,
  actor_login IN (SELECT low_activity_actor FROM repo_low_activity_actors) AS low_activity
FROM events
WHERE
  is_star = true
  AND repo_name IN (SELECT repo_name FROM repo_low_activity_actors)
