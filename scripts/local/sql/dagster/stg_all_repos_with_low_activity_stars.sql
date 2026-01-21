-- Find repositories with low-activity stargazers
-- DuckDB version of scripts/dagster/queries/stg_all_repos_with_low_activity_stars.sql
--
-- Parameters:
--   {parquet_glob} - Glob pattern for Parquet files
--   {start_date} - Start date in YYYY-MM-DD format
--   {end_date} - End date in YYYY-MM-DD format
--   {min_stars_low_activity} - Minimum number of low-activity stars per repo

-- Optimized query: only select needed columns to reduce memory/temp usage
WITH
  low_activity_users AS (
    -- First pass: identify low-activity users (only need actor, date, repo, org)
    SELECT
      actor_login AS actor
    FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    WHERE created_at >= '{start_date}' AND created_at < '{end_date}'
    GROUP BY actor_login
    HAVING
      DATE(MIN(created_at)) = DATE(MAX(created_at))  -- single day activity
      AND COUNT(DISTINCT created_at) <= 2             -- max 2 actions
      AND COUNT(DISTINCT repo_name) <= 1              -- max 1 repo
      AND COUNT(DISTINCT org_login) <= 1              -- max 1 org
  ),
  stars AS (
    -- Second pass: only read star events with minimal columns
    SELECT
      actor_login AS actor,
      repo_name
    FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    WHERE created_at >= '{start_date}' AND created_at < '{end_date}'
      AND is_star = true
  )
SELECT
  s.repo_name,
  COUNT(DISTINCT s.actor) AS n_stars,
  list(DISTINCT s.actor) FILTER (WHERE l.actor IS NOT NULL) AS low_activity_actors
FROM stars s
LEFT JOIN low_activity_users l ON s.actor = l.actor
GROUP BY s.repo_name
HAVING len(low_activity_actors) >= {min_stars_low_activity}
ORDER BY len(low_activity_actors) DESC
