-- Update cluster centers based on user assignments
-- DuckDB version of scripts/copycatch/queries/reduce_centers.sql
--
-- Parameters:
--   {centers_checkpoint} - Path to centers checkpoint parquet
--   {users_checkpoint} - Path to users checkpoint parquet
--   {stargazers_checkpoint} - Path to stargazers checkpoint parquet
--   {delta_t} - Time window in seconds
--   {relaxed_m} - Relaxed cluster size limit (e.g., 200)
--   {m} - Final cluster size limit (e.g., 10)
--
-- Optimization: UNNEST the centers MAP early to avoid carrying MAP through JOINs.
-- This produces identical results but uses less memory.

WITH
  -- Flatten centers MAP into rows: (repo_name, repo_center, center_repo, center_time)
  centers_flat AS (
    SELECT
      c.repo_name,
      c.repo_center,
      UNNEST(map_keys(c.centers)) AS center_repo,
      UNNEST(map_values(c.centers)) AS center_time
    FROM read_parquet('{centers_checkpoint}') c
  ),
  -- Get distinct repo_name, repo_center pairs for fallback
  centers_base AS (
    SELECT DISTINCT repo_name, repo_center
    FROM read_parquet('{centers_checkpoint}')
  ),
  -- Sample users (same 20% as original)
  sampled_users AS (
    SELECT repo_name, actor
    FROM read_parquet('{users_checkpoint}') USING SAMPLE 20%
  ),
  -- Join centers with sampled users (no MAP carried through)
  center_with_actor AS (
    SELECT
      cb.repo_name,
      cb.repo_center,
      u.actor
    FROM centers_base cb
    JOIN sampled_users u ON cb.repo_name = u.repo_name
  ),
  -- Join with stargazers and lookup center time via flat table
  new_centers AS (
    SELECT
      c.repo_name,
      c.repo_center,
      c.actor,
      s.repo_name AS center_repo_name,
      epoch(s.starred_at) AS starred_at
    FROM center_with_actor c
    JOIN read_parquet('{stargazers_checkpoint}') s
      ON c.actor = s.actor
    LEFT JOIN centers_flat cf
      ON c.repo_name = cf.repo_name AND s.repo_name = cf.center_repo
    WHERE ABS(epoch(s.starred_at) - COALESCE(cf.center_time, c.repo_center)) <= {delta_t}
  ),
  new_center2 AS (
    SELECT
      repo_name,
      repo_center,
      {{
        'r': center_repo_name,
        'c': AVG(starred_at),
        'p': COUNT(starred_at),
        'v': VARIANCE(starred_at)
      }} AS centers,
    FROM new_centers
    GROUP BY
      repo_name,
      repo_center,
      center_repo_name
  ),
  new_center3 AS (
    SELECT
      repo_name,
      repo_center,
      list(centers ORDER BY centers.p DESC, centers.v ASC NULLS LAST)[:{relaxed_m}] AS centers
    FROM new_center2
    GROUP BY
      repo_name,
      repo_center
  )
SELECT
  repo_name,
  repo_center,
  MAP_FROM_ENTRIES(list_transform(centers[:{m}], x -> (x.r, x.c))) AS centers,
  list_transform(centers[:{m}], x -> x.r) AS cluster,
FROM new_center3
