-- Map users to clusters based on temporal proximity
-- DuckDB version of scripts/copycatch/queries/map_users.sql
--
-- Parameters:
--   {centers_checkpoint} - Path to centers checkpoint parquet
--   {stargazers_checkpoint} - Path to stargazers checkpoint parquet
--   {delta_t} - Time window in seconds (e.g., 1296000 for 15 days)
--   {rho} - Cluster cohesion threshold (e.g., 0.5)

WITH
  clusters AS (
    SELECT
      c.repo_name,
      UNNEST(c.cluster) AS cluster_repo_name,
      c.centers[UNNEST(c.cluster)] AS cluster_repo_center,
      len(c.cluster) AS cluster_size
    FROM read_parquet('{centers_checkpoint}') c
  ),
  cluster_id_to_actor AS (
    SELECT
      clusters.repo_name,
      clusters.cluster_size,
      s.actor,
    FROM clusters
    JOIN read_parquet('{stargazers_checkpoint}') s
      ON clusters.cluster_repo_name = s.repo_name
    WHERE ABS(clusters.cluster_repo_center - epoch(s.starred_at)) <= {delta_t}
  )
SELECT
  repo_name,
  actor,
FROM cluster_id_to_actor
GROUP BY
  repo_name,
  actor,
  cluster_size
HAVING
  COUNT(*) >= {rho} * cluster_size - 0.001  -- handle numerical imprecision
