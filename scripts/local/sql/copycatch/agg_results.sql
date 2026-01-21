-- Aggregate final CopyCatch results
-- DuckDB version of scripts/copycatch/queries/agg_results.sql
--
-- Parameters:
--   {centers_checkpoint} - Path to final centers checkpoint
--   {users_checkpoint} - Path to final users checkpoint
--   {n} - Minimum actors per cluster (e.g., 50)
--   {m} - Minimum repos per cluster (e.g., 10)

WITH
  final_clusters AS (
    SELECT
      c.repo_name,
      c.cluster,
      u.actor,
    FROM read_parquet('{centers_checkpoint}') c
    JOIN read_parquet('{users_checkpoint}') u
      ON c.repo_name = u.repo_name
  )
SELECT
  repo_name,
  cluster,
  list(DISTINCT actor) AS actors
FROM final_clusters
GROUP BY repo_name, cluster
HAVING
  len(actors) >= {n}
  AND len(cluster) >= {m}
