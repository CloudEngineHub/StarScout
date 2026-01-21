-- Initialize cluster centers from repositories with enough stars
-- DuckDB version of scripts/copycatch/queries/stg_initial_centers.sql
--
-- Parameters:
--   {stargazers_checkpoint} - Path to stargazers checkpoint parquet
--   {min_stars_copycatch_seed} - Minimum stars to seed a cluster

WITH
  df1 AS (
    SELECT
      repo_name,
      COUNT(actor) AS n_stars,
      AVG(epoch(starred_at)) AS repo_center,
    FROM read_parquet('{stargazers_checkpoint}')
    GROUP BY repo_name
    HAVING n_stars >= {min_stars_copycatch_seed}
  )
SELECT
  repo_name,
  repo_center,
  -- Store centers as a MAP instead of JSON for easier access
  MAP {{repo_name: repo_center}} AS centers,
  [repo_name] AS cluster,
FROM df1
