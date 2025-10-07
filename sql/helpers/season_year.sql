CREATE OR REPLACE VIEW helper.helper_season_year AS
SELECT
  season_id,
  CASE
    WHEN LENGTH(season_id) = 5 AND REGEXP_MATCHES(season_id, '^[0-9]+$') THEN
      CASE
        WHEN CAST(SUBSTR(season_id, 4, 2) AS INTEGER) >= 50
          THEN 1900 + CAST(SUBSTR(season_id, 4, 2) AS INTEGER)
        ELSE 2000 + CAST(SUBSTR(season_id, 4, 2) AS INTEGER)
      END
    ELSE NULL
  END AS start_year
FROM (
  SELECT CAST(season_id AS VARCHAR) AS season_id
  FROM bronze_game_log_team
  GROUP BY 1
);
