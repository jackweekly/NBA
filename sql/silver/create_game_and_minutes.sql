-- Canonical game metadata: season_type, game_date, periods/OT, minutes targets
CREATE SCHEMA IF NOT EXISTS silver;

-- bronze_game_norm was created by apply_schema.py, if not, switch to bronze_game + COALESCE derivation
CREATE OR REPLACE VIEW silver.game_enriched AS
WITH base AS (
  SELECT
    g.game_id,
    /* normalize date */
    CAST(g.game_date AS DATE) AS game_date,
    /* normalize season type */
    COALESCE(g.season_type,
             CASE
               WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 1 THEN 'Pre Season'
               WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 2 THEN 'Regular Season'
               WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 4 THEN 'Playoffs'
               ELSE NULL
             END) AS season_type,
    /* regulation periods (NBA) */
    4 AS regulation_periods,
    /* OT periods, if known; otherwise infer 0 */
    0 AS ot_periods
  FROM bronze_game_norm AS g
)
SELECT
  game_id,
  game_date,
  season_type,
  regulation_periods,
  ot_periods,
  /* per-team regulation minutes (48) + per-OT minutes (5) */
  (regulation_periods * 12 * 5) AS regulation_minutes_per_team,
  (ot_periods * 5 * 5)          AS ot_minutes_per_team,
  ((regulation_periods * 12 * 5) + (ot_periods * 5 * 5)) AS target_minutes_per_team
FROM base;

-- Team minutes from bronze_game
CREATE OR REPLACE VIEW silver.team_minutes AS
SELECT
  game_id,
  team_id,
  CASE
    WHEN typeof(min) IN ('DOUBLE', 'DECIMAL', 'BIGINT', 'INTEGER') THEN CAST(min AS DOUBLE)
    WHEN CAST(min AS VARCHAR) ~ '^[0-9]+:[0-9]{2}$' THEN
         CAST(SPLIT_PART(CAST(min AS VARCHAR), ':', 1) AS DOUBLE) + CAST(SPLIT_PART(CAST(min AS VARCHAR), ':', 2) AS DOUBLE)/60.0
    ELSE NULL
  END AS minutes_raw
FROM bronze_game_norm;

-- Final resolved home/away (from overrides already written earlier)
CREATE OR REPLACE VIEW silver.home_away_resolved AS
SELECT
  o.game_id,
  o.team_id_home,
  o.team_id_away
FROM silver.home_away_overrides o;