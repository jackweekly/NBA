-- Canonical game metadata: season_type, game_date, periods/OT, minutes targets
CREATE SCHEMA IF NOT EXISTS silver;

-- bronze_game_norm was created by apply_schema.py, if not, switch to bronze_game + COALESCE derivation
CREATE OR REPLACE VIEW silver.game_enriched AS
WITH base AS (
  SELECT
    g.game_id,
    CAST(g.game_date AS DATE) AS game_date,
    COALESCE(
      g.season_type,
      CASE
        WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 1 THEN 'Pre Season'
        WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 2 THEN 'Regular Season'
        WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 4 THEN 'Playoffs'
        ELSE NULL
      END
    ) AS season_type,
    4 AS regulation_periods,
    CAST(NULL AS INTEGER) AS ot_periods_raw
  FROM bronze_game_norm AS g
),
tm AS (
  -- average per-team minutes actually observed (sum of player minutes)
  SELECT game_id, AVG(minutes_raw) AS avg_minutes_per_team
  FROM silver.team_minutes
  GROUP BY 1
),
ot_infer AS (
  -- infer OT periods if raw is missing; each OT adds +25 team minutes (5 players × 5 minutes)
  SELECT
    b.game_id,
    CASE
      WHEN tm.avg_minutes_per_team IS NULL THEN NULL
      WHEN tm.avg_minutes_per_team <= 240 + 1 THEN 0
      ELSE ROUND( (tm.avg_minutes_per_team - 240) / 25.0 )
    END AS ot_periods_inferred
  FROM base b
  LEFT JOIN tm USING (game_id)
)
SELECT
  b.game_id,
  b.game_date,
  b.season_type,
  b.regulation_periods,
  COALESCE(b.ot_periods_raw, o.ot_periods_inferred, 0)                           AS ot_periods,
  (b.regulation_periods * 12 * 5)                                                 AS regulation_minutes_per_team, -- 48*5 = 240
  (COALESCE(b.ot_periods_raw, o.ot_periods_inferred, 0) * 25)                     AS ot_minutes_per_team,         -- each OT adds 25
  ((b.regulation_periods * 12 * 5) + (COALESCE(b.ot_periods_raw, o.ot_periods_inferred, 0) * 25)) AS target_minutes_per_team
FROM base b
LEFT JOIN ot_infer o USING (game_id);

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