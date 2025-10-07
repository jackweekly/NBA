CREATE SCHEMA IF NOT EXISTS silver;

-- 1) Team-row home/away resolver (1 row per team per game)
CREATE OR REPLACE VIEW silver.home_away_resolved AS
WITH base_teams AS (
  SELECT
    game_id,
    team_id_home AS team_id,
    'home' AS side
  FROM bronze_game_norm
  UNION ALL
  SELECT
    game_id,
    team_id_away AS team_id,
    'away' AS side
  FROM bronze_game_norm
),
overridden_teams AS (
  SELECT
    bt.game_id,
    COALESCE(
      CASE
        WHEN hoa.home_override THEN CAST(hoa.team_id_home AS VARCHAR)
        WHEN hoa.away_override THEN CAST(hoa.team_id_away AS VARCHAR)
        ELSE NULL
      END,
      bt.team_id
    ) AS team_id,
    COALESCE(
      CASE
        WHEN hoa.home_override THEN 'home'
        WHEN hoa.away_override THEN 'away'
        ELSE NULL
      END,
      bt.side
    ) AS side
  FROM base_teams bt
  LEFT JOIN silver.home_away_overrides hoa ON bt.game_id = hoa.game_id
)
SELECT * FROM overridden_teams WHERE team_id IS NOT NULL;

-- 2) Team rows from bronze_box_score (unpivot home/away into rows; no minutes here)
CREATE OR REPLACE VIEW silver.team_rows_from_game AS
SELECT
  game_id,
  team_id_home      AS team_id,
  team_abbreviation_home AS team_abbreviation,
  'home'            AS side,
  pts_ot1_home, pts_ot2_home, pts_ot3_home, pts_ot4_home, pts_ot5_home,
  pts_ot6_home, pts_ot7_home, pts_ot8_home, pts_ot9_home, pts_ot10_home
FROM bronze_box_score
UNION ALL
SELECT
  game_id,
  team_id_away      AS team_id,
  team_abbreviation_away AS team_abbreviation,
  'away'            AS side,
  pts_ot1_away, pts_ot2_away, pts_ot3_away, pts_ot4_away, pts_ot5_away,
  pts_ot6_away, pts_ot7_away, pts_ot8_away, pts_ot9_away, pts_ot10_away
FROM bronze_box_score;

-- 3) Team "minutes" placeholder (no player minutes available in this dataset)
--    Keep the interface stable: expose (game_id, team_id, minutes_raw) so QC can skip NULLs.
CREATE OR REPLACE VIEW silver.team_minutes_from_players AS
SELECT
  game_id,
  team_id,
  CAST(NULL AS DOUBLE) AS minutes_raw
FROM silver.team_rows_from_game;

CREATE OR REPLACE VIEW silver.team_minutes_from_teamline AS
SELECT
  game_id,
  team_id,
  CAST(NULL AS DOUBLE) AS minutes_raw
FROM silver.team_rows_from_game;

CREATE OR REPLACE VIEW silver.team_minutes AS
SELECT * FROM silver.team_minutes_from_players;  -- single source, NULL minutes (safe)

-- 4) Game-level enrichment with **OT inference from OT points columns**
--    Target minutes are TEAM TOTAL player-minutes: 240 + 25 * OT
CREATE OR REPLACE VIEW silver.game_enriched AS
WITH base AS (
  SELECT
    CAST(g.game_id AS BIGINT) AS game_id,
    CAST(g.game_date AS DATE) AS game_date,
    COALESCE(
      g.season_type,
      CASE
        WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 1 THEN 'Pre Season'
        WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 2 THEN 'Regular Season'
        WHEN CAST(SUBSTR(CAST(g.game_id AS VARCHAR), 1, 3) AS INT) = 4 THEN 'Playoffs'
        ELSE NULL
      END
    ) AS season_type
  FROM bronze_game_norm AS g
),
ot_from_pts AS (
  SELECT
    game_id,
    /* Count how many OT frames have any non-null value on either side */
    CAST( (pts_ot1_home  IS NOT NULL OR pts_ot1_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot2_home  IS NOT NULL OR pts_ot2_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot3_home  IS NOT NULL OR pts_ot3_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot4_home  IS NOT NULL OR pts_ot4_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot5_home  IS NOT NULL OR pts_ot5_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot6_home  IS NOT NULL OR pts_ot6_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot7_home  IS NOT NULL OR pts_ot7_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot8_home  IS NOT NULL OR pts_ot8_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot9_home  IS NOT NULL OR pts_ot9_away  IS NOT NULL) AS INTEGER) +
    CAST( (pts_ot10_home IS NOT NULL OR pts_ot10_away IS NOT NULL) AS INTEGER) AS ot_periods
  FROM bronze_box_score
)
SELECT
  b.game_id,
  b.game_date,
  b.season_type,
  COALESCE(o.ot_periods, 0) AS ot_periods,
  240 AS regulation_minutes_per_team,
  COALESCE(o.ot_periods,0)*25 AS ot_minutes_per_team,
  240 + COALESCE(o.ot_periods,0)*25 AS target_minutes_per_team
FROM base b
LEFT JOIN ot_from_pts o USING (game_id);