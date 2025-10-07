CREATE OR REPLACE VIEW silver.team_dim AS
WITH base AS (
  SELECT DISTINCT
    CAST(id AS VARCHAR) AS team_id,
    NULLIF(TRIM(abbreviation), '') AS team_abbreviation,
    NULLIF(TRIM(nickname), '') AS team_name,
    'NBA' AS league,
    2 AS priority
  FROM bronze_team
),
observed_modern AS (
  SELECT DISTINCT
    CAST(team_id AS VARCHAR) AS team_id,
    NULLIF(TRIM(team_abbreviation), '') AS team_abbreviation,
    NULLIF(TRIM(team_name), '') AS team_name,
    'NBA' AS league,
    1 AS priority
  FROM (
    SELECT
      team_id_home AS team_id,
      team_abbreviation_home AS team_abbreviation,
      team_name_home AS team_name,
      game_date,
      season_id
    FROM bronze_game_norm
    UNION ALL
    SELECT
      team_id_away AS team_id,
      team_abbreviation_away AS team_abbreviation,
      team_name_away AS team_name,
      game_date,
      season_id
    FROM bronze_game_norm
  ) AS unpivoted_teams
  WHERE team_id IS NOT NULL
    AND (
      CASE
        WHEN game_date IS NOT NULL THEN
          CASE
            WHEN EXTRACT(MONTH FROM game_date) >= 8 THEN EXTRACT(YEAR FROM game_date)
            ELSE EXTRACT(YEAR FROM game_date) - 1
          END
        ELSE TRY_CAST(SUBSTR(CAST(season_id AS VARCHAR), 1, 4) AS INT)
      END
    ) >= 2010
),
allstar AS (
  SELECT * FROM (
    VALUES
      ('1610616833', 'EST', 'East All-Stars (NBA)', 'NBA', 0),
      ('1610616834', 'WST', 'West All-Stars (NBA)', 'NBA', 0)
  ) AS v(team_id, team_abbreviation, team_name, league, priority)
),
rolled AS (
  SELECT * FROM allstar
  UNION ALL SELECT * FROM observed_modern
  UNION ALL SELECT * FROM base
)
SELECT team_id, team_abbreviation, team_name, league
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY team_id
      ORDER BY priority, team_abbreviation IS NULL, team_name IS NULL
    ) AS rn
  FROM rolled
) ranked
WHERE rn = 1 AND team_id IS NOT NULL;
