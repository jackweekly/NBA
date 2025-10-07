CREATE OR REPLACE VIEW silver.team_dim AS
WITH bronze AS (
  SELECT
    CAST(id AS VARCHAR) AS team_id,
    NULLIF(TRIM(abbreviation), '') AS team_abbreviation,
    NULLIF(TRIM(nickname), '') AS team_name,
    'NBA' AS league,
    1 AS priority
  FROM bronze_team
),
gl_teams AS (
  SELECT DISTINCT
    CAST(team_id AS VARCHAR) AS team_id,
    NULLIF(TRIM(team_abbreviation), '') AS team_abbreviation,
    NULLIF(TRIM(team_name), '') AS team_name,
    'NBA' AS league,
    2 AS priority
  FROM bronze_game_log_team
  WHERE team_id IS NOT NULL
),
bx_teams AS (
  SELECT DISTINCT
    CAST(team_id AS VARCHAR) AS team_id,
    NULLIF(TRIM(bx_team_abbreviation), '') AS team_abbreviation,
    NULLIF(TRIM(bx_team_nickname), '') AS team_name,
    'NBA' AS league,
    3 AS priority
  FROM silver.box_score_team_norm
  WHERE team_id IS NOT NULL
),
extras AS (
  SELECT * FROM (
    VALUES
      ('1610616833', 'EST', 'East All-Stars (NBA)', 'NBA', 0),
      ('1610616834', 'WST', 'West All-Stars (NBA)', 'NBA', 0)
  ) AS v(team_id, team_abbreviation, team_name, league, priority)
)
SELECT team_id, team_abbreviation, team_name, league
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY team_id
           ORDER BY priority, team_name IS NULL, team_abbreviation IS NULL
         ) AS rn
  FROM (
    SELECT * FROM extras
    UNION ALL SELECT * FROM bronze
    UNION ALL SELECT * FROM gl_teams
    UNION ALL SELECT * FROM bx_teams
  )
) ranked
WHERE rn = 1 AND team_id IS NOT NULL;
