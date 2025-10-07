CREATE OR REPLACE VIEW silver.team_game AS
WITH canon AS (
  SELECT
    LPAD(CAST(game_id AS VARCHAR), 10, '0') AS game_id,
    CAST(team_id AS VARCHAR)                 AS team_id,
    CAST(season_id AS VARCHAR)               AS season_id,
    CASE LOWER(TRIM(season_type))
      WHEN 'preseason'             THEN 'Pre Season'
      WHEN 'pre season'            THEN 'Pre Season'
      WHEN 'regular season'        THEN 'Regular Season'
      WHEN 'play-in'               THEN 'PlayIn'
      WHEN 'playin'                THEN 'PlayIn'
      WHEN 'all star'              THEN 'All-Star'
      WHEN 'all-star'              THEN 'All-Star'
      WHEN 'in-season tournament'  THEN 'In-Season Tournament'
      WHEN 'in season tournament'  THEN 'In-Season Tournament'
      ELSE season_type
    END AS season_type,
    TRY_CAST(game_date AS DATE)             AS game_date,
    TRY_CAST(pts AS DOUBLE)                 AS pts_n,
    TRY_CAST(fgm AS DOUBLE)                 AS fgm_n,
    TRY_CAST(fga AS DOUBLE)                 AS fga_n,
    TRY_CAST(fg3m AS DOUBLE)                AS fg3m_n,
    TRY_CAST(fg3a AS DOUBLE)                AS fg3a_n,
    TRY_CAST(ftm AS DOUBLE)                 AS ftm_n,
    TRY_CAST(fta AS DOUBLE)                 AS fta_n,
    TRY_CAST(reb AS DOUBLE)                 AS reb_n,
    TRY_CAST(oreb AS DOUBLE)                AS oreb_n,
    TRY_CAST(dreb AS DOUBLE)                AS dreb_n,
    TRY_CAST(ast AS DOUBLE)                 AS ast_n,
    TRY_CAST(stl AS DOUBLE)                 AS stl_n,
    TRY_CAST(blk AS DOUBLE)                 AS blk_n,
    TRY_CAST(tov AS DOUBLE)                 AS tov_n,
    TRY_CAST(pf AS DOUBLE)                  AS pf_n,
    TRY_CAST(min AS DOUBLE)                 AS min_n,
    TRY_CAST(fg_pct AS DOUBLE)              AS fg_pct_raw,
    TRY_CAST(fg3_pct AS DOUBLE)             AS fg3_pct_raw,
    TRY_CAST(ft_pct AS DOUBLE)              AS ft_pct_raw,
    TRY_CAST(plus_minus AS DOUBLE)          AS plus_minus_n,
    TRY_CAST(video_available AS BOOLEAN)    AS video_available,
    wl,
    team_abbreviation,
    team_name,
    matchup
  FROM bronze_game_log_team
  WHERE team_id IS NOT NULL
    AND LENGTH(LPAD(CAST(game_id AS VARCHAR), 10, '0')) = 10
    AND SUBSTR(LPAD(CAST(game_id AS VARCHAR), 10, '0'), 1, 3) IN ('001','002','003','004','005')
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY game_id, team_id, season_type
      ORDER BY (wl IS NOT NULL) DESC, game_date DESC
    ) AS rn
  FROM canon
),
with_home AS (
  SELECT
    *,
    CASE
      WHEN POSITION('vs.' IN LOWER(COALESCE(matchup, ''))) > 0 THEN TRUE
      WHEN POSITION('@'   IN COALESCE(matchup, '')) > 0 THEN FALSE
      ELSE NULL
    END AS inferred_is_home
  FROM dedup
  WHERE rn = 1
),
with_overrides AS (
  SELECT
    wh.*, COALESCE(o.is_home, wh.inferred_is_home) AS is_home
  FROM with_home wh
  LEFT JOIN silver.home_away_overrides o
    ON o.game_id = wh.game_id AND o.team_id = wh.team_id
),
with_pcts AS (
  SELECT
    *,
    CASE WHEN fga_n  > 0 THEN ROUND(fgm_n  / fga_n,  3) END AS fg_pct_canon,
    CASE WHEN fg3a_n > 0 THEN ROUND(fg3m_n / fg3a_n, 3) END AS fg3_pct_canon,
    CASE WHEN fta_n  > 0 THEN ROUND(ftm_n  / fta_n,  3) END AS ft_pct_canon
  FROM with_overrides
),
final AS (
  SELECT
    game_id,
    team_id,
    season_id,
    season_type,
    game_date,
    is_home,
    wl,
    team_abbreviation,
    team_name,
    matchup,
    video_available,
    min_n,
    fgm_n,
    fga_n,
    fg3m_n,
    fg3a_n,
    ftm_n,
    fta_n,
    oreb_n,
    dreb_n,
    reb_n,
    ast_n,
    stl_n,
    blk_n,
    tov_n,
    pf_n,
    pts_n,
    plus_minus_n,
    CASE WHEN fg_pct_canon  BETWEEN 0 AND 1 THEN fg_pct_canon  ELSE NULL END AS fg_pct,
    CASE WHEN fg3_pct_canon BETWEEN 0 AND 1 THEN fg3_pct_canon ELSE NULL END AS fg3_pct,
    CASE WHEN ft_pct_canon  BETWEEN 0 AND 1 THEN ft_pct_canon  ELSE NULL END AS ft_pct,
    ((COALESCE(fgm_n, 0) - COALESCE(fg3m_n, 0)) * 2
      + COALESCE(fg3m_n, 0) * 3
      + COALESCE(ftm_n, 0)) AS calc_pts
  FROM with_pcts
)
SELECT
  game_id,
  team_id,
  season_id,
  season_type,
  game_date,
  is_home,
  wl,
  team_abbreviation,
  team_name,
  matchup,
  video_available,
  min_n        AS min,
  fgm_n        AS fgm,
  fga_n        AS fga,
  fg_pct,
  fg3m_n       AS fg3m,
  fg3a_n       AS fg3a,
  fg3_pct,
  ftm_n        AS ftm,
  fta_n        AS fta,
  ft_pct,
  oreb_n       AS oreb,
  dreb_n       AS dreb,
  reb_n        AS reb,
  ast_n        AS ast,
  stl_n        AS stl,
  blk_n        AS blk,
  tov_n        AS tov,
  pf_n         AS pf,
  pts_n        AS pts,
  plus_minus_n AS plus_minus,
  calc_pts,
  CASE
    WHEN pts_n IS NULL THEN FALSE
    ELSE ABS(pts_n - calc_pts) > 2
  END AS pts_mismatch
FROM final;
