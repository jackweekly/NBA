CREATE OR REPLACE VIEW silver.team_game AS
WITH gl_raw AS (
  SELECT
    LPAD(CAST(game_id AS VARCHAR), 10, '0')              AS game_id,
    CAST(team_id               AS VARCHAR)               AS team_id,
    CAST(season_id             AS VARCHAR)               AS season_id,
    CAST(NULLIF(TRIM(season_type), '') AS VARCHAR)       AS season_type_raw,
    TRY_CAST(NULLIF(CAST(game_date AS VARCHAR), '') AS DATE) AS game_date,
    CAST(NULLIF(TRIM(team_abbreviation), '') AS VARCHAR) AS team_abbreviation,
    CAST(NULLIF(TRIM(team_name), '') AS VARCHAR)         AS team_name,
    CAST(NULLIF(TRIM(matchup), '') AS VARCHAR)           AS matchup,
    CAST(NULLIF(TRIM(wl), '') AS VARCHAR)                AS wl,
    TRY_CAST(NULLIF(CAST(pts       AS VARCHAR), '') AS DOUBLE) AS pts,
    TRY_CAST(NULLIF(CAST(fgm       AS VARCHAR), '') AS DOUBLE) AS fgm,
    TRY_CAST(NULLIF(CAST(fga       AS VARCHAR), '') AS DOUBLE) AS fga,
    TRY_CAST(NULLIF(CAST(fg3m      AS VARCHAR), '') AS DOUBLE) AS fg3m,
    TRY_CAST(NULLIF(CAST(fg3a      AS VARCHAR), '') AS DOUBLE) AS fg3a,
    TRY_CAST(NULLIF(CAST(ftm       AS VARCHAR), '') AS DOUBLE) AS ftm,
    TRY_CAST(NULLIF(CAST(fta       AS VARCHAR), '') AS DOUBLE) AS fta,
    TRY_CAST(NULLIF(CAST(oreb      AS VARCHAR), '') AS DOUBLE) AS oreb,
    TRY_CAST(NULLIF(CAST(dreb      AS VARCHAR), '') AS DOUBLE) AS dreb,
    TRY_CAST(NULLIF(CAST(reb       AS VARCHAR), '') AS DOUBLE) AS reb,
    TRY_CAST(NULLIF(CAST(ast       AS VARCHAR), '') AS DOUBLE) AS ast,
    TRY_CAST(NULLIF(CAST(stl       AS VARCHAR), '') AS DOUBLE) AS stl,
    TRY_CAST(NULLIF(CAST(blk       AS VARCHAR), '') AS DOUBLE) AS blk,
    TRY_CAST(NULLIF(CAST(tov       AS VARCHAR), '') AS DOUBLE) AS tov,
    TRY_CAST(NULLIF(CAST(pf        AS VARCHAR), '') AS DOUBLE) AS pf,
    TRY_CAST(NULLIF(CAST(min       AS VARCHAR), '') AS DOUBLE) AS minutes_raw,
    TRY_CAST(NULLIF(CAST(fg_pct    AS VARCHAR), '') AS DOUBLE) AS fg_pct_raw,
    TRY_CAST(NULLIF(CAST(fg3_pct   AS VARCHAR), '') AS DOUBLE) AS fg3_pct_raw,
    TRY_CAST(NULLIF(CAST(ft_pct    AS VARCHAR), '') AS DOUBLE) AS ft_pct_raw,
    TRY_CAST(NULLIF(CAST(plus_minus AS VARCHAR), '') AS DOUBLE) AS plus_minus,
    TRY_CAST(NULLIF(CAST(video_available AS VARCHAR), '') AS BOOLEAN) AS video_available
  FROM bronze_game_log_team
  WHERE team_id IS NOT NULL
),
canon AS (
  SELECT
    *,
    LOWER(season_type_raw) AS season_type_lower,
    TRY_CAST(SUBSTR(season_id, 1, 4) AS INT) AS start_year
  FROM gl_raw
),
mapped AS (
  SELECT
    *,
    CASE
      WHEN season_type_lower IN ('preseason', 'pre season', 'pre-season') THEN 'Pre Season'
      WHEN season_type_lower IN ('all-star', 'all star', 'allstar') THEN 'All-Star'
      WHEN season_type_lower IN ('play-in', 'play in', 'play-in tournament', 'play in tournament', 'playin') THEN 'PlayIn'
      WHEN season_type_lower = 'regular season' THEN 'Regular Season'
      WHEN season_type_lower = 'playoffs' THEN 'Playoffs'
      ELSE season_type_raw
    END AS season_type
  FROM canon
),
enriched AS (
  SELECT
    *,
    SUBSTR(game_id, 1, 3) AS game_id_prefix,
    CASE WHEN SUBSTR(game_id, 1, 3) IN ('001', '002', '003', '004', '005') THEN TRUE ELSE FALSE END AS game_id_prefix_known
  FROM mapped
),
pbp AS (
  SELECT DISTINCT LPAD(CAST(game_id AS VARCHAR), 10, '0') AS game_id
  FROM bronze_play_by_play
),
joined AS (
  SELECT
    e.*,
    bx.bx_side,
    bx.bx_pts,
    pbp.game_id AS pbp_game_id
  FROM enriched e
  LEFT JOIN silver.box_score_team_norm bx USING (game_id, team_id)
  LEFT JOIN pbp ON pbp.game_id = e.game_id
),
with_home AS (
  SELECT
    *,
    CASE
      WHEN bx_side = 'home' THEN TRUE
      WHEN bx_side = 'away' THEN FALSE
      WHEN POSITION('vs.' IN LOWER(CAST(COALESCE(matchup, '') AS VARCHAR))) > 0 THEN TRUE
      WHEN POSITION('@' IN CAST(matchup AS VARCHAR)) > 0 THEN FALSE
      ELSE NULL
    END AS is_home_inferred
  FROM joined
),
overridden AS (
  SELECT
    wh.*,
    COALESCE(hoa.is_home, wh.is_home_inferred) AS is_home
  FROM with_home wh
  LEFT JOIN silver.home_away_overrides hoa
    ON hoa.game_id = wh.game_id AND hoa.team_id = wh.team_id
),
with_stats AS (
  SELECT
    *,
    CASE WHEN fga  > 0 THEN GREATEST(0, LEAST(1, CAST(fgm AS DOUBLE)  / fga))  ELSE NULL END AS fg_pct_silver,
    CASE WHEN fg3a > 0 THEN GREATEST(0, LEAST(1, CAST(fg3m AS DOUBLE) / fg3a)) ELSE NULL END AS fg3_pct_silver,
    CASE WHEN fta  > 0 THEN GREATEST(0, LEAST(1, CAST(ftm AS DOUBLE)  / fta))  ELSE NULL END AS ft_pct_silver,
    CASE WHEN bx_pts IS NOT NULL THEN bx_pts ELSE NULL END AS pts_from_bx,
    ((COALESCE(fgm, 0) - COALESCE(fg3m, 0)) * 2
      + COALESCE(fg3m, 0) * 3
      + COALESCE(ftm, 0)) AS pts_calc_formula
  FROM overridden
),
with_pts AS (
  SELECT
    *,
    COALESCE(pts_from_bx, pts_calc_formula) AS pts_silver,
    CASE WHEN CAST(minutes_raw AS DOUBLE) BETWEEN 200 AND 330 THEN minutes_raw ELSE NULL END AS min_silver,
    CASE WHEN minutes_raw IS NOT NULL AND (minutes_raw < 200 OR minutes_raw > 330)
         THEN TRUE ELSE FALSE END AS min_bad
  FROM with_stats
),
with_flags AS (
  SELECT
    w.*,
    CASE
      WHEN pts IS NULL OR pts_silver IS NULL THEN NULL
      ELSE ABS(pts - pts_silver) > 2
    END AS pts_mismatch_flag,
    (pts_from_bx IS NOT NULL) AS has_pts_from_bx,
    (bx_side IS NOT NULL) AS has_bx,
    (pbp_game_id IS NOT NULL) AS has_pbp
  FROM with_pts w
),
annotated AS (
  SELECT
    wf.*,
    hsy.start_year,
    (dim.team_id IS NOT NULL) AS team_known
  FROM with_flags wf
  LEFT JOIN helper.helper_season_year hsy ON hsy.season_id = wf.season_id
  LEFT JOIN silver.team_dim dim ON dim.team_id = wf.team_id
)
SELECT
  game_id,
  team_id,
  season_id,
  start_year,
  season_type,
  season_type_raw,
  game_date,
  game_id_prefix,
  game_id_prefix_known,
  team_abbreviation,
  team_name,
  matchup,
  wl,
  is_home,
  video_available,
  pts AS pts_original,
  pts_silver,
  pts_from_bx,
  pts_calc_formula AS pts_calc,
  pts_mismatch_flag,
  fgm,
  fga,
  fg_pct_raw,
  fg_pct_silver,
  fg3m,
  fg3a,
  fg3_pct_raw,
  fg3_pct_silver,
  ftm,
  fta,
  ft_pct_raw,
  ft_pct_silver,
  oreb,
  dreb,
  reb,
  ast,
  stl,
  blk,
  tov,
  pf,
  plus_minus,
  minutes_raw,
  min_silver,
  min_bad,
  has_pts_from_bx,
  has_bx,
  has_pbp,
  team_known,
  (team_known) AS row_valid_any,
  (team_known AND start_year >= 2010 AND is_home IS NOT NULL) AS row_valid_modern
FROM annotated;
