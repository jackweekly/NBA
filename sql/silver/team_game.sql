CREATE OR REPLACE VIEW silver.team_game AS
WITH gl_raw AS (
  SELECT
    LPAD(CAST(game_id AS VARCHAR), 10, '0') AS game_id,
    CAST(team_id AS VARCHAR) AS team_id,
    CAST(season_id AS VARCHAR) AS season_id,
    TRIM(CAST(season_type AS VARCHAR)) AS season_type_raw,
    LOWER(TRIM(CAST(season_type AS VARCHAR))) AS season_type_lower,
    TRY_CAST(game_date AS DATE) AS game_date,
    NULLIF(TRIM(team_abbreviation), '') AS team_abbreviation,
    NULLIF(TRIM(team_name), '') AS team_name,
    NULLIF(TRIM(matchup), '') AS matchup,
    NULLIF(TRIM(wl), '') AS wl,
    TRY_CAST(NULLIF(pts, '') AS DOUBLE) AS pts_raw,
    TRY_CAST(NULLIF(fgm, '') AS DOUBLE) AS fgm,
    TRY_CAST(NULLIF(fga, '') AS DOUBLE) AS fga,
    TRY_CAST(NULLIF(fg3m, '') AS DOUBLE) AS fg3m,
    TRY_CAST(NULLIF(fg3a, '') AS DOUBLE) AS fg3a,
    TRY_CAST(NULLIF(ftm, '') AS DOUBLE) AS ftm,
    TRY_CAST(NULLIF(fta, '') AS DOUBLE) AS fta,
    TRY_CAST(NULLIF(oreb, '') AS DOUBLE) AS oreb,
    TRY_CAST(NULLIF(dreb, '') AS DOUBLE) AS dreb,
    TRY_CAST(NULLIF(reb, '') AS DOUBLE) AS reb,
    TRY_CAST(NULLIF(ast, '') AS DOUBLE) AS ast,
    TRY_CAST(NULLIF(stl, '') AS DOUBLE) AS stl,
    TRY_CAST(NULLIF(blk, '') AS DOUBLE) AS blk,
    TRY_CAST(NULLIF(tov, '') AS DOUBLE) AS tov,
    TRY_CAST(NULLIF(pf, '') AS DOUBLE) AS pf,
    TRY_CAST(NULLIF(min, '') AS DOUBLE) AS minutes_raw,
    TRY_CAST(NULLIF(fg_pct, '') AS DOUBLE) AS fg_pct_raw,
    TRY_CAST(NULLIF(fg3_pct, '') AS DOUBLE) AS fg3_pct_raw,
    TRY_CAST(NULLIF(ft_pct, '') AS DOUBLE) AS ft_pct_raw,
    TRY_CAST(NULLIF(plus_minus, '') AS DOUBLE) AS plus_minus,
    TRY_CAST(NULLIF(video_available, '') AS BOOLEAN) AS video_available
  FROM bronze_game_log_team
  WHERE team_id IS NOT NULL
),
gl AS (
  SELECT
    *,
    CASE
      WHEN season_type_lower IN ('preseason', 'pre season', 'pre-season') THEN 'Pre Season'
      WHEN season_type_lower IN ('all-star', 'all star', 'allstar') THEN 'All-Star'
      WHEN season_type_lower IN ('play-in', 'play in', 'play-in tournament', 'play in tournament', 'playin') THEN 'PlayIn'
      WHEN season_type_lower IN ('regular season') THEN 'Regular Season'
      WHEN season_type_lower IN ('playoffs', 'playoff') THEN 'Playoffs'
      ELSE NULL
    END AS season_type_mapped
  FROM gl_raw
),
canon AS (
  SELECT
    *,
    COALESCE(season_type_mapped, season_type_raw) AS season_type,
    SUBSTRING(game_id, 1, 3) AS game_id_prefix,
    CASE WHEN SUBSTRING(game_id, 1, 3) IN ('001','002','003','004','005') THEN TRUE ELSE FALSE END AS game_id_prefix_known
  FROM gl
),
bx AS (
  SELECT
    LPAD(CAST(game_id AS VARCHAR), 10, '0') AS game_id,
    CAST(team_id_home AS VARCHAR) AS team_id_home,
    CAST(team_id_away AS VARCHAR) AS team_id_away,
    TRY_CAST(NULLIF(pts_home, '') AS DOUBLE) AS pts_home,
    TRY_CAST(NULLIF(pts_away, '') AS DOUBLE) AS pts_away
  FROM bronze_box_score_team
),
pbp AS (
  SELECT DISTINCT LPAD(CAST(game_id AS VARCHAR), 10, '0') AS game_id
  FROM bronze_play_by_play
),
joined AS (
  SELECT
    c.*,
    bx.team_id_home,
    bx.team_id_away,
    bx.pts_home,
    bx.pts_away,
    pbp.game_id AS pbp_game_id
  FROM canon c
  LEFT JOIN bx ON bx.game_id = c.game_id
  LEFT JOIN pbp ON pbp.game_id = c.game_id
),
with_home AS (
  SELECT
    *,
    CASE
      WHEN team_id = team_id_home THEN TRUE
      WHEN team_id = team_id_away THEN FALSE
      WHEN POSITION('vs.' IN LOWER(COALESCE(matchup, ''))) > 0 THEN TRUE
      WHEN POSITION('@' IN COALESCE(matchup, '')) > 0 THEN FALSE
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
    CASE
      WHEN fga > 0 THEN GREATEST(0, LEAST(1, fgm / fga))
      ELSE NULL
    END AS fg_pct_silver,
    CASE
      WHEN fg3a > 0 THEN GREATEST(0, LEAST(1, fg3m / fg3a))
      ELSE NULL
    END AS fg3_pct_silver,
    CASE
      WHEN fta > 0 THEN GREATEST(0, LEAST(1, ftm / fta))
      ELSE NULL
    END AS ft_pct_silver,
    CASE
      WHEN team_id = team_id_home THEN pts_home
      WHEN team_id = team_id_away THEN pts_away
      ELSE NULL
    END AS pts_from_bx,
    ((COALESCE(fgm, 0) - COALESCE(fg3m, 0)) * 2
      + COALESCE(fg3m, 0) * 3
      + COALESCE(ftm, 0)) AS pts_calc_formula
  FROM overridden
),
with_pts AS (
  SELECT
    *,
    COALESCE(pts_from_bx, pts_calc_formula) AS pts_silver,
    CASE WHEN minutes_raw IS NOT NULL AND (minutes_raw < 200 OR minutes_raw > 330)
         THEN TRUE ELSE FALSE END AS min_bad
  FROM with_stats
),
with_flags AS (
  SELECT
    w.*,
    CASE
      WHEN pts_raw IS NULL OR pts_silver IS NULL THEN NULL
      ELSE ABS(pts_raw - pts_silver) > 2
    END AS pts_mismatch_flag,
    CASE WHEN NOT min_bad THEN minutes_raw ELSE NULL END AS min_silver,
    (pts_from_bx IS NOT NULL) AS has_pts_from_bx,
    (team_id_home IS NOT NULL OR team_id_away IS NOT NULL) AS has_bx,
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
  pts_raw AS pts_original,
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
