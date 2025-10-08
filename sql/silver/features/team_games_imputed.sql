-- 5A. Medians at three granularities
create or replace table silver__med_league as
select
  'ALL' as scope, null::int as season, null::int as team_id,
  median(pts) as pts, median(fgm) as fgm, median(fga) as fga,
  median(tpm) as tpm, median(tpa) as tpa, median(ftm) as ftm, median(fta) as fta,
  median(oreb) as oreb, median(dreb) as dreb, median(ast) as ast,
  median(stl) as stl, median(blk) as blk, median(tov) as tov, median(pf) as pf
from silver__team_games
where season_type='REG';

create or replace table silver__med_season as
select
  'SEASON' as scope, season, null::int as team_id,
  median(pts) as pts, median(fgm) as fgm, median(fga) as fga,
  median(tpm) as tpm, median(tpa) as tpa, median(ftm) as ftm, median(fta) as fta,
  median(oreb) as oreb, median(dreb) as dreb, median(ast) as ast,
  median(stl) as stl, median(blk) as blk, median(tov) as tov, median(pf) as pf
from silver__team_games
where season_type='REG'
group by season;

create or replace table silver__med_team_season as
select
  'TEAM_SEASON' as scope, season, team_id,
  median(pts) as pts, median(fgm) as fgm, median(fga) as fga,
  median(tpm) as tpm, median(tpa) as tpa, median(ftm) as ftm, median(fta) as fta,
  median(oreb) as oreb, median(dreb) as dreb, median(ast) as ast,
  median(stl) as stl, median(blk) as blk, median(tov) as tov, median(pf) as pf
from silver__team_games
where season_type='REG'
group by season, team_id;

-- 5B. Coalesce in order: TEAM_SEASON -> SEASON -> LEAGUE
create or replace table silver__team_games_imputed as
with src as (select * from silver__team_games),
m as (
  select s.game_id, s.team_id, s.season,
    coalesce(ts.pts, se.pts, le.pts) as pts_med,
    coalesce(ts.fgm, se.fgm, le.fgm) as fgm_med,
    coalesce(ts.fga, se.fga, le.fga) as fga_med,
    coalesce(ts.tpm, se.tpm, le.tpm) as tpm_med,
    coalesce(ts.tpa, se.tpa, le.tpa) as tpa_med,
    coalesce(ts.ftm, se.ftm, le.ftm) as ftm_med,
    coalesce(ts.fta, se.fta, le.fta) as fta_med,
    coalesce(ts.oreb, se.oreb, le.oreb) as oreb_med,
    coalesce(ts.dreb, se.dreb, le.dreb) as dreb_med,
    coalesce(ts.ast, se.ast, le.ast) as ast_med,
    coalesce(ts.stl, se.stl, le.stl) as stl_med,
    coalesce(ts.blk, se.blk, le.blk) as blk_med,
    coalesce(ts.tov, se.tov, le.tov) as tov_med,
    coalesce(ts.pf,  se.pf,  le.pf)  as pf_med
  from silver__team_games s
  left join silver__med_team_season ts on ts.season = s.season and ts.team_id = s.team_id
  left join silver__med_season se      on se.season = s.season
  cross join silver__med_league le
)
select
  s.*,
  coalesce(s.pts,  m.pts_med)  as pts_i,
  coalesce(s.fgm,  m.fgm_med)  as fgm_i,
  coalesce(s.fga,  m.fga_med)  as fga_i,
  coalesce(s.tpm,  m.tpm_med)  as tpm_i,
  coalesce(s.tpa,  m.tpa_med)  as tpa_i,
  coalesce(s.ftm,  m.ftm_med)  as ftm_i,
  coalesce(s.fta,  m.fta_med)  as fta_i,
  coalesce(s.oreb, m.oreb_med) as oreb_i,
  coalesce(s.dreb, m.dreb_med) as dreb_i,
  coalesce(s.ast,  m.ast_med)  as ast_i,
  coalesce(s.stl,  m.stl_med)  as stl_i,
  coalesce(s.blk,  m.blk_med)  as blk_i,
  coalesce(s.tov,  m.tov_med)  as tov_i,
  coalesce(s.pf,   m.pf_med)   as pf_i,

  (s.pts  is null) as was_imputed_pts,
  (s.fgm  is null) as was_imputed_fgm,
  (s.fga  is null) as was_imputed_fga,
  (s.tpm  is null) as was_imputed_tpm,
  (s.tpa  is null) as was_imputed_tpa,
  (s.ftm  is null) as was_imputed_ftm,
  (s.fta  is null) as was_imputed_fta,
  (s.oreb is null) as was_imputed_oreb,
  (s.dreb is null) as was_imputed_dreb,
  (s.ast  is null) as was_imputed_ast,
  (s.stl  is null) as was_imputed_stl,
  (s.blk  is null) as was_imputed_blk,
  (s.tov  is null) as was_imputed_tov,
  (s.pf   is null) as was_imputed_pf
from src s
join m using (game_id, team_id, season);