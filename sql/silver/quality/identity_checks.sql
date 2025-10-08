create or replace table silver__team_games_idchecks as
select
  season, season_type, game_id, team_id, is_home,
  pts, fgm, fga, tpm, tpa, ftm, fta,
  (2*(fgm - tpm) + 3*tpm + ftm) as pts_formula,
  case when pts is not null and fgm is not null and tpm is not null and ftm is not null
       then (pts = (2*(fgm - tpm) + 3*tpm + ftm)) else null end as pts_ok
from silver__team_games;

create or replace table silver__team_games_idissues as
select *
from silver__team_games_idchecks
where pts_ok = false
order by game_id, team_id;