create or replace view ml__team_game_features as
with tg as (
  select
    season, season_type, game_date_local, game_id,
    team_id, opp_team_id, is_home,
    pts, fga, fta, tpm, tov,
    (oreb + dreb) as reb, pf
  from silver__team_games
),
sf as (
  select
    game_id, team_id,
    days_rest, rest_bucket, is_b2b, is_3in4, is_4in6,
    pts_l5_avg, tpm_l10_avg, reb_l10_avg
  from silver__team_schedule_features
),
opp as (
  -- opponent's pre-game rolling features for the same game
  select
    game_id, team_id as opp_team_id,
    pts_l5_avg  as pts_l5_avg_opp,
    tpm_l10_avg as tpm_l10_avg_opp,
    reb_l10_avg as reb_l10_avg_opp
  from silver__team_schedule_features
)
select
  tg.season, tg.season_type, tg.game_date_local,
  tg.game_id, tg.team_id, tg.opp_team_id, tg.is_home,

  -- label
  case when tg.pts is not null and tg.pts > (
      select x.pts from silver__team_games x
      where x.game_id=tg.game_id and x.team_id=tg.opp_team_id
  ) then 1
  when tg.pts is not null and (
      select x.pts from silver__team_games x
      where x.game_id=tg.game_id and x.team_id=tg.opp_team_id
  ) is not null then 0
  else null end as y_win,

  -- box
  tg.pts, tg.fga, tg.fta, tg.tpm, tg.tov, tg.reb, tg.pf,

  -- schedule (history-safe)
  sf.days_rest, sf.rest_bucket, sf.is_b2b, sf.is_3in4, sf.is_4in6,
  sf.pts_l5_avg, sf.tpm_l10_avg, sf.reb_l10_avg,

  -- opponent rolling strength (history-safe)
  opp.pts_l5_avg_opp, opp.tpm_l10_avg_opp, opp.reb_l10_avg_opp
from tg
left join sf
  on sf.game_id = tg.game_id and sf.team_id = tg.team_id
left join opp
  on opp.game_id = tg.game_id and opp.opp_team_id = tg.opp_team_id;