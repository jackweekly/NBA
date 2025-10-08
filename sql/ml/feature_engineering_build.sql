PRAGMA enable_profiling=false;

BEGIN TRANSACTION;

-- ---------------------------------------------------------------------
-- 1) Base with opponent box + win label
-- ---------------------------------------------------------------------
create or replace table silver__team_games_w_opp as
with t as (
  select
    season, season_type, game_date_local, game_id,
    team_id, opp_team_id, is_home,
    pts, fga, fta, tpm, tov, oreb, dreb, pf
  from silver__team_games
),
o as (
  select
    game_id, team_id as opp_team_id,
    pts as pts_opp, fga as fga_opp, fta as fta_opp, tpm as tpm_opp,
    tov as tov_opp, oreb as oreb_opp, dreb as dreb_opp, pf as pf_opp
  from silver__team_games
)
select
  t.*, o.pts_opp, o.fga_opp, o.fta_opp, o.tpm_opp,
  o.tov_opp, o.oreb_opp, o.dreb_opp, o.pf_opp,
  case
    when t.pts is not null and o.pts_opp is not null
      then (t.pts > o.pts_opp)::int
    else null
  end as y_win
from t
left join o using (game_id, opp_team_id);

-- ---------------------------------------------------------------------
-- 2) Per-game derived metrics (possessions, 4-factors, ratings)
--     Note: eFG uses a pts/fta/tpm fallback since fgm/ftm aren’t present.
-- ---------------------------------------------------------------------
create or replace table silver__team_game_features_derived as
with base as (
  select * from silver__team_games_w_opp
),
calc as (
  select
    b.*,
    -- Dean Oliver possession estimate
    (fga + 0.44*fta - oreb + tov)::double                            as poss_team,
    (fga_opp + 0.44*fta_opp - oreb_opp + tov_opp)::double            as poss_opp,
    (( (fga + 0.44*fta - oreb + tov) + (fga_opp + 0.44*fta_opp - oreb_opp + tov_opp) )/2.0)::double
                                                                     as poss_game,
    nullif(fga, 0)                                                   as fga_nz,
    nullif(fga_opp, 0)                                               as fga_opp_nz
  from base b
),
feat as (
  select
    *,
    -- eFG% fallback (bounded [0,1]); replace with exact fgm when available
    case when fga_nz is null then null
      else least(1.0, greatest(0.0, (pts - 0.44*fta + 0.5*tpm) / fga_nz))
    end                                                              as efg,
    case when fga_opp_nz is null then null
      else least(1.0, greatest(0.0, (pts_opp - 0.44*fta_opp + 0.5*tpm_opp) / fga_opp_nz))
    end                                                              as efg_opp,

    case when coalesce(poss_team,0)=0 then null else tov/poss_team end        as tov_pct,
    case when coalesce(poss_opp ,0)=0 then null else tov_opp/poss_opp end     as opp_tov_pct,

    case when (oreb + dreb_opp) = 0 then null else oreb::double / (oreb + dreb_opp) end as orb_pct,
    case when (oreb_opp + dreb) = 0 then null else oreb_opp::double / (oreb_opp + dreb) end as opp_orb_pct,

    case when fga_nz is null then null else fta / fga_nz end         as ftr,
    case when fga_opp_nz is null then null else fta_opp / fga_opp_nz end     as opp_ftr,

    case when coalesce(poss_team,0)=0 then null else 100.0*pts     / poss_team end as ortg,
    case when coalesce(poss_opp ,0)=0 then null else 100.0*pts_opp / poss_opp  end as drtg_opp
  from calc
)
select
  season, season_type, game_date_local, game_id, team_id, opp_team_id, is_home, y_win,
  pts, fga, fta, tpm, tov, (oreb+dreb) as reb, pf,
  pts_opp, fga_opp, fta_opp, tpm_opp, tov_opp, (oreb_opp+dreb_opp) as reb_opp, pf_opp,
  poss_team, poss_opp, poss_game,
  efg, tov_pct, orb_pct, ftr,
  efg_opp, opp_tov_pct, opp_orb_pct, opp_ftr,
  ortg, drtg_opp
from feat;

-- ---------------------------------------------------------------------
-- 3) History-safe rolling windows (team & opponent strength)
-- ---------------------------------------------------------------------
create or replace table silver__team_game_features_rolling as
select
  d.*,
  -- recent form
  avg(y_win)     over w10                                              as win_pct_l10,

  -- ratings & pace
  avg(ortg)      over w10                                              as ortg_l10,
  avg(drtg_opp)  over w10                                              as drtg_l10,
  avg(poss_game) over w10                                              as pace_l10,

  -- 4 factors
  avg(efg)       over w10                                              as efg_l10,
  avg(tov_pct)   over w10                                              as tov_pct_l10,
  avg(orb_pct)   over w10                                              as orb_pct_l10,
  avg(ftr)       over w10                                              as ftr_l10,

  -- opponent defensive strength prior to this game
  avg(drtg_opp) over (partition by opp_team_id, season, season_type
                      order by game_date_local
                      rows between 10 preceding and 1 preceding)      as opp_drtg_l10
from silver__team_game_features_derived d
window w10 as (
  partition by team_id, season, season_type
  order by game_date_local
  rows between 10 preceding and 1 preceding
);

-- ---------------------------------------------------------------------
-- 4) Schedule density & seasonality
-- ---------------------------------------------------------------------
create or replace table silver__team_game_features_schedule as
with base as (
  select
    r.*,
    r.poss_game as pace, -- Explicitly select poss_game as pace
    s.days_rest, s.rest_bucket, s.is_b2b, s.is_3in4, s.is_4in6,
    s.gnum, s.prev_date -- Include gnum and prev_date from existing schedule features
  from silver__team_game_features_rolling r
  join silver__team_schedule_features s
    on r.game_id = s.game_id and r.team_id = s.team_id
),
streaks as (
  select
    base.*, -- Select all columns from base
    null::int as road_trip_len, -- Placeholder
    null::int as home_stand_len -- Placeholder
  from base
)
select
  streaks.*, -- Select all columns from streaks
  (extract(dow from game_date_local))::int    as dow,
  (extract(month from game_date_local))::int  as month
from streaks;

-- ---------------------------------------------------------------------
-- 5) League monthly context (means) for simple z-like normalization
-- ---------------------------------------------------------------------
create or replace table silver__league_context_monthly as
with b as (
  select
    season, season_type,
    date_trunc('month', game_date_local)::date as ym,
    ortg, pace, efg -- Use pace here
  from silver__team_game_features_schedule
)
select
  season, season_type, ym,
  avg(ortg)      as league_ortg_mean,
  avg(pace) as league_pace_mean,
  avg(efg)       as league_efg_mean
from b
group by 1,2,3;

-- ---------------------------------------------------------------------
-- 6) Final ML view (single row per game-team)  -- FIXED: no road/home streak cols
-- ---------------------------------------------------------------------
create or replace view ml__team_game_features_v2 as
with f as (
  select s.* from silver__team_game_features_schedule s
),
ctx as (
  select * from silver__league_context_monthly
)
select
  f.season, f.season_type, f.game_date_local,
  f.game_id, f.team_id, f.opp_team_id, f.is_home,
  f.y_win,

  -- box
  f.pts, f.fga, f.fta, f.tpm, f.tov, f.reb, f.pf,

  -- per-game derived
  f.ortg, f.drtg_l10 as drtg, f.efg, f.tov_pct, f.orb_pct, f.ftr, f.pace,

  -- rolling
  f.ortg_l10, f.drtg_l10, f.pace_l10,
  f.efg_l10, f.tov_pct_l10, f.orb_pct_l10, f.ftr_l10,
  f.opp_drtg_l10, f.win_pct_l10,

  -- schedule
  f.days_rest, f.rest_bucket, f.is_b2b, f.is_3in4, f.is_4in6,
  f.gnum, f.dow, f.month,

  -- league-normalized (centered)
  (f.ortg - c.league_ortg_mean)  as ortg_z,
  (f.pace - c.league_pace_mean)  as pace_z,
  (f.efg  - c.league_efg_mean)   as efg_z
from f
left join ctx c
  on c.season = f.season
 and c.season_type = f.season_type
 and c.ym = date_trunc('month', f.game_date_local)::date;

COMMIT;