-- Crafted-style feature engineering (player-season + player-game)
-- Assumptions / inputs (create tiny views if names differ in your repo):
--   silver__player_games           : one row per (game_id, player_id, team_id)
--       cols needed: season, season_type, game_id, game_date_local, player_id, team_id,
--                    minutes, pts, fga, fgm, fg3a, fg3m, fta, ftm, tov, ast, oreb, dreb
--
--   silver__league_context_season  : one row per season with league means you already compute monthly
--       cols: season, league_efg_mean  (if missing, we compute below from team data as fallback)
--
--   ext__player_pm_metrics         : external advanced metrics per player-season
--       cols (nullable): season, player_id,
--         odarko, o_lebron, o_drip, o_bpm, o_la3rapm,
--         ddarko, d_lebron, d_drip, d_bpm, d_la3rapm
--
--   ref__player_bio (optional)     : player_id, height_inches (for Passer Rating / Portability)
--   ref__player_position (opt)     : player_id, season, pos_group (G/W/F/C) for positional standardization
--   silver__def_versatility (opt)  : player_id, season, versatility_1_100
--
-- Everything creates or replaces tables/views in-place.
----------------------------------------------------------------------

----------------------------------------------------------------------
-- 0) Fallback league context per season (EFG%) if you don't have it
----------------------------------------------------------------------
create or replace table silver__league_context_season as
with team as (
  select start_year as season,
         sum(pts_silver) as pts,
         sum(fgm) as fgm,
         sum(fg3m) as fg3m,
         sum(fga) as fga
  from silver.team_game
  group by 1
)
select
  season,
  -- league eFG% = (FG + 0.5*3P) / FGA
  case when sum(fga)=0 then null
       else (sum(fgm) + 0.5*sum(fg3m)) / nullif(sum(fga),0)::double
  end as league_efg_mean
from team
group by season;

----------------------------------------------------------------------
-- 1) Player season aggregates (per 100 basis helpers)
----------------------------------------------------------------------
create or replace table silver__player_season as
with pg as (
  select
    season,
    player_id,
    team_id,
    sum(minutes)           as mp,
    sum(pts)               as pts,
    sum(fga)               as fga,
    sum(fgm)               as fgm,
    sum(fg3a)              as fg3a,
    sum(fg3m)              as fg3m,
    sum(fta)               as fta,
    sum(ftm)               as ftm,
    sum(tov)               as tov,
    sum(ast)               as ast,
    sum(oreb + dreb)       as trb
  from silver__player_games
  group by 1,2,3
),
rates as (
  select
    season, player_id, team_id,
    mp, pts, fga, fgm, fg3a, fg3m, fta, ftm, tov, ast, trb,
    -- per-100 possession approximations (use 0.44*FTA for possessions proxy)
    -- For Crafted formulas we primarily need attempts per 100; keep it simple
    100.0 * fg3a / nullif(mp,0) * 48.0 as fg3a_per100_minutes,
    100.0 * fga  / nullif(mp,0) * 48.0 as fga_per100_minutes,
    100.0 * fta  / nullif(mp,0) * 48.0 as fta_per100_minutes
  from pg
)
select * from rates;

----------------------------------------------------------------------
-- 2) Standardize external PM metrics by season (z-scores)
--    We standardize within season to align scales.
----------------------------------------------------------------------
create or replace table silver__standardized_pm as
with base as (
  select
    m.season, m.player_id,
    m.odarko,  m.o_lebron,  m.o_drip,  m.o_bpm,  m.o_la3rapm,
    m.ddarko,  m.d_lebron,  m.d_drip,  m.d_bpm,  m.d_la3rapm
  from ext__player_pm_metrics m
),
stats as (
  select season,
    avg(odarko)   as mu_odarko,   stddev_samp(odarko)   as sd_odarko,
    avg(o_lebron) as mu_oleb,     stddev_samp(o_lebron) as sd_oleb,
    avg(o_drip)   as mu_odrip,    stddev_samp(o_drip)   as sd_odrip,
    avg(o_bpm)    as mu_obpm,     stddev_samp(o_bpm)    as sd_obpm,
    avg(o_la3rapm)as mu_ola3,     stddev_samp(o_la3rapm)as sd_ola3,

    avg(ddarko)   as mu_ddarko,   stddev_samp(ddarko)   as sd_ddarko,
    avg(d_lebron) as mu_dleb,     stddev_samp(d_lebron) as sd_dleb,
    avg(d_drip)   as mu_ddrip,    stddev_samp(d_drip)   as sd_ddrip,
    avg(d_bpm)    as mu_dbpm,     stddev_samp(d_bpm)    as sd_dbpm,
    avg(d_la3rapm)as mu_dla3,     stddev_samp(d_la3rapm)as sd_dla3
  from base
  group by 1
)
select
  b.season, b.player_id,
  case when s.sd_odarko=0 or s.sd_odarko is null then null else (b.odarko  - s.mu_odarko)/s.sd_odarko end   as z_odarko,
  case when s.sd_oleb  =0 or s.sd_oleb   is null then null else (b.o_lebron- s.mu_oleb  )/s.sd_oleb   end   as z_olebron,
  case when s.sd_odrip =0 or s.sd_odrip  is null then null else (b.o_drip  - s.mu_odrip )/s.sd_odrip  end   as z_odrip,
  case when s.sd_obpm  =0 or s.sd_obpm   is null then null else (b.o_bpm   - s.mu_obpm  )/s.sd_obpm   end   as z_obpm,
  case when s.sd_ola3  =0 or s.sd_ola3   is null then null else (b.o_la3rapm- s.mu_ola3)/s.sd_ola3    end   as z_ola3,

  case when s.sd_ddarko=0 or s.sd_ddarko is null then null else (b.ddarko  - s.mu_ddarko)/s.sd_ddarko end   as z_ddarko,
  case when s.sd_dleb  =0 or s.sd_dleb   is null then null else (b.d_lebron- s.mu_dleb  )/s.sd_dleb   end   as z_dlebron,
  case when s.sd_ddrip =0 or s.sd_ddrip  is null then null else (b.d_drip  - s.mu_ddrip )/s.sd_ddrip  end   as z_ddrip,
  case when s.sd_dbpm  =0 or s.sd_dbpm   is null then null else (b.d_bpm   - s.mu_dbpm  )/s.sd_dbpm   end   as z_dbpm,
  case when s.sd_dla3  =0 or s.sd_dla3   is null then null else (b.d_la3rapm- s.mu_dla3)/s.sd_dla3    end   as z_dla3
from base b
left join stats s using (season);

----------------------------------------------------------------------
-- 3) CraftedOPM / CraftedDPM (z-combos)
-- Note: Crafted converts final result to MYRPM scale; here we keep z-scale
-- (mean≈0, sd≈1 within-season). You can add an affine transform later if desired.
----------------------------------------------------------------------
create or replace table silver__crafted_pm as
select
  z.season,
  z.player_id,

  -- Offensive: ((2*ODARKO) + OLEBRON + ODRIP + OBPM + OLA3RAPM) / 6
  ((2*z.z_odarko) + z.z_olebron + z.z_odrip + z.z_obpm + z.z_ola3) / 6.0  as crafted_opm_z,

  -- Defensive: ((2*DDARKO) + DLEBRON + DDRIP + (DBPM/2) + (2*DLA3RAPM)) / 6.5
  ((2*z.z_ddarko) + z.z_dlebron + z.z_ddrip + (z.z_dbpm/2.0) + (2*z.z_dla3)) / 6.5 as crafted_dpm_z
from silver__standardized_pm z;

----------------------------------------------------------------------
-- 4) Shooting Proficiency & Spacing (season scale)
-- Shooting Proficiency = (2/(1+exp(-3PA per 100)) - 1) * 3FG%
-- Spacing = (3PA * (3P% * 1.5)) - league EFG%
----------------------------------------------------------------------
create or replace table silver__shooting_profiles as
with agg as (
  select s.season, s.player_id,
         sum(s.fg3a) as fg3a, sum(s.fg3m) as fg3m, sum(s.fga) as fga, sum(s.fgm) as fgm, sum(s.minutes) as mp
  from silver__player_games s
  group by 1,2
),
pct as (
  select
    a.season, a.player_id, a.fg3a, a.fg3m, a.fga, a.fgm, a.mp,
    case when a.fg3a=0 then 0 else a.fg3m::double/a.fg3a end as pct3,
    case when a.fga=0 then 0 else (a.fgm + 0.5*a.fg3m)::double/a.fga end as efg,
    100.0 * a.fg3a / nullif(a.mp,0) * 48.0 as fg3a_per100
  from agg a
),
ctx as (select season, league_efg_mean from silver__league_context_season)
select
  p.season, p.player_id,
  -- Shooting Proficiency
  (2.0/(1.0 + exp(-p.fg3a_per100)) - 1.0) * p.pct3                               as shooting_proficiency,

  -- Spacing (uses league eFG% from season context)
  (p.fg3a * (p.pct3 * 1.5)) - c.league_efg_mean                                   as spacing
from pct p
left join ctx c using (season);

----------------------------------------------------------------------
-- 5) eFG%, TS%, 3PAr, FTr, TRB%, AST%, STL%, BLK%, TOV% (season)
-- Simple, box-derived variants (player-season). Team/opp & on-court minutes
-- versions can be added later.
----------------------------------------------------------------------
create or replace table silver__bbref_style_rates as
with agg as (
  select season, player_id,
         sum(pts) pts, sum(fga) fga, sum(fgm) fgm, sum(fg3a) fg3a, sum(fg3m) fg3m,
         sum(fta) fta, sum(ftm) ftm, sum(tov) tov, sum(ast) ast, sum(oreb+dreb) trb,
         sum(minutes) mp
  from silver__player_games
  group by 1,2
)
select
  season, player_id, mp,

  case when fga=0 then null else (fgm + 0.5*fg3m)::double / fga end                        as efg_pct,
  case when (2*(fga + 0.44*fta))=0 then null else pts::double / (2*(fga + 0.44*fta)) end   as ts_pct,

  case when fga=0 then null else fg3a::double / fga end                                     as three_par,
  case when fga=0 then null else fta::double / fga end                                      as ftr,

  -- ‘True’ TRB%, AST% etc need on/off + team/opp minutes; provide per-100 minute proxies
  100.0 * trb / nullif(mp,0) * 48.0          as trb_per100_min,
  100.0 * ast / nullif(mp,0) * 48.0          as ast_per100_min,
  100.0 * tov / nullif(mp,0) * 48.0          as tov_per100_min
from agg;

----------------------------------------------------------------------
-- 6) Box Creation (per 100), Offensive Load
--  Box Creation (Ben Taylor):
--    BC/100 = Ast*0.1843 + (Pts+TOV)*0.0969 - 2.3021*(3pt proficiency)
--             + 0.0582*(Ast*(Pts+TOV)*3pt proficiency) - 1.1942
--  3pt proficiency ≈ Shooting Proficiency (above)
--  Offensive Load (per game): ((Ast - 0.38*BoxCreation)*0.75) + FGA + 0.44*FTA + BoxCreation + TOV
----------------------------------------------------------------------
create or replace table silver__creation_load as
with season_boxes as (
  select
    g.season, g.player_id,
    sum(g.ast) as ast, sum(g.pts) as pts, sum(g.tov) as tov,
    sum(g.fga) as fga, sum(g.fta) as fta
  from silver__player_games g
  group by 1,2
),
joiner as (
  select
    s.season, s.player_id, s.ast, s.pts, s.tov, s.fga, s.fta,
    sp.shooting_proficiency
  from season_boxes s
  left join silver__shooting_profiles sp
    on sp.season = s.season and sp.player_id = s.player_id
),
calc as (
  select
    season, player_id, ast, pts, tov, fga, fta,
    -- Box Creation per 100 (season scale)
    ( ast*0.1843
      + ( (pts + tov)*0.0969 )
      - 2.3021 * coalesce(shooting_proficiency, 0)
      + 0.0582 * ( ast * (pts + tov) * coalesce(shooting_proficiency, 0) )
      - 1.1942
    ) as box_creation_per100
  from joiner
)
select
  c.season, c.player_id,
  c.box_creation_per100,
  -- Offensive Load (season per-game estimate using season totals)
  ((c.ast - (0.38 * c.box_creation_per100)) * 0.75)
    + c.fga
    + 0.44 * c.fta
    + c.box_creation_per100
    + c.tov                       as offensive_load
from calc c;

----------------------------------------------------------------------
-- 7) Passer Rating (scaffold)
--    PR_raw = z(Load) + 3*z(AST/Load) + (z(AST/Load positional)/1.75) - 2*z(TOV/Load)
--             + 0.5*z(BoxCreation/Load) + 0.2*z(Height)
--    We compute with available inputs; missing inputs => NULL pieces.
----------------------------------------------------------------------
create or replace table silver__passer_rating as
with base as (
  select
    c.season, c.player_id,
    c.offensive_load                                   as load,
    ps.ast, ps.tov,
    c.box_creation_per100                              as box_creation,
    b.height_inches
  from silver__creation_load c
  left join ( -- pull season totals for AST/TOV
    select season, player_id, sum(ast) ast, sum(tov) tov
    from silver__player_games
    group by 1,2
  ) ps using (season, player_id)
  left join ref__player_bio b using (player_id)
),
ratios as (
  select
    season, player_id, load, ast, tov, box_creation, height_inches,
    case when load is null or load=0 then null else ast::double / load end as ast_load,
    case when load is null or load=0 then null else tov::double / load end as tov_load,
    case when load is null or load=0 then null else box_creation::double / load end as bc_load
  from base
),
season_stats as (
  select
    season,
    avg(load)         as mu_load,   stddev_samp(load)         as sd_load,
    avg(ast_load)     as mu_ast_load, stddev_samp(ast_load)     as sd_ast_load,
    avg(tov_load)     as mu_tov_load, stddev_samp(tov_load)     as sd_tov_load,
    avg(bc_load)      as mu_bc_load,  stddev_samp(bc_load)      as sd_bc_load,
    avg(height_inches)as mu_h,      stddev_samp(height_inches)as sd_h
  from ratios
  group by season
),
z_scores as (
  select
    r.season, r.player_id,
    case when ss.sd_load=0 or ss.sd_load is null then null else (r.load - ss.mu_load)/ss.sd_load end                 as z_load,
    case when ss.sd_ast_load=0 or ss.sd_ast_load is null then null else (r.ast_load - ss.mu_ast_load)/ss.sd_ast_load end as z_ast_load,
    case when ss.sd_tov_load=0 or ss.sd_tov_load is null then null else (r.tov_load - ss.mu_tov_load)/ss.sd_tov_load end as z_tov_load,
    case when ss.sd_bc_load=0 or ss.sd_bc_load is null then null else (r.bc_load  - ss.mu_bc_load )/ss.sd_bc_load end  as z_bc_load,
    case when ss.sd_h=0 or ss.sd_h is null then null else (r.height_inches - ss.mu_h)/ss.sd_h end                      as z_height
  from ratios r
  left join season_stats ss using (season)
)
select
  zs.season, zs.player_id,
  -- positional AST/Load would require ref__player_position; we omit that term for now
  (coalesce(z_load,0)
   + 3*coalesce(z_ast_load,0)
   - 2*coalesce(z_tov_load,0)
   + 0.5*coalesce(z_bc_load,0)
   + 0.2*coalesce(z_height,0)
  ) as passer_rating_raw
from z_scores zs;

----------------------------------------------------------------------
-- 8) Portability (scaffold)
--    Portability = TS% + 2.5*z(Shooting Ability) + 0.5*z(CraftedDPM positional)
--                  + (1/3)*z(Def Versatility) + 1.5*z(Passer Rating)
--                  - 0.25*z(Load) - 0.15*z(BoxCreation)
--    We use available ingredients; positional/versatility => optional inputs.
----------------------------------------------------------------------
create or replace table silver__portability as
with need as (
  select
    br.season, br.player_id,
    br.ts_pct,
    sp.shooting_proficiency as shooting_ability,
    c.crafted_dpm_z,
    pr.passer_rating_raw,
    cl.offensive_load as load,
    cl.box_creation_per100 as box_creation,
    v.versatility_1_100 as versatility
  from silver__bbref_style_rates br
  left join silver__shooting_profiles sp using (season, player_id)
  left join silver__crafted_pm c using (season, player_id)
  left join silver__passer_rating pr using (season, player_id)
  left join silver__creation_load cl using (season, player_id)
  left join silver__def_versatility v using (season, player_id)
),
z as (
  select
    season, player_id, ts_pct, shooting_ability, crafted_dpm_z, passer_rating_raw, load, box_creation, versatility,
    avg(shooting_ability) over (partition by season) as mu_sa,
    stddev_samp(shooting_ability) over (partition by season) as sd_sa,
    avg(crafted_dpm_z) over (partition by season) as mu_dpm,
    stddev_samp(crafted_dpm_z) over (partition by season) as sd_dpm,
    avg(passer_rating_raw) over (partition by season) as mu_pr,
    stddev_samp(passer_rating_raw) over (partition by season) as sd_pr,
    avg(load) over (partition by season) as mu_load,
    stddev_samp(load) over (partition by season) as sd_load,
    avg(box_creation) over (partition by season) as mu_bc,
    stddev_samp(box_creation) over (partition by season) as sd_bc,
    avg(versatility) over (partition by season) as mu_v,
    stddev_samp(versatility) over (partition by season) as sd_v
  from need
)
select
  season, player_id,
  ts_pct
  + 2.5 * case when sd_sa=0 or sd_sa is null then null else (shooting_ability - mu_sa)/sd_sa end
  + 0.5 * case when sd_dpm=0 or sd_dpm is null then null else (crafted_dpm_z   - mu_dpm)/sd_dpm end
  + (1.0/3.0) * case when sd_v =0 or sd_v  is null then null else (versatility     - mu_v )/sd_v  end
  + 1.5 * case when sd_pr=0 or sd_pr is null then null else (passer_rating_raw- mu_pr)/sd_pr end
  - 0.25* case when sd_load=0 or sd_load is null then null else (load           - mu_load)/sd_load end
  - 0.15* case when sd_bc=0 or sd_bc is null then null else (box_creation    - mu_bc)/sd_bc end
  as portability_raw
from z;

----------------------------------------------------------------------
-- 9) PTS_GAINED / PTS_SAVED stubs (needs shot-location model)
-- Provide views returning NULLs until shot charts are wired in.
----------------------------------------------------------------------
create or replace view silver__pts_gained as
select season, player_id, cast(null as double) as pts_gained
from (select distinct season, player_id from silver__player_games);

create or replace view silver__pts_saved as
select season, player_id, cast(null as double) as pts_saved
from (select distinct season, player_id from silver__player_games);

----------------------------------------------------------------------
-- 10) Final joined player-season feature view
----------------------------------------------------------------------
create or replace view ml__player_season_features as
select
  coalesce(pm.season::INTEGER, sp.season::INTEGER, br.season::INTEGER, cl.season::INTEGER) as season,
  coalesce(pm.player_id::INTEGER, sp.player_id::INTEGER, br.player_id::INTEGER, cl.player_id::INTEGER) as player_id,

  pm.crafted_opm_z, pm.crafted_dpm_z,
  sp.shooting_proficiency, sp.spacing,
  br.efg_pct, br.ts_pct, br.three_par, br.ftr,
  br.trb_per100_min, br.ast_per100_min, br.tov_per100_min,

  cl.box_creation_per100, cl.offensive_load,
  pr.passer_rating_raw, po.portability_raw,

  pg.pts_gained, ps.pts_saved
from silver__crafted_pm pm
full outer join silver__shooting_profiles   sp ON pm.season::INTEGER = sp.season::INTEGER AND pm.player_id::INTEGER = sp.player_id::INTEGER
full outer join silver__bbref_style_rates   br ON pm.season::INTEGER = br.season::INTEGER AND pm.player_id::INTEGER = br.player_id::INTEGER
full outer join silver__creation_load       cl ON pm.season::INTEGER = cl.season::INTEGER AND pm.player_id::INTEGER = cl.player_id::INTEGER
full outer join silver__passer_rating       pr ON pm.season::INTEGER = pr.season::INTEGER AND pm.player_id::INTEGER = pr.player_id::INTEGER
full outer join silver__portability         po ON pm.season::INTEGER = po.season::INTEGER AND pm.player_id::INTEGER = po.player_id::INTEGER
left join silver__pts_gained                pg ON pm.season::INTEGER = pg.season::INTEGER AND pm.player_id::INTEGER = pg.player_id::INTEGER
left join silver__pts_saved                 ps ON pm.season::INTEGER = ps.season::INTEGER AND pm.player_id::INTEGER = ps.player_id::INTEGER;