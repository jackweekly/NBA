-- Player keys PK uniqueness
create or replace table silver__player_keys_checks as
select
  (select count(*) from silver__player_keys)                                as rows_total,
  (select count(*) from (select distinct player_id from silver__player_keys)) as rows_distinct;

-- Team keys PK uniqueness
create or replace table silver__team_keys_checks as
select
  (select count(*) from silver__team_keys)                                as rows_total,
  (select count(*) from (select distinct team_id from silver__team_keys)) as rows_distinct;