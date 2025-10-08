-- silver__team_keys
-- Source: bronze_team (observed columns: id, abbreviation, full_name)
create or replace table silver__team_keys as
with base as (
  select
    cast(id as int)                as team_id,
    coalesce(abbreviation, '')     as team_abbr,
    coalesce(full_name, '')        as team_name
  from bronze_team
)
select team_id, team_abbr, team_name
from base
where team_id is not null
qualify row_number() over (partition by team_id) = 1;