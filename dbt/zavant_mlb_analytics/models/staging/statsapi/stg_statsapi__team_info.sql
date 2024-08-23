with src as (
    select
        *,
        row_number() over(partition by a.team_id order by a.game_pk desc) as rn
    from {{ source('statsapi', 'game_teams') }} a
)

select 
    a.team_id,
    a.abbreviation as team_short,
    a.franchisename as team_loc,
    a.clubname as team,
    a.name as team_long,
    a.division_id,
    a.division_name,
    a.league_id,
    a.league_name
from src a
where a.rn = 1
