with

src as (
    select * from {{ source('statsapi', 'game_teams') }}
),

ordered as (
    select
        *,
        row_number() over (partition by team_id order by game_pk desc) as rn
    from src
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
from ordered as a
where a.rn = 1
