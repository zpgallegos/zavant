-- current team rosters. determines who gets a page on the site

with src as (
    select a.* 
    from {{ ref('int_rosters__last_played_game') }} a
    where a.last_played_season = '2024'
)

select
    a.team_id,
    b.team_short,
    b.team_long,
    b.division_id,
    b.division_name,
    b.league_id,
    b.league_name,
    a.player_id,
    c.fullname,
    c.pos_abbr,
    c.pos_code,
    c.pos_name,
    c.pos_type,
    a.last_played_season

from src a
    inner join {{ ref('stg_statsapi__team_info') }} b on a.team_id = b.team_id
    inner join {{ ref('stg_statsapi__player_info') }} c on a.player_id = c.player_id
