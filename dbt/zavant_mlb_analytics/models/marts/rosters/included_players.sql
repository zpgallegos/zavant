-- players that should get a page on the site

with src as (
    select * from {{ ref('current_rosters') }}
),
ab_filter as (
    -- set a minimum number of at-bats with their current team. uses the standard batting metrics
    
    select a.*
    from src a
        inner join {{ ref('standard_batting') }} b on
            a.player_id = b.player_id and
            a.team_id = b.team_id and
            a.last_played_season = b.season
    where b.AB >= 100
)

select * from ab_filter