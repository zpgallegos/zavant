with src as (
    select a.* 
    from {{ ref('int_standard_batting__play_info') }} a
    where a.event_code like '%strikeout%'
),
tbl as (
    select
        a.batter_id as player_id,
        a.season,
        a.is_single_team_player as complete,
        a.offense_team_id as team_id,
        'strikeouts' as stat,
        count(1) as value
    from src a
    group by 1, 2, 3, 4, 5
),
tot as (
    select
        a.batter_id as player_id,
        a.season,
        1 as complete,
        0 as team_id,
        'strikeouts' as stat,
        count(1) as value
    from src a
    where a.is_single_team_player = 0
    group by 1, 2, 3, 4, 5
)

select * from tbl union
select * from tot
