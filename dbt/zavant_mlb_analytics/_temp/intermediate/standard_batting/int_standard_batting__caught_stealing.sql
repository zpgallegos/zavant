with src as (
    select a.*
    from {{ ref('int_standard_batting__runners') }} as a
    where
        a.run_event_code like 'caught_stealing%'
        or a.run_event_code like 'pickoff_caught_stealing%'
),

tbl as (
    select
        a.runner_id as player_id,
        a.season,
        a.is_single_team_player as complete,
        a.team_id,
        'caught_stealing' as stat,
        count(*) as value
    from src as a
    group by 1, 2, 3, 4, 5
),

tot as (
    select
        a.runner_id as player_id,
        a.season,
        1 as complete,
        0 as team_id,
        'caught_stealing' as stat,
        count(*) as value
    from src as a
    where a.is_single_team_player = 0
    group by 1, 2, 3, 4, 5
)

select * from tbl
union
select * from tot
