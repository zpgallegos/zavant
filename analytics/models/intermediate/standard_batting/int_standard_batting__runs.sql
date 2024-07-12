with src as (
    select * from {{ ref('int_standard_batting__runners') }}
)

select
    a.season,
    a.runner_id as player_id,
    a.team_id,
    'runs' as stat,
    count(1) as value
from src a
where a.movement_end = 'score'
group by 1, 2, 3, 4