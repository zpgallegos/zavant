with src as (
    select * from {{ ref('int_standard_batting__runners') }}
)

select
    a.season,
    a.runner_id as player_id,
    a.team_id,
    'caught_stealing' as stat,
    count(1) as value
from src a
where 
    a.run_event_code like 'caught_stealing%' or
    a.run_event_code like 'pickoff_caught_stealing%'
group by 1, 2, 3, 4