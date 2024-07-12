with src as (
    select * from {{ ref('stg_statsapi__play_info') }}
)

select
    a.season,
    a.batter_id as player_id,
    a.offense_team_id as team_id,
    a.event_code as stat,
    count(1) as value
from src a
where
    a.event_code in(
        'walk',
        'single',
        'double',
        'triple',
        'home_run',
        'intent_walk',
        'hit_by_pitch',
        'sac_bunt',
        'sac_fly',
        'catcher_interf',
        'grounded_into_double_play'
    )
group by 1, 2, 3, 4