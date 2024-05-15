select
    partition_0,
    matchup_batter_id as player_id,
    offense_team_id as team_id,
    result_eventtype as stat,
    count(1) as value
from zavant.play_info
where 
    result_eventtype in(
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
group by 1, 2, 3, 4;