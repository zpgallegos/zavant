with src as (
    select a.* 
    from {{ ref('int_standard_batting__play_info') }} a
    where a.play_result_code in(
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
), tbl as (
    select
        a.batter_id as player_id,
        a.season,
        a.is_single_team_player as complete,
        a.offense_team_id as team_id,
        a.play_result_code as stat,
        count(1) as value
    from src a
    group by 1, 2, 3, 4, 5
), tot as (
    select
        a.batter_id as player_id,
        a.season,
        1 as complete,
        0 as team_id,
        a.play_result_code as stat,
        count(1) as value
    from src a
    where a.is_single_team_player = 0
    group by 1, 2, 3, 4, 5
)

select * from tbl union
select * from tot