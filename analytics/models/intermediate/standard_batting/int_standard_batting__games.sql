select
    partition_0,
    person_id as player_id,
    team_id,
    'games_played' as stat,
    count(distinct game_pk) as value
from zavant.d_game_boxscore
where gamestatus_isonbench = false
group by 1, 2, 3, 4;