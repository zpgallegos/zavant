select
    runner_movement.game_pk,
    runner_movement.at_bat_index,
    runner_movement.runner_index
from {{ ref("stg_runner_movements") }} as runner_movement
left join {{ ref("stg_plays") }} as play
    on
        runner_movement.game_pk = play.game_pk
        and runner_movement.at_bat_index = play.at_bat_index
where play.game_pk is null
