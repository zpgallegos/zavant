select
    runner_movement.game_pk,
    runner_movement.at_bat_index,
    runner_movement.runner_index,
    runner_movement.source_revision_id,
    runner_movement.projection_contract_version
from {{ ref("stg_runner_movements") }} as runner_movement
left join {{ ref("stg_plays") }} as play
    on
        runner_movement.game_pk = play.game_pk
        and runner_movement.at_bat_index = play.at_bat_index
        and runner_movement.source_revision_id = play.source_revision_id
        and runner_movement.projection_contract_version = play.projection_contract_version
where play.game_pk is null
