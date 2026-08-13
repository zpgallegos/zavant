select
    fielding_credit.game_pk,
    fielding_credit.at_bat_index,
    fielding_credit.runner_index,
    fielding_credit.credit_index,
    fielding_credit.source_revision_id,
    fielding_credit.projection_contract_version
from {{ ref("stg_fielding_credits") }} as fielding_credit
left join {{ ref("stg_runner_movements") }} as runner_movement
    on
        fielding_credit.game_pk = runner_movement.game_pk
        and fielding_credit.at_bat_index = runner_movement.at_bat_index
        and fielding_credit.runner_index = runner_movement.runner_index
        and fielding_credit.source_revision_id = runner_movement.source_revision_id
        and fielding_credit.projection_contract_version = runner_movement.projection_contract_version
where runner_movement.game_pk is null
