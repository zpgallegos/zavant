select
    fielding_credit.game_pk,
    fielding_credit.at_bat_index,
    fielding_credit.runner_index,
    fielding_credit.credit_index
from {{ ref("stg_fielding_credits") }} as fielding_credit
left join {{ ref("stg_runner_movements") }} as runner_movement
    on
        fielding_credit.game_pk = runner_movement.game_pk
        and fielding_credit.at_bat_index = runner_movement.at_bat_index
        and fielding_credit.runner_index = runner_movement.runner_index
where runner_movement.game_pk is null
