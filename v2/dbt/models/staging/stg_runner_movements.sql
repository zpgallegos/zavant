with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

runner_movements as (
    select * from {{ source("zavant_analytical_prod", "runner_movements") }}
)

select
    -- grain
    runner_movements.game_pk,
    runner_movements.at_bat_index,
    runner_movements.runner_index,

    -- attributes
    runner_movements.earned,
    runner_movements.end_base,
    runner_movements.event,
    runner_movements.event_type,
    runner_movements.is_out,
    runner_movements.is_scoring_event,
    runner_movements.movement_reason,
    runner_movements.origin_base,
    runner_movements.out_base,
    runner_movements.out_number,
    runner_movements.play_event_index,
    runner_movements.rbi,
    runner_movements.responsible_pitcher_id,
    runner_movements.runner_id,
    runner_movements.start_base,
    runner_movements.team_unearned,

    -- metadata
    runner_movements.official_date,
    runner_movements.projected_at,
    runner_movements.projection_contract_version,
    runner_movements.projection_run_id,
    runner_movements.season,
    runner_movements.source_revision_id
from runner_movements
inner join current_revision on
    runner_movements.game_pk = current_revision.game_pk
    and runner_movements.source_revision_id = current_revision.source_revision_id
    and runner_movements.projection_contract_version = current_revision.projection_contract_version
