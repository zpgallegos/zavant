with runner_movements as (
    select * from {{ source("zavant_analytical_prod", "runner_movements") }}
)

select
    -- grain
    runner_movements.source_revision_id,
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
    runner_movements.projection_run_id,
    runner_movements.season
from runner_movements
