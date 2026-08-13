with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

batted_balls as (
    select * from {{ source("zavant_analytical_prod", "batted_balls") }}
)

select
    -- grain
    batted_balls.game_pk,
    batted_balls.at_bat_index,
    batted_balls.event_index,

    -- attributes
    batted_balls.coordinate_x,
    batted_balls.coordinate_y,
    batted_balls.hardness,
    batted_balls.launch_angle,
    batted_balls.launch_speed,
    batted_balls.location,
    batted_balls.pitch_number,
    batted_balls.play_id,
    batted_balls.total_distance,
    batted_balls.trajectory,

    -- metadata
    batted_balls.official_date,
    batted_balls.projected_at,
    batted_balls.projection_contract_version,
    batted_balls.projection_run_id,
    batted_balls.season,
    batted_balls.source_revision_id
from batted_balls
inner join current_revision on
    batted_balls.game_pk = current_revision.game_pk
    and batted_balls.source_revision_id = current_revision.source_revision_id
    and batted_balls.projection_contract_version = current_revision.projection_contract_version
