with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

player_positions as (
    select * from {{ source("zavant_analytical_prod", "player_positions") }}
)

select
    -- grain
    player_positions.game_pk,
    player_positions.player_id,
    player_positions.team_id,
    player_positions.position_sequence,

    -- attributes
    player_positions.position_abbreviation,
    player_positions.position_code,
    player_positions.position_name,
    player_positions.position_type,
    player_positions.team_side,

    -- metadata
    player_positions.official_date,
    player_positions.projected_at,
    player_positions.projection_contract_version,
    player_positions.projection_run_id,
    player_positions.season,
    player_positions.source_revision_id
from player_positions
inner join current_revision on
    player_positions.game_pk = current_revision.game_pk
    and player_positions.source_revision_id = current_revision.source_revision_id
    and player_positions.projection_contract_version = current_revision.projection_contract_version
