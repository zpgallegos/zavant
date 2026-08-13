with player_positions as (
    select * from {{ source("zavant_analytical_prod", "player_positions") }}
)

select
    -- grain
    player_positions.source_revision_id,
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
    player_positions.projection_run_id,
    player_positions.season
from player_positions
