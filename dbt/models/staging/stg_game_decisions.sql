with game_decisions as (
    select * from {{ source("zavant_analytical_prod", "game_decisions") }}
)

select
    -- grain
    game_decisions.source_revision_id,
    game_decisions.game_pk,
    game_decisions.decision_type,

    -- attributes
    game_decisions.player_id,
    game_decisions.player_name,

    -- metadata
    game_decisions.official_date,
    game_decisions.projected_at,
    game_decisions.projection_run_id,
    game_decisions.season
from game_decisions
