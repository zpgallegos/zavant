with game_officials as (
    select * from {{ source("zavant_analytical_prod", "game_officials") }}
)

select
    -- grain
    game_officials.source_revision_id,
    game_officials.game_pk,
    game_officials.official_index,

    -- attributes
    game_officials.official_id,
    game_officials.official_name,
    game_officials.official_type,

    -- metadata
    game_officials.official_date,
    game_officials.projected_at,
    game_officials.projection_run_id,
    game_officials.season
from game_officials
