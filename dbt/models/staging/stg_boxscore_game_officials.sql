with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

game_officials as (
    select * from {{ source("zavant_analytical_prod", "game_officials") }}
)

select
    -- grain
    game_officials.game_pk,
    game_officials.official_index,

    -- attributes
    game_officials.official_id,
    game_officials.official_name,
    game_officials.official_type,

    -- metadata
    game_officials.official_date,
    game_officials.projected_at,
    game_officials.projection_contract_version,
    game_officials.projection_run_id,
    game_officials.season,
    game_officials.source_revision_id
from game_officials
inner join current_revision on
    game_officials.game_pk = current_revision.game_pk
    and game_officials.source_revision_id = current_revision.source_revision_id
    and game_officials.projection_contract_version = current_revision.projection_contract_version
