with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

game_decisions as (
    select * from {{ source("zavant_analytical_prod", "game_decisions") }}
)

select
    -- grain
    game_decisions.game_pk,
    game_decisions.decision_type,

    -- attributes
    game_decisions.player_id,
    game_decisions.player_name,

    -- metadata
    game_decisions.official_date,
    game_decisions.projected_at,
    game_decisions.projection_contract_version,
    game_decisions.projection_run_id,
    game_decisions.season,
    game_decisions.source_revision_id
from game_decisions
inner join current_revision on
    game_decisions.game_pk = current_revision.game_pk
    and game_decisions.source_revision_id = current_revision.source_revision_id
    and game_decisions.projection_contract_version = current_revision.projection_contract_version
