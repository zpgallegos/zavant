with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

substitutions as (
    select * from {{ source("zavant_analytical_prod", "substitutions") }}
)

select
    -- grain
    substitutions.game_pk,
    substitutions.at_bat_index,
    substitutions.event_index,

    -- attributes
    substitutions.base_number,
    substitutions.batting_order,
    substitutions.description,
    substitutions.incoming_player_id,
    substitutions.play_id,
    substitutions.position_abbreviation,
    substitutions.position_code,
    substitutions.position_name,
    substitutions.replaced_player_id,
    substitutions.substitution_type,

    -- metadata
    substitutions.official_date,
    substitutions.projected_at,
    substitutions.projection_contract_version,
    substitutions.projection_run_id,
    substitutions.season,
    substitutions.source_revision_id
from substitutions
inner join current_revision on
    substitutions.game_pk = current_revision.game_pk
    and substitutions.source_revision_id = current_revision.source_revision_id
    and substitutions.projection_contract_version = current_revision.projection_contract_version
