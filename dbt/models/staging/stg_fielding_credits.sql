with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

fielding_credits as (
    select * from {{ source("zavant_analytical_prod", "fielding_credits") }}
)

select
    -- grain
    fielding_credits.game_pk,
    fielding_credits.at_bat_index,
    fielding_credits.runner_index,
    fielding_credits.credit_index,

    -- attributes
    fielding_credits.credit,
    fielding_credits.play_event_index,
    fielding_credits.player_id,
    fielding_credits.position_abbreviation,
    fielding_credits.position_code,
    fielding_credits.position_name,
    fielding_credits.position_type,

    -- metadata
    fielding_credits.official_date,
    fielding_credits.projected_at,
    fielding_credits.projection_contract_version,
    fielding_credits.projection_run_id,
    fielding_credits.season,
    fielding_credits.source_revision_id
from fielding_credits
inner join current_revision on
    fielding_credits.game_pk = current_revision.game_pk
    and fielding_credits.source_revision_id = current_revision.source_revision_id
    and fielding_credits.projection_contract_version = current_revision.projection_contract_version
