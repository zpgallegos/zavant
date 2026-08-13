with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

actions as (
    select * from {{ source("zavant_analytical_prod", "actions") }}
)

select
    -- grain
    actions.game_pk,
    actions.at_bat_index,
    actions.event_index,

    -- attributes
    actions.action_play_id,
    actions.balls,
    actions.base_number,
    actions.batting_order,
    actions.description,
    actions.disengagement_number,
    actions.ended_at,
    actions.event,
    actions.event_code,
    actions.event_type,
    actions.has_review,
    actions.is_out,
    actions.is_substitution,
    actions.outs,
    actions.play_id,
    actions.player_id,
    actions.position_abbreviation,
    actions.position_code,
    actions.position_name,
    actions.replaced_player_id,
    actions.started_at,
    actions.strikes,
    actions.umpire_id,

    -- metadata
    actions.official_date,
    actions.projected_at,
    actions.projection_contract_version,
    actions.projection_run_id,
    actions.season,
    actions.source_revision_id
from actions
inner join current_revision on
    actions.game_pk = current_revision.game_pk
    and actions.source_revision_id = current_revision.source_revision_id
    and actions.projection_contract_version = current_revision.projection_contract_version
