with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

disengagements as (
    select * from {{ source("zavant_analytical_prod", "disengagements") }}
)

select
    -- grain
    disengagements.game_pk,
    disengagements.at_bat_index,
    disengagements.event_index,

    -- attributes
    disengagements.action_play_id,
    disengagements.balls,
    disengagements.description,
    disengagements.disengagement_number,
    disengagements.event,
    disengagements.event_code,
    disengagements.event_kind,
    disengagements.event_type,
    disengagements.from_catcher,
    disengagements.has_review,
    disengagements.is_out,
    disengagements.outs,
    disengagements.play_id,
    disengagements.strikes,

    -- metadata
    disengagements.official_date,
    disengagements.projected_at,
    disengagements.projection_contract_version,
    disengagements.projection_run_id,
    disengagements.season,
    disengagements.source_revision_id
from disengagements
inner join current_revision on
    disengagements.game_pk = current_revision.game_pk
    and disengagements.source_revision_id = current_revision.source_revision_id
    and disengagements.projection_contract_version = current_revision.projection_contract_version
