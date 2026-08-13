with disengagements as (
    select * from {{ source("zavant_analytical_prod", "disengagements") }}
)

select
    -- grain
    disengagements.source_revision_id,
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
    disengagements.projection_run_id,
    disengagements.season
from disengagements
