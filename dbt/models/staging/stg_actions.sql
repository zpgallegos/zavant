with actions as (
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
    actions.season
from actions
