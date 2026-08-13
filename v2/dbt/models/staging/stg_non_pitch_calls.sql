with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

non_pitch_calls as (
    select * from {{ source("zavant_analytical_prod", "non_pitch_calls") }}
)

select
    -- grain
    non_pitch_calls.game_pk,
    non_pitch_calls.at_bat_index,
    non_pitch_calls.event_index,

    -- attributes
    non_pitch_calls.balls,
    non_pitch_calls.call_code,
    non_pitch_calls.call_description,
    non_pitch_calls.description,
    non_pitch_calls.ended_at,
    non_pitch_calls.has_review,
    non_pitch_calls.is_ball,
    non_pitch_calls.is_in_play,
    non_pitch_calls.is_out,
    non_pitch_calls.is_strike,
    non_pitch_calls.outs,
    non_pitch_calls.pitch_number,
    non_pitch_calls.play_id,
    non_pitch_calls.started_at,
    non_pitch_calls.strikes,

    -- metadata
    non_pitch_calls.official_date,
    non_pitch_calls.projected_at,
    non_pitch_calls.projection_contract_version,
    non_pitch_calls.projection_run_id,
    non_pitch_calls.season,
    non_pitch_calls.source_revision_id
from non_pitch_calls
inner join current_revision on
    non_pitch_calls.game_pk = current_revision.game_pk
    and non_pitch_calls.source_revision_id = current_revision.source_revision_id
    and non_pitch_calls.projection_contract_version = current_revision.projection_contract_version
