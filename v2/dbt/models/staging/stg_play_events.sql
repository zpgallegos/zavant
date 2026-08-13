with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

play_events as (
    select * from {{ source("zavant_analytical_prod", "play_events") }}
)

select
    -- grain
    play_events.game_pk,
    play_events.at_bat_index,
    play_events.event_index,

    -- attributes
    play_events.away_score,
    play_events.balls,
    play_events.base_number,
    play_events.batting_order,
    play_events.description,
    play_events.ended_at,
    play_events.event,
    play_events.event_code,
    play_events.event_kind,
    play_events.event_type,
    play_events.has_review,
    play_events.home_score,
    play_events.injury_type,
    play_events.is_ball,
    play_events.is_base_running_play,
    play_events.is_in_play,
    play_events.is_out,
    play_events.is_pitch,
    play_events.is_scoring_play,
    play_events.is_strike,
    play_events.is_substitution,
    play_events.outs,
    play_events.pitch_number,
    play_events.play_id,
    play_events.player_id,
    play_events.position_abbreviation,
    play_events.position_code,
    play_events.position_name,
    play_events.replaced_player_id,
    play_events.started_at,
    play_events.strikes,

    -- metadata
    play_events.official_date,
    play_events.projected_at,
    play_events.projection_contract_version,
    play_events.projection_run_id,
    play_events.season,
    play_events.source_revision_id
from play_events
inner join current_revision on
    play_events.game_pk = current_revision.game_pk
    and play_events.source_revision_id = current_revision.source_revision_id
    and play_events.projection_contract_version = current_revision.projection_contract_version
