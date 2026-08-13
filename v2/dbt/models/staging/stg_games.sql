with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

games as (
    select * from {{ source("zavant_analytical_prod", "games") }}
)

select
    -- grain
    games.game_pk,

    -- attributes
    games.abs_has_challenges,
    games.abstract_game_state,
    games.attendance,
    games.away_abs_challenges_failed,
    games.away_abs_challenges_remaining,
    games.away_abs_challenges_successful,
    games.away_mound_visits_remaining,
    games.away_mound_visits_used,
    games.away_replay_challenges_remaining,
    games.away_replay_challenges_used,
    games.away_score,
    games.away_team_id,
    games.away_team_no_hitter,
    games.away_team_perfect_game,
    games.calendar_event_id,
    games.coded_game_state,
    games.day_night,
    games.delay_duration_minutes,
    games.detailed_state,
    games.double_header,
    games.first_pitch_at,
    games.game_duration_minutes,
    games.game_id,
    games.game_number,
    games.game_type,
    games.gameday_type,
    games.home_abs_challenges_failed,
    games.home_abs_challenges_remaining,
    games.home_abs_challenges_successful,
    games.home_mound_visits_remaining,
    games.home_mound_visits_used,
    games.home_replay_challenges_remaining,
    games.home_replay_challenges_used,
    games.home_score,
    games.home_team_id,
    games.home_team_no_hitter,
    games.home_team_perfect_game,
    games.no_hitter,
    games.original_date,
    games.perfect_game,
    games.replay_has_challenges,
    games.resume_date,
    games.resumed_from_date,
    games.scheduled_innings,
    games.scheduled_start_at,
    games.start_time_tbd,
    games.status_code,
    games.temperature_fahrenheit,
    games.tiebreaker,
    games.venue_id,
    games.venue_name,
    games.weather_condition,
    games.wind,

    -- metadata
    games.feed_timecode,
    games.official_date,
    games.projected_at,
    games.projection_contract_version,
    games.projection_run_id,
    games.raw_object_uri,
    games.season,
    games.source_observed_at,
    games.source_revision_id,
    games.source_uri
from games
inner join current_revision on
    games.game_pk = current_revision.game_pk
    and games.source_revision_id = current_revision.source_revision_id
    and games.projection_contract_version = current_revision.projection_contract_version
