-- subset of play outcome types that qualify as a plate appearance
with qualifying_event_types (event_type) as (
    values
    ('catcher_interf'),
    ('double'),
    ('double_play'),
    ('field_error'),
    ('field_out'),
    ('fielders_choice'),
    ('fielders_choice_out'),
    ('force_out'),
    ('grounded_into_double_play'),
    ('hit_by_pitch'),
    ('home_run'),
    ('intent_walk'),
    ('sac_bunt'),
    ('sac_bunt_double_play'),
    ('sac_fly'),
    ('sac_fly_double_play'),
    ('single'),
    ('strikeout'),
    ('strikeout_double_play'),
    ('triple'),
    ('triple_play'),
    ('walk')
),

games as (
    select
        game_pk,
        source_revision_id
    from {{ ref("stg_games") }}
),

plays as (
    select
        a.*,
        b.source_revision_id
    from {{ ref("stg_plays") }} as a
    inner join games as b on a.game_pk = b.game_pk
),

plays_with_prior_state as (
    select
        plays.*,
        coalesce(lag(plays.away_score) over game_window, 0) as away_score_before,
        coalesce(lag(plays.home_score) over game_window, 0) as home_score_before,
        coalesce(lag(plays.outs) over half_inning_window, 0) as outs_before,
        lag(plays.post_on_first_id) over half_inning_window as runner_on_first_id_before,
        lag(plays.post_on_second_id) over half_inning_window as runner_on_second_id_before,
        lag(plays.post_on_third_id) over half_inning_window as runner_on_third_id_before
    from plays
    window
        game_window as (partition by plays.game_pk order by plays.at_bat_index),
        half_inning_window as (
            partition by
                plays.game_pk,
                plays.inning,
                plays.half_inning
            order by plays.at_bat_index
        )
)

select
    -- grain
    plays_with_prior_state.game_pk,
    plays_with_prior_state.at_bat_index,

    -- attributes
    plays_with_prior_state.away_score,
    plays_with_prior_state.away_score_before,
    plays_with_prior_state.balls,
    plays_with_prior_state.bat_side_code,
    plays_with_prior_state.batter_id,
    plays_with_prior_state.batter_split,
    plays_with_prior_state.captivating_index,
    plays_with_prior_state.defense_team_id,
    plays_with_prior_state.description,
    plays_with_prior_state.ended_at,
    plays_with_prior_state.event,
    plays_with_prior_state.event_type,
    plays_with_prior_state.half_inning,
    plays_with_prior_state.has_out,
    plays_with_prior_state.has_review,
    plays_with_prior_state.home_score,
    plays_with_prior_state.home_score_before,
    plays_with_prior_state.inning,
    plays_with_prior_state.is_complete,
    plays_with_prior_state.is_out,
    plays_with_prior_state.is_scoring_play,
    plays_with_prior_state.is_top_inning,
    plays_with_prior_state.men_on_base_split,
    plays_with_prior_state.offense_team_id,
    plays_with_prior_state.outs,
    plays_with_prior_state.outs_before,
    plays_with_prior_state.pitch_hand_code,
    plays_with_prior_state.pitcher_id,
    plays_with_prior_state.pitcher_split,
    plays_with_prior_state.play_type,
    plays_with_prior_state.post_on_first_id,
    plays_with_prior_state.post_on_second_id,
    plays_with_prior_state.post_on_third_id,
    plays_with_prior_state.rbi,
    plays_with_prior_state.runner_on_first_id_before,
    plays_with_prior_state.runner_on_second_id_before,
    plays_with_prior_state.runner_on_third_id_before,
    plays_with_prior_state.started_at,
    plays_with_prior_state.strikes,

    -- metadata
    plays_with_prior_state.source_revision_id,
    plays_with_prior_state.official_date,
    plays_with_prior_state.season
from plays_with_prior_state
inner join qualifying_event_types on plays_with_prior_state.event_type = qualifying_event_types.event_type
