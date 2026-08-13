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

plays as (
    select * from {{ ref("stg_plays") }}
)

select
    -- grain
    plays.source_revision_id,
    plays.game_pk,
    plays.at_bat_index,

    -- attributes
    plays.away_score,
    plays.balls,
    plays.bat_side_code,
    plays.batter_id,
    plays.batter_split,
    plays.captivating_index,
    plays.defense_team_id,
    plays.description,
    plays.ended_at,
    plays.event,
    plays.event_type,
    plays.half_inning,
    plays.has_out,
    plays.has_review,
    plays.home_score,
    plays.inning,
    plays.is_complete,
    plays.is_out,
    plays.is_scoring_play,
    plays.is_top_inning,
    plays.men_on_base_split,
    plays.offense_team_id,
    plays.outs,
    plays.pitch_hand_code,
    plays.pitcher_id,
    plays.pitcher_split,
    plays.play_type,
    plays.post_on_first_id,
    plays.post_on_second_id,
    plays.post_on_third_id,
    plays.rbi,
    plays.started_at,
    plays.strikes,

    -- metadata
    plays.official_date,
    plays.projected_at,
    plays.projection_run_id,
    plays.season
from plays
inner join qualifying_event_types
    on plays.event_type = qualifying_event_types.event_type
