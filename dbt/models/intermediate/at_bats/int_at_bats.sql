with non_at_bat_field_errors as (
    -- identify fielding-credit signals that can mark a field_error as an award of
    -- first base for interference or a defensive-shift violation. Interference
    -- credits also occur on other outcomes, so only field_error plays are excluded
    -- in the final filter.

    select distinct
        game_pk,
        at_bat_index
    from {{ ref("stg_fielding_credits") }}
    where
        credit in (
            'f_defensive_shift_violation_error',
            'f_interference'
        )
),

plate_appearances as (
    select * from {{ ref("int_plate_appearances") }}
)

select
    -- grain
    plate_appearances.game_pk,
    plate_appearances.at_bat_index,

    -- attributes
    plate_appearances.away_score,
    plate_appearances.balls,
    plate_appearances.bat_side_code,
    plate_appearances.batter_id,
    plate_appearances.batter_split,
    plate_appearances.captivating_index,
    plate_appearances.defense_team_id,
    plate_appearances.description,
    plate_appearances.ended_at,
    plate_appearances.event,
    plate_appearances.event_type,
    plate_appearances.half_inning,
    plate_appearances.has_out,
    plate_appearances.has_review,
    plate_appearances.home_score,
    plate_appearances.inning,
    plate_appearances.is_complete,
    plate_appearances.is_out,
    plate_appearances.is_scoring_play,
    plate_appearances.is_top_inning,
    plate_appearances.men_on_base_split,
    plate_appearances.offense_team_id,
    plate_appearances.outs,
    plate_appearances.pitch_hand_code,
    plate_appearances.pitcher_id,
    plate_appearances.pitcher_split,
    plate_appearances.play_type,
    plate_appearances.post_on_first_id,
    plate_appearances.post_on_second_id,
    plate_appearances.post_on_third_id,
    plate_appearances.rbi,
    plate_appearances.started_at,
    plate_appearances.strikes,

    -- metadata
    plate_appearances.official_date,
    plate_appearances.season
from plate_appearances
left join non_at_bat_field_errors
    on
        plate_appearances.game_pk = non_at_bat_field_errors.game_pk
        and plate_appearances.at_bat_index = non_at_bat_field_errors.at_bat_index
where
    plate_appearances.event_type not in (
        -- further subset of play event outcomes that qualify as plate
        -- appearances but are not at-bats
        'catcher_interf',
        'hit_by_pitch',
        'intent_walk',
        'sac_bunt',
        'sac_bunt_double_play',
        'sac_fly',
        'sac_fly_double_play',
        'walk'
    )
    and not (
        plate_appearances.event_type = 'field_error'
        and non_at_bat_field_errors.game_pk is not null
    )
