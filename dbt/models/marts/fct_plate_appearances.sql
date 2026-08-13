{{
    config(
        materialized="table",
        table_type="iceberg",
        format="parquet",
        partitioned_by=["season"]
    )
}}

with source_plays as (
    select * from {{ ref("stg_plays") }}
),

plays_with_prior_state as (
    select
        source_plays.*,
        coalesce(
            lag(source_plays.away_score) over (
                partition by
                    source_plays.game_pk
                order by source_plays.at_bat_index
            ),
            0
        ) as away_score_before,
        coalesce(
            lag(source_plays.home_score) over (
                partition by
                    source_plays.game_pk
                order by source_plays.at_bat_index
            ),
            0
        ) as home_score_before,
        coalesce(
            lag(source_plays.outs) over (
                partition by
                    source_plays.game_pk,
                    source_plays.inning,
                    source_plays.half_inning
                order by source_plays.at_bat_index
            ),
            0
        ) as outs_before,
        lag(source_plays.post_on_first_id) over (
            partition by
                source_plays.game_pk,
                source_plays.inning,
                source_plays.half_inning
            order by source_plays.at_bat_index
        ) as runner_on_first_id_before,
        lag(source_plays.post_on_second_id) over (
            partition by
                source_plays.game_pk,
                source_plays.inning,
                source_plays.half_inning
            order by source_plays.at_bat_index
        ) as runner_on_second_id_before,
        lag(source_plays.post_on_third_id) over (
            partition by
                source_plays.game_pk,
                source_plays.inning,
                source_plays.half_inning
            order by source_plays.at_bat_index
        ) as runner_on_third_id_before
    from source_plays
),

plate_appearances as (
    select plays_with_prior_state.*
    from plays_with_prior_state
    inner join {{ ref("int_plate_appearances") }} as qualifying_plate_appearances
        on
            plays_with_prior_state.game_pk = qualifying_plate_appearances.game_pk
            and plays_with_prior_state.at_bat_index
            = qualifying_plate_appearances.at_bat_index
),

field_error_exceptions as (
    select
        fielding_credits.game_pk,
        fielding_credits.at_bat_index,
        count_if(fielding_credits.credit = 'f_interference') > 0
            as has_interference,
        count_if(
            fielding_credits.credit = 'f_defensive_shift_violation_error'
        ) > 0 as has_defensive_shift_violation
    from {{ ref("stg_fielding_credits") }} as fielding_credits
    where
        fielding_credits.credit in (
            'f_defensive_shift_violation_error',
            'f_interference'
        )
    group by 1, 2
),

pitch_counts as (
    select
        pitches.game_pk,
        pitches.at_bat_index,
        count(*) as pitch_count
    from {{ ref("stg_pitches") }} as pitches
    group by 1, 2
),

classified_plate_appearances as (
    select
        plate_appearances.*,
        coalesce(field_error_exceptions.has_interference, false) as has_interference,
        coalesce(
            field_error_exceptions.has_defensive_shift_violation,
            false
        ) as has_defensive_shift_violation,
        plate_appearances.event_type not in (
            'catcher_interf',
            'hit_by_pitch',
            'intent_walk',
            'sac_bunt',
            'sac_bunt_double_play',
            'sac_fly',
            'sac_fly_double_play',
            'walk'
        )
        and not coalesce(
            plate_appearances.event_type = 'field_error'
            and (
                coalesce(field_error_exceptions.has_interference, false)
                or coalesce(
                    field_error_exceptions.has_defensive_shift_violation,
                    false
                )
            ),
            false
        ) as is_at_bat,
        coalesce(pitch_counts.pitch_count, 0) as pitch_count
    from plate_appearances
    left join field_error_exceptions
        on
            plate_appearances.game_pk = field_error_exceptions.game_pk
            and plate_appearances.at_bat_index = field_error_exceptions.at_bat_index
    left join pitch_counts
        on
            plate_appearances.game_pk = pitch_counts.game_pk
            and plate_appearances.at_bat_index = pitch_counts.at_bat_index
),

sequenced_plate_appearances as (
    select
        classified_plate_appearances.*,
        row_number() over (
            partition by
                classified_plate_appearances.game_pk
            order by classified_plate_appearances.at_bat_index
        ) as plate_appearance_number,
        row_number() over (
            partition by
                classified_plate_appearances.game_pk,
                classified_plate_appearances.offense_team_id
            order by classified_plate_appearances.at_bat_index
        ) as team_plate_appearance_number,
        row_number() over (
            partition by
                classified_plate_appearances.game_pk,
                classified_plate_appearances.batter_id
            order by classified_plate_appearances.at_bat_index
        ) as result_batter_plate_appearance_number
    from classified_plate_appearances
)

select
    -- keys and grain
    {{ dbt_utils.generate_surrogate_key([
        "game_pk",
        "at_bat_index"
    ]) }} as plate_appearance_key,
    game_pk,
    at_bat_index,

    -- participants
    batter_id as result_batter_id,
    pitcher_id as result_pitcher_id,
    offense_team_id,
    defense_team_id,

    -- sequence and game state
    plate_appearance_number,
    team_plate_appearance_number,
    result_batter_plate_appearance_number,
    inning,
    half_inning,
    is_top_inning,
    outs_before,
    outs as outs_after,
    greatest(outs - outs_before, 0) as outs_recorded,
    away_score_before,
    away_score as away_score_after,
    home_score_before,
    home_score as home_score_after,
    case when is_top_inning then away_score_before else home_score_before end
        as offense_score_before,
    case when is_top_inning then away_score else home_score end
        as offense_score_after,
    case when is_top_inning then home_score_before else away_score_before end
        as defense_score_before,
    case when is_top_inning then home_score else away_score end
        as defense_score_after,
    case
        when is_top_inning then away_score_before - home_score_before
        else home_score_before - away_score_before
    end as offense_score_differential_before,
    case
        when is_top_inning then away_score - home_score
        else home_score - away_score
    end as offense_score_differential_after,
    runner_on_first_id_before,
    runner_on_second_id_before,
    runner_on_third_id_before,
    post_on_first_id as runner_on_first_id_after,
    post_on_second_id as runner_on_second_id_after,
    post_on_third_id as runner_on_third_id_after,
    concat(
        if(runner_on_first_id_before is null, '0', '1'),
        if(runner_on_second_id_before is null, '0', '1'),
        if(runner_on_third_id_before is null, '0', '1')
    ) as base_state_before,
    concat(
        if(post_on_first_id is null, '0', '1'),
        if(post_on_second_id is null, '0', '1'),
        if(post_on_third_id is null, '0', '1')
    ) as base_state_after,

    -- outcome
    event,
    event_type,
    description,
    case
        when event_type in ('single', 'double', 'triple', 'home_run') then 'hit'
        when event_type in ('walk', 'intent_walk') then 'walk'
        when event_type = 'hit_by_pitch' then 'hit_by_pitch'
        when event_type in (
            'sac_bunt',
            'sac_bunt_double_play',
            'sac_fly',
            'sac_fly_double_play'
        ) then 'sacrifice'
        when event_type = 'catcher_interf' or has_interference then 'interference'
        when has_defensive_shift_violation then 'defensive_shift_violation'
        when event_type = 'field_error' then 'reached_on_error'
        when event_type in (
            'fielders_choice',
            'fielders_choice_out',
            'force_out'
        ) then 'fielders_choice'
        else 'out'
    end as outcome_group,
    is_at_bat,
    event_type in ('single', 'double', 'triple', 'home_run') as is_hit,
    event_type = 'single' as is_single,
    event_type = 'double' as is_double,
    event_type = 'triple' as is_triple,
    event_type = 'home_run' as is_home_run,
    event_type in ('walk', 'intent_walk') as is_walk,
    event_type = 'intent_walk' as is_intentional_walk,
    event_type = 'hit_by_pitch' as is_hit_by_pitch,
    event_type in ('strikeout', 'strikeout_double_play') as is_strikeout,
    event_type in (
        'sac_bunt',
        'sac_bunt_double_play',
        'sac_fly',
        'sac_fly_double_play'
    ) as is_sacrifice,
    event_type in ('sac_bunt', 'sac_bunt_double_play') as is_sac_bunt,
    event_type in ('sac_fly', 'sac_fly_double_play') as is_sac_fly,
    event_type = 'field_error' and is_at_bat as is_reached_on_error,
    event_type in (
        'fielders_choice',
        'fielders_choice_out',
        'force_out'
    ) as is_fielders_choice,
    event_type = 'catcher_interf' or has_interference as is_interference,
    has_defensive_shift_violation as is_defensive_shift_violation,
    is_out,
    is_scoring_play,
    has_review,
    has_out,

    -- additive measures
    1 as plate_appearance_count,
    if(is_at_bat, 1, 0) as at_bat_count,
    if(event_type in ('single', 'double', 'triple', 'home_run'), 1, 0)
        as hit_count,
    if(event_type = 'single', 1, 0) as single_count,
    if(event_type = 'double', 1, 0) as double_count,
    if(event_type = 'triple', 1, 0) as triple_count,
    if(event_type = 'home_run', 1, 0) as home_run_count,
    case event_type
        when 'single' then 1
        when 'double' then 2
        when 'triple' then 3
        when 'home_run' then 4
        else 0
    end as total_bases,
    if(event_type in ('walk', 'intent_walk'), 1, 0) as walk_count,
    if(event_type = 'intent_walk', 1, 0) as intentional_walk_count,
    if(event_type = 'hit_by_pitch', 1, 0) as hit_by_pitch_count,
    if(event_type in ('strikeout', 'strikeout_double_play'), 1, 0)
        as strikeout_count,
    if(
        event_type in (
            'sac_bunt',
            'sac_bunt_double_play',
            'sac_fly',
            'sac_fly_double_play'
        ),
        1,
        0
    ) as sacrifice_count,
    if(event_type in ('sac_bunt', 'sac_bunt_double_play'), 1, 0)
        as sac_bunt_count,
    if(event_type in ('sac_fly', 'sac_fly_double_play'), 1, 0)
        as sac_fly_count,
    if(event_type = 'field_error' and is_at_bat, 1, 0)
        as reached_on_error_count,
    rbi,
    case
        when is_top_inning then away_score - away_score_before
        else home_score - home_score_before
    end as runs_scored_during_plate_appearance,
    pitch_count,

    -- matchup and source attributes
    balls,
    strikes,
    bat_side_code,
    pitch_hand_code,
    batter_split,
    pitcher_split,
    men_on_base_split,
    play_type,
    started_at,
    ended_at,
    is_complete,
    captivating_index,

    -- metadata
    official_date,
    season
from sequenced_plate_appearances
