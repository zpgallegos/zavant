{% set fact_grain = ["season", "game_pk", "at_bat_index"] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=fact_grain,
        delete_condition="src._dbt_is_deleted",
        insert_condition="not src._dbt_is_deleted",
        on_schema_change="ignore",
        table_type="iceberg",
        format="parquet",
        partitioned_by=["season"]
    )
}}

with changed_games as (
    {{ changed_statsapi_and_savant_game_revisions() }}
),

plate_appearances as (
    select
        a.*,
        b.savant_source_revision_id
    from {{ ref("int_plate_appearances") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

at_bats as (
    select
        a.game_pk,
        a.at_bat_index
    from {{ ref("int_at_bats") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

field_error_exceptions as (
    select
        a.game_pk,
        a.at_bat_index,
        count(*) filter (where a.credit = 'f_interference') > 0 as has_interference,
        count(*) filter (where a.credit = 'f_defensive_shift_violation_error') > 0 as has_defensive_shift_violation
    from {{ ref("stg_fielding_credits") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
    where
        a.credit in (
            'f_interference',
            'f_defensive_shift_violation_error'
        )
    group by 1, 2
),

pitch_counts as (
    select
        a.game_pk,
        a.at_bat_index,
        count(*) as pitch_count
    from {{ ref("stg_pitches") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
    group by 1, 2
),

statcast_batting_events as (
    select a.*
    from {{ ref("stg_statcast_batting_events") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

classified_plate_appearances as (
    select
        a.*,
        coalesce(c.has_interference, false) as has_interference,
        coalesce(c.has_defensive_shift_violation, false) as is_defensive_shift_violation,
        b.game_pk is not null as is_at_bat,
        coalesce(d.pitch_count, 0) as pitch_count,
        e.launch_speed_angle as statcast_launch_speed_angle_code,
        coalesce(e.launch_speed_angle = 6, false) as is_barrel,
        e.estimated_ba_using_speedangle as expected_batting_average,
        e.estimated_slg_using_speedangle as expected_slugging_percentage,
        e.estimated_woba_using_speedangle as expected_woba_value,
        e.woba_denom as woba_denominator
    from plate_appearances as a
    left join at_bats as b
        on
            a.game_pk = b.game_pk
            and a.at_bat_index = b.at_bat_index
    left join field_error_exceptions as c
        on
            a.game_pk = c.game_pk
            and a.at_bat_index = c.at_bat_index
    left join pitch_counts as d
        on
            a.game_pk = d.game_pk
            and a.at_bat_index = d.at_bat_index
    left join statcast_batting_events as e
        on
            a.game_pk = e.game_pk
            -- Stats API play indexes are zero-based; Savant at-bat numbers
            -- are one-based. Pitch numbers are not stable across the sources.
            and a.at_bat_index + 1 = e.at_bat_number
),

event_flags as (
    select
        a.*,
        a.event_type in (
            'single',
            'double',
            'triple',
            'home_run'
        ) as is_hit,
        a.event_type = 'single' as is_single,
        a.event_type = 'double' as is_double,
        a.event_type = 'triple' as is_triple,
        a.event_type = 'home_run' as is_home_run,
        a.event_type in (
            'walk',
            'intent_walk'
        ) as is_walk,
        a.event_type = 'intent_walk' as is_intentional_walk,
        a.event_type = 'hit_by_pitch' as is_hit_by_pitch,
        a.event_type in (
            'strikeout',
            'strikeout_double_play'
        ) as is_strikeout,
        a.event_type in (
            'sac_bunt',
            'sac_bunt_double_play',
            'sac_fly',
            'sac_fly_double_play'
        ) as is_sacrifice,
        a.event_type in (
            'sac_bunt',
            'sac_bunt_double_play'
        ) as is_sac_bunt,
        a.event_type in (
            'sac_fly',
            'sac_fly_double_play'
        ) as is_sac_fly,
        a.event_type = 'field_error' and a.is_at_bat as is_reached_on_error,
        a.event_type in (
            'fielders_choice',
            'fielders_choice_out',
            'force_out'
        ) as is_fielders_choice,
        a.event_type = 'catcher_interf' or a.has_interference as is_interference
    from classified_plate_appearances as a
),

classified_outcomes as (
    select
        a.*,
        case
            when a.is_hit then 'hit'
            when a.is_walk then 'walk'
            when a.is_hit_by_pitch then 'hit_by_pitch'
            when a.is_sacrifice then 'sacrifice'
            when a.is_interference then 'interference'
            when a.is_defensive_shift_violation then 'defensive_shift_violation'
            when a.is_reached_on_error then 'reached_on_error'
            when a.is_fielders_choice then 'fielders_choice'
            else 'out'
        end as outcome_group,
        case a.event_type
            when 'single' then 1
            when 'double' then 2
            when 'triple' then 3
            when 'home_run' then 4
            else 0
        end as total_bases
    from event_flags as a
),

sequenced_plate_appearances as (
    select
        a.*,
        row_number() over (
            partition by a.game_pk
            order by a.at_bat_index
        ) as plate_appearance_number,
        row_number() over (
            partition by
                a.game_pk,
                a.offense_team_id
            order by a.at_bat_index
        ) as team_plate_appearance_number,
        row_number() over (
            partition by
                a.game_pk,
                a.batter_id
            order by a.at_bat_index
        ) as result_batter_plate_appearance_number
    from classified_outcomes as a
),

final as (
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
        case when is_top_inning then away_score_before else home_score_before end as offense_score_before,
        case when is_top_inning then away_score else home_score end as offense_score_after,
        case when is_top_inning then home_score_before else away_score_before end as defense_score_before,
        case when is_top_inning then home_score else away_score end as defense_score_after,
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
        outcome_group,
        is_at_bat,
        is_hit,
        is_single,
        is_double,
        is_triple,
        is_home_run,
        is_walk,
        is_intentional_walk,
        is_hit_by_pitch,
        is_strikeout,
        is_sacrifice,
        is_sac_bunt,
        is_sac_fly,
        is_reached_on_error,
        is_fielders_choice,
        is_interference,
        is_defensive_shift_violation,
        is_out,
        is_scoring_play,
        has_review,
        has_out,

        -- Savant outcome values
        statcast_launch_speed_angle_code,
        is_barrel,
        expected_batting_average,
        expected_slugging_percentage,
        expected_woba_value,
        woba_denominator,

        -- additive indicators and measures
        1 as plate_appearance_ind,
        if(is_at_bat, 1, 0) as at_bat_ind,
        if(is_hit, 1, 0) as hit_ind,
        if(is_single, 1, 0) as single_ind,
        if(is_double, 1, 0) as double_ind,
        if(is_triple, 1, 0) as triple_ind,
        if(is_home_run, 1, 0) as home_run_ind,
        total_bases,
        if(is_walk, 1, 0) as walk_ind,
        if(is_intentional_walk, 1, 0) as intentional_walk_ind,
        if(is_hit_by_pitch, 1, 0) as hit_by_pitch_ind,
        if(is_strikeout, 1, 0) as strikeout_ind,
        if(is_sacrifice, 1, 0) as sacrifice_ind,
        if(is_sac_bunt, 1, 0) as sac_bunt_ind,
        if(is_sac_fly, 1, 0) as sac_fly_ind,
        if(is_reached_on_error, 1, 0) as reached_on_error_ind,
        if(is_barrel, 1, 0) as barrel_ind,
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
        statsapi_source_revision_id,
        savant_source_revision_id,
        official_date,
        season
    from sequenced_plate_appearances
)

{{ correction_safe_merge_rows("final", "changed_games", fact_grain) }}
