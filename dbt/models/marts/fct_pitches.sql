{% set fact_grain = ["season", "game_pk", "at_bat_index", "event_index"] %}

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
    {{ changed_game_revisions() }}
),

pitches as (
    select
        a.*,
        b.source_revision_id
    from {{ ref("stg_pitches") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

play_events as (
    select a.*
    from {{ ref("stg_play_events") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

events_with_prior_count as (
    select
        game_pk,
        at_bat_index,
        event_index,
        coalesce(lag(balls) over event_window, 0) as balls_before,
        coalesce(lag(strikes) over event_window, 0) as strikes_before,
        coalesce(lag(outs) over event_window, 0) as outs_before
    from play_events
    window event_window as (
        partition by game_pk, at_bat_index
        order by event_index
    )
),

plays as (
    select a.*
    from {{ ref("stg_plays") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

games as (
    select
        a.game_pk,
        a.game_type
    from {{ ref("stg_games") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

enriched_pitches as (
    select
        a.*,
        b.batter_id,
        b.pitcher_id,
        b.offense_team_id,
        b.defense_team_id,
        b.inning,
        b.half_inning,
        b.is_top_inning,
        b.event as play_result,
        b.event_type as play_result_type,
        b.description as play_result_description,
        b.bat_side_code,
        b.pitch_hand_code,
        b.batter_split,
        b.pitcher_split,
        b.men_on_base_split,
        c.balls_before,
        c.strikes_before,
        c.outs_before,
        d.game_type
    from pitches as a
    inner join plays as b
        on
            a.game_pk = b.game_pk
            and a.at_bat_index = b.at_bat_index
    inner join events_with_prior_count as c
        on
            a.game_pk = c.game_pk
            and a.at_bat_index = c.at_bat_index
            and a.event_index = c.event_index
    inner join games as d on a.game_pk = d.game_pk
),

classified_pitches as (
    select
        enriched_pitches.*,
        case
            when enriched_pitches.pitch_type_code in ('FA', 'FC', 'FF', 'SI')
                then 'fastball'
            when enriched_pitches.pitch_type_code in ('CH', 'FO', 'FS', 'SC')
                then 'offspeed'
            when enriched_pitches.pitch_type_code in (
                'CS',
                'CU',
                'EP',
                'KC',
                'KN',
                'SL',
                'ST',
                'SV',
                'UN'
            ) then 'breaking'
            else 'other'
        end as pitch_type_group,
        case enriched_pitches.pitch_hand_code
            when 'L' then 'left'
            when 'R' then 'right'
            else 'unknown'
        end as pitcher_hand,
        concat(
            cast(enriched_pitches.balls_before as varchar),
            '-',
            cast(enriched_pitches.strikes_before as varchar)
        ) as count_before,
        case
            when
                (
                    enriched_pitches.balls_before = 1
                    and enriched_pitches.strikes_before = 0
                )
                or (
                    enriched_pitches.balls_before = 2
                    and enriched_pitches.strikes_before in (0, 1)
                )
                or (
                    enriched_pitches.balls_before = 3
                    and enriched_pitches.strikes_before in (0, 1)
                )
                then 'batter_ahead'
            when
                (
                    enriched_pitches.balls_before = 0
                    and enriched_pitches.strikes_before in (1, 2)
                )
                or (
                    enriched_pitches.balls_before = 1
                    and enriched_pitches.strikes_before = 2
                )
                then 'batter_behind'
            else 'even'
        end as count_leverage,
        case enriched_pitches.game_type
            when 'R' then 'regular_season'
            when 'S' then 'spring_training'
            when 'A' then 'all_star'
            when 'D' then 'postseason'
            when 'F' then 'postseason'
            when 'L' then 'postseason'
            when 'W' then 'postseason'
            else 'other'
        end as season_phase,
        enriched_pitches.balls_before = 0
        and enriched_pitches.strikes_before = 0 as is_first_pitch_count,
        enriched_pitches.strikes_before = 2 as is_two_strike_count,
        enriched_pitches.balls_before = 3 as is_three_ball_count,
        enriched_pitches.balls_before = 3
        and enriched_pitches.strikes_before = 2 as is_full_count
    from enriched_pitches
),

final as (
    select
        -- keys and grain
        {{ dbt_utils.generate_surrogate_key([
            "game_pk",
            "at_bat_index",
            "event_index"
        ]) }} as pitch_key,
        {{ dbt_utils.generate_surrogate_key([
            "game_pk",
            "at_bat_index"
        ]) }} as play_key,
        game_pk,
        at_bat_index,
        event_index,

        -- participants
        batter_id,
        pitcher_id,
        offense_team_id,
        defense_team_id,

        -- game and matchup context
        inning,
        half_inning,
        is_top_inning,
        bat_side_code,
        pitch_hand_code,
        batter_split,
        pitcher_split,
        men_on_base_split,
        pitcher_hand,

        -- containing play result
        play_result,
        play_result_type,
        play_result_description,

        -- pitch sequence and result
        play_id,
        pitch_number,
        balls_before,
        strikes_before,
        outs_before,
        balls as balls_after,
        strikes as strikes_after,
        outs as outs_after,
        count_before,
        count_leverage,
        is_first_pitch_count,
        is_two_strike_count,
        is_three_ball_count,
        is_full_count,
        call_code,
        call_description,
        description as pitch_description,
        pitch_type_code,
        pitch_type_description,
        pitch_type_group,
        is_ball,
        is_strike,
        is_in_play,
        is_out,
        has_review,

        -- pitch tracking
        start_speed,
        end_speed,
        spin_rate,
        spin_direction,
        extension,
        plate_time,
        zone,
        strike_zone_bottom,
        strike_zone_top,
        strike_zone_depth,
        strike_zone_width,
        type_confidence,
        break_angle,
        break_length,
        break_y,
        break_horizontal,
        break_vertical,
        break_vertical_induced,
        coordinate_p_x,
        coordinate_p_z,
        coordinate_pfx_x,
        coordinate_pfx_z,
        coordinate_x,
        coordinate_y,
        coordinate_x0,
        coordinate_y0,
        coordinate_z0,
        coordinate_v_x0,
        coordinate_v_y0,
        coordinate_v_z0,
        coordinate_a_x,
        coordinate_a_y,
        coordinate_a_z,

        -- additive indicators
        1 as pitch_ind,
        if(is_ball, 1, 0) as ball_ind,
        if(is_strike, 1, 0) as strike_ind,
        if(is_in_play, 1, 0) as ball_in_play_ind,
        if(is_out, 1, 0) as out_ind,
        if(start_speed is not null, 1, 0) as velocity_tracked_ind,
        if(
            coordinate_p_x is not null and coordinate_p_z is not null,
            1,
            0
        ) as location_tracked_ind,
        if(spin_rate is not null, 1, 0) as spin_rate_tracked_ind,
        if(is_first_pitch_count, 1, 0) as first_pitch_count_ind,
        if(is_two_strike_count, 1, 0) as two_strike_count_ind,
        if(is_three_ball_count, 1, 0) as three_ball_count_ind,
        if(is_full_count, 1, 0) as full_count_ind,

        -- metadata
        source_revision_id,
        game_type,
        season_phase,
        official_date,
        season
    from classified_pitches
)

{{ correction_safe_merge_rows("final", "changed_games", fact_grain) }}
