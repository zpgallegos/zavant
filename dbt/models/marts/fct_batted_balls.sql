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

batted_balls as (
    select a.*
    from {{ ref("stg_batted_balls") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

plate_appearances as (
    select a.*
    from {{ ref("int_plate_appearances") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

pitches as (
    select a.*
    from {{ ref("stg_pitches") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

classified_batted_balls as (
    select
        a.*,
        b.source_revision_id,
        b.batter_id,
        b.pitcher_id,
        b.offense_team_id,
        b.defense_team_id,
        b.inning,
        b.half_inning,
        b.is_top_inning,
        b.outs_before,
        b.runner_on_first_id_before,
        b.runner_on_second_id_before,
        b.runner_on_third_id_before,
        b.event,
        b.event_type,
        b.description as result_description,
        b.bat_side_code,
        b.pitch_hand_code,
        b.batter_split,
        b.pitcher_split,
        b.men_on_base_split,
        c.pitch_type_code,
        c.pitch_type_description,
        c.start_speed as pitch_speed,
        c.coordinate_p_x as pitch_plate_x,
        c.coordinate_p_z as pitch_plate_z,
        c.zone as pitch_zone,
        b.event_type in (
            'single',
            'double',
            'triple',
            'home_run'
        ) as is_hit,
        b.event_type = 'home_run' as is_home_run,
        coalesce(a.launch_speed >= 95.0, false) as is_hard_hit,
        coalesce(a.launch_angle between 8.0 and 32.0, false) as is_sweet_spot,
        a.trajectory in (
            'bunt_grounder',
            'bunt_line_drive',
            'bunt_popup'
        ) as is_bunt,
        a.launch_speed is not null as has_exit_velocity,
        a.launch_angle is not null as has_launch_angle,
        a.launch_speed is not null and a.launch_angle is not null
            as has_statcast_tracking
    from batted_balls as a
    inner join plate_appearances as b
        on
            a.game_pk = b.game_pk
            and a.at_bat_index = b.at_bat_index
    left join pitches as c
        on
            a.game_pk = c.game_pk
            and a.at_bat_index = c.at_bat_index
            and a.event_index = c.event_index
),

final as (
    select
        -- keys and grain
        {{ dbt_utils.generate_surrogate_key([
            "game_pk",
            "at_bat_index",
            "event_index"
        ]) }} as batted_ball_key,
        {{ dbt_utils.generate_surrogate_key([
            "game_pk",
            "at_bat_index"
        ]) }} as plate_appearance_key,
        game_pk,
        at_bat_index,
        event_index,

        -- participants
        batter_id as result_batter_id,
        pitcher_id as result_pitcher_id,
        offense_team_id,
        defense_team_id,

        -- game state
        inning,
        half_inning,
        is_top_inning,
        outs_before,
        concat(
            if(runner_on_first_id_before is null, '0', '1'),
            if(runner_on_second_id_before is null, '0', '1'),
            if(runner_on_third_id_before is null, '0', '1')
        ) as base_state_before,

        -- plate-appearance outcome
        event,
        event_type,
        result_description,
        is_hit,
        is_home_run,

        -- contact measurements and classifications
        play_id,
        pitch_number,
        coordinate_x,
        coordinate_y,
        hardness,
        launch_angle,
        launch_speed,
        location,
        total_distance,
        trajectory,
        has_exit_velocity,
        has_launch_angle,
        has_statcast_tracking,
        is_hard_hit,
        is_sweet_spot,
        is_bunt,

        -- additive indicators and measures
        1 as batted_ball_ind,
        if(has_exit_velocity, 1, 0) as exit_velocity_tracked_ind,
        if(has_launch_angle, 1, 0) as launch_angle_tracked_ind,
        if(has_statcast_tracking, 1, 0) as statcast_tracked_batted_ball_ind,
        if(is_hard_hit, 1, 0) as hard_hit_ind,
        if(is_sweet_spot, 1, 0) as sweet_spot_ind,
        if(is_bunt, 1, 0) as bunt_ind,
        if(is_hit, 1, 0) as hit_ind,
        if(is_home_run, 1, 0) as home_run_ind,
        case event_type
            when 'single' then 1
            when 'double' then 2
            when 'triple' then 3
            when 'home_run' then 4
            else 0
        end as total_bases,

        -- matchup and pitch attributes
        bat_side_code,
        pitch_hand_code,
        batter_split,
        pitcher_split,
        men_on_base_split,
        pitch_type_code,
        pitch_type_description,
        pitch_speed,
        pitch_plate_x,
        pitch_plate_z,
        pitch_zone,

        -- metadata
        source_revision_id,
        official_date,
        season
    from classified_batted_balls
)

{{ correction_safe_merge_rows("final", "changed_games", fact_grain) }}
