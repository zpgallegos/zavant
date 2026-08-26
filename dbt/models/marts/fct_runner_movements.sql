{% set fact_grain = ["season", "game_pk", "at_bat_index", "runner_index"] %}

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
    {{ changed_statsapi_game_revisions() }}
),

runner_movements as (
    select a.*
    from {{ ref("stg_runner_movements") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

plays as (
    select a.*
    from {{ ref("stg_plays") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

pitches as (
    select a.*
    from {{ ref("stg_pitches") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

classified_runner_movements as (
    select
        a.*,
        c.statsapi_source_revision_id,
        b.batter_id as play_batter_id,
        b.pitcher_id as play_pitcher_id,
        b.offense_team_id,
        b.defense_team_id,
        b.inning,
        b.half_inning,
        b.is_top_inning,
        d.play_id as pitch_play_id,
        d.pitch_number,
        d.call_code as pitch_call_code,
        d.call_description as pitch_call_description,
        d.pitch_type_code,
        d.pitch_type_description,
        d.start_speed as pitch_speed,
        d.coordinate_p_x as pitch_plate_x,
        d.coordinate_p_z as pitch_plate_z,
        d.zone as pitch_zone,
        d.balls as pitch_balls,
        d.strikes as pitch_strikes,
        d.is_ball as pitch_is_ball,
        d.is_strike as pitch_is_strike,
        d.is_in_play as pitch_is_in_play,
        d.game_pk is not null as has_associated_pitch,
        coalesce(a.is_out, false) as runner_is_out,
        coalesce(a.is_scoring_event, false) as runner_scored,
        coalesce(a.earned, false) as run_is_earned,
        coalesce(a.team_unearned, false) as run_is_team_unearned,
        coalesce(a.movement_reason like 'r_stolen_base_%', false) as is_stolen_base,
        coalesce(
            a.movement_reason like 'r_caught_stealing_%'
            or a.movement_reason like 'r_pickoff_caught_stealing_%',
            false
        ) as is_caught_stealing,
        coalesce(a.movement_reason like 'r_pickoff_caught_stealing_%', false) as is_pickoff_caught_stealing
    from runner_movements as a
    inner join plays as b
        on
            a.game_pk = b.game_pk
            and a.at_bat_index = b.at_bat_index
    inner join changed_games as c on a.game_pk = c.game_pk
    left join pitches as d
        on
            a.game_pk = d.game_pk
            and a.at_bat_index = d.at_bat_index
            and a.play_event_index = d.event_index
),

final as (
    select
        -- keys and grain
        {{ dbt_utils.generate_surrogate_key([
            "game_pk",
            "at_bat_index",
            "runner_index"
        ]) }} as runner_movement_key,
        {{ dbt_utils.generate_surrogate_key([
            "game_pk",
            "at_bat_index"
        ]) }} as play_key,
        game_pk,
        at_bat_index,
        runner_index,

        -- participants
        runner_id,
        responsible_pitcher_id,
        play_batter_id,
        play_pitcher_id,
        offense_team_id,
        defense_team_id,

        -- game state
        inning,
        half_inning,
        is_top_inning,

        -- movement attributes
        play_event_index,
        event,
        event_type,
        movement_reason,
        origin_base,
        start_base,
        end_base,
        out_base,
        out_number,
        rbi,
        runner_is_out as is_out,
        runner_scored as is_scoring_event,
        run_is_earned as earned,
        run_is_team_unearned as team_unearned,
        is_stolen_base,
        is_caught_stealing,
        is_pickoff_caught_stealing,
        case
            when is_stolen_base then 'stolen_base'
            when is_caught_stealing then 'caught_stealing'
            when runner_scored then 'run_scored'
            when runner_is_out then 'runner_out'
            when start_base != end_base then 'advanced'
            else 'other'
        end as movement_outcome,

        -- associated pitch
        has_associated_pitch,
        pitch_play_id,
        pitch_number,
        pitch_call_code,
        pitch_call_description,
        pitch_type_code,
        pitch_type_description,
        pitch_speed,
        pitch_plate_x,
        pitch_plate_z,
        pitch_zone,
        pitch_balls,
        pitch_strikes,
        pitch_is_ball,
        pitch_is_strike,
        pitch_is_in_play,

        -- additive indicators
        1 as runner_movement_ind,
        if(runner_scored, 1, 0) as run_scored_ind,
        if(runner_scored and run_is_earned, 1, 0) as earned_run_ind,
        if(runner_scored and not run_is_earned, 1, 0) as unearned_run_ind,
        if(runner_scored and run_is_team_unearned, 1, 0) as team_unearned_run_ind,
        if(is_stolen_base, 1, 0) as stolen_base_ind,
        if(is_caught_stealing, 1, 0) as caught_stealing_ind,
        if(is_stolen_base or is_caught_stealing, 1, 0) as stolen_base_attempt_ind,
        if(is_pickoff_caught_stealing, 1, 0) as pickoff_caught_stealing_ind,

        -- metadata
        statsapi_source_revision_id,
        official_date,
        season
    from classified_runner_movements
)

{{ correction_safe_merge_rows("final", "changed_games", fact_grain) }}
