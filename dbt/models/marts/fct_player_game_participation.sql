{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["season", "game_pk", "player_id", "team_id"],
        delete_condition="src._dbt_is_deleted",
        insert_condition="not src._dbt_is_deleted",
        on_schema_change="ignore",
        table_type="iceberg",
        format="parquet",
        partitioned_by=["season"]
    )
}}

with current_game_revisions as (
    select
        game_pk,
        source_revision_id
    from {{ ref("stg_games") }}
),

changed_games as (
    select
        a.game_pk,
        a.source_revision_id
    from current_game_revisions as a
    {% if is_incremental() %}
        where not exists (
            select 1 as row_exists
            from {{ this }} as b
            where
                a.game_pk = b.game_pk
                and a.source_revision_id = b.source_revision_id
        )
    {% endif %}
),

boxscore_players as (
    select
        a.*,
        b.source_revision_id
    from {{ ref("stg_boxscore_players") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
    where not a.is_on_bench
),

player_batting as (
    select a.*
    from {{ ref("stg_boxscore_player_batting") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

player_pitching as (
    select a.*
    from {{ ref("stg_boxscore_player_pitching") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
),

player_positions as (
    select
        a.game_pk,
        a.player_id,
        a.team_id,
        count(*) as position_count
    from {{ ref("stg_boxscore_player_positions") }} as a
    inner join changed_games as b on a.game_pk = b.game_pk
    group by 1, 2, 3
),

classified_participations as (
    select
        a.*,
        coalesce(b.games_played, 0) > 0 as participated_as_batter,
        (
            coalesce(c.games_played, 0) > 0
            or coalesce(c.games_pitched, 0) > 0
        ) as participated_as_pitcher,
        coalesce(d.position_count, 0) as position_count
    from boxscore_players as a
    left join player_batting as b
        on
            a.game_pk = b.game_pk
            and a.player_id = b.player_id
            and a.team_id = b.team_id
    left join player_pitching as c
        on
            a.game_pk = c.game_pk
            and a.player_id = c.player_id
            and a.team_id = c.team_id
    left join player_positions as d
        on
            a.game_pk = d.game_pk
            and a.player_id = d.player_id
            and a.team_id = d.team_id
),

final as (
    select
        -- keys and grain
        {{ dbt_utils.generate_surrogate_key([
            "game_pk",
            "player_id",
            "team_id"
        ]) }} as player_game_team_participation_key,
        {{ dbt_utils.generate_surrogate_key([
            "game_pk",
            "player_id"
        ]) }} as player_game_key,
        game_pk,
        player_id,
        team_id,

        -- game and team context
        team_side,

        -- participation attributes
        batting_order,
        boxscore_position_abbreviation,
        boxscore_position_code,
        boxscore_position_name,
        boxscore_position_type,
        is_substitute as entered_as_substitute,
        participated_as_batter,
        participated_as_pitcher,
        position_count,

        -- additive indicators
        1 as player_game_team_participation_ind,
        if(is_substitute, 1, 0) as substitute_participation_ind,
        if(participated_as_batter, 1, 0) as batter_participation_ind,
        if(participated_as_pitcher, 1, 0) as pitcher_participation_ind,

        -- metadata
        source_revision_id,
        official_date,
        season
    from classified_participations
)

{% if is_incremental() %}
    select
        a.*,
        false as _dbt_is_deleted
    from final as a

    union all

    select
        b.*,
        true as _dbt_is_deleted
    from changed_games as a
    inner join {{ this }} as b on a.game_pk = b.game_pk
    left join final as c
        on
            b.season = c.season
            and b.game_pk = c.game_pk
            and b.player_id = c.player_id
            and b.team_id = c.team_id
    where c.game_pk is null
{% else %}
    select * from final
{% endif %}
