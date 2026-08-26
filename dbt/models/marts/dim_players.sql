{{
    config(
        materialized="table",
        table_type="iceberg",
        format="parquet",
    )
}}

with player_game_records as (
    select
        a.*,
        coalesce(b.resume_date, a.official_date) as game_activity_date,
        coalesce(b.first_pitch_at, b.scheduled_start_at) as game_started_at
    from {{ ref("stg_boxscore_players") }} as a
    inner join {{ ref("stg_games") }} as b on a.game_pk = b.game_pk
),

ranked_player_game_records as (
    select
        a.*,
        dense_rank() over game_recency as game_recency_rank,
        row_number() over profile_recency as profile_recency_rank
    from player_game_records as a
    window
        game_recency as (
            partition by a.player_id
            order by
                a.game_activity_date desc,
                a.game_started_at desc nulls last,
                a.game_pk desc
        ),
        profile_recency as (
            partition by a.player_id
            order by
                a.game_activity_date desc,
                a.game_started_at desc nulls last,
                a.game_pk desc,
                a.team_id desc
        )
),

most_recent_game_teams as (
    select
        a.player_id,
        if(count(distinct a.team_id) = 1, max(a.team_id), cast(null as bigint)) as most_recent_game_team_id
    from ranked_player_game_records as a
    where a.game_recency_rank = 1
    group by 1
),

final as (
    select
        -- grain
        a.player_id,

        -- profile attributes
        a.bat_side_code,
        a.bat_side_description,
        a.birth_city,
        a.birth_country,
        a.birth_date,
        a.birth_state_province,
        a.boxscore_name,
        a.draft_year,
        a.first_name,
        a.full_name,
        a.gender,
        a.height,
        a.active as is_active_as_of_most_recent_game,
        a.last_name,
        a.middle_name,
        a.mlb_debut_date,
        a.name_slug,
        a.nickname,
        a.pitch_hand_code,
        a.pitch_hand_description,
        a.primary_number,
        a.primary_position_abbreviation,
        a.primary_position_code,
        a.primary_position_name,
        a.primary_position_type,
        a.pronunciation,
        a.strike_zone_bottom,
        a.strike_zone_top,
        a.use_last_name,
        a.use_name,
        a.weight,

        -- most recent game context
        a.game_activity_date as most_recent_game_activity_date,
        a.jersey_number as most_recent_game_jersey_number,
        a.official_date as most_recent_game_official_date,
        a.game_pk as most_recent_game_pk,
        a.season as most_recent_game_season,
        a.game_started_at as most_recent_game_started_at,
        b.most_recent_game_team_id
    from ranked_player_game_records as a
    inner join most_recent_game_teams as b on a.player_id = b.player_id
    where a.profile_recency_rank = 1
)

select * from final
