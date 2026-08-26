{{
    config(
        materialized="table",
        table_type="iceberg",
        format="parquet",
    )
}}

with ranked_team_game_records as (
    select
        a.*,
        b.first_pitch_at,
        b.scheduled_start_at,
        row_number() over game_recency as game_recency_rank
    from {{ ref("stg_game_teams") }} as a
    inner join {{ ref("stg_games") }} as b on a.game_pk = b.game_pk
    window game_recency as (
        partition by a.team_id
        order by
            coalesce(b.first_pitch_at, b.scheduled_start_at) desc nulls last,
            b.scheduled_start_at desc nulls last,
            a.game_pk desc
    )
),

final as (
    select
        -- grain
        a.team_id,

        -- team attributes
        a.abbreviation,
        a.club_name,
        a.division_id,
        a.division_name,
        a.file_code,
        a.first_year_of_play,
        a.franchise_name,
        a.active as is_active_as_of_most_recent_game,
        a.league_id,
        a.league_name,
        case
            when a.league_name = 'American League' then 'AL'
            when a.league_name = 'National League' then 'NL'
            else a.league_name
        end as league_name_short,
        a.location_name,
        a.short_name,
        a.team_code,
        a.team_name,
        a.team_name_short,
        a.venue_id,
        a.venue_name,

        -- most recent game context
        a.game_pk as most_recent_game_pk,
        a.official_date as most_recent_game_official_date,
        a.season as most_recent_game_season,
        coalesce(a.first_pitch_at, a.scheduled_start_at) as most_recent_game_started_at,
        a.team_side as most_recent_game_team_side
    from ranked_team_game_records as a
    where a.game_recency_rank = 1
)

select * from final
