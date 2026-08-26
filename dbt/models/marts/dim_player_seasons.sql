{{
    config(
        materialized="table",
        table_type="iceberg",
        format="parquet",
    )
}}

with player_seasons as (
    select
        player_id,
        season,
        min(official_date) as first_game_official_date,
        max(official_date) as last_game_official_date
    from {{ ref("fct_player_game_participation") }}
    group by 1, 2
),

players as (
    select
        player_id,
        birth_date
    from {{ ref("dim_players") }}
),

transformed_player_seasons as (
    select
        a.player_id,
        a.season,
        b.birth_date,
        a.first_game_official_date,
        a.last_game_official_date,
        -- "baseball age": player's age as of June 30th of that season
        cast(
            if(
                b.birth_date is null,
                null,
                a.season
                - year(b.birth_date)
                - if(month(b.birth_date) > 6, 1, 0)
            ) as integer
        ) as season_age,
        cast(concat(cast(a.season as varchar), '-06-30') as date) as season_age_cutoff_date
    from player_seasons as a
    inner join players as b on a.player_id = b.player_id
),

final as (
    select
        -- grain
        {{ dbt_utils.generate_surrogate_key([
            "player_id",
            "season"
        ]) }} as player_season_key,
        player_id,
        season,

        -- season attributes
        birth_date,
        first_game_official_date,
        last_game_official_date,
        season_age,
        season_age_cutoff_date
    from transformed_player_seasons
)

select * from final
