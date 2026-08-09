with

src as (
    select * from {{ ref('stg_statsapi__game_players') }}
),

ordered as (
    select
        *,
        row_number() over (partition by player_id order by game_pk desc) as rn
    from src
),

last_game as (
    select * from ordered where rn = 1
)

select * from last_game
