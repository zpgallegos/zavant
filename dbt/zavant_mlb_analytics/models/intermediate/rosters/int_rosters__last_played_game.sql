with src as (
    select * from {{ ref('stg_statsapi__box_games') }}
),
cte as (
    select
        a.season,
        a.player_id,
        a.team_id,
        a.game_pk,
        row_number() over(partition by a.player_id order by a.game_pk desc) as rn
    from src a
)

select
    a.player_id,
    a.team_id,
    a.game_pk as last_game_pk,
    a.season as last_played_season
from cte a
where rn = 1
