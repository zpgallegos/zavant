with src as (
    select * from {{ ref('stg_statsapi__box_games') }}
)

select
    season,
    player_id,
    team_id,
    'games_played' as stat,
    count(distinct game_pk) as value
from src
where not is_on_bench
group by 1, 2, 3, 4