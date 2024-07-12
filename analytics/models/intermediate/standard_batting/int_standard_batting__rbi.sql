with src as (
    select * from {{ ref('stg_statsapi__play_info') }}
)

select
    season,
    batter_id as player_id,
    offense_team_id as team_id,
    'rbi' as stat,
    sum(rbi) as value
from src
group by 1, 2, 3, 4