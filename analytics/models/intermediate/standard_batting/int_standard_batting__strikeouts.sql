with src as (
    select * from {{ ref('stg_statsapi__play_info') }}
)

select
    season,
    batter_id as player_id,
    offense_team_id as team_id,
    'strikeouts' as stat,
    count(1) as value
from src a
where event_code like '%strikeout%'
group by 1, 2, 3, 4
