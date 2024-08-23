with src as (
    select * from {{ ref('stg_statsapi__play_info') }}
), multi as (
    select a.batter_id, a.season
    from src a
    group by a.batter_id, a.season
    having count(distinct a.offense_team_id) > 1
)

select
    a.*,
    if(b.batter_id is null, 1, 0) as is_single_team_player
from src a
    left join multi b on a.batter_id = b.batter_id and a.season = b.season