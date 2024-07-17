with src as (
    select * from {{ ref('stg_statsapi__box_games') }}
), multi as (
    select a.player_id, a.season
    from src a
    group by a.player_id, a.season
    having count(distinct a.team_id) > 1
)

select
    a.*,
    if(b.player_id is null, 1, 0) as is_single_team_player
from src a
    left join multi b on a.player_id = b.player_id and a.season = b.season