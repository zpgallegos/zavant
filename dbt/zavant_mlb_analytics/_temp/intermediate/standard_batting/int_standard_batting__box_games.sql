with src as (
    select * from {{ ref('stg_statsapi__box_games') }}
),

multi as (
    select
        a.player_id,
        a.season
    from src as a
    group by 1, 2
    having count(distinct a.team_id) > 1
)

select
    a.*,
    if(b.player_id is null, 1, 0) as is_single_team_player
from src as a
left join multi as b on a.player_id = b.player_id and a.season = b.season
