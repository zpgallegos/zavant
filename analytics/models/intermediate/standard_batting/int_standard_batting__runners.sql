with src as (
    select 
        a.*,
        b.offense_team_id as team_id
    from {{ ref('stg_statsapi__runners') }} a
        inner join {{ ref('stg_statsapi__play_info') }} b on a.play_id = b.play_id
),
multi as (
    select a.runner_id, a.season
    from src a
    group by a.runner_id, a.season
    having count(distinct a.team_id) > 1
)

select
    a.*,
    if(b.runner_id is null, 1, 0) as is_single_team_player
from src a
    left join multi b on a.runner_id = b.runner_id and a.season = b.season