with src as (
    select * from {{ ref('stg_statsapi__game_players') }}
), pos as (
    select distinct
        a.player_id,
        case pos_code
        when '7' then 'Left Field'
        when '8' then 'Center Field'
        when '9' then 'Right Field'
        else a.pos_name
        end as pos_name  
    from src a
)

select
    a.player_id,
    array_join(array_agg(a.pos_name order by a.pos_name), ', ') AS positions
from pos a
group by a.player_id
