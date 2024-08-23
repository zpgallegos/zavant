with src as (
    select
        a.player_id,
        b.team_long as team,
        b.division_name,
        row_number() over(partition by a.player_id order by a.game_pk desc) as rn

    from {{ ref('stg_statsapi__box_games') }} a
        inner join {{ ref('stg_statsapi__team_info') }} b on a.team_id = b.team_id
)

select player_id, team, division_name
from src
where rn = 1