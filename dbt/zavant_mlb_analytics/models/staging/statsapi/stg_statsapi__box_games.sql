with src as (
    select * from {{ source('statsapi', 'game_boxscore') }}
)

select
    a.partition_0 as season,
    a.game_pk,
    a.person_id as player_id,
    a.team_id,
    a.gamestatus_isonbench as is_on_bench,
    a.gamestatus_issubstitute as is_substitute
from src a