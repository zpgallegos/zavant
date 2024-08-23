with src as (
    select * from {{ source('statsapi', 'game_players') }}
)

select
    a.game_pk,
    a.player_id,
    a.fullname,
    a.boxscorename as boxscore_name,
    a.initlastname as initlast_name,
    a.currentage as current_age,
    a.primaryposition_abbreviation as pos_abbr,
    a.primaryposition_code as pos_code,
    a.primaryposition_name as pos_name,
    a.primaryposition_type as pos_type

from src a