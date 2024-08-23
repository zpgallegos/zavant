with src as (
    select
        *,
        row_number() over(partition by player_id order by game_pk desc) as rn
    from {{ source('statsapi', 'game_players') }}
)

select
    a.player_id,
    a.fullname,
    a.boxscorename as boxscore_name,
    a.initlastname as initlast_name,
    a.height,
    a.weight,
    a.birthdate,
    a.currentage as current_age,
    a.batside_code,
    a.batside_description as batside_desc,
    a.strikezonetop as strikezone_top,
    a.strikezonebottom as strikezone_bottom,
    a.pitchhand_code,
    a.pitchhand_description as pitchhand_desc,
    a.primaryposition_abbreviation as pos_abbr,
    a.primaryposition_code as pos_code,
    a.primaryposition_name as pos_name,
    a.primaryposition_type as pos_type,
    a.birthcity,
    a.birthstateprovince,
    a.birthcountry,
    a.game_pk as last_played_game
from src a
where a.rn = 1
