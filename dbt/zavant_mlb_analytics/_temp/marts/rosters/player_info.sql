-- information about each player from their latest game

with

src as (
    select * from {{ ref('stg_statsapi__game_players') }}
),

ordered as (
    select
        *,
        row_number() over (partition by player_id order by game_pk desc) as rn
    from src
)


select
    player_id,
    fullname,
    boxscorename as boxscore_name,
    initlastname as initlast_name,
    height,
    weight,
    birthdate,
    currentage as current_age,
    batside_code,
    batside_description as batside_desc,
    strikezonetop as strikezone_top,
    strikezonebottom as strikezone_bottom,
    pitchhand_code,
    pitchhand_description as pitchhand_desc,
    primaryposition_abbreviation as pos_abbr,
    primaryposition_code as pos_code,
    primaryposition_name as pos_name,
    primaryposition_type as pos_type,
    birthcity,
    birthstateprovince,
    birthcountry,
    game_pk as last_played_game
from ordered
where rn = 1
