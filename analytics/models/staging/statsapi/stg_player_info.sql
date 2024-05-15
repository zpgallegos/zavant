with source as (
    select
        *,
        row_number() over(partition by player_id order by game_pk desc) as rn
    from zavant.game_players
)

select
    player_id,
    batside_code,
    batside_description,
    birthcity,
    birthcountry,
    birthdate,
    birthstateprovince,
    boxscorename,
    currentage,
    firstlastname,
    firstname,
    fullfmlname,
    fulllfmname,
    fullname,
    height,
    initlastname,
    lastfirstname,
    lastinitname,
    lastname,
    lastplayeddate,
    middlename,
    mlbdebutdate,
    namefirstlast,
    namematrilineal,
    pitchhand_code,
    pitchhand_description,
    primaryposition_abbreviation,
    primaryposition_code,
    primaryposition_name,
    primaryposition_type,
    strikezonebottom,
    strikezonetop,
    uselastname,
    usename,
    weight,
    partition_0
from source
where rn = 1
