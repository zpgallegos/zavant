with

src as (
    select * from {{ source('statsapi', 'game_info') }}
)

select
    partition_0 as season,
    game_pk,
    venue_id,
    date(datetime_originaldate) as game_original_date,
    date(datetime_officialdate) as game_official_date,
    from_iso8601_date(datetime_datetime) as game_datetime,
    if(datetime_daynight = 'day', true, false) as is_day_game,
    if(game_doubleheader in ('Y', 'S'), true, false) as is_doubleheader,
    game_gamenumber as game_number,
    probablepitchers_away_id as away_probable_pitcher_id,
    probablepitchers_home_id as home_probable_pitcher_id,
    gameinfo_attendance as attendance,
    gameinfo_gamedurationminutes as game_duration
from src
