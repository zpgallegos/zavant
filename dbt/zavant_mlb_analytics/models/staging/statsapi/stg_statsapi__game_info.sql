with src as (
    select * from {{ source('statsapi', 'game_info') }}
)

select
    a.game_pk,
    date(a.datetime_officialdate) as official_date,
    from_iso8601_date(a.datetime_datetime) as datetime
from src a