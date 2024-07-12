with src as (
    select * from {{ ref('stg_statsapi__player_info') }}
)

select
    a.player_id,
    a.fullname,
    a.batside_desc,
    a.pitchhand_desc,
    a.height,
    a.weight,
    a.current_age,
    case
    when a.birthcountry = 'USA' then a.birthcity || ', ' || a.birthstateprovince
    else a.birthcity || ', ' || a.birthcountry
    end as birthplace
from src a