select
    a.game_pk,
    a.at_bat_index,
    a.expected_woba_value,
    b.estimated_woba_using_speedangle,
    a.woba_denominator as fact_woba_denominator,
    b.woba_denom as statcast_woba_denominator
from {{ ref("fct_plate_appearances") }} as a
inner join {{ ref("stg_statcast_batting_events") }} as b
    on
        a.game_pk = b.game_pk
        and a.at_bat_index + 1 = b.at_bat_number
where
    a.expected_woba_value is distinct from b.estimated_woba_using_speedangle
    or a.woba_denominator is distinct from b.woba_denom
