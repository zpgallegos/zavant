select
    a.game_pk,
    a.at_bat_index,
    a.statcast_launch_speed_angle_code,
    b.launch_speed_angle,
    a.is_barrel,
    a.barrel_ind,
    a.expected_batting_average,
    b.estimated_ba_using_speedangle,
    a.expected_slugging_percentage,
    b.estimated_slg_using_speedangle,
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
    a.statcast_launch_speed_angle_code is distinct from b.launch_speed_angle
    or a.is_barrel is distinct from coalesce(b.launch_speed_angle = 6, false)
    or a.barrel_ind is distinct from if(b.launch_speed_angle = 6, 1, 0)
    or a.expected_batting_average is distinct from b.estimated_ba_using_speedangle
    or a.expected_slugging_percentage is distinct from b.estimated_slg_using_speedangle
    or a.expected_woba_value is distinct from b.estimated_woba_using_speedangle
    or a.woba_denominator is distinct from b.woba_denom
