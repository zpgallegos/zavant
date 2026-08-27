select
    a.game_pk,
    a.at_bat_index,
    a.statcast_launch_speed_angle_code,
    b.launch_speed_angle,
    a.is_barrel
from {{ ref("fct_batted_balls") }} as a
inner join {{ ref("stg_statcast_batting_events") }} as b
    on
        a.game_pk = b.game_pk
        and a.at_bat_index + 1 = b.at_bat_number
where
    a.statcast_launch_speed_angle_code is distinct from b.launch_speed_angle
    or a.is_barrel is distinct from coalesce(b.launch_speed_angle = 6, false)
