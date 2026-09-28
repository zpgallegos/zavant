select
    a.game_pk,
    a.at_bat_index,
    a.event_index,
    a.launch_angle,
    b.launch_angle as savant_launch_angle,
    a.launch_speed,
    b.launch_speed as savant_launch_speed,
    a.statsapi_launch_angle,
    c.launch_angle as original_launch_angle,
    a.statsapi_launch_speed,
    c.launch_speed as original_launch_speed
from {{ ref("fct_batted_balls") }} as a
left join {{ ref("stg_statcast_batting_events") }} as b
    on
        a.game_pk = b.game_pk
        and a.at_bat_index + 1 = b.at_bat_number
left join {{ ref("stg_batted_balls") }} as c
    on
        a.game_pk = c.game_pk
        and a.at_bat_index = c.at_bat_index
        and a.event_index = c.event_index
where
    a.launch_angle is distinct from b.launch_angle
    or a.launch_speed is distinct from b.launch_speed
    or a.statsapi_launch_angle is distinct from c.launch_angle
    or a.statsapi_launch_speed is distinct from c.launch_speed
    or a.has_exit_velocity is distinct from (b.launch_speed is not null)
    or a.has_launch_angle is distinct from (b.launch_angle is not null)
    or a.has_statcast_tracking is distinct from (b.launch_speed is not null and b.launch_angle is not null)
    or a.is_hard_hit is distinct from coalesce(b.launch_speed >= 95.0, false)
    or a.hard_hit_ind is distinct from if(b.launch_speed >= 95.0, 1, 0)
    or a.exit_velocity_tracked_ind is distinct from if(b.launch_speed is not null, 1, 0)
    or a.launch_angle_tracked_ind is distinct from if(b.launch_angle is not null, 1, 0)
    or a.statcast_tracked_batted_ball_ind is distinct from if(
        b.launch_speed is not null and b.launch_angle is not null, 1, 0
    )
    -- Sweet spots intentionally continue using the original feed and boundaries.
    or a.is_sweet_spot is distinct from coalesce(c.launch_angle between 8.0 and 32.0, false)
    or a.sweet_spot_ind is distinct from if(c.launch_angle between 8.0 and 32.0, 1, 0)
