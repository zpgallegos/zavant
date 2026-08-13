with batted_balls as (
    select * from {{ source("zavant_analytical_prod", "batted_balls") }}
)

select
    -- grain
    batted_balls.game_pk,
    batted_balls.at_bat_index,
    batted_balls.event_index,

    -- attributes
    batted_balls.coordinate_x,
    batted_balls.coordinate_y,
    batted_balls.hardness,
    batted_balls.launch_angle,
    batted_balls.launch_speed,
    batted_balls.location,
    batted_balls.pitch_number,
    batted_balls.play_id,
    batted_balls.total_distance,
    batted_balls.trajectory,

    -- metadata
    batted_balls.official_date,
    batted_balls.season
from batted_balls
