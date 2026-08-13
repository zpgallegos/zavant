select
    batted_ball.game_pk,
    batted_ball.at_bat_index,
    batted_ball.event_index
from {{ ref("stg_batted_balls") }} as batted_ball
left join {{ ref("stg_pitches") }} as pitch
    on
        batted_ball.game_pk = pitch.game_pk
        and batted_ball.at_bat_index = pitch.at_bat_index
        and batted_ball.event_index = pitch.event_index
where pitch.game_pk is null
