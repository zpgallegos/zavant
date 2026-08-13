select
    event.game_pk,
    event.at_bat_index,
    event.event_index
from {{ ref("stg_play_events") }} as event
left join {{ ref("stg_plays") }} as play
    on
        event.game_pk = play.game_pk
        and event.at_bat_index = play.at_bat_index
where play.game_pk is null
