select
    pitch.game_pk,
    pitch.at_bat_index,
    pitch.event_index
from {{ ref("stg_pitches") }} as pitch
left join {{ ref("stg_play_events") }} as event
    on
        pitch.game_pk = event.game_pk
        and pitch.at_bat_index = event.at_bat_index
        and pitch.event_index = event.event_index
where event.game_pk is null
