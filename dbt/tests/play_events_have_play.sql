select
    event.game_pk,
    event.at_bat_index,
    event.event_index,
    event.source_revision_id,
    event.projection_contract_version
from {{ ref("stg_play_events") }} as event
left join {{ ref("stg_plays") }} as play
    on
        event.game_pk = play.game_pk
        and event.at_bat_index = play.at_bat_index
        and event.source_revision_id = play.source_revision_id
        and event.projection_contract_version = play.projection_contract_version
where play.game_pk is null
