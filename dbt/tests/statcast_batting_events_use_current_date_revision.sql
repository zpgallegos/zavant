select
    a.game_date,
    a.game_pk,
    a.at_bat_number,
    a.savant_source_revision_id as event_savant_source_revision_id,
    b.savant_source_revision_id as current_savant_source_revision_id
from {{ ref("stg_statcast_batting_events") }} as a
left join {{ ref("stg_statcast_date_revisions") }} as b
    on a.game_date = b.game_date
where
    b.game_date is null
    or a.savant_source_revision_id is distinct from b.savant_source_revision_id
