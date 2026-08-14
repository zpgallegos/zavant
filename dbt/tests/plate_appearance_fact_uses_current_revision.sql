select
    a.game_pk,
    a.source_revision_id as fact_source_revision_id,
    b.source_revision_id as current_source_revision_id
from {{ ref("fct_plate_appearances") }} as a
inner join {{ ref("stg_games") }} as b on a.game_pk = b.game_pk
where a.source_revision_id != b.source_revision_id
