select
    a.game_pk,
    a.statsapi_source_revision_id as fact_statsapi_source_revision_id,
    b.statsapi_source_revision_id as current_statsapi_source_revision_id
from {{ ref("fct_plate_appearances") }} as a
inner join {{ ref("stg_games") }} as b on a.game_pk = b.game_pk
where a.statsapi_source_revision_id != b.statsapi_source_revision_id
