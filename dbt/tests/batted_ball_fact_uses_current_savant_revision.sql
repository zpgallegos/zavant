select
    a.game_pk,
    a.savant_source_revision_id as fact_savant_source_revision_id,
    b.savant_source_revision_id as current_savant_source_revision_id
from {{ ref("fct_batted_balls") }} as a
inner join {{ ref("stg_statcast_date_revisions") }} as b
    on a.official_date = b.game_date
where a.savant_source_revision_id is distinct from b.savant_source_revision_id
