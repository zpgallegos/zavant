select
    current_revision.game_pk,
    current_revision.source_revision_id,
    current_revision.projection_contract_version
from {{ ref("stg_current_game_revisions") }} as current_revision
left join {{ ref("stg_projection_revisions") }} as projection_revision
    on
        current_revision.game_pk = projection_revision.game_pk
        and current_revision.source_revision_id = projection_revision.source_revision_id
        and current_revision.projection_contract_version = projection_revision.projection_contract_version
where projection_revision.game_pk is null
