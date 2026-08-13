select
    current_revision.game_pk,
    current_revision.source_revision_id,
    current_revision.projection_contract_version
from {{ ref("stg_current_game_revisions") }} as current_revision
left join {{ ref("stg_games") }} as game
    on
        current_revision.game_pk = game.game_pk
        and current_revision.source_revision_id = game.source_revision_id
        and current_revision.projection_contract_version = game.projection_contract_version
where game.game_pk is null
