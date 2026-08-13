with source as (
select
    -- grain
    game_pk,

    -- metadata
    projection_contract_version,
    projection_run_id,
    raw_object_uri,
    reconciled_at,
    season,
    source_revision_id
from {{ source("zavant_analytical_prod", "current_game_revisions") }}
where projection_contract_version = '{{ var("current_projection_contract_version") }}'
)

select * from source
