select
    game_date,
    season,
    source_revision_id as savant_source_revision_id,
    projection_contract_version,
    projection_run_id,
    reconciled_at,
    raw_object_uri
from {{ source("zavant_analytical_prod", "statcast_date_revisions") }}
