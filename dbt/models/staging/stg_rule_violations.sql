with rule_violations as (
    select * from {{ source("zavant_analytical_prod", "rule_violations") }}
)

select
    -- grain
    rule_violations.source_revision_id,
    rule_violations.game_pk,
    rule_violations.at_bat_index,
    rule_violations.event_index,

    -- attributes
    rule_violations.description,
    rule_violations.player_id,
    rule_violations.violation_type,

    -- metadata
    rule_violations.official_date,
    rule_violations.projected_at,
    rule_violations.projection_run_id,
    rule_violations.season
from rule_violations
