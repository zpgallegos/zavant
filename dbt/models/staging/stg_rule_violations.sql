with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

rule_violations as (
    select * from {{ source("zavant_analytical_prod", "rule_violations") }}
)

select
    -- grain
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
    rule_violations.projection_contract_version,
    rule_violations.projection_run_id,
    rule_violations.season,
    rule_violations.source_revision_id
from rule_violations
inner join current_revision on
    rule_violations.game_pk = current_revision.game_pk
    and rule_violations.source_revision_id = current_revision.source_revision_id
    and rule_violations.projection_contract_version = current_revision.projection_contract_version
