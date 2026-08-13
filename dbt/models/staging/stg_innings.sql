with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

innings as (
    select * from {{ source("zavant_analytical_prod", "innings") }}
)

select
    -- grain
    innings.game_pk,
    innings.inning_number,

    -- attributes
    innings.away_errors,
    innings.away_hits,
    innings.away_left_on_base,
    innings.away_runs,
    innings.home_errors,
    innings.home_hits,
    innings.home_left_on_base,
    innings.home_runs,
    innings.ordinal,

    -- metadata
    innings.official_date,
    innings.projected_at,
    innings.projection_contract_version,
    innings.projection_run_id,
    innings.season,
    innings.source_revision_id
from innings
inner join current_revision on
    innings.game_pk = current_revision.game_pk
    and innings.source_revision_id = current_revision.source_revision_id
    and innings.projection_contract_version = current_revision.projection_contract_version
