with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

team_fielding as (
    select * from {{ source("zavant_analytical_prod", "team_fielding") }}
)

select
    -- grain
    team_fielding.game_pk,
    team_fielding.team_id,

    -- attributes
    team_fielding.assists,
    team_fielding.caught_stealing,
    team_fielding.caught_stealing_percentage,
    team_fielding.chances,
    team_fielding.errors,
    team_fielding.fielding_percentage,
    team_fielding.games_started,
    team_fielding.passed_balls,
    team_fielding.pickoffs,
    team_fielding.put_outs,
    team_fielding.stolen_base_percentage,
    team_fielding.stolen_bases,
    team_fielding.team_side,

    -- metadata
    team_fielding.official_date,
    team_fielding.projected_at,
    team_fielding.projection_contract_version,
    team_fielding.projection_run_id,
    team_fielding.season,
    team_fielding.source_revision_id
from team_fielding
inner join current_revision on
    team_fielding.game_pk = current_revision.game_pk
    and team_fielding.source_revision_id = current_revision.source_revision_id
    and team_fielding.projection_contract_version = current_revision.projection_contract_version
