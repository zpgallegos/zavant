with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

player_fielding as (
    select * from {{ source("zavant_analytical_prod", "player_fielding") }}
)

select
    -- grain
    player_fielding.game_pk,
    player_fielding.player_id,
    player_fielding.team_id,

    -- attributes
    player_fielding.assists,
    player_fielding.caught_stealing,
    player_fielding.caught_stealing_percentage,
    player_fielding.chances,
    player_fielding.errors,
    player_fielding.fielding_percentage,
    player_fielding.games_started,
    player_fielding.passed_balls,
    player_fielding.pickoffs,
    player_fielding.put_outs,
    player_fielding.stolen_base_percentage,
    player_fielding.stolen_bases,
    player_fielding.team_side,

    -- metadata
    player_fielding.official_date,
    player_fielding.projected_at,
    player_fielding.projection_contract_version,
    player_fielding.projection_run_id,
    player_fielding.season,
    player_fielding.source_revision_id
from player_fielding
inner join current_revision on
    player_fielding.game_pk = current_revision.game_pk
    and player_fielding.source_revision_id = current_revision.source_revision_id
    and player_fielding.projection_contract_version = current_revision.projection_contract_version
