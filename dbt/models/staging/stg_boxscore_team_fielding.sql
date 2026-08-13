with team_fielding as (
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
    team_fielding.season
from team_fielding
