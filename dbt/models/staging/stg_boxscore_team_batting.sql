with team_batting as (
    select * from {{ source("zavant_analytical_prod", "team_batting") }}
)

select
    -- grain
    team_batting.source_revision_id,
    team_batting.game_pk,
    team_batting.team_id,

    -- attributes
    team_batting.air_outs,
    team_batting.at_bats,
    team_batting.at_bats_per_home_run,
    team_batting.average,
    team_batting.base_on_balls,
    team_batting.catchers_interference,
    team_batting.caught_stealing,
    team_batting.doubles,
    team_batting.fly_outs,
    team_batting.games_played,
    team_batting.ground_into_double_play,
    team_batting.ground_into_triple_play,
    team_batting.ground_outs,
    team_batting.hit_by_pitch,
    team_batting.hits,
    team_batting.home_runs,
    team_batting.intentional_walks,
    team_batting.left_on_base,
    team_batting.line_outs,
    team_batting.note,
    team_batting.on_base_percentage,
    team_batting.on_base_plus_slugging,
    team_batting.pickoffs,
    team_batting.plate_appearances,
    team_batting.pop_outs,
    team_batting.rbi,
    team_batting.runs,
    team_batting.sac_bunts,
    team_batting.sac_flies,
    team_batting.slugging_percentage,
    team_batting.stolen_base_percentage,
    team_batting.stolen_bases,
    team_batting.strike_outs,
    team_batting.summary,
    team_batting.team_side,
    team_batting.total_bases,
    team_batting.triples,

    -- metadata
    team_batting.official_date,
    team_batting.projected_at,
    team_batting.projection_run_id,
    team_batting.season
from team_batting
