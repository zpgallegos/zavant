with player_batting as (
    select * from {{ source("zavant_analytical_prod", "player_batting") }}
)

select
    -- grain
    player_batting.source_revision_id,
    player_batting.game_pk,
    player_batting.player_id,
    player_batting.team_id,

    -- attributes
    player_batting.air_outs,
    player_batting.at_bats,
    player_batting.at_bats_per_home_run,
    player_batting.average,
    player_batting.base_on_balls,
    player_batting.catchers_interference,
    player_batting.caught_stealing,
    player_batting.doubles,
    player_batting.fly_outs,
    player_batting.games_played,
    player_batting.ground_into_double_play,
    player_batting.ground_into_triple_play,
    player_batting.ground_outs,
    player_batting.hit_by_pitch,
    player_batting.hits,
    player_batting.home_runs,
    player_batting.intentional_walks,
    player_batting.left_on_base,
    player_batting.line_outs,
    player_batting.note,
    player_batting.on_base_percentage,
    player_batting.on_base_plus_slugging,
    player_batting.pickoffs,
    player_batting.plate_appearances,
    player_batting.pop_outs,
    player_batting.rbi,
    player_batting.runs,
    player_batting.sac_bunts,
    player_batting.sac_flies,
    player_batting.slugging_percentage,
    player_batting.stolen_base_percentage,
    player_batting.stolen_bases,
    player_batting.strike_outs,
    player_batting.summary,
    player_batting.team_side,
    player_batting.total_bases,
    player_batting.triples,

    -- metadata
    player_batting.official_date,
    player_batting.projected_at,
    player_batting.projection_run_id,
    player_batting.season
from player_batting
