with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

team_pitching as (
    select * from {{ source("zavant_analytical_prod", "team_pitching") }}
)

select
    -- grain
    team_pitching.game_pk,
    team_pitching.team_id,

    -- attributes
    team_pitching.air_outs,
    team_pitching.at_bats,
    team_pitching.balks,
    team_pitching.balls,
    team_pitching.base_on_balls,
    team_pitching.batters_faced,
    team_pitching.blown_saves,
    team_pitching.catchers_interference,
    team_pitching.caught_stealing,
    team_pitching.caught_stealing_percentage,
    team_pitching.complete_games,
    team_pitching.doubles,
    team_pitching.earned_run_average,
    team_pitching.earned_runs,
    team_pitching.fly_outs,
    team_pitching.games_finished,
    team_pitching.games_pitched,
    team_pitching.games_played,
    team_pitching.games_started,
    team_pitching.ground_outs,
    team_pitching.ground_outs_to_air_outs,
    team_pitching.hit_batsmen,
    team_pitching.hit_by_pitch,
    team_pitching.hits,
    team_pitching.holds,
    team_pitching.home_runs,
    team_pitching.home_runs_per_nine,
    team_pitching.inherited_runners,
    team_pitching.inherited_runners_scored,
    team_pitching.innings_pitched,
    team_pitching.intentional_walks,
    team_pitching.line_outs,
    team_pitching.losses,
    team_pitching.note,
    team_pitching.number_of_pitches,
    team_pitching.outs,
    team_pitching.passed_balls,
    team_pitching.pickoffs,
    team_pitching.pitches_per_inning,
    team_pitching.pitches_thrown,
    team_pitching.pop_outs,
    team_pitching.rbi,
    team_pitching.runs,
    team_pitching.runs_scored_per_nine,
    team_pitching.sac_bunts,
    team_pitching.sac_flies,
    team_pitching.save_opportunities,
    team_pitching.saves,
    team_pitching.shutouts,
    team_pitching.stolen_base_percentage,
    team_pitching.stolen_bases,
    team_pitching.strike_outs,
    team_pitching.strike_percentage,
    team_pitching.strikes,
    team_pitching.summary,
    team_pitching.team_side,
    team_pitching.triples,
    team_pitching.walks_hits_per_inning,
    team_pitching.wild_pitches,
    team_pitching.wins,

    -- metadata
    team_pitching.official_date,
    team_pitching.projected_at,
    team_pitching.projection_contract_version,
    team_pitching.projection_run_id,
    team_pitching.season,
    team_pitching.source_revision_id
from team_pitching
inner join current_revision on
    team_pitching.game_pk = current_revision.game_pk
    and team_pitching.source_revision_id = current_revision.source_revision_id
    and team_pitching.projection_contract_version = current_revision.projection_contract_version
