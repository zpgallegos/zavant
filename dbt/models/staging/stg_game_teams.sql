with game_teams as (
    select * from {{ source("zavant_analytical_prod", "game_teams") }}
)

select
    -- grain
    game_teams.source_revision_id,
    game_teams.game_pk,
    game_teams.team_side,

    -- attributes
    game_teams.abbreviation,
    game_teams.active,
    game_teams.club_name,
    game_teams.division_id,
    game_teams.division_leader,
    game_teams.division_name,
    game_teams.file_code,
    game_teams.first_year_of_play,
    game_teams.franchise_name,
    game_teams.games_played,
    game_teams.league_id,
    game_teams.league_name,
    game_teams.location_name,
    game_teams.losses,
    game_teams.score,
    game_teams.short_name,
    game_teams.team_code,
    game_teams.team_id,
    game_teams.team_name,
    game_teams.team_name_short,
    game_teams.ties,
    game_teams.venue_id,
    game_teams.venue_name,
    game_teams.winning_percentage,
    game_teams.wins,

    -- metadata
    game_teams.official_date,
    game_teams.projected_at,
    game_teams.projection_run_id,
    game_teams.season
from game_teams
