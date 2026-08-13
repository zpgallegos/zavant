with players as (
    select * from {{ source("zavant_analytical_prod", "players") }}
)

select
    -- grain
    players.game_pk,
    players.player_id,
    players.team_id,

    -- attributes
    players.active,
    players.bat_side_code,
    players.bat_side_description,
    players.batting_order,
    players.birth_city,
    players.birth_country,
    players.birth_date,
    players.birth_state_province,
    players.boxscore_name,
    players.boxscore_position_abbreviation,
    players.boxscore_position_code,
    players.boxscore_position_name,
    players.boxscore_position_type,
    players.draft_year,
    players.first_name,
    players.full_name,
    players.gender,
    players.height,
    players.is_current_batter,
    players.is_current_pitcher,
    players.is_on_bench,
    players.is_substitute,
    players.jersey_number,
    players.last_name,
    players.middle_name,
    players.mlb_debut_date,
    players.name_slug,
    players.nickname,
    players.pitch_hand_code,
    players.pitch_hand_description,
    players.primary_number,
    players.primary_position_abbreviation,
    players.primary_position_code,
    players.primary_position_name,
    players.primary_position_type,
    players.pronunciation,
    players.roster_status_code,
    players.roster_status_description,
    players.strike_zone_bottom,
    players.strike_zone_top,
    players.team_side,
    players.use_last_name,
    players.use_name,
    players.weight,

    -- metadata
    players.official_date,
    players.season
from players
