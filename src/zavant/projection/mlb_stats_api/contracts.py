"""Explicit contracts for revision-aware Stats API game tables."""

from __future__ import annotations

from typing import Dict, Sequence, Tuple

from zavant.projection.contracts import Column, ColumnKind, TableContract
from zavant.projection.mlb_stats_api.stat_fields import (
    BATTING_FIELDS,
    FIELDING_FIELDS,
    PITCHING_FIELDS,
    StatField,
)


PROJECTION_CONTRACT_VERSION = "zavant-analytical-game-projection/v1"

CURRENT_REVISION_CONTRACT = TableContract(
    name="current_game_revisions",
    columns=(
        Column("game_pk", "int64", False),
        Column("season", "int32", False),
        Column("source_revision_id", "string", False),
        Column("projection_contract_version", "string", False),
        Column("projection_run_id", "string", False),
        Column("reconciled_at", "timestamp", False),
        Column("raw_object_uri", "string", False),
    ),
    primary_key=("game_pk",),
)


def _column(name: str, kind: ColumnKind, nullable: bool = True) -> Column:
    return Column(name=name, kind=kind, nullable=nullable)


IDENTITY_COLUMNS = (
    _column("game_pk", "int64", False),
    _column("season", "int32", False),
    _column("official_date", "date", False),
    _column("source_revision_id", "string", False),
    _column("projection_contract_version", "string", False),
    _column("projection_run_id", "string", False),
    _column("projected_at", "timestamp", False),
)

GAME_COLUMNS = IDENTITY_COLUMNS + (
    _column("raw_object_uri", "string", False),
    _column("source_uri", "string"),
    _column("source_observed_at", "timestamp", False),
    _column("feed_timecode", "string"),
    _column("game_type", "string", False),
    _column("game_id", "string"),
    _column("double_header", "string"),
    _column("gameday_type", "string"),
    _column("tiebreaker", "string"),
    _column("game_number", "int32"),
    _column("calendar_event_id", "string"),
    _column("scheduled_start_at", "timestamp"),
    _column("original_date", "date"),
    _column("resume_date", "date"),
    _column("resumed_from_date", "date"),
    _column("day_night", "string"),
    _column("abstract_game_state", "string"),
    _column("coded_game_state", "string"),
    _column("detailed_state", "string"),
    _column("status_code", "string"),
    _column("start_time_tbd", "boolean"),
    _column("away_team_id", "int64", False),
    _column("home_team_id", "int64", False),
    _column("away_score", "int32"),
    _column("home_score", "int32"),
    _column("venue_id", "int64"),
    _column("venue_name", "string"),
    _column("attendance", "int64"),
    _column("first_pitch_at", "timestamp"),
    _column("game_duration_minutes", "int32"),
    _column("delay_duration_minutes", "int32"),
    _column("scheduled_innings", "int32"),
    _column("weather_condition", "string"),
    _column("temperature_fahrenheit", "int32"),
    _column("wind", "string"),
    _column("no_hitter", "boolean"),
    _column("perfect_game", "boolean"),
    _column("away_team_no_hitter", "boolean"),
    _column("away_team_perfect_game", "boolean"),
    _column("home_team_no_hitter", "boolean"),
    _column("home_team_perfect_game", "boolean"),
    _column("replay_has_challenges", "boolean"),
    _column("away_replay_challenges_used", "int32"),
    _column("away_replay_challenges_remaining", "int32"),
    _column("home_replay_challenges_used", "int32"),
    _column("home_replay_challenges_remaining", "int32"),
    _column("abs_has_challenges", "boolean"),
    _column("away_abs_challenges_successful", "int32"),
    _column("away_abs_challenges_failed", "int32"),
    _column("away_abs_challenges_remaining", "int32"),
    _column("home_abs_challenges_successful", "int32"),
    _column("home_abs_challenges_failed", "int32"),
    _column("home_abs_challenges_remaining", "int32"),
    _column("away_mound_visits_used", "int32"),
    _column("away_mound_visits_remaining", "int32"),
    _column("home_mound_visits_used", "int32"),
    _column("home_mound_visits_remaining", "int32"),
)

PLAY_COLUMNS = IDENTITY_COLUMNS + (
    _column("at_bat_index", "int32", False),
    _column("play_type", "string"),
    _column("event", "string"),
    _column("event_type", "string"),
    _column("description", "string"),
    _column("rbi", "int32"),
    _column("away_score", "int32"),
    _column("home_score", "int32"),
    _column("is_out", "boolean"),
    _column("inning", "int32", False),
    _column("half_inning", "string", False),
    _column("is_top_inning", "boolean", False),
    _column("started_at", "timestamp"),
    _column("ended_at", "timestamp"),
    _column("is_complete", "boolean"),
    _column("is_scoring_play", "boolean"),
    _column("has_review", "boolean"),
    _column("has_out", "boolean"),
    _column("captivating_index", "int32"),
    _column("balls", "int32"),
    _column("strikes", "int32"),
    _column("outs", "int32"),
    _column("batter_id", "int64", False),
    _column("pitcher_id", "int64", False),
    _column("bat_side_code", "string"),
    _column("pitch_hand_code", "string"),
    _column("offense_team_id", "int64", False),
    _column("defense_team_id", "int64", False),
    _column("post_on_first_id", "int64"),
    _column("post_on_second_id", "int64"),
    _column("post_on_third_id", "int64"),
    _column("batter_split", "string"),
    _column("pitcher_split", "string"),
    _column("men_on_base_split", "string"),
)

EVENT_COLUMNS = IDENTITY_COLUMNS + (
    _column("at_bat_index", "int32", False),
    _column("event_index", "int32", False),
    _column("event_kind", "string", False),
    _column("play_id", "string"),
    _column("pitch_number", "int32"),
    _column("started_at", "timestamp"),
    _column("ended_at", "timestamp"),
    _column("is_pitch", "boolean"),
    _column("is_base_running_play", "boolean"),
    _column("is_substitution", "boolean"),
    _column("balls", "int32"),
    _column("strikes", "int32"),
    _column("outs", "int32"),
    _column("description", "string"),
    _column("event", "string"),
    _column("event_type", "string"),
    _column("event_code", "string"),
    _column("is_in_play", "boolean"),
    _column("is_strike", "boolean"),
    _column("is_ball", "boolean"),
    _column("is_out", "boolean"),
    _column("is_scoring_play", "boolean"),
    _column("has_review", "boolean"),
    _column("away_score", "int32"),
    _column("home_score", "int32"),
    _column("player_id", "int64"),
    _column("replaced_player_id", "int64"),
    _column("position_code", "string"),
    _column("position_name", "string"),
    _column("position_abbreviation", "string"),
    _column("batting_order", "string"),
    _column("base_number", "int32"),
    _column("injury_type", "string"),
)

PITCH_COLUMNS = IDENTITY_COLUMNS + (
    _column("at_bat_index", "int32", False),
    _column("event_index", "int32", False),
    _column("play_id", "string"),
    _column("pitch_number", "int32"),
    _column("call_code", "string"),
    _column("call_description", "string"),
    _column("pitch_type_code", "string"),
    _column("pitch_type_description", "string"),
    _column("description", "string"),
    _column("is_in_play", "boolean"),
    _column("is_strike", "boolean"),
    _column("is_ball", "boolean"),
    _column("is_out", "boolean"),
    _column("has_review", "boolean"),
    _column("balls", "int32"),
    _column("strikes", "int32"),
    _column("outs", "int32"),
    _column("start_speed", "float64"),
    _column("end_speed", "float64"),
    _column("strike_zone_top", "float64"),
    _column("strike_zone_bottom", "float64"),
    _column("strike_zone_width", "float64"),
    _column("strike_zone_depth", "float64"),
    _column("zone", "int32"),
    _column("type_confidence", "float64"),
    _column("plate_time", "float64"),
    _column("extension", "float64"),
    _column("coordinate_a_x", "float64"),
    _column("coordinate_a_y", "float64"),
    _column("coordinate_a_z", "float64"),
    _column("coordinate_pfx_x", "float64"),
    _column("coordinate_pfx_z", "float64"),
    _column("coordinate_p_x", "float64"),
    _column("coordinate_p_z", "float64"),
    _column("coordinate_v_x0", "float64"),
    _column("coordinate_v_y0", "float64"),
    _column("coordinate_v_z0", "float64"),
    _column("coordinate_x", "float64"),
    _column("coordinate_y", "float64"),
    _column("coordinate_x0", "float64"),
    _column("coordinate_y0", "float64"),
    _column("coordinate_z0", "float64"),
    _column("break_angle", "float64"),
    _column("break_length", "float64"),
    _column("break_y", "float64"),
    _column("break_vertical", "float64"),
    _column("break_vertical_induced", "float64"),
    _column("break_horizontal", "float64"),
    _column("spin_rate", "float64"),
    _column("spin_direction", "float64"),
)

BATTED_BALL_COLUMNS = IDENTITY_COLUMNS + (
    _column("at_bat_index", "int32", False),
    _column("event_index", "int32", False),
    _column("play_id", "string"),
    _column("pitch_number", "int32"),
    _column("launch_speed", "float64"),
    _column("launch_angle", "float64"),
    _column("total_distance", "float64"),
    _column("trajectory", "string"),
    _column("hardness", "string"),
    _column("location", "string"),
    _column("coordinate_x", "float64"),
    _column("coordinate_y", "float64"),
)

EVENT_KEY_COLUMNS = (
    _column("at_bat_index", "int32", False),
    _column("event_index", "int32", False),
)

ACTION_COLUMNS = IDENTITY_COLUMNS + EVENT_KEY_COLUMNS + (
    _column("play_id", "string"),
    _column("action_play_id", "string"),
    _column("started_at", "timestamp"),
    _column("ended_at", "timestamp"),
    _column("event", "string"),
    _column("event_type", "string"),
    _column("description", "string"),
    _column("event_code", "string"),
    _column("is_substitution", "boolean"),
    _column("is_out", "boolean"),
    _column("has_review", "boolean"),
    _column("balls", "int32"),
    _column("strikes", "int32"),
    _column("outs", "int32"),
    _column("player_id", "int64"),
    _column("replaced_player_id", "int64"),
    _column("umpire_id", "int64"),
    _column("position_code", "string"),
    _column("position_name", "string"),
    _column("position_abbreviation", "string"),
    _column("batting_order", "string"),
    _column("base_number", "int32"),
    _column("disengagement_number", "int32"),
)

SUBSTITUTION_COLUMNS = IDENTITY_COLUMNS + EVENT_KEY_COLUMNS + (
    _column("play_id", "string"),
    _column("substitution_type", "string"),
    _column("description", "string"),
    _column("incoming_player_id", "int64", False),
    _column("replaced_player_id", "int64"),
    _column("position_code", "string"),
    _column("position_name", "string"),
    _column("position_abbreviation", "string"),
    _column("batting_order", "string"),
    _column("base_number", "int32"),
)

DISENGAGEMENT_COLUMNS = IDENTITY_COLUMNS + EVENT_KEY_COLUMNS + (
    _column("event_kind", "string", False),
    _column("play_id", "string"),
    _column("action_play_id", "string"),
    _column("event", "string"),
    _column("event_type", "string"),
    _column("description", "string"),
    _column("event_code", "string"),
    _column("from_catcher", "boolean"),
    _column("is_out", "boolean"),
    _column("has_review", "boolean"),
    _column("disengagement_number", "int32"),
    _column("balls", "int32"),
    _column("strikes", "int32"),
    _column("outs", "int32"),
)

NON_PITCH_CALL_COLUMNS = IDENTITY_COLUMNS + EVENT_KEY_COLUMNS + (
    _column("play_id", "string"),
    _column("pitch_number", "int32"),
    _column("started_at", "timestamp"),
    _column("ended_at", "timestamp"),
    _column("call_code", "string"),
    _column("call_description", "string"),
    _column("description", "string"),
    _column("is_in_play", "boolean"),
    _column("is_strike", "boolean"),
    _column("is_ball", "boolean"),
    _column("is_out", "boolean"),
    _column("has_review", "boolean"),
    _column("balls", "int32"),
    _column("strikes", "int32"),
    _column("outs", "int32"),
)

RUNNER_MOVEMENT_COLUMNS = IDENTITY_COLUMNS + (
    _column("at_bat_index", "int32", False),
    _column("runner_index", "int32", False),
    _column("play_event_index", "int32"),
    _column("runner_id", "int64", False),
    _column("responsible_pitcher_id", "int64"),
    _column("event", "string"),
    _column("event_type", "string"),
    _column("movement_reason", "string"),
    _column("origin_base", "string"),
    _column("start_base", "string"),
    _column("end_base", "string"),
    _column("out_base", "string"),
    _column("is_out", "boolean"),
    _column("out_number", "int32"),
    _column("is_scoring_event", "boolean"),
    _column("rbi", "boolean"),
    _column("earned", "boolean"),
    _column("team_unearned", "boolean"),
)

FIELDING_CREDIT_COLUMNS = IDENTITY_COLUMNS + (
    _column("at_bat_index", "int32", False),
    _column("runner_index", "int32", False),
    _column("credit_index", "int32", False),
    _column("play_event_index", "int32"),
    _column("player_id", "int64", False),
    _column("credit", "string", False),
    _column("position_code", "string"),
    _column("position_name", "string"),
    _column("position_type", "string"),
    _column("position_abbreviation", "string"),
)

REVIEW_COLUMNS = IDENTITY_COLUMNS + (
    _column("at_bat_index", "int32", False),
    _column("review_id", "string", False),
    _column("review_scope", "string", False),
    _column("event_index", "int32"),
    _column("review_sequence", "int32", False),
    _column("review_type", "string"),
    _column("challenge_team_id", "int64"),
    _column("player_id", "int64"),
    _column("in_progress", "boolean"),
    _column("is_overturned", "boolean"),
)

RULE_VIOLATION_COLUMNS = IDENTITY_COLUMNS + EVENT_KEY_COLUMNS + (
    _column("violation_type", "string", False),
    _column("description", "string"),
    _column("player_id", "int64"),
)

GAME_TEAM_COLUMNS = IDENTITY_COLUMNS + (
    _column("team_side", "string", False),
    _column("team_id", "int64", False),
    _column("team_name", "string", False),
    _column("team_code", "string"),
    _column("file_code", "string"),
    _column("abbreviation", "string"),
    _column("team_name_short", "string"),
    _column("location_name", "string"),
    _column("short_name", "string"),
    _column("franchise_name", "string"),
    _column("club_name", "string"),
    _column("first_year_of_play", "string"),
    _column("active", "boolean"),
    _column("league_id", "int64"),
    _column("league_name", "string"),
    _column("division_id", "int64"),
    _column("division_name", "string"),
    _column("venue_id", "int64"),
    _column("venue_name", "string"),
    _column("games_played", "int32"),
    _column("wins", "int32"),
    _column("losses", "int32"),
    _column("ties", "int32"),
    _column("winning_percentage", "string"),
    _column("division_leader", "boolean"),
    _column("score", "int32"),
)

INNING_COLUMNS = IDENTITY_COLUMNS + (
    _column("inning_number", "int32", False),
    _column("ordinal", "string"),
    _column("away_runs", "int32"),
    _column("away_hits", "int32"),
    _column("away_errors", "int32"),
    _column("away_left_on_base", "int32"),
    _column("home_runs", "int32"),
    _column("home_hits", "int32"),
    _column("home_errors", "int32"),
    _column("home_left_on_base", "int32"),
)

GAME_OFFICIAL_COLUMNS = IDENTITY_COLUMNS + (
    _column("official_index", "int32", False),
    _column("official_type", "string", False),
    _column("official_id", "int64", False),
    _column("official_name", "string"),
)

GAME_DECISION_COLUMNS = IDENTITY_COLUMNS + (
    _column("decision_type", "string", False),
    _column("player_id", "int64", False),
    _column("player_name", "string"),
)

PLAYER_COLUMNS = IDENTITY_COLUMNS + (
    _column("player_id", "int64", False),
    _column("team_id", "int64", False),
    _column("team_side", "string", False),
    _column("full_name", "string", False),
    _column("first_name", "string"),
    _column("last_name", "string"),
    _column("use_name", "string"),
    _column("use_last_name", "string"),
    _column("middle_name", "string"),
    _column("boxscore_name", "string"),
    _column("nickname", "string"),
    _column("pronunciation", "string"),
    _column("name_slug", "string"),
    _column("primary_number", "string"),
    _column("jersey_number", "string"),
    _column("batting_order", "string"),
    _column("birth_date", "date"),
    _column("birth_city", "string"),
    _column("birth_state_province", "string"),
    _column("birth_country", "string"),
    _column("height", "string"),
    _column("weight", "int32"),
    _column("active", "boolean"),
    _column("gender", "string"),
    _column("draft_year", "int32"),
    _column("mlb_debut_date", "date"),
    _column("bat_side_code", "string"),
    _column("bat_side_description", "string"),
    _column("pitch_hand_code", "string"),
    _column("pitch_hand_description", "string"),
    _column("primary_position_code", "string"),
    _column("primary_position_name", "string"),
    _column("primary_position_type", "string"),
    _column("primary_position_abbreviation", "string"),
    _column("boxscore_position_code", "string"),
    _column("boxscore_position_name", "string"),
    _column("boxscore_position_type", "string"),
    _column("boxscore_position_abbreviation", "string"),
    _column("roster_status_code", "string"),
    _column("roster_status_description", "string"),
    _column("is_current_batter", "boolean"),
    _column("is_current_pitcher", "boolean"),
    _column("is_on_bench", "boolean"),
    _column("is_substitute", "boolean"),
    _column("strike_zone_top", "float64"),
    _column("strike_zone_bottom", "float64"),
)

PLAYER_POSITION_COLUMNS = IDENTITY_COLUMNS + (
    _column("player_id", "int64", False),
    _column("position_sequence", "int32", False),
    _column("team_id", "int64", False),
    _column("team_side", "string", False),
    _column("position_code", "string", False),
    _column("position_name", "string"),
    _column("position_type", "string"),
    _column("position_abbreviation", "string"),
)


def _stat_columns(fields: Sequence[StatField], player: bool) -> Tuple[Column, ...]:
    stat_columns = tuple(
        _column(output_name, kind) for output_name, _, kind in fields
    )
    if player:
        return (
            IDENTITY_COLUMNS
            + (
                _column("player_id", "int64", False),
                _column("team_id", "int64", False),
                _column("team_side", "string", False),
            )
            + stat_columns
        )
    return IDENTITY_COLUMNS + (
        _column("team_id", "int64", False),
        _column("team_side", "string", False),
    ) + stat_columns


PLAYER_BATTING_COLUMNS = _stat_columns(BATTING_FIELDS, player=True)
PLAYER_PITCHING_COLUMNS = _stat_columns(PITCHING_FIELDS, player=True)
PLAYER_FIELDING_COLUMNS = _stat_columns(FIELDING_FIELDS, player=True)
TEAM_BATTING_COLUMNS = _stat_columns(BATTING_FIELDS, player=False)
TEAM_PITCHING_COLUMNS = _stat_columns(PITCHING_FIELDS, player=False)
TEAM_FIELDING_COLUMNS = _stat_columns(FIELDING_FIELDS, player=False)


def _table(
    name: str,
    columns: Tuple[Column, ...],
    *grain: str,
) -> TableContract:
    return TableContract(
        name,
        columns,
        (
            "game_pk",
            "source_revision_id",
            *grain,
        ),
    )

# This registry is the authoritative analytical surface. Projection validation,
# local Parquet output, Iceberg DDL/merges, and public current views all derive
# their table inventory and natural keys from it.
TABLE_CONTRACTS: Dict[str, TableContract] = {
    contract.name: contract
    for contract in (
        TableContract(
            "games",
            GAME_COLUMNS,
            ("game_pk", "source_revision_id"),
        ),
        TableContract(
            "plays",
            PLAY_COLUMNS,
            (
                "game_pk",
                "source_revision_id",
                "at_bat_index",
            ),
        ),
        TableContract(
            "play_events",
            EVENT_COLUMNS,
            (
                "game_pk",
                "source_revision_id",
                "at_bat_index",
                "event_index",
            ),
        ),
        TableContract(
            "pitches",
            PITCH_COLUMNS,
            (
                "game_pk",
                "source_revision_id",
                "at_bat_index",
                "event_index",
            ),
        ),
        TableContract(
            "batted_balls",
            BATTED_BALL_COLUMNS,
            (
                "game_pk",
                "source_revision_id",
                "at_bat_index",
                "event_index",
            ),
        ),
        _table("actions", ACTION_COLUMNS, "at_bat_index", "event_index"),
        _table(
            "substitutions", SUBSTITUTION_COLUMNS, "at_bat_index", "event_index"
        ),
        _table(
            "disengagements", DISENGAGEMENT_COLUMNS, "at_bat_index", "event_index"
        ),
        _table(
            "non_pitch_calls", NON_PITCH_CALL_COLUMNS, "at_bat_index", "event_index"
        ),
        _table(
            "runner_movements", RUNNER_MOVEMENT_COLUMNS, "at_bat_index", "runner_index"
        ),
        _table(
            "fielding_credits",
            FIELDING_CREDIT_COLUMNS,
            "at_bat_index",
            "runner_index",
            "credit_index",
        ),
        _table("reviews", REVIEW_COLUMNS, "at_bat_index", "review_id"),
        _table(
            "rule_violations", RULE_VIOLATION_COLUMNS, "at_bat_index", "event_index"
        ),
        _table("game_teams", GAME_TEAM_COLUMNS, "team_side"),
        _table("innings", INNING_COLUMNS, "inning_number"),
        _table("game_officials", GAME_OFFICIAL_COLUMNS, "official_index"),
        _table("game_decisions", GAME_DECISION_COLUMNS, "decision_type"),
        _table("players", PLAYER_COLUMNS, "player_id", "team_id"),
        _table(
            "player_positions",
            PLAYER_POSITION_COLUMNS,
            "player_id",
            "team_id",
            "position_sequence",
        ),
        _table("player_batting", PLAYER_BATTING_COLUMNS, "player_id", "team_id"),
        _table("player_pitching", PLAYER_PITCHING_COLUMNS, "player_id", "team_id"),
        _table("player_fielding", PLAYER_FIELDING_COLUMNS, "player_id", "team_id"),
        _table("team_batting", TEAM_BATTING_COLUMNS, "team_id"),
        _table("team_pitching", TEAM_PITCHING_COLUMNS, "team_id"),
        _table("team_fielding", TEAM_FIELDING_COLUMNS, "team_id"),
    )
}
