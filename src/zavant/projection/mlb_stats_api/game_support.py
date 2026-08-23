"""Stats API game-team, inning, official, and decision projections."""

from __future__ import annotations

from typing import Any, Dict, List

from zavant.projection.mlb_stats_api._values import (
    array_value,
    boolean_value,
    integer_value,
    object_value,
    player_id,
    string_value,
)
from zavant.projection.contracts import ProjectionContractError, ProjectionRow


TableRows = Dict[str, List[ProjectionRow]]


def project_game_support(
    payload: Dict[str, Any], identity: ProjectionRow
) -> TableRows:
    """Project descriptive and line-score tables at their natural grains."""

    game_data = object_value(payload.get("gameData"), "gameData", required=True)
    live_data = object_value(payload.get("liveData"), "liveData", required=True)
    source_teams = object_value(
        game_data.get("teams"), "gameData.teams", required=True
    )
    linescore = object_value(live_data.get("linescore"), "liveData.linescore")
    line_teams = object_value(linescore.get("teams"), "liveData.linescore.teams")
    boxscore = object_value(live_data.get("boxscore"), "liveData.boxscore", required=True)

    team_rows = []
    for side in ("away", "home"):
        team_rows.append(
            _project_team(
                object_value(
                    source_teams.get(side), f"gameData.teams.{side}", required=True
                ),
                object_value(line_teams.get(side), f"liveData.linescore.teams.{side}"),
                side,
                identity,
            )
        )

    inning_rows = []
    for inning_index, inning_value in enumerate(
        array_value(linescore.get("innings"), "liveData.linescore.innings")
    ):
        path = f"liveData.linescore.innings[{inning_index}]"
        inning = object_value(inning_value, path, required=True)
        inning_rows.append(_project_inning(inning, path, identity))

    official_rows = []
    for official_index, official_value in enumerate(
        array_value(boxscore.get("officials"), "liveData.boxscore.officials")
    ):
        path = f"liveData.boxscore.officials[{official_index}]"
        official_record = object_value(official_value, path, required=True)
        official = object_value(
            official_record.get("official"), f"{path}.official", required=True
        )
        source_id = player_id(official, f"{path}.official", required=True)
        official_type = string_value(
            official_record.get("officialType"), f"{path}.officialType", required=True
        )
        if source_id is None or official_type is None:
            raise ProjectionContractError(f"{path} has null required official values")
        official_rows.append(
            {
                **identity,
                "official_index": official_index,
                "official_type": official_type,
                "official_id": source_id,
                "official_name": string_value(
                    official.get("fullName"), f"{path}.official.fullName"
                ),
            }
        )

    decision_rows = []
    decisions = object_value(live_data.get("decisions"), "liveData.decisions")
    for decision_type in ("winner", "loser", "save"):
        decision = object_value(
            decisions.get(decision_type), f"liveData.decisions.{decision_type}"
        )
        if not decision:
            continue
        source_id = player_id(
            decision, f"liveData.decisions.{decision_type}", required=True
        )
        if source_id is None:
            raise ProjectionContractError(
                f"liveData.decisions.{decision_type}.id must not be null"
            )
        decision_rows.append(
            {
                **identity,
                "decision_type": decision_type,
                "player_id": source_id,
                "player_name": string_value(
                    decision.get("fullName"),
                    f"liveData.decisions.{decision_type}.fullName",
                ),
            }
        )

    return {
        "game_teams": team_rows,
        "innings": inning_rows,
        "game_officials": official_rows,
        "game_decisions": decision_rows,
    }


def _project_team(
    team: Dict[str, Any],
    line_team: Dict[str, Any],
    side: str,
    identity: ProjectionRow,
) -> ProjectionRow:
    path = f"gameData.teams.{side}"
    team_id = integer_value(team.get("id"), f"{path}.id", required=True)
    team_name = string_value(team.get("name"), f"{path}.name", required=True)
    if team_id is None or team_name is None:
        raise ProjectionContractError(f"{path} has null required team values")
    league = object_value(team.get("league"), f"{path}.league")
    division = object_value(team.get("division"), f"{path}.division")
    venue = object_value(team.get("venue"), f"{path}.venue")
    record = object_value(team.get("record"), f"{path}.record")
    league_record = object_value(record.get("leagueRecord"), f"{path}.record.leagueRecord")
    return {
        **identity,
        "team_side": side,
        "team_id": team_id,
        "team_name": team_name,
        "team_code": string_value(team.get("teamCode"), f"{path}.teamCode"),
        "file_code": string_value(team.get("fileCode"), f"{path}.fileCode"),
        "abbreviation": string_value(team.get("abbreviation"), f"{path}.abbreviation"),
        "team_name_short": string_value(team.get("teamName"), f"{path}.teamName"),
        "location_name": string_value(team.get("locationName"), f"{path}.locationName"),
        "short_name": string_value(team.get("shortName"), f"{path}.shortName"),
        "franchise_name": string_value(
            team.get("franchiseName"), f"{path}.franchiseName"
        ),
        "club_name": string_value(team.get("clubName"), f"{path}.clubName"),
        "first_year_of_play": string_value(
            team.get("firstYearOfPlay"), f"{path}.firstYearOfPlay"
        ),
        "active": boolean_value(team.get("active"), f"{path}.active"),
        "league_id": integer_value(league.get("id"), f"{path}.league.id"),
        "league_name": string_value(league.get("name"), f"{path}.league.name"),
        "division_id": integer_value(division.get("id"), f"{path}.division.id"),
        "division_name": string_value(division.get("name"), f"{path}.division.name"),
        "venue_id": integer_value(venue.get("id"), f"{path}.venue.id"),
        "venue_name": string_value(venue.get("name"), f"{path}.venue.name"),
        "games_played": integer_value(record.get("gamesPlayed"), f"{path}.record.gamesPlayed"),
        "wins": integer_value(league_record.get("wins"), f"{path}.record.leagueRecord.wins"),
        "losses": integer_value(
            league_record.get("losses"), f"{path}.record.leagueRecord.losses"
        ),
        "ties": integer_value(league_record.get("ties"), f"{path}.record.leagueRecord.ties"),
        "winning_percentage": string_value(
            record.get("winningPercentage"), f"{path}.record.winningPercentage"
        ),
        "division_leader": boolean_value(
            record.get("divisionLeader"), f"{path}.record.divisionLeader"
        ),
        "score": integer_value(line_team.get("runs"), f"liveData.linescore.teams.{side}.runs"),
    }


def _project_inning(
    inning: Dict[str, Any], path: str, identity: ProjectionRow
) -> ProjectionRow:
    number = integer_value(inning.get("num"), f"{path}.num", required=True)
    if number is None:
        raise ProjectionContractError(f"{path}.num must not be null")
    away = object_value(inning.get("away"), f"{path}.away", required=True)
    home = object_value(inning.get("home"), f"{path}.home", required=True)
    return {
        **identity,
        "inning_number": number,
        "ordinal": string_value(inning.get("ordinalNum"), f"{path}.ordinalNum"),
        "away_runs": integer_value(away.get("runs"), f"{path}.away.runs"),
        "away_hits": integer_value(away.get("hits"), f"{path}.away.hits"),
        "away_errors": integer_value(away.get("errors"), f"{path}.away.errors"),
        "away_left_on_base": integer_value(
            away.get("leftOnBase"), f"{path}.away.leftOnBase"
        ),
        "home_runs": integer_value(home.get("runs"), f"{path}.home.runs"),
        "home_hits": integer_value(home.get("hits"), f"{path}.home.hits"),
        "home_errors": integer_value(home.get("errors"), f"{path}.home.errors"),
        "home_left_on_base": integer_value(
            home.get("leftOnBase"), f"{path}.home.leftOnBase"
        ),
    }
