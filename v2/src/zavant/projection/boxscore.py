"""Player identity, positions, and player/team game-stat projections."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from zavant.projection._values import (
    array_value,
    boolean_value,
    date_value,
    float_value,
    integer_value,
    object_value,
    string_value,
)
from zavant.projection.contracts import ProjectionContractError, ProjectionRow
from zavant.projection.stat_fields import (
    BATTING_FIELDS,
    FIELDING_FIELDS,
    PITCHING_FIELDS,
    StatField,
)


TableRows = Dict[str, List[ProjectionRow]]
BoxscorePlayer = Tuple[str, int, Dict[str, Any]]


def project_boxscore(payload: Dict[str, Any], identity: ProjectionRow) -> TableRows:
    """Project game-scoped players, positions, and non-season statistics."""

    game_data = object_value(payload.get("gameData"), "gameData", required=True)
    live_data = object_value(payload.get("liveData"), "liveData", required=True)
    people = object_value(game_data.get("players"), "gameData.players", required=True)
    boxscore = object_value(live_data.get("boxscore"), "liveData.boxscore", required=True)
    boxscore_teams = object_value(
        boxscore.get("teams"), "liveData.boxscore.teams", required=True
    )
    box_players = _boxscore_players(boxscore_teams)

    rows: TableRows = {
        "players": [],
        "player_positions": [],
        "player_batting": [],
        "player_pitching": [],
        "player_fielding": [],
        "team_batting": [],
        "team_pitching": [],
        "team_fielding": [],
    }
    observed_player_ids = set()
    for person_key, person_value in people.items():
        path = f"gameData.players.{person_key}"
        person = object_value(person_value, path, required=True)
        source_player_id = integer_value(person.get("id"), f"{path}.id", required=True)
        if source_player_id is None:
            raise ProjectionContractError(f"{path}.id must not be null")
        if source_player_id in observed_player_ids:
            raise ProjectionContractError(f"gameData.players duplicates {source_player_id}")
        observed_player_ids.add(source_player_id)
        box_entry = box_players.get(source_player_id)
        side = box_entry[0] if box_entry is not None else None
        box_team_id = box_entry[1] if box_entry is not None else None
        box_player = box_entry[2] if box_entry is not None else {}
        rows["players"].append(
            _project_player(
                person,
                path,
                box_player,
                box_team_id,
                side,
                identity,
            )
        )
        rows["player_positions"].extend(
            _project_positions(
                box_player,
                source_player_id,
                box_team_id,
                side,
                identity,
            )
        )
        stats = object_value(
            box_player.get("stats"), f"liveData.boxscore player {source_player_id}.stats"
        )
        for output_name, source_name, fields in (
            ("player_batting", "batting", BATTING_FIELDS),
            ("player_pitching", "pitching", PITCHING_FIELDS),
            ("player_fielding", "fielding", FIELDING_FIELDS),
        ):
            source_stats = object_value(
                stats.get(source_name),
                f"liveData.boxscore player {source_player_id}.stats.{source_name}",
            )
            if source_stats:
                rows[output_name].append(
                    {
                        **identity,
                        "player_id": source_player_id,
                        **_project_stats(
                            source_stats,
                            f"liveData.boxscore player {source_player_id}.stats.{source_name}",
                            fields,
                        ),
                    }
                )

    for side in ("away", "home"):
        path = f"liveData.boxscore.teams.{side}"
        box_team = object_value(boxscore_teams.get(side), path, required=True)
        team = object_value(box_team.get("team"), f"{path}.team", required=True)
        team_id = integer_value(team.get("id"), f"{path}.team.id", required=True)
        if team_id is None:
            raise ProjectionContractError(f"{path}.team.id must not be null")
        team_stats = object_value(
            box_team.get("teamStats"), f"{path}.teamStats", required=True
        )
        for output_name, source_name, fields in (
            ("team_batting", "batting", BATTING_FIELDS),
            ("team_pitching", "pitching", PITCHING_FIELDS),
            ("team_fielding", "fielding", FIELDING_FIELDS),
        ):
            source_stats = object_value(
                team_stats.get(source_name), f"{path}.teamStats.{source_name}", required=True
            )
            rows[output_name].append(
                {
                    **identity,
                    "team_id": team_id,
                    "team_side": side,
                    **_project_stats(
                        source_stats, f"{path}.teamStats.{source_name}", fields
                    ),
                }
            )
    return rows


def _boxscore_players(teams: Dict[str, Any]) -> Dict[int, BoxscorePlayer]:
    players: Dict[int, BoxscorePlayer] = {}
    for side in ("away", "home"):
        team = object_value(
            teams.get(side), f"liveData.boxscore.teams.{side}", required=True
        )
        team_data = object_value(
            team.get("team"), f"liveData.boxscore.teams.{side}.team", required=True
        )
        team_id = integer_value(
            team_data.get("id"), f"liveData.boxscore.teams.{side}.team.id", required=True
        )
        if team_id is None:
            raise ProjectionContractError(
                f"liveData.boxscore.teams.{side}.team.id must not be null"
            )
        source_players = object_value(
            team.get("players"), f"liveData.boxscore.teams.{side}.players", required=True
        )
        for key, value in source_players.items():
            path = f"liveData.boxscore.teams.{side}.players.{key}"
            player = object_value(value, path, required=True)
            person = object_value(player.get("person"), f"{path}.person", required=True)
            player_id = integer_value(person.get("id"), f"{path}.person.id", required=True)
            if player_id is None:
                raise ProjectionContractError(f"{path}.person.id must not be null")
            if player_id in players:
                players[player_id] = _select_boxscore_player(
                    player_id,
                    players[player_id],
                    (side, team_id, player),
                )
            else:
                players[player_id] = (side, team_id, player)
    return players


def _select_boxscore_player(
    player_id: int,
    existing: BoxscorePlayer,
    candidate: BoxscorePlayer,
) -> BoxscorePlayer:
    """Discard a stale roster entry when only one duplicate played in the game."""

    existing_participated = _has_game_participation(existing[2])
    candidate_participated = _has_game_participation(candidate[2])
    if existing_participated != candidate_participated:
        return existing if existing_participated else candidate
    raise ProjectionContractError(
        f"boxscore contains ambiguous duplicate player {player_id}"
    )


def _has_game_participation(player: Dict[str, Any]) -> bool:
    stats = object_value(player.get("stats"), "duplicate boxscore player.stats")
    has_stats = any(
        object_value(stats.get(category), f"duplicate boxscore player.stats.{category}")
        for category in ("batting", "pitching", "fielding")
    )
    return bool(
        has_stats
        or player.get("battingOrder") is not None
        or array_value(
            player.get("allPositions"), "duplicate boxscore player.allPositions"
        )
    )


def _project_player(
    person: Dict[str, Any],
    path: str,
    box_player: Dict[str, Any],
    box_team_id: Optional[int],
    side: Optional[str],
    identity: ProjectionRow,
) -> ProjectionRow:
    player_id = integer_value(person.get("id"), f"{path}.id", required=True)
    full_name = string_value(person.get("fullName"), f"{path}.fullName", required=True)
    if player_id is None or full_name is None:
        raise ProjectionContractError(f"{path} has null required player values")
    primary_position = object_value(
        person.get("primaryPosition"), f"{path}.primaryPosition"
    )
    box_position = object_value(
        box_player.get("position"), f"boxscore player {player_id}.position"
    )
    status = object_value(box_player.get("status"), f"boxscore player {player_id}.status")
    game_status = object_value(
        box_player.get("gameStatus"), f"boxscore player {player_id}.gameStatus"
    )
    bat_side = object_value(person.get("batSide"), f"{path}.batSide")
    pitch_hand = object_value(person.get("pitchHand"), f"{path}.pitchHand")
    return {
        **identity,
        "player_id": player_id,
        "team_id": box_team_id,
        "team_side": side,
        "full_name": full_name,
        "first_name": string_value(person.get("firstName"), f"{path}.firstName"),
        "last_name": string_value(person.get("lastName"), f"{path}.lastName"),
        "use_name": string_value(person.get("useName"), f"{path}.useName"),
        "use_last_name": string_value(person.get("useLastName"), f"{path}.useLastName"),
        "middle_name": string_value(person.get("middleName"), f"{path}.middleName"),
        "boxscore_name": string_value(person.get("boxscoreName"), f"{path}.boxscoreName"),
        "nickname": string_value(person.get("nickName"), f"{path}.nickName"),
        "pronunciation": string_value(person.get("pronunciation"), f"{path}.pronunciation"),
        "name_slug": string_value(person.get("nameSlug"), f"{path}.nameSlug"),
        "primary_number": string_value(person.get("primaryNumber"), f"{path}.primaryNumber"),
        "jersey_number": string_value(
            box_player.get("jerseyNumber"), f"boxscore player {player_id}.jerseyNumber"
        ),
        "batting_order": string_value(
            box_player.get("battingOrder"), f"boxscore player {player_id}.battingOrder"
        ),
        "birth_date": date_value(person.get("birthDate"), f"{path}.birthDate"),
        "birth_city": string_value(person.get("birthCity"), f"{path}.birthCity"),
        "birth_state_province": string_value(
            person.get("birthStateProvince"), f"{path}.birthStateProvince"
        ),
        "birth_country": string_value(person.get("birthCountry"), f"{path}.birthCountry"),
        "height": string_value(person.get("height"), f"{path}.height"),
        "weight": integer_value(person.get("weight"), f"{path}.weight"),
        "active": boolean_value(person.get("active"), f"{path}.active"),
        "gender": string_value(person.get("gender"), f"{path}.gender"),
        "draft_year": integer_value(person.get("draftYear"), f"{path}.draftYear"),
        "mlb_debut_date": date_value(
            person.get("mlbDebutDate"), f"{path}.mlbDebutDate"
        ),
        "bat_side_code": string_value(bat_side.get("code"), f"{path}.batSide.code"),
        "bat_side_description": string_value(
            bat_side.get("description"), f"{path}.batSide.description"
        ),
        "pitch_hand_code": string_value(
            pitch_hand.get("code"), f"{path}.pitchHand.code"
        ),
        "pitch_hand_description": string_value(
            pitch_hand.get("description"), f"{path}.pitchHand.description"
        ),
        **_position_values("primary_position", primary_position, f"{path}.primaryPosition"),
        **_position_values(
            "boxscore_position", box_position, f"boxscore player {player_id}.position"
        ),
        "roster_status_code": string_value(
            status.get("code"), f"boxscore player {player_id}.status.code"
        ),
        "roster_status_description": string_value(
            status.get("description"), f"boxscore player {player_id}.status.description"
        ),
        "is_current_batter": boolean_value(
            game_status.get("isCurrentBatter"),
            f"boxscore player {player_id}.gameStatus.isCurrentBatter",
        ),
        "is_current_pitcher": boolean_value(
            game_status.get("isCurrentPitcher"),
            f"boxscore player {player_id}.gameStatus.isCurrentPitcher",
        ),
        "is_on_bench": boolean_value(
            game_status.get("isOnBench"),
            f"boxscore player {player_id}.gameStatus.isOnBench",
        ),
        "is_substitute": boolean_value(
            game_status.get("isSubstitute"),
            f"boxscore player {player_id}.gameStatus.isSubstitute",
        ),
        "strike_zone_top": float_value(
            person.get("strikeZoneTop"), f"{path}.strikeZoneTop"
        ),
        "strike_zone_bottom": float_value(
            person.get("strikeZoneBottom"), f"{path}.strikeZoneBottom"
        ),
    }


def _project_positions(
    box_player: Dict[str, Any],
    player_id: int,
    box_team_id: Optional[int],
    side: Optional[str],
    identity: ProjectionRow,
) -> List[ProjectionRow]:
    rows = []
    for sequence, position_value in enumerate(
        array_value(box_player.get("allPositions"), f"boxscore player {player_id}.allPositions")
    ):
        path = f"boxscore player {player_id}.allPositions[{sequence}]"
        position = object_value(position_value, path, required=True)
        code = string_value(position.get("code"), f"{path}.code", required=True)
        if code is None:
            raise ProjectionContractError(f"{path}.code must not be null")
        rows.append(
            {
                **identity,
                "player_id": player_id,
                "position_sequence": sequence,
                "team_id": box_team_id,
                "team_side": side,
                "position_code": code,
                "position_name": string_value(position.get("name"), f"{path}.name"),
                "position_type": string_value(position.get("type"), f"{path}.type"),
                "position_abbreviation": string_value(
                    position.get("abbreviation"), f"{path}.abbreviation"
                ),
            }
        )
    return rows


def _position_values(
    prefix: str, position: Dict[str, Any], path: str
) -> Dict[str, Optional[str]]:
    return {
        f"{prefix}_code": string_value(position.get("code"), f"{path}.code"),
        f"{prefix}_name": string_value(position.get("name"), f"{path}.name"),
        f"{prefix}_type": string_value(position.get("type"), f"{path}.type"),
        f"{prefix}_abbreviation": string_value(
            position.get("abbreviation"), f"{path}.abbreviation"
        ),
    }


def _project_stats(
    stats: Dict[str, Any], path: str, fields: Tuple[StatField, ...]
) -> ProjectionRow:
    row: ProjectionRow = {}
    for output_name, source_name, kind in fields:
        value = stats.get(source_name)
        row[output_name] = (
            integer_value(value, f"{path}.{source_name}")
            if kind == "int32"
            else string_value(value, f"{path}.{source_name}")
        )
    return row
