"""Projection of Stats API plays, events, pitches, and batted balls."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Tuple

from zavant.projection.mlb_stats_api._values import (
    array_value,
    boolean_value,
    float_value,
    integer_value,
    object_value,
    player_id,
    string_value,
    timestamp_value,
)
from zavant.projection.contracts import ProjectionContractError, ProjectionRow


PlayProjection = Tuple[
    List[ProjectionRow],
    List[ProjectionRow],
    List[ProjectionRow],
    List[ProjectionRow],
    Dict[str, int],
]


def project_play_data(
    payload: Dict[str, Any],
    identity: ProjectionRow,
) -> PlayProjection:
    """Project the core play spine, pitches, and batted balls."""

    game_data = object_value(payload.get("gameData"), "gameData", required=True)
    live_data = object_value(payload.get("liveData"), "liveData", required=True)
    teams = object_value(game_data.get("teams"), "gameData.teams", required=True)
    away_team = object_value(teams.get("away"), "gameData.teams.away", required=True)
    home_team = object_value(teams.get("home"), "gameData.teams.home", required=True)
    away_team_id = integer_value(
        away_team.get("id"), "gameData.teams.away.id", required=True
    )
    home_team_id = integer_value(
        home_team.get("id"), "gameData.teams.home.id", required=True
    )
    if away_team_id is None or home_team_id is None:
        raise ProjectionContractError("game team IDs must not be null")

    plays_container = object_value(
        live_data.get("plays"), "liveData.plays", required=True
    )
    source_plays = array_value(
        plays_container.get("allPlays"), "liveData.plays.allPlays", required=True
    )
    plays: List[ProjectionRow] = []
    events: List[ProjectionRow] = []
    pitches: List[ProjectionRow] = []
    batted_balls: List[ProjectionRow] = []
    event_kinds: Counter[str] = Counter()

    for play_number, source_play_value in enumerate(source_plays):
        play_path = f"liveData.plays.allPlays[{play_number}]"
        source_play = object_value(source_play_value, play_path, required=True)
        play_row, at_bat_index = _project_play(
            source_play,
            play_path,
            identity,
            away_team_id,
            home_team_id,
        )
        plays.append(play_row)

        source_events = array_value(
            source_play.get("playEvents"), f"{play_path}.playEvents", required=True
        )
        for event_number, source_event_value in enumerate(source_events):
            event_path = f"{play_path}.playEvents[{event_number}]"
            source_event = object_value(
                source_event_value, event_path, required=True
            )
            event_row, event_index, event_kind = _project_event(
                source_event,
                event_path,
                identity,
                at_bat_index,
            )
            events.append(event_row)
            event_kinds[event_kind] += 1
            if event_kind == "pitch":
                pitches.append(
                    _project_pitch(
                        source_event,
                        event_path,
                        identity,
                        at_bat_index,
                        event_index,
                    )
                )
            if "hitData" in source_event:
                if event_kind != "pitch":
                    raise ProjectionContractError(
                        f"{event_path}.hitData belongs to a non-pitch event"
                    )
                batted_balls.append(
                    _project_batted_ball(
                        source_event,
                        event_path,
                        identity,
                        at_bat_index,
                        event_index,
                    )
                )

    return plays, events, pitches, batted_balls, dict(event_kinds)


def _project_play(
    play: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    away_team_id: int,
    home_team_id: int,
) -> Tuple[ProjectionRow, int]:
    result = object_value(play.get("result"), f"{path}.result", required=True)
    about = object_value(play.get("about"), f"{path}.about", required=True)
    count = object_value(play.get("count"), f"{path}.count", required=True)
    matchup = object_value(play.get("matchup"), f"{path}.matchup", required=True)
    splits = object_value(matchup.get("splits"), f"{path}.matchup.splits")
    at_bat_index = integer_value(
        play.get("atBatIndex"), f"{path}.atBatIndex", required=True
    )
    is_top_inning = boolean_value(
        about.get("isTopInning"), f"{path}.about.isTopInning", required=True
    )
    if at_bat_index is None or is_top_inning is None:
        raise ProjectionContractError(f"{path} has a null required play key")
    about_index = integer_value(
        about.get("atBatIndex"), f"{path}.about.atBatIndex", required=True
    )
    if about_index != at_bat_index:
        raise ProjectionContractError(f"{path} contains inconsistent atBatIndex")

    row: Dict[str, Any] = dict(identity)
    row.update(
        {
            "at_bat_index": at_bat_index,
            "play_type": string_value(result.get("type"), f"{path}.result.type"),
            "event": string_value(result.get("event"), f"{path}.result.event"),
            "event_type": string_value(
                result.get("eventType"), f"{path}.result.eventType"
            ),
            "description": string_value(
                result.get("description"), f"{path}.result.description"
            ),
            "rbi": integer_value(result.get("rbi"), f"{path}.result.rbi"),
            "away_score": integer_value(
                result.get("awayScore"), f"{path}.result.awayScore"
            ),
            "home_score": integer_value(
                result.get("homeScore"), f"{path}.result.homeScore"
            ),
            "is_out": boolean_value(result.get("isOut"), f"{path}.result.isOut"),
            "inning": integer_value(
                about.get("inning"), f"{path}.about.inning", required=True
            ),
            "half_inning": string_value(
                about.get("halfInning"), f"{path}.about.halfInning", required=True
            ),
            "is_top_inning": is_top_inning,
            "started_at": timestamp_value(
                about.get("startTime"), f"{path}.about.startTime"
            ),
            "ended_at": timestamp_value(
                about.get("endTime"), f"{path}.about.endTime"
            ),
            "is_complete": boolean_value(
                about.get("isComplete"), f"{path}.about.isComplete"
            ),
            "is_scoring_play": boolean_value(
                about.get("isScoringPlay"), f"{path}.about.isScoringPlay"
            ),
            "has_review": boolean_value(
                about.get("hasReview"), f"{path}.about.hasReview"
            ),
            "has_out": boolean_value(
                about.get("hasOut"), f"{path}.about.hasOut"
            ),
            "captivating_index": integer_value(
                about.get("captivatingIndex"), f"{path}.about.captivatingIndex"
            ),
            "balls": integer_value(count.get("balls"), f"{path}.count.balls"),
            "strikes": integer_value(
                count.get("strikes"), f"{path}.count.strikes"
            ),
            "outs": integer_value(count.get("outs"), f"{path}.count.outs"),
            "batter_id": player_id(
                matchup.get("batter"), f"{path}.matchup.batter", required=True
            ),
            "pitcher_id": player_id(
                matchup.get("pitcher"), f"{path}.matchup.pitcher", required=True
            ),
            "bat_side_code": _nested_string(
                matchup, "batSide", "code", f"{path}.matchup"
            ),
            "pitch_hand_code": _nested_string(
                matchup, "pitchHand", "code", f"{path}.matchup"
            ),
            "offense_team_id": away_team_id if is_top_inning else home_team_id,
            "defense_team_id": home_team_id if is_top_inning else away_team_id,
            "post_on_first_id": player_id(
                matchup.get("postOnFirst"), f"{path}.matchup.postOnFirst"
            ),
            "post_on_second_id": player_id(
                matchup.get("postOnSecond"), f"{path}.matchup.postOnSecond"
            ),
            "post_on_third_id": player_id(
                matchup.get("postOnThird"), f"{path}.matchup.postOnThird"
            ),
            "batter_split": string_value(
                splits.get("batter"), f"{path}.matchup.splits.batter"
            ),
            "pitcher_split": string_value(
                splits.get("pitcher"), f"{path}.matchup.splits.pitcher"
            ),
            "men_on_base_split": string_value(
                splits.get("menOnBase"), f"{path}.matchup.splits.menOnBase"
            ),
        }
    )
    return row, at_bat_index


def _project_event(
    event: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
) -> Tuple[ProjectionRow, int, str]:
    details = object_value(event.get("details"), f"{path}.details", required=True)
    count = object_value(event.get("count"), f"{path}.count", required=True)
    position = object_value(event.get("position"), f"{path}.position")
    event_index = integer_value(event.get("index"), f"{path}.index", required=True)
    event_kind = string_value(event.get("type"), f"{path}.type", required=True)
    if event_index is None or event_kind is None:
        raise ProjectionContractError(f"{path} has a null required event key")

    row: Dict[str, Any] = dict(identity)
    row.update(
        {
            "at_bat_index": at_bat_index,
            "event_index": event_index,
            "event_kind": event_kind,
            "play_id": string_value(event.get("playId"), f"{path}.playId"),
            "pitch_number": integer_value(
                event.get("pitchNumber"), f"{path}.pitchNumber"
            ),
            "started_at": timestamp_value(
                event.get("startTime"), f"{path}.startTime"
            ),
            "ended_at": timestamp_value(event.get("endTime"), f"{path}.endTime"),
            "is_pitch": boolean_value(event.get("isPitch"), f"{path}.isPitch"),
            "is_base_running_play": boolean_value(
                event.get("isBaseRunningPlay"), f"{path}.isBaseRunningPlay"
            ),
            "is_substitution": boolean_value(
                event.get("isSubstitution"), f"{path}.isSubstitution"
            ),
            "balls": integer_value(count.get("balls"), f"{path}.count.balls"),
            "strikes": integer_value(
                count.get("strikes"), f"{path}.count.strikes"
            ),
            "outs": integer_value(count.get("outs"), f"{path}.count.outs"),
            "description": string_value(
                details.get("description"), f"{path}.details.description"
            ),
            "event": string_value(details.get("event"), f"{path}.details.event"),
            "event_type": string_value(
                details.get("eventType"), f"{path}.details.eventType"
            ),
            "event_code": string_value(details.get("code"), f"{path}.details.code"),
            "is_in_play": boolean_value(
                details.get("isInPlay"), f"{path}.details.isInPlay"
            ),
            "is_strike": boolean_value(
                details.get("isStrike"), f"{path}.details.isStrike"
            ),
            "is_ball": boolean_value(
                details.get("isBall"), f"{path}.details.isBall"
            ),
            "is_out": boolean_value(details.get("isOut"), f"{path}.details.isOut"),
            "is_scoring_play": boolean_value(
                details.get("isScoringPlay"), f"{path}.details.isScoringPlay"
            ),
            "has_review": boolean_value(
                details.get("hasReview"), f"{path}.details.hasReview"
            ),
            "away_score": integer_value(
                details.get("awayScore"), f"{path}.details.awayScore"
            ),
            "home_score": integer_value(
                details.get("homeScore"), f"{path}.details.homeScore"
            ),
            "player_id": player_id(event.get("player"), f"{path}.player"),
            "replaced_player_id": player_id(
                event.get("replacedPlayer"), f"{path}.replacedPlayer"
            ),
            "position_code": string_value(
                position.get("code"), f"{path}.position.code"
            ),
            "position_name": string_value(
                position.get("name"), f"{path}.position.name"
            ),
            "position_abbreviation": string_value(
                position.get("abbreviation"), f"{path}.position.abbreviation"
            ),
            "batting_order": string_value(
                event.get("battingOrder"), f"{path}.battingOrder"
            ),
            "base_number": integer_value(event.get("base"), f"{path}.base"),
            "injury_type": string_value(
                event.get("injuryType"), f"{path}.injuryType"
            ),
        }
    )
    return row, event_index, event_kind


def _project_pitch(
    event: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    event_index: int,
) -> ProjectionRow:
    details = object_value(event.get("details"), f"{path}.details", required=True)
    count = object_value(event.get("count"), f"{path}.count", required=True)
    pitch_data = object_value(
        event.get("pitchData"), f"{path}.pitchData", required=True
    )
    call = object_value(details.get("call"), f"{path}.details.call")
    pitch_type = object_value(details.get("type"), f"{path}.details.type")
    coordinates = object_value(
        pitch_data.get("coordinates"), f"{path}.pitchData.coordinates"
    )
    breaks = object_value(pitch_data.get("breaks"), f"{path}.pitchData.breaks")

    row: Dict[str, Any] = dict(identity)
    row.update(
        {
            "at_bat_index": at_bat_index,
            "event_index": event_index,
            "play_id": string_value(event.get("playId"), f"{path}.playId"),
            "pitch_number": integer_value(
                event.get("pitchNumber"), f"{path}.pitchNumber"
            ),
            "call_code": string_value(call.get("code"), f"{path}.details.call.code"),
            "call_description": string_value(
                call.get("description"), f"{path}.details.call.description"
            ),
            "pitch_type_code": string_value(
                pitch_type.get("code"), f"{path}.details.type.code"
            ),
            "pitch_type_description": string_value(
                pitch_type.get("description"), f"{path}.details.type.description"
            ),
            "description": string_value(
                details.get("description"), f"{path}.details.description"
            ),
            "is_in_play": boolean_value(
                details.get("isInPlay"), f"{path}.details.isInPlay"
            ),
            "is_strike": boolean_value(
                details.get("isStrike"), f"{path}.details.isStrike"
            ),
            "is_ball": boolean_value(
                details.get("isBall"), f"{path}.details.isBall"
            ),
            "is_out": boolean_value(details.get("isOut"), f"{path}.details.isOut"),
            "has_review": boolean_value(
                details.get("hasReview"), f"{path}.details.hasReview"
            ),
            "balls": integer_value(count.get("balls"), f"{path}.count.balls"),
            "strikes": integer_value(
                count.get("strikes"), f"{path}.count.strikes"
            ),
            "outs": integer_value(count.get("outs"), f"{path}.count.outs"),
            "start_speed": float_value(
                pitch_data.get("startSpeed"), f"{path}.pitchData.startSpeed"
            ),
            "end_speed": float_value(
                pitch_data.get("endSpeed"), f"{path}.pitchData.endSpeed"
            ),
            "strike_zone_top": float_value(
                pitch_data.get("strikeZoneTop"), f"{path}.pitchData.strikeZoneTop"
            ),
            "strike_zone_bottom": float_value(
                pitch_data.get("strikeZoneBottom"),
                f"{path}.pitchData.strikeZoneBottom",
            ),
            "strike_zone_width": float_value(
                pitch_data.get("strikeZoneWidth"),
                f"{path}.pitchData.strikeZoneWidth",
            ),
            "strike_zone_depth": float_value(
                pitch_data.get("strikeZoneDepth"),
                f"{path}.pitchData.strikeZoneDepth",
            ),
            "zone": integer_value(pitch_data.get("zone"), f"{path}.pitchData.zone"),
            "type_confidence": float_value(
                pitch_data.get("typeConfidence"),
                f"{path}.pitchData.typeConfidence",
            ),
            "plate_time": float_value(
                pitch_data.get("plateTime"), f"{path}.pitchData.plateTime"
            ),
            "extension": float_value(
                pitch_data.get("extension"), f"{path}.pitchData.extension"
            ),
            **_coordinate_fields(coordinates, path),
            **_break_fields(breaks, path),
        }
    )
    return row


def _project_batted_ball(
    event: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    event_index: int,
) -> ProjectionRow:
    hit_data = object_value(event.get("hitData"), f"{path}.hitData", required=True)
    coordinates = object_value(
        hit_data.get("coordinates"), f"{path}.hitData.coordinates"
    )
    row: Dict[str, Any] = dict(identity)
    row.update(
        {
            "at_bat_index": at_bat_index,
            "event_index": event_index,
            "play_id": string_value(event.get("playId"), f"{path}.playId"),
            "pitch_number": integer_value(
                event.get("pitchNumber"), f"{path}.pitchNumber"
            ),
            "launch_speed": float_value(
                hit_data.get("launchSpeed"), f"{path}.hitData.launchSpeed"
            ),
            "launch_angle": float_value(
                hit_data.get("launchAngle"), f"{path}.hitData.launchAngle"
            ),
            "total_distance": float_value(
                hit_data.get("totalDistance"), f"{path}.hitData.totalDistance"
            ),
            "trajectory": string_value(
                hit_data.get("trajectory"), f"{path}.hitData.trajectory"
            ),
            "hardness": string_value(
                hit_data.get("hardness"), f"{path}.hitData.hardness"
            ),
            "location": string_value(
                hit_data.get("location"), f"{path}.hitData.location"
            ),
            "coordinate_x": float_value(
                coordinates.get("coordX"), f"{path}.hitData.coordinates.coordX"
            ),
            "coordinate_y": float_value(
                coordinates.get("coordY"), f"{path}.hitData.coordinates.coordY"
            ),
        }
    )
    return row


def _nested_string(
    parent: Dict[str, Any],
    object_name: str,
    value_name: str,
    path: str,
) -> Any:
    child = object_value(parent.get(object_name), f"{path}.{object_name}")
    return string_value(child.get(value_name), f"{path}.{object_name}.{value_name}")


def _coordinate_fields(coordinates: Dict[str, Any], path: str) -> Dict[str, Any]:
    mapping = {
        "coordinate_a_x": "aX",
        "coordinate_a_y": "aY",
        "coordinate_a_z": "aZ",
        "coordinate_pfx_x": "pfxX",
        "coordinate_pfx_z": "pfxZ",
        "coordinate_p_x": "pX",
        "coordinate_p_z": "pZ",
        "coordinate_v_x0": "vX0",
        "coordinate_v_y0": "vY0",
        "coordinate_v_z0": "vZ0",
        "coordinate_x": "x",
        "coordinate_y": "y",
        "coordinate_x0": "x0",
        "coordinate_y0": "y0",
        "coordinate_z0": "z0",
    }
    return {
        target: float_value(
            coordinates.get(source), f"{path}.pitchData.coordinates.{source}"
        )
        for target, source in mapping.items()
    }


def _break_fields(breaks: Dict[str, Any], path: str) -> Dict[str, Any]:
    mapping = {
        "break_angle": "breakAngle",
        "break_length": "breakLength",
        "break_y": "breakY",
        "break_vertical": "breakVertical",
        "break_vertical_induced": "breakVerticalInduced",
        "break_horizontal": "breakHorizontal",
        "spin_rate": "spinRate",
        "spin_direction": "spinDirection",
    }
    return {
        target: float_value(breaks.get(source), f"{path}.pitchData.breaks.{source}")
        for target, source in mapping.items()
    }
