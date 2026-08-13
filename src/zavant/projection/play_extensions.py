"""Sparse play-event, runner, review, and fielding projections."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from zavant.projection._values import (
    array_value,
    boolean_value,
    integer_value,
    object_value,
    player_id,
    string_value,
    timestamp_value,
)
from zavant.projection.contracts import ProjectionContractError, ProjectionRow


TableRows = Dict[str, List[ProjectionRow]]


def project_play_extensions(
    payload: Dict[str, Any], identity: ProjectionRow
) -> TableRows:
    """Project sparse event families and ordered children of plays."""

    live_data = object_value(payload.get("liveData"), "liveData", required=True)
    plays_container = object_value(
        live_data.get("plays"), "liveData.plays", required=True
    )
    source_plays = array_value(
        plays_container.get("allPlays"), "liveData.plays.allPlays", required=True
    )
    rows: TableRows = {
        "actions": [],
        "substitutions": [],
        "disengagements": [],
        "non_pitch_calls": [],
        "runner_movements": [],
        "fielding_credits": [],
        "reviews": [],
        "rule_violations": [],
    }
    for play_number, play_value in enumerate(source_plays):
        path = f"liveData.plays.allPlays[{play_number}]"
        play = object_value(play_value, path, required=True)
        at_bat_index = integer_value(
            play.get("atBatIndex"), f"{path}.atBatIndex", required=True
        )
        if at_bat_index is None:
            raise ProjectionContractError(f"{path}.atBatIndex must not be null")

        play_review = object_value(play.get("reviewDetails"), f"{path}.reviewDetails")
        if play_review:
            rows["reviews"].extend(
                _project_reviews(
                    play_review, f"{path}.reviewDetails", identity, at_bat_index, None
                )
            )

        events = array_value(play.get("playEvents"), f"{path}.playEvents", required=True)
        for event_number, event_value in enumerate(events):
            event_path = f"{path}.playEvents[{event_number}]"
            event = object_value(event_value, event_path, required=True)
            event_index = integer_value(
                event.get("index"), f"{event_path}.index", required=True
            )
            event_kind = string_value(
                event.get("type"), f"{event_path}.type", required=True
            )
            if event_index is None or event_kind is None:
                raise ProjectionContractError(f"{event_path} has a null event key")
            if event_kind == "action":
                rows["actions"].append(
                    _project_action(event, event_path, identity, at_bat_index, event_index)
                )
                if event.get("isSubstitution") is True:
                    rows["substitutions"].append(
                        _project_substitution(
                            event, event_path, identity, at_bat_index, event_index
                        )
                    )
            elif event_kind in {"pickoff", "stepoff"}:
                rows["disengagements"].append(
                    _project_disengagement(
                        event,
                        event_path,
                        identity,
                        at_bat_index,
                        event_index,
                        event_kind,
                    )
                )
            elif event_kind == "no_pitch":
                rows["non_pitch_calls"].append(
                    _project_non_pitch_call(
                        event, event_path, identity, at_bat_index, event_index
                    )
                )

            review = object_value(
                event.get("reviewDetails"), f"{event_path}.reviewDetails"
            )
            if review:
                rows["reviews"].extend(
                    _project_reviews(
                        review,
                        f"{event_path}.reviewDetails",
                        identity,
                        at_bat_index,
                        event_index,
                    )
                )
            details = object_value(event.get("details"), f"{event_path}.details")
            violation = object_value(
                details.get("violation"), f"{event_path}.details.violation"
            )
            if violation:
                rows["rule_violations"].append(
                    _project_violation(
                        violation,
                        f"{event_path}.details.violation",
                        identity,
                        at_bat_index,
                        event_index,
                    )
                )

        for runner_index, runner_value in enumerate(
            array_value(play.get("runners"), f"{path}.runners", required=True)
        ):
            runner_path = f"{path}.runners[{runner_index}]"
            runner = object_value(runner_value, runner_path, required=True)
            movement_row, credit_rows = _project_runner(
                runner, runner_path, identity, at_bat_index, runner_index
            )
            rows["runner_movements"].append(movement_row)
            rows["fielding_credits"].extend(credit_rows)
    return rows


def _event_base(
    identity: ProjectionRow, at_bat_index: int, event_index: int
) -> Dict[str, Any]:
    return {
        **identity,
        "at_bat_index": at_bat_index,
        "event_index": event_index,
    }


def _project_action(
    event: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    event_index: int,
) -> ProjectionRow:
    details = object_value(event.get("details"), f"{path}.details", required=True)
    count = object_value(event.get("count"), f"{path}.count", required=True)
    position = object_value(event.get("position"), f"{path}.position")
    return {
        **_event_base(identity, at_bat_index, event_index),
        "play_id": string_value(event.get("playId"), f"{path}.playId"),
        "action_play_id": string_value(
            event.get("actionPlayId"), f"{path}.actionPlayId"
        ),
        "started_at": timestamp_value(event.get("startTime"), f"{path}.startTime"),
        "ended_at": timestamp_value(event.get("endTime"), f"{path}.endTime"),
        "event": string_value(details.get("event"), f"{path}.details.event"),
        "event_type": string_value(
            details.get("eventType"), f"{path}.details.eventType"
        ),
        "description": string_value(
            details.get("description"), f"{path}.details.description"
        ),
        "event_code": string_value(details.get("code"), f"{path}.details.code"),
        "is_substitution": boolean_value(
            event.get("isSubstitution"), f"{path}.isSubstitution"
        ),
        "is_out": boolean_value(details.get("isOut"), f"{path}.details.isOut"),
        "has_review": boolean_value(
            details.get("hasReview"), f"{path}.details.hasReview"
        ),
        "balls": integer_value(count.get("balls"), f"{path}.count.balls"),
        "strikes": integer_value(count.get("strikes"), f"{path}.count.strikes"),
        "outs": integer_value(count.get("outs"), f"{path}.count.outs"),
        "player_id": player_id(event.get("player"), f"{path}.player"),
        "replaced_player_id": player_id(
            event.get("replacedPlayer"), f"{path}.replacedPlayer"
        ),
        "umpire_id": player_id(event.get("umpire"), f"{path}.umpire"),
        "position_code": string_value(position.get("code"), f"{path}.position.code"),
        "position_name": string_value(position.get("name"), f"{path}.position.name"),
        "position_abbreviation": string_value(
            position.get("abbreviation"), f"{path}.position.abbreviation"
        ),
        "batting_order": string_value(
            event.get("battingOrder"), f"{path}.battingOrder"
        ),
        "base_number": integer_value(event.get("base"), f"{path}.base"),
        "disengagement_number": integer_value(
            details.get("disengagementNum"), f"{path}.details.disengagementNum"
        ),
    }


def _project_substitution(
    event: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    event_index: int,
) -> ProjectionRow:
    details = object_value(event.get("details"), f"{path}.details", required=True)
    position = object_value(event.get("position"), f"{path}.position")
    incoming_player_id = player_id(event.get("player"), f"{path}.player", required=True)
    if incoming_player_id is None:
        raise ProjectionContractError(f"{path}.player.id must not be null")
    return {
        **_event_base(identity, at_bat_index, event_index),
        "play_id": string_value(event.get("playId"), f"{path}.playId"),
        "substitution_type": string_value(
            details.get("eventType"), f"{path}.details.eventType"
        ),
        "description": string_value(
            details.get("description"), f"{path}.details.description"
        ),
        "incoming_player_id": incoming_player_id,
        "replaced_player_id": player_id(
            event.get("replacedPlayer"), f"{path}.replacedPlayer"
        ),
        "position_code": string_value(position.get("code"), f"{path}.position.code"),
        "position_name": string_value(position.get("name"), f"{path}.position.name"),
        "position_abbreviation": string_value(
            position.get("abbreviation"), f"{path}.position.abbreviation"
        ),
        "batting_order": string_value(
            event.get("battingOrder"), f"{path}.battingOrder"
        ),
        "base_number": integer_value(event.get("base"), f"{path}.base"),
    }


def _project_disengagement(
    event: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    event_index: int,
    event_kind: str,
) -> ProjectionRow:
    details = object_value(event.get("details"), f"{path}.details", required=True)
    count = object_value(event.get("count"), f"{path}.count", required=True)
    return {
        **_event_base(identity, at_bat_index, event_index),
        "event_kind": event_kind,
        "play_id": string_value(event.get("playId"), f"{path}.playId"),
        "action_play_id": string_value(
            event.get("actionPlayId"), f"{path}.actionPlayId"
        ),
        "event": string_value(details.get("event"), f"{path}.details.event"),
        "event_type": string_value(
            details.get("eventType"), f"{path}.details.eventType"
        ),
        "description": string_value(
            details.get("description"), f"{path}.details.description"
        ),
        "event_code": string_value(details.get("code"), f"{path}.details.code"),
        "from_catcher": boolean_value(
            details.get("fromCatcher"), f"{path}.details.fromCatcher"
        ),
        "is_out": boolean_value(details.get("isOut"), f"{path}.details.isOut"),
        "has_review": boolean_value(
            details.get("hasReview"), f"{path}.details.hasReview"
        ),
        "disengagement_number": integer_value(
            details.get("disengagementNum"), f"{path}.details.disengagementNum"
        ),
        "balls": integer_value(count.get("balls"), f"{path}.count.balls"),
        "strikes": integer_value(count.get("strikes"), f"{path}.count.strikes"),
        "outs": integer_value(count.get("outs"), f"{path}.count.outs"),
    }


def _project_non_pitch_call(
    event: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    event_index: int,
) -> ProjectionRow:
    details = object_value(event.get("details"), f"{path}.details", required=True)
    call = object_value(details.get("call"), f"{path}.details.call")
    count = object_value(event.get("count"), f"{path}.count", required=True)
    return {
        **_event_base(identity, at_bat_index, event_index),
        "play_id": string_value(event.get("playId"), f"{path}.playId"),
        "pitch_number": integer_value(event.get("pitchNumber"), f"{path}.pitchNumber"),
        "started_at": timestamp_value(event.get("startTime"), f"{path}.startTime"),
        "ended_at": timestamp_value(event.get("endTime"), f"{path}.endTime"),
        "call_code": string_value(call.get("code"), f"{path}.details.call.code"),
        "call_description": string_value(
            call.get("description"), f"{path}.details.call.description"
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
        "is_ball": boolean_value(details.get("isBall"), f"{path}.details.isBall"),
        "is_out": boolean_value(details.get("isOut"), f"{path}.details.isOut"),
        "has_review": boolean_value(
            details.get("hasReview"), f"{path}.details.hasReview"
        ),
        "balls": integer_value(count.get("balls"), f"{path}.count.balls"),
        "strikes": integer_value(count.get("strikes"), f"{path}.count.strikes"),
        "outs": integer_value(count.get("outs"), f"{path}.count.outs"),
    }


def _project_runner(
    runner: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    runner_index: int,
) -> tuple[ProjectionRow, List[ProjectionRow]]:
    movement = object_value(runner.get("movement"), f"{path}.movement", required=True)
    details = object_value(runner.get("details"), f"{path}.details", required=True)
    source_runner_id = player_id(
        details.get("runner"), f"{path}.details.runner", required=True
    )
    if source_runner_id is None:
        raise ProjectionContractError(f"{path}.details.runner.id must not be null")
    play_event_index = integer_value(
        details.get("playIndex"), f"{path}.details.playIndex"
    )
    movement_row: ProjectionRow = {
        **identity,
        "at_bat_index": at_bat_index,
        "runner_index": runner_index,
        "play_event_index": play_event_index,
        "runner_id": source_runner_id,
        "responsible_pitcher_id": player_id(
            details.get("responsiblePitcher"), f"{path}.details.responsiblePitcher"
        ),
        "event": string_value(details.get("event"), f"{path}.details.event"),
        "event_type": string_value(
            details.get("eventType"), f"{path}.details.eventType"
        ),
        "movement_reason": string_value(
            details.get("movementReason"), f"{path}.details.movementReason"
        ),
        "origin_base": string_value(
            movement.get("originBase"), f"{path}.movement.originBase"
        ),
        "start_base": string_value(
            movement.get("start"), f"{path}.movement.start"
        ),
        "end_base": string_value(movement.get("end"), f"{path}.movement.end"),
        "out_base": string_value(
            movement.get("outBase"), f"{path}.movement.outBase"
        ),
        "is_out": boolean_value(movement.get("isOut"), f"{path}.movement.isOut"),
        "out_number": integer_value(
            movement.get("outNumber"), f"{path}.movement.outNumber"
        ),
        "is_scoring_event": boolean_value(
            details.get("isScoringEvent"), f"{path}.details.isScoringEvent"
        ),
        "rbi": boolean_value(details.get("rbi"), f"{path}.details.rbi"),
        "earned": boolean_value(details.get("earned"), f"{path}.details.earned"),
        "team_unearned": boolean_value(
            details.get("teamUnearned"), f"{path}.details.teamUnearned"
        ),
    }
    credits: List[ProjectionRow] = []
    for credit_index, credit_value in enumerate(
        array_value(runner.get("credits"), f"{path}.credits")
    ):
        credit_path = f"{path}.credits[{credit_index}]"
        credit = object_value(credit_value, credit_path, required=True)
        position = object_value(
            credit.get("position"), f"{credit_path}.position", required=True
        )
        credited_player_id = player_id(
            credit.get("player"), f"{credit_path}.player", required=True
        )
        credit_type = string_value(
            credit.get("credit"), f"{credit_path}.credit", required=True
        )
        position_code = string_value(
            position.get("code"), f"{credit_path}.position.code", required=True
        )
        if credited_player_id is None or credit_type is None or position_code is None:
            raise ProjectionContractError(f"{credit_path} has null required values")
        credits.append(
            {
                **identity,
                "at_bat_index": at_bat_index,
                "runner_index": runner_index,
                "credit_index": credit_index,
                "play_event_index": play_event_index,
                "player_id": credited_player_id,
                "credit": credit_type,
                "position_code": position_code,
                "position_name": string_value(
                    position.get("name"), f"{credit_path}.position.name"
                ),
                "position_type": string_value(
                    position.get("type"), f"{credit_path}.position.type"
                ),
                "position_abbreviation": string_value(
                    position.get("abbreviation"),
                    f"{credit_path}.position.abbreviation",
                ),
            }
        )
    return movement_row, credits


def _project_reviews(
    review: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    event_index: Optional[int],
) -> List[ProjectionRow]:
    source_reviews = [review]
    additional = object_value(review.get("additionalReview"), f"{path}.additionalReview")
    if additional:
        source_reviews.append(additional)
    scope = "play" if event_index is None else "event"
    rows = []
    for sequence, source in enumerate(source_reviews):
        source_path = path if sequence == 0 else f"{path}.additionalReview"
        review_id = f"{scope}:{event_index if event_index is not None else 'play'}:{sequence}"
        rows.append(
            {
                **identity,
                "at_bat_index": at_bat_index,
                "review_id": review_id,
                "review_scope": scope,
                "event_index": event_index,
                "review_sequence": sequence,
                "review_type": string_value(
                    source.get("reviewType"), f"{source_path}.reviewType"
                ),
                "challenge_team_id": integer_value(
                    source.get("challengeTeamId"), f"{source_path}.challengeTeamId"
                ),
                "player_id": player_id(source.get("player"), f"{source_path}.player"),
                "in_progress": boolean_value(
                    source.get("inProgress"), f"{source_path}.inProgress"
                ),
                "is_overturned": boolean_value(
                    source.get("isOverturned"), f"{source_path}.isOverturned"
                ),
            }
        )
    return rows


def _project_violation(
    violation: Dict[str, Any],
    path: str,
    identity: ProjectionRow,
    at_bat_index: int,
    event_index: int,
) -> ProjectionRow:
    violation_type = string_value(
        violation.get("type"), f"{path}.type", required=True
    )
    if violation_type is None:
        raise ProjectionContractError(f"{path}.type must not be null")
    return {
        **_event_base(identity, at_bat_index, event_index),
        "violation_type": violation_type,
        "description": string_value(
            violation.get("description"), f"{path}.description"
        ),
        "player_id": player_id(violation.get("player"), f"{path}.player"),
    }
