"""Pure mutation shared by processing-manifest game entries."""

from typing import Any, Dict, List, Mapping, Optional, cast


OUTCOME_FIELDS = (
    "error_message",
    "error_type",
    "http_attempts",
    "reason",
    "revision_created",
    "revision_id",
    "source_uri",
)


def apply_processing_outcome(
    game: Dict[str, Any],
    status: str,
    details: Optional[Mapping[str, Any]],
    recorded_at: str,
) -> None:
    """Append an attempt and promote its current outcome fields onto a game."""
    outcome = dict(details or {})
    outcome["recorded_at"] = recorded_at
    outcome["status"] = status
    attempts = cast(List[Any], game.get("processing_attempts", []))
    attempts.append(outcome)

    for field in OUTCOME_FIELDS:
        game.pop(field, None)
    for field in OUTCOME_FIELDS:
        if field in outcome:
            game[field] = outcome[field]
    game["processing_attempts"] = attempts
    game["processing_status"] = status
