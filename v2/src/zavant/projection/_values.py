"""Strict JSON value extraction shared by analytical projectors."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Optional, Sequence

from zavant.projection.contracts import ProjectionContractError


JsonObject = Dict[str, Any]


def object_value(value: Any, path: str, required: bool = False) -> JsonObject:
    if value is None and not required:
        return {}
    if not isinstance(value, dict):
        raise ProjectionContractError(f"{path} must be an object")
    return value


def array_value(value: Any, path: str, required: bool = False) -> Sequence[Any]:
    if value is None and not required:
        return ()
    if not isinstance(value, list):
        raise ProjectionContractError(f"{path} must be an array")
    return value


def string_value(value: Any, path: str, required: bool = False) -> Optional[str]:
    if value is None and not required:
        return None
    if not isinstance(value, str):
        raise ProjectionContractError(f"{path} must be a string")
    return value


def integer_value(value: Any, path: str, required: bool = False) -> Optional[int]:
    if value is None and not required:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ProjectionContractError(f"{path} must be an integer")
    return value


def float_value(value: Any, path: str) -> Optional[float]:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ProjectionContractError(f"{path} must be numeric")
    return float(value)


def boolean_value(value: Any, path: str, required: bool = False) -> Optional[bool]:
    if value is None and not required:
        return None
    if not isinstance(value, bool):
        raise ProjectionContractError(f"{path} must be a boolean")
    return value


def date_value(value: Any, path: str) -> Optional[date]:
    text = string_value(value, path)
    if text is None:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ProjectionContractError(f"{path} must use YYYY-MM-DD") from exc


def timestamp_value(value: Any, path: str) -> Optional[datetime]:
    text = string_value(value, path)
    if text is None:
        return None
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ProjectionContractError(f"{path} must use ISO-8601") from exc
    if parsed.utcoffset() is None:
        raise ProjectionContractError(f"{path} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def numeric_string_integer(value: Any, path: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if not isinstance(value, str):
        raise ProjectionContractError(f"{path} must be an integer or numeric string")
    try:
        return int(value)
    except ValueError as exc:
        raise ProjectionContractError(f"{path} must be a numeric string") from exc


def player_id(value: Any, path: str, required: bool = False) -> Optional[int]:
    player = object_value(value, path, required=required)
    if not player and not required:
        return None
    return integer_value(player.get("id"), f"{path}.id", required=required)
