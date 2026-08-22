"""Shared time primitives for ingestion workflows and stores."""

from datetime import datetime, timezone
from typing import Callable


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def as_utc(value: datetime, name: str) -> datetime:
    """Require an aware timestamp and normalize it to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a UTC offset")
    return value.astimezone(timezone.utc)
