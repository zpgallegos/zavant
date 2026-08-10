"""Minimal contract for an MLB Stats API live-game response."""

from dataclasses import dataclass
from datetime import date
import json
from typing import Any, Dict, Optional


class RawGameContractError(ValueError):
    """Raised when a payload cannot be treated as a raw MLB game."""


@dataclass(frozen=True)
class RawGameResponse:
    """Validated routing fields plus the unmodified source payload.

    Attributes:
        game_pk: MLB's primary identifier for the game.
        official_date: Official date assigned to the game by MLB.
        feed_timecode: MLB timecode for the source feed revision, when present.
        payload: Parsed but otherwise unmodified source response.
    """

    game_pk: int
    official_date: date
    feed_timecode: Optional[str]
    payload: Dict[str, Any]

    @property
    def season(self) -> int:
        """Return the season used to partition the game.

        Returns:
            The year component of the official game date.
        """

        return self.official_date.year

    @classmethod
    def from_bytes(cls, raw: bytes) -> "RawGameResponse":
        """Validate source bytes and extract fields needed for routing.

        Args:
            raw: UTF-8 JSON bytes returned by the MLB Stats API.

        Returns:
            A validated raw-game value.

        Raises:
            RawGameContractError: If the payload is malformed or omits a
                required routing field.
        """

        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawGameContractError("payload is not valid UTF-8 JSON") from exc

        if not isinstance(payload, dict):
            raise RawGameContractError("payload root must be a JSON object")

        game_pk = payload.get("gamePk")
        if not isinstance(game_pk, int):
            raise RawGameContractError("gamePk must be an integer")

        game_data = payload.get("gameData")
        live_data = payload.get("liveData")
        if not isinstance(game_data, dict) or not isinstance(live_data, dict):
            raise RawGameContractError("gameData and liveData must be JSON objects")

        datetime_data = game_data.get("datetime")
        official_date_text = (
            datetime_data.get("officialDate")
            if isinstance(datetime_data, dict)
            else None
        )
        if not isinstance(official_date_text, str):
            raise RawGameContractError("gameData.datetime.officialDate is required")

        try:
            official_date = date.fromisoformat(official_date_text)
        except ValueError as exc:
            raise RawGameContractError(
                "gameData.datetime.officialDate must use YYYY-MM-DD"
            ) from exc

        metadata = payload.get("metaData")
        feed_timecode_value = (
            metadata.get("timeStamp") if isinstance(metadata, dict) else None
        )
        feed_timecode = (
            feed_timecode_value if isinstance(feed_timecode_value, str) else None
        )

        return cls(
            game_pk=game_pk,
            official_date=official_date,
            feed_timecode=feed_timecode,
            payload=payload,
        )
