"""Contracts for MLB's corrected-game change feed and poll requests."""

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
from typing import Any, Dict, Tuple


class GameChangesContractError(ValueError):
    """Raised when a payload cannot be treated as a game-changes response."""


@dataclass(frozen=True)
class ChangedGame:
    """Game identified by MLB as having corrected non-Statcast data.

    Attributes:
        game_pk: MLB's primary identifier for the changed game.
        official_date: Official date assigned to the game by MLB.
        status_code: MLB's coded game state at observation time.
        detailed_state: Human-readable game state at observation time.
        live_feed_link: Relative link to the game's complete live feed.
    """

    game_pk: int
    official_date: date
    status_code: str
    detailed_state: str
    live_feed_link: str

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the changed game.

        Returns:
            Changed-game fields suitable for a poll manifest.
        """

        return {
            "detailed_state": self.detailed_state,
            "game_pk": self.game_pk,
            "live_feed_link": self.live_feed_link,
            "official_date": self.official_date.isoformat(),
            "processing_status": "pending",
            "status_code": self.status_code,
        }


@dataclass(frozen=True)
class GameChangesResponse:
    """Validated routing data plus an unmodified game-changes response.

    Attributes:
        changed_games: Deduplicated games identified by the response.
        total_items: Number of items reported by MLB.
        total_games: Number of games reported by MLB.
        payload: Parsed but otherwise unmodified source response.
    """

    changed_games: Tuple[ChangedGame, ...]
    total_items: int
    total_games: int
    payload: Dict[str, Any]

    @property
    def game_pks(self) -> Tuple[int, ...]:
        """Return sorted, deduplicated game identifiers.

        Returns:
            Game identifiers included in the response.
        """

        return tuple(game.game_pk for game in self.changed_games)

    @classmethod
    def from_bytes(cls, raw: bytes) -> "GameChangesResponse":
        """Validate source bytes and extract changed-game routing fields.

        Args:
            raw: UTF-8 JSON bytes returned by MLB's game-changes endpoint.

        Returns:
            A validated game-changes response.

        Raises:
            GameChangesContractError: If the response is malformed or omits
                required routing fields.
        """

        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GameChangesContractError(
                "payload is not valid UTF-8 JSON"
            ) from exc

        if not isinstance(payload, dict):
            raise GameChangesContractError("payload root must be a JSON object")

        total_items = payload.get("totalItems")
        total_games = payload.get("totalGames")
        if type(total_items) is not int or type(total_games) is not int:
            raise GameChangesContractError(
                "totalItems and totalGames must be integers"
            )

        dates = payload.get("dates")
        if not isinstance(dates, list):
            raise GameChangesContractError("dates must be an array")

        games_by_pk: Dict[int, ChangedGame] = {}
        for date_index, date_entry in enumerate(dates):
            if not isinstance(date_entry, dict):
                raise GameChangesContractError(
                    f"dates[{date_index}] must be a JSON object"
                )
            games = date_entry.get("games")
            if not isinstance(games, list):
                raise GameChangesContractError(
                    f"dates[{date_index}].games must be an array"
                )

            for game_index, game_entry in enumerate(games):
                location = f"dates[{date_index}].games[{game_index}]"
                changed_game = cls._parse_changed_game(game_entry, location)
                games_by_pk[changed_game.game_pk] = changed_game

        changed_games = tuple(games_by_pk[key] for key in sorted(games_by_pk))
        return cls(
            changed_games=changed_games,
            total_items=total_items,
            total_games=total_games,
            payload=payload,
        )

    @staticmethod
    def _parse_changed_game(value: Any, location: str) -> ChangedGame:
        """Validate and parse one changed-game object.

        Args:
            value: Candidate changed-game value.
            location: Human-readable JSON location for validation errors.

        Returns:
            A validated changed game.

        Raises:
            GameChangesContractError: If a required field is invalid.
        """

        if not isinstance(value, dict):
            raise GameChangesContractError(f"{location} must be a JSON object")

        game_pk = value.get("gamePk")
        if type(game_pk) is not int:
            raise GameChangesContractError(f"{location}.gamePk must be an integer")

        official_date_text = value.get("officialDate")
        if not isinstance(official_date_text, str):
            raise GameChangesContractError(
                f"{location}.officialDate must be a string"
            )
        try:
            official_date = date.fromisoformat(official_date_text)
        except ValueError as exc:
            raise GameChangesContractError(
                f"{location}.officialDate must use YYYY-MM-DD"
            ) from exc

        live_feed_link = value.get("link")
        if not isinstance(live_feed_link, str) or not live_feed_link:
            raise GameChangesContractError(f"{location}.link must be a string")

        status = value.get("status")
        if not isinstance(status, dict):
            raise GameChangesContractError(
                f"{location}.status must be a JSON object"
            )
        status_code = status.get("codedGameState")
        detailed_state = status.get("detailedState")
        if not isinstance(status_code, str) or not isinstance(detailed_state, str):
            raise GameChangesContractError(
                f"{location}.status must include string state fields"
            )

        return ChangedGame(
            game_pk=game_pk,
            official_date=official_date,
            status_code=status_code,
            detailed_state=detailed_state,
            live_feed_link=live_feed_link,
        )


@dataclass(frozen=True)
class GameChangesRequest:
    """Request metadata defining one page in a bounded correction poll.

    Attributes:
        updated_since: Inclusive lower watermark sent to MLB.
        window_end: Durable checkpoint candidate captured before polling.
        page_number: Zero-based logical page number within the poll.
        limit: Requested maximum number of results.
        offset: Requested result offset.
        source_uri: Complete source URI used for the request.
    """

    updated_since: datetime
    window_end: datetime
    page_number: int
    limit: int
    offset: int
    source_uri: str

    def __post_init__(self) -> None:
        """Validate poll boundaries and pagination values.

        Raises:
            ValueError: If timestamps are naive or pagination is invalid.
        """

        if self.updated_since.tzinfo is None or self.window_end.tzinfo is None:
            raise ValueError("poll timestamps must include a UTC offset")
        if self.updated_since > self.window_end:
            raise ValueError("updated_since must not be after window_end")
        if self.page_number < 0:
            raise ValueError("page_number must not be negative")
        if self.limit <= 0:
            raise ValueError("limit must be positive")
        if self.offset < 0:
            raise ValueError("offset must not be negative")
        if not self.source_uri:
            raise ValueError("source_uri must not be empty")

    def as_dict(self) -> Dict[str, Any]:
        """Return normalized request metadata for persistence.

        Returns:
            JSON-serializable poll boundaries and pagination values.
        """

        return {
            "limit": self.limit,
            "offset": self.offset,
            "page_number": self.page_number,
            "source_uri": self.source_uri,
            "updated_since": self.updated_since.astimezone(timezone.utc).isoformat(),
            "window_end": self.window_end.astimezone(timezone.utc).isoformat(),
        }
