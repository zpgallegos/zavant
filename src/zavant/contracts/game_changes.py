"""Contracts for MLB's corrected-game change feed and poll requests."""

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
from typing import Any, Dict, Tuple


class GameChangesContractError(ValueError):
    """Raised when a payload cannot be treated as a game-changes response."""


@dataclass(frozen=True)
class ChangedGame:
    """Game identified by MLB as having corrected non-Statcast data."""

    game_pk: int
    official_date: date
    season: int
    status_code: str
    detailed_state: str
    live_feed_link: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "detailed_state": self.detailed_state,
            "game_pk": self.game_pk,
            "live_feed_link": self.live_feed_link,
            "official_date": self.official_date.isoformat(),
            "processing_status": "pending",
            "season": self.season,
            "status_code": self.status_code,
        }


@dataclass(frozen=True)
class GameChangesResponse:
    """Validated routing data plus an unmodified game-changes response."""

    changed_games: Tuple[ChangedGame, ...]
    total_items: int
    total_games: int
    payload: Dict[str, Any]

    @property
    def game_pks(self) -> Tuple[int, ...]:
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
            raise GameChangesContractError("payload is not valid UTF-8 JSON") from exc

        if not isinstance(payload, dict):
            raise GameChangesContractError("payload root must be a JSON object")

        total_items = payload.get("totalItems")
        total_games = payload.get("totalGames")
        if type(total_items) is not int or type(total_games) is not int:
            raise GameChangesContractError("totalItems and totalGames must be integers")
        if total_items < 0 or total_games < 0:
            raise GameChangesContractError(
                "totalItems and totalGames must not be negative"
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
        if not isinstance(value, dict):
            raise GameChangesContractError(f"{location} must be a JSON object")

        game_pk = value.get("gamePk")
        if type(game_pk) is not int:
            raise GameChangesContractError(f"{location}.gamePk must be an integer")

        official_date_text = value.get("officialDate")
        if not isinstance(official_date_text, str):
            raise GameChangesContractError(f"{location}.officialDate must be a string")
        try:
            official_date = date.fromisoformat(official_date_text)
        except ValueError as exc:
            raise GameChangesContractError(
                f"{location}.officialDate must use YYYY-MM-DD"
            ) from exc

        live_feed_link = value.get("link")
        if not isinstance(live_feed_link, str) or not live_feed_link:
            raise GameChangesContractError(f"{location}.link must be a string")

        season_value = value.get("season")
        season: int
        if type(season_value) is int:
            season = season_value
        elif isinstance(season_value, str) and season_value.isdecimal():
            season = int(season_value)
        else:
            raise GameChangesContractError(
                f"{location}.season must be an integer-like value"
            )
        if season <= 0:
            raise GameChangesContractError(
                f"{location}.season must be a positive integer-like value"
            )

        status = value.get("status")
        if not isinstance(status, dict):
            raise GameChangesContractError(f"{location}.status must be a JSON object")
        status_code = status.get("codedGameState")
        detailed_state = status.get("detailedState")
        if not isinstance(status_code, str) or not isinstance(detailed_state, str):
            raise GameChangesContractError(
                f"{location}.status must include string state fields"
            )

        return ChangedGame(
            game_pk=game_pk,
            official_date=official_date,
            season=season,
            status_code=status_code,
            detailed_state=detailed_state,
            live_feed_link=live_feed_link,
        )


@dataclass(frozen=True)
class GameChangesRequest:
    """Request metadata defining one page in a bounded correction poll."""

    updated_since: datetime
    window_end: datetime
    page_number: int
    limit: int
    offset: int
    source_uri: str

    def __post_init__(self) -> None:
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
        return {
            "limit": self.limit,
            "offset": self.offset,
            "page_number": self.page_number,
            "source_uri": self.source_uri,
            "updated_since": self.updated_since.astimezone(timezone.utc).isoformat(),
            "window_end": self.window_end.astimezone(timezone.utc).isoformat(),
        }
