"""Contracts for bounded MLB schedule responses and their requests."""

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
from typing import Any, Dict, Optional, Tuple


class ScheduleContractError(ValueError):
    """Raised when a payload cannot be treated as a schedule response."""


@dataclass(frozen=True)
class ScheduledGame:
    """Game discovered in a bounded MLB schedule response.

    Attributes:
        game_pk: MLB's primary identifier for the game.
        official_date: Official date assigned to the game by MLB.
        scheduled_start: Current scheduled start time reported by MLB.
        season: MLB season identifier.
        game_type: MLB game-type code, such as `R` for regular season.
        status_code: MLB's coded game state at observation time.
        detailed_state: Human-readable game state at observation time.
        live_feed_link: Relative link to the game's complete live feed.
        series_description: Human-readable series classification, if present.
    """

    game_pk: int
    official_date: date
    scheduled_start: datetime
    season: int
    game_type: str
    status_code: str
    detailed_state: str
    live_feed_link: str
    series_description: Optional[str]

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the scheduled game.

        Returns:
            Scheduled-game fields suitable for an acquisition manifest.
        """

        return {
            "detailed_state": self.detailed_state,
            "game_pk": self.game_pk,
            "game_type": self.game_type,
            "live_feed_link": self.live_feed_link,
            "official_date": self.official_date.isoformat(),
            "processing_status": "pending",
            "scheduled_start": self.scheduled_start.astimezone(
                timezone.utc
            ).isoformat(),
            "season": self.season,
            "series_description": self.series_description,
            "status_code": self.status_code,
        }


@dataclass(frozen=True)
class ScheduleResponse:
    """Validated discovery data plus an unmodified schedule response.

    Attributes:
        scheduled_games: Deduplicated games discovered by the response.
        total_items: Number of items reported by MLB.
        total_games: Number of games reported by MLB.
        payload: Parsed but otherwise unmodified source response.
    """

    scheduled_games: Tuple[ScheduledGame, ...]
    total_items: int
    total_games: int
    payload: Dict[str, Any]

    @property
    def game_pks(self) -> Tuple[int, ...]:
        """Return sorted, deduplicated game identifiers.

        Returns:
            Game identifiers included in the response.
        """

        return tuple(game.game_pk for game in self.scheduled_games)

    @classmethod
    def from_bytes(cls, raw: bytes) -> "ScheduleResponse":
        """Validate source bytes and extract scheduled-game routing fields.

        Args:
            raw: UTF-8 JSON bytes returned by MLB's schedule endpoint.

        Returns:
            A validated schedule response.

        Raises:
            ScheduleContractError: If the response is malformed, omits
                required routing fields, or reports an inconsistent game total.
        """

        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ScheduleContractError("payload is not valid UTF-8 JSON") from exc

        if not isinstance(payload, dict):
            raise ScheduleContractError("payload root must be a JSON object")

        total_items = payload.get("totalItems")
        total_games = payload.get("totalGames")
        if type(total_items) is not int or type(total_games) is not int:
            raise ScheduleContractError("totalItems and totalGames must be integers")

        dates = payload.get("dates")
        if not isinstance(dates, list):
            raise ScheduleContractError("dates must be an array")

        games_by_pk: Dict[int, ScheduledGame] = {}
        for date_index, date_entry in enumerate(dates):
            if not isinstance(date_entry, dict):
                raise ScheduleContractError(
                    f"dates[{date_index}] must be a JSON object"
                )
            games = date_entry.get("games")
            if not isinstance(games, list):
                raise ScheduleContractError(
                    f"dates[{date_index}].games must be an array"
                )
            for game_index, game_entry in enumerate(games):
                location = f"dates[{date_index}].games[{game_index}]"
                scheduled_game = cls._parse_scheduled_game(game_entry, location)
                games_by_pk[scheduled_game.game_pk] = scheduled_game

        scheduled_games = tuple(games_by_pk[key] for key in sorted(games_by_pk))
        if len(scheduled_games) != total_games:
            raise ScheduleContractError(
                "totalGames does not match the number of unique scheduled games"
            )

        return cls(
            scheduled_games=scheduled_games,
            total_items=total_items,
            total_games=total_games,
            payload=payload,
        )

    @staticmethod
    def _parse_scheduled_game(value: Any, location: str) -> ScheduledGame:
        """Validate and parse one scheduled-game object.

        Args:
            value: Candidate scheduled-game value.
            location: Human-readable JSON location for validation errors.

        Returns:
            A validated scheduled game.

        Raises:
            ScheduleContractError: If a required field is invalid.
        """

        if not isinstance(value, dict):
            raise ScheduleContractError(f"{location} must be a JSON object")

        game_pk = value.get("gamePk")
        if type(game_pk) is not int:
            raise ScheduleContractError(f"{location}.gamePk must be an integer")

        official_date = ScheduleResponse._parse_date_field(
            value.get("officialDate"), f"{location}.officialDate"
        )
        scheduled_start = ScheduleResponse._parse_datetime_field(
            value.get("gameDate"), f"{location}.gameDate"
        )

        season_value = value.get("season")
        season: int
        if type(season_value) is int:
            season = season_value
        elif isinstance(season_value, str) and season_value.isdecimal():
            season = int(season_value)
        else:
            raise ScheduleContractError(
                f"{location}.season must be an integer-like value"
            )
        if season <= 0:
            raise ScheduleContractError(
                f"{location}.season must be a positive integer-like value"
            )

        game_type = value.get("gameType")
        live_feed_link = value.get("link")
        if not isinstance(game_type, str) or not game_type:
            raise ScheduleContractError(f"{location}.gameType must be a string")
        if not isinstance(live_feed_link, str) or not live_feed_link:
            raise ScheduleContractError(f"{location}.link must be a string")

        status = value.get("status")
        if not isinstance(status, dict):
            raise ScheduleContractError(f"{location}.status must be a JSON object")
        status_code = status.get("codedGameState")
        detailed_state = status.get("detailedState")
        if not isinstance(status_code, str) or not isinstance(detailed_state, str):
            raise ScheduleContractError(
                f"{location}.status must include string state fields"
            )

        series_value = value.get("seriesDescription")
        series_description = series_value if isinstance(series_value, str) else None
        return ScheduledGame(
            game_pk=game_pk,
            official_date=official_date,
            scheduled_start=scheduled_start,
            season=season,
            game_type=game_type,
            status_code=status_code,
            detailed_state=detailed_state,
            live_feed_link=live_feed_link,
            series_description=series_description,
        )

    @staticmethod
    def _parse_date_field(value: Any, location: str) -> date:
        """Parse an ISO date field.

        Args:
            value: Candidate ISO date value.
            location: Human-readable JSON location for validation errors.

        Returns:
            Parsed date.

        Raises:
            ScheduleContractError: If the value is not a valid ISO date.
        """

        if not isinstance(value, str):
            raise ScheduleContractError(f"{location} must be a string")
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ScheduleContractError(f"{location} must use YYYY-MM-DD") from exc

    @staticmethod
    def _parse_datetime_field(value: Any, location: str) -> datetime:
        """Parse a timezone-aware ISO datetime field.

        Args:
            value: Candidate ISO datetime value.
            location: Human-readable JSON location for validation errors.

        Returns:
            Parsed timezone-aware datetime.

        Raises:
            ScheduleContractError: If the value is not a valid aware datetime.
        """

        if not isinstance(value, str):
            raise ScheduleContractError(f"{location} must be a string")
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ScheduleContractError(f"{location} must use ISO-8601 format") from exc
        if parsed.tzinfo is None:
            raise ScheduleContractError(f"{location} must include a UTC offset")
        return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class ScheduleRequest:
    """Request metadata defining one bounded schedule snapshot.

    Attributes:
        start_date: Inclusive first official date requested.
        end_date: Inclusive last official date requested.
        sport_id: MLB sport identifier used to filter the schedule.
        requested_at: Time at which the source request was made.
        source_uri: Complete source URI used for the request.
    """

    start_date: date
    end_date: date
    sport_id: int
    requested_at: datetime
    source_uri: str

    def __post_init__(self) -> None:
        """Validate the request boundary and routing values.

        Raises:
            ValueError: If dates, sport ID, timestamp, or source URI are invalid.
        """

        if self.start_date > self.end_date:
            raise ValueError("start_date must not be after end_date")
        if type(self.sport_id) is not int or self.sport_id <= 0:
            raise ValueError("sport_id must be positive")
        if self.requested_at.tzinfo is None:
            raise ValueError("requested_at must include a UTC offset")
        if not self.source_uri:
            raise ValueError("source_uri must not be empty")

    def as_dict(self) -> Dict[str, Any]:
        """Return normalized request metadata for persistence.

        Returns:
            JSON-serializable request boundaries and routing values.
        """

        return {
            "end_date": self.end_date.isoformat(),
            "requested_at": self.requested_at.astimezone(timezone.utc).isoformat(),
            "source_uri": self.source_uri,
            "sport_id": self.sport_id,
            "start_date": self.start_date.isoformat(),
        }
