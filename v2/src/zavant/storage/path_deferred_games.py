"""Path-backed durable state for non-final scheduled games."""

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

from zavant.contracts.schedule import ScheduledGame
from zavant.storage._path_io import atomic_write, encode_json, read_json_object
from zavant.storage.errors import DeferredGameConflictError
from zavant.storage.models import DeferredScheduledGame


Clock = Callable[[], datetime]
CONTRACT = "zavant-deferred-scheduled-games/v1"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PathDeferredGameStore:
    """Maintain one conditionally written worklist below a lake root.

    The state contains only unresolved games. Schedule manifests retain the
    complete discovery and disposition history after an entry is removed.
    """

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.path = (
            storage_root
            / "state"
            / "mlb_stats_api"
            / "schedules"
            / "deferred_games.json"
        )
        self.clock = clock

    def pending(self) -> Tuple[DeferredScheduledGame, ...]:
        payload = self._read()
        return tuple(self._from_payload(value) for value in payload["games"])

    def defer(self, game: ScheduledGame) -> None:
        payload = self._read()
        now = self._now()
        games = payload["games"]
        existing = next(
            (value for value in games if value.get("game_pk") == game.game_pk),
            None,
        )
        refreshed = {
            "game_pk": game.game_pk,
            "season": game.season,
            "official_date": game.official_date.isoformat(),
            "live_feed_link": game.live_feed_link,
            "first_deferred_at": (
                existing.get("first_deferred_at")
                if existing is not None
                else now.isoformat()
            ),
            "last_evaluated_at": now.isoformat(),
        }
        if existing is None:
            games.append(refreshed)
        else:
            existing.clear()
            existing.update(refreshed)
        games.sort(key=lambda value: value["game_pk"])
        self._write(payload, now)

    def resolve(self, game_pk: int) -> None:
        if type(game_pk) is not int or game_pk <= 0:
            raise ValueError("game_pk must be a positive integer")
        payload = self._read()
        games = payload["games"]
        remaining = [value for value in games if value.get("game_pk") != game_pk]
        if len(remaining) == len(games):
            return
        payload["games"] = remaining
        self._write(payload, self._now())

    def _read(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"contract": CONTRACT, "games": []}
        try:
            payload = read_json_object(self.path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise DeferredGameConflictError("deferred-game state is invalid") from exc
        if payload.get("contract") != CONTRACT:
            raise DeferredGameConflictError("deferred-game contract is unsupported")
        games = payload.get("games")
        if not isinstance(games, list):
            raise DeferredGameConflictError("deferred-game entries are invalid")
        parsed = tuple(self._from_payload(value) for value in games)
        if len({game.game_pk for game in parsed}) != len(parsed):
            raise DeferredGameConflictError("deferred-game identifiers are duplicated")
        return payload

    def _write(self, payload: Dict[str, Any], updated_at: datetime) -> None:
        payload["contract"] = CONTRACT
        payload["updated_at"] = updated_at.isoformat()
        atomic_write(self.path, encode_json(payload))

    @classmethod
    def _from_payload(cls, value: Any) -> DeferredScheduledGame:
        if not isinstance(value, dict):
            raise DeferredGameConflictError("deferred-game entry is invalid")
        game_pk = value.get("game_pk")
        season = value.get("season")
        live_feed_link = value.get("live_feed_link")
        if type(game_pk) is not int or game_pk <= 0:
            raise DeferredGameConflictError("deferred game_pk is invalid")
        if type(season) is not int or season <= 0:
            raise DeferredGameConflictError(f"deferred game {game_pk} season is invalid")
        if not isinstance(live_feed_link, str) or not live_feed_link:
            raise DeferredGameConflictError(
                f"deferred game {game_pk} live-feed link is invalid"
            )
        try:
            official_date_value = value.get("official_date")
            if not isinstance(official_date_value, str):
                raise ValueError("official_date is invalid")
            official_date = date.fromisoformat(official_date_value)
            first_deferred_at = cls._timestamp(value, "first_deferred_at")
            last_evaluated_at = cls._timestamp(value, "last_evaluated_at")
        except ValueError as exc:
            raise DeferredGameConflictError(
                f"deferred game {game_pk} timestamps are invalid"
            ) from exc
        if first_deferred_at > last_evaluated_at:
            raise DeferredGameConflictError(
                f"deferred game {game_pk} timestamps are not ordered"
            )
        return DeferredScheduledGame(
            game_pk=game_pk,
            season=season,
            official_date=official_date,
            live_feed_link=live_feed_link,
            first_deferred_at=first_deferred_at,
            last_evaluated_at=last_evaluated_at,
        )

    @staticmethod
    def _timestamp(value: Dict[str, Any], key: str) -> datetime:
        raw = value.get(key)
        if not isinstance(raw, str):
            raise ValueError(f"{key} is invalid")
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{key} has no UTC offset")
        return parsed.astimezone(timezone.utc)

    def _now(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock result must include a UTC offset")
        return now.astimezone(timezone.utc)
