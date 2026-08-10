"""Local implementation of revision-aware raw-game storage."""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Callable, Optional

from zavant.contracts.raw_game import RawGameResponse
from zavant.storage._local_files import (
    atomic_write,
    canonical_json_sha256,
    encode_json,
    local_artifact_reference,
    read_json_object,
    sha256_bytes,
)
from zavant.storage.errors import RawGameConflictError
from zavant.storage.models import LandedRawGame


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        A timezone-aware UTC timestamp.
    """

    return datetime.now(timezone.utc)


class LocalRawGameStore:
    """Persist immutable source revisions using a deterministic local layout.

    Args:
        data_dir: Root directory under which raw objects are stored.
        clock: Function returning the current timezone-aware UTC time.
    """

    def __init__(self, data_dir: Path, clock: Clock = utc_now) -> None:
        """Initialize the local store.

        Args:
            data_dir: Root directory under which raw objects are stored.
            clock: Function returning the current timezone-aware UTC time.
        """

        self.data_dir = data_dir
        self.clock = clock

    def land(
        self,
        game: RawGameResponse,
        raw: bytes,
        source_uri: str,
        trigger: str = "manual",
    ) -> LandedRawGame:
        """Persist a raw game as an immutable, content-addressed revision.

        Args:
            game: Validated routing fields for the source response.
            raw: Unmodified MLB response bytes.
            source_uri: URI describing where the response came from.
            trigger: Reason the response was retrieved.

        Returns:
            Revision paths, hashes, identifiers, and creation status.

        Raises:
            RawGameConflictError: If an existing revision contains content
                inconsistent with its revision identifier.
            OSError: If local persistence fails.
        """

        raw_checksum = sha256_bytes(raw)
        canonical_checksum = canonical_json_sha256(game.payload)
        revision_id = canonical_checksum
        game_directory = (
            self.data_dir
            / "raw"
            / "mlb_stats_api"
            / "games"
            / f"season={game.season}"
            / f"game_pk={game.game_pk}"
        )
        revision_directory = game_directory / f"revision={revision_id}"
        object_path = revision_directory / "game.json"
        metadata_path = revision_directory / "metadata.json"
        current_pointer_path = game_directory / "current.json"

        previous_revision_id = self._read_current_revision(current_pointer_path)
        revision_previous_id = previous_revision_id
        revision_exists = object_path.exists() and metadata_path.exists()
        stored_raw_checksum = raw_checksum
        stored_content_length = len(raw)

        if object_path.exists():
            existing_raw = object_path.read_bytes()
            stored_raw_checksum = sha256_bytes(existing_raw)
            stored_content_length = len(existing_raw)
            try:
                existing_payload = json.loads(existing_raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RawGameConflictError(
                    f"revision {revision_id} contains invalid JSON"
                ) from exc
            if not isinstance(existing_payload, dict):
                raise RawGameConflictError(
                    f"revision {revision_id} does not contain a JSON object"
                )
            if canonical_json_sha256(existing_payload) != revision_id:
                raise RawGameConflictError(
                    f"revision {revision_id} contains different content"
                )

        if metadata_path.exists():
            try:
                existing_metadata = read_json_object(metadata_path)
            except (ValueError, json.JSONDecodeError) as exc:
                raise RawGameConflictError(
                    f"revision {revision_id} metadata is invalid"
                ) from exc
            metadata_previous = existing_metadata.get("previous_revision_id")
            if metadata_previous is not None and not isinstance(metadata_previous, str):
                raise RawGameConflictError(
                    f"revision {revision_id} metadata has an invalid previous revision"
                )
            revision_previous_id = metadata_previous

        observed_at = self.clock().astimezone(timezone.utc)
        if not object_path.exists():
            atomic_write(object_path, raw)

        if not metadata_path.exists():
            metadata = {
                "canonical_sha256": canonical_checksum,
                "content_length": stored_content_length,
                "contract": "mlb-stats-api-raw-game/v2",
                "feed_timecode": game.feed_timecode,
                "game_pk": game.game_pk,
                "observed_at": observed_at.isoformat(),
                "official_date": game.official_date.isoformat(),
                "previous_revision_id": revision_previous_id,
                "raw_sha256": stored_raw_checksum,
                "revision_id": revision_id,
                "season": game.season,
                "source_uri": source_uri,
                "trigger": trigger,
            }
            atomic_write(metadata_path, encode_json(metadata))

        created = not revision_exists
        if created or previous_revision_id is None:
            current_pointer = {
                "canonical_sha256": canonical_checksum,
                "contract": "mlb-stats-api-raw-game-current/v1",
                "game_pk": game.game_pk,
                "revision_id": revision_id,
                "updated_at": observed_at.isoformat(),
            }
            atomic_write(current_pointer_path, encode_json(current_pointer))

        return LandedRawGame(
            game_pk=game.game_pk,
            season=game.season,
            revision_id=revision_id,
            previous_revision_id=revision_previous_id,
            object_path=local_artifact_reference(self.data_dir, object_path),
            metadata_path=local_artifact_reference(self.data_dir, metadata_path),
            current_pointer_path=local_artifact_reference(
                self.data_dir, current_pointer_path
            ),
            raw_sha256=stored_raw_checksum,
            canonical_sha256=canonical_checksum,
            created=created,
        )

    def current_revision_id(self, season: int, game_pk: int) -> Optional[str]:
        """Return the current revision for one season and game.

        Args:
            season: MLB season partition containing the game.
            game_pk: MLB's primary game identifier.

        Returns:
            Current revision identifier, or `None` if the game is not landed.

        Raises:
            ValueError: If the season or game identifier is invalid.
            RawGameConflictError: If the current pointer is malformed.
            OSError: If the current pointer cannot be read.
        """

        if type(season) is not int or season <= 0:
            raise ValueError("season must be a positive integer")
        if type(game_pk) is not int or game_pk <= 0:
            raise ValueError("game_pk must be a positive integer")
        current_pointer_path = (
            self.data_dir
            / "raw"
            / "mlb_stats_api"
            / "games"
            / f"season={season}"
            / f"game_pk={game_pk}"
            / "current.json"
        )
        return self._read_current_revision(current_pointer_path)

    @staticmethod
    def _read_current_revision(current_pointer_path: Path) -> Optional[str]:
        """Read the current revision identifier when a pointer exists.

        Args:
            current_pointer_path: Path to the game's current-revision pointer.

        Returns:
            The current revision identifier, or `None` when no pointer exists.

        Raises:
            RawGameConflictError: If the pointer is malformed.
            OSError: If the pointer cannot be read.
        """

        if not current_pointer_path.exists():
            return None

        try:
            pointer = read_json_object(current_pointer_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise RawGameConflictError("current revision pointer is invalid") from exc

        revision_id = pointer.get("revision_id")
        if not isinstance(revision_id, str):
            raise RawGameConflictError("current revision pointer has no revision_id")
        return revision_id
