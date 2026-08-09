"""Local storage for immutable game-change pages and poll manifests."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from uuid import UUID

from zavant.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.storage._local_files import (
    atomic_write,
    encode_json,
    read_json_object,
    sha256_bytes,
)


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        A timezone-aware UTC timestamp.
    """

    return datetime.now(timezone.utc)


class GameChangesConflictError(RuntimeError):
    """Raised when a poll page or manifest conflicts with stored content."""


@dataclass(frozen=True)
class LandedGameChangesPage:
    """Result of landing one page from a game-change poll.

    Attributes:
        run_id: Unique identifier shared by every page in the poll.
        poll_date: UTC date partition for the poll.
        page_number: Zero-based logical page number.
        response_path: Path containing the unmodified API response.
        metadata_path: Path containing page request and provenance metadata.
        manifest_path: Path containing the merged poll manifest.
        response_sha256: Digest of the exact response bytes.
        changed_game_pks: Deduplicated identifiers found on this page.
        created: Whether this call created the page response.
    """

    run_id: UUID
    poll_date: str
    page_number: int
    response_path: Path
    metadata_path: Path
    manifest_path: Path
    response_sha256: str
    changed_game_pks: Tuple[int, ...]
    created: bool

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the result.

        Returns:
            Landing result fields suitable for CLI output.
        """

        return {
            "changed_game_pks": list(self.changed_game_pks),
            "created": self.created,
            "manifest_path": str(self.manifest_path),
            "metadata_path": str(self.metadata_path),
            "page_number": self.page_number,
            "poll_date": self.poll_date,
            "response_path": str(self.response_path),
            "response_sha256": self.response_sha256,
            "run_id": str(self.run_id),
        }


class LocalGameChangesStore:
    """Persist correction-feed pages and merge their routing manifest.

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

    def land_page(
        self,
        changes: GameChangesResponse,
        request: GameChangesRequest,
        raw: bytes,
        run_id: UUID,
    ) -> LandedGameChangesPage:
        """Persist one immutable response page and update its poll manifest.

        Args:
            changes: Validated change-feed response.
            request: Poll window and pagination metadata for the page.
            raw: Unmodified API response bytes.
            run_id: Identifier shared by every page in this poll.

        Returns:
            Paths, identifiers, checksum, changed games, and creation status.

        Raises:
            GameChangesConflictError: If the page or run conflicts with
                previously stored data.
            OSError: If local persistence fails.
        """

        poll_date = request.window_end.astimezone(timezone.utc).date().isoformat()
        run_directory = (
            self.data_dir
            / "raw"
            / "mlb_stats_api"
            / "game_changes"
            / f"poll_date={poll_date}"
            / f"run_id={run_id}"
        )
        page_directory = run_directory / f"page={request.page_number:04d}"
        response_path = page_directory / "response.json"
        metadata_path = page_directory / "metadata.json"
        manifest_path = run_directory / "manifest.json"
        response_checksum = sha256_bytes(raw)
        observed_at = self.clock().astimezone(timezone.utc)
        manifest = self._load_or_create_manifest(
            manifest_path=manifest_path,
            request=request,
            run_id=run_id,
            observed_at=observed_at,
        )

        created = not response_path.exists()
        if response_path.exists():
            existing_checksum = sha256_bytes(response_path.read_bytes())
            if existing_checksum != response_checksum:
                raise GameChangesConflictError(
                    f"poll page {request.page_number} already has different content"
                )
        else:
            atomic_write(response_path, raw)

        page_metadata = {
            "contract": "mlb-stats-api-game-changes-page/v1",
            "observed_at": observed_at.isoformat(),
            "request": request.as_dict(),
            "response_sha256": response_checksum,
            "run_id": str(run_id),
            "total_games": changes.total_games,
            "total_items": changes.total_items,
        }
        if metadata_path.exists():
            try:
                existing_metadata = read_json_object(metadata_path)
            except (ValueError, json.JSONDecodeError) as exc:
                raise GameChangesConflictError("poll page metadata is invalid") from exc
            immutable_fields = (
                "contract",
                "request",
                "response_sha256",
                "run_id",
                "total_games",
                "total_items",
            )
            if any(
                existing_metadata.get(key) != page_metadata[key]
                for key in immutable_fields
            ):
                raise GameChangesConflictError(
                    f"poll page {request.page_number} metadata conflicts"
                )
        else:
            atomic_write(metadata_path, encode_json(page_metadata))
        manifest_changed = self._merge_page(
            manifest=manifest,
            changes=changes,
            request=request,
            response_path=response_path,
            metadata_path=metadata_path,
            response_checksum=response_checksum,
            observed_at=observed_at,
        )
        if manifest_changed or not manifest_path.exists():
            atomic_write(manifest_path, encode_json(manifest))

        return LandedGameChangesPage(
            run_id=run_id,
            poll_date=poll_date,
            page_number=request.page_number,
            response_path=response_path,
            metadata_path=metadata_path,
            manifest_path=manifest_path,
            response_sha256=response_checksum,
            changed_game_pks=changes.game_pks,
            created=created,
        )

    def _load_or_create_manifest(
        self,
        manifest_path: Path,
        request: GameChangesRequest,
        run_id: UUID,
        observed_at: datetime,
    ) -> Dict[str, Any]:
        """Load a compatible manifest or initialize a new one.

        Args:
            manifest_path: Poll manifest path.
            request: Current page's poll request metadata.
            run_id: Identifier for the poll run.
            observed_at: Time at which this page was observed.

        Returns:
            A mutable poll manifest.

        Raises:
            GameChangesConflictError: If the existing manifest is malformed or
                describes a different poll window.
            OSError: If the manifest cannot be read.
        """

        normalized_request = request.as_dict()
        if not manifest_path.exists():
            return {
                "changed_games": [],
                "contract": "mlb-stats-api-game-changes-manifest/v1",
                "created_at": observed_at.isoformat(),
                "pages": [],
                "run_id": str(run_id),
                "status": "open",
                "updated_at": observed_at.isoformat(),
                "updated_since": normalized_request["updated_since"],
                "window_end": normalized_request["window_end"],
            }

        try:
            manifest = read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise GameChangesConflictError("poll manifest is invalid") from exc

        expected = {
            "run_id": str(run_id),
            "updated_since": normalized_request["updated_since"],
            "window_end": normalized_request["window_end"],
        }
        for key, expected_value in expected.items():
            if manifest.get(key) != expected_value:
                raise GameChangesConflictError(
                    f"poll manifest has a conflicting {key}"
                )

        if not isinstance(manifest.get("pages"), list) or not isinstance(
            manifest.get("changed_games"), list
        ):
            raise GameChangesConflictError("poll manifest collections are invalid")
        return manifest

    def _merge_page(
        self,
        manifest: Dict[str, Any],
        changes: GameChangesResponse,
        request: GameChangesRequest,
        response_path: Path,
        metadata_path: Path,
        response_checksum: str,
        observed_at: datetime,
    ) -> bool:
        """Merge one page's provenance and games into a poll manifest.

        Args:
            manifest: Mutable poll manifest.
            changes: Validated change-feed response.
            request: Poll request metadata for this page.
            response_path: Persisted raw response path.
            metadata_path: Persisted page metadata path.
            response_checksum: SHA-256 digest of the response bytes.
            observed_at: Time at which this page was observed.

        Returns:
            Whether the manifest was changed.

        Raises:
            GameChangesConflictError: If the page number already describes a
                different response.
        """

        pages = manifest["pages"]
        assert isinstance(pages, list)
        existing_page: Optional[Dict[str, Any]] = None
        for value in pages:
            if (
                isinstance(value, dict)
                and value.get("page_number") == request.page_number
            ):
                existing_page = value
                break

        if existing_page is not None:
            if existing_page.get("response_sha256") != response_checksum:
                raise GameChangesConflictError(
                    f"poll manifest page {request.page_number} has a different response"
                )
            return False

        page_entry = {
            "limit": request.limit,
            "metadata_path": self._relative_path(metadata_path),
            "offset": request.offset,
            "page_number": request.page_number,
            "response_path": self._relative_path(response_path),
            "response_sha256": response_checksum,
            "total_games": changes.total_games,
            "total_items": changes.total_items,
        }
        pages.append(page_entry)
        pages.sort(key=lambda value: value.get("page_number", -1))

        changed_games = manifest["changed_games"]
        assert isinstance(changed_games, list)
        games_by_pk: Dict[int, Dict[str, Any]] = {}
        for value in changed_games:
            if isinstance(value, dict) and type(value.get("game_pk")) is int:
                games_by_pk[value["game_pk"]] = value
        for changed_game in changes.changed_games:
            prior = games_by_pk.get(changed_game.game_pk)
            game_entry = changed_game.as_dict()
            if prior is not None:
                prior_status = prior.get("processing_status")
                if isinstance(prior_status, str):
                    game_entry["processing_status"] = prior_status
            games_by_pk[changed_game.game_pk] = game_entry
        manifest["changed_games"] = [
            games_by_pk[game_pk] for game_pk in sorted(games_by_pk)
        ]
        manifest["updated_at"] = observed_at.isoformat()
        return True

    def _relative_path(self, path: Path) -> str:
        """Return a path relative to the configured data directory.

        Args:
            path: Persisted path under the configured data directory.

        Returns:
            Portable POSIX-style relative path.
        """

        return path.relative_to(self.data_dir).as_posix()
