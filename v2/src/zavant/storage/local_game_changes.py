"""Local storage for immutable game-change pages and poll manifests."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple
from uuid import UUID

from zavant.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.storage._local_files import (
    atomic_write,
    encode_json,
    read_json_object,
    sha256_bytes,
)


Clock = Callable[[], datetime]
GAME_CHANGE_PROCESSING_STATUSES = ("pending", "skipped", "succeeded", "failed")
GAME_CHANGE_PROCESSING_OUTCOMES = GAME_CHANGE_PROCESSING_STATUSES[1:]


def utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        A timezone-aware UTC timestamp.
    """

    return datetime.now(timezone.utc)


class GameChangesConflictError(RuntimeError):
    """Raised when a poll page or manifest conflicts with stored content."""


@dataclass(frozen=True)
class ChangedGameWorkItem:
    """One changed game awaiting or retrying live-feed retrieval.

    Attributes:
        game_pk: MLB's primary game identifier.
        season: MLB season partition containing the raw game.
        live_feed_link: Relative complete-game feed link reported by MLB.
        processing_status: Current manifest processing state.
    """

    game_pk: int
    season: int
    live_feed_link: str
    processing_status: str


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

    def finalize_manifest(
        self,
        manifest_path: Path,
        expected_page_count: int,
        expected_total_items: int,
        watermark_before: datetime,
    ) -> Dict[str, int]:
        """Validate and mark a fully landed correction poll complete.

        Args:
            manifest_path: Poll manifest to validate and complete.
            expected_page_count: Page count derived from the first response.
            expected_total_items: Item count reported by the first response.
            watermark_before: Logical checkpoint from which the poll began,
                before applying its safety overlap.

        Returns:
            Counts for landed pages, source items, and pending games.

        Raises:
            ValueError: If expected counts or the watermark are invalid.
            GameChangesConflictError: If the manifest is malformed,
                incomplete, or inconsistent with the poll.
            OSError: If the manifest cannot be read or written.
        """

        if type(expected_page_count) is not int or expected_page_count <= 0:
            raise ValueError("expected_page_count must be a positive integer")
        if type(expected_total_items) is not int or expected_total_items < 0:
            raise ValueError("expected_total_items must be a non-negative integer")
        if watermark_before.tzinfo is None or watermark_before.utcoffset() is None:
            raise ValueError("watermark_before must include a UTC offset")

        try:
            manifest = read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise GameChangesConflictError("poll manifest is invalid") from exc

        pages_value = manifest.get("pages")
        if not isinstance(pages_value, list):
            raise GameChangesConflictError("poll manifest collections are invalid")
        pages = self._validated_pages(pages_value, expected_page_count)
        changed_games = self._validated_changed_games(manifest)

        first_total_items = pages[0].get("total_items")
        if first_total_items != expected_total_items:
            raise GameChangesConflictError(
                "poll manifest first page has a conflicting total_items"
            )

        updated_since = self._manifest_timestamp(manifest, "updated_since")
        window_end = self._manifest_timestamp(manifest, "window_end")
        normalized_watermark = watermark_before.astimezone(timezone.utc)
        if updated_since > normalized_watermark:
            raise GameChangesConflictError(
                "poll query boundary is after its logical watermark"
            )
        if normalized_watermark >= window_end:
            raise GameChangesConflictError(
                "poll watermark must be before its window end"
            )

        summary = {
            "changed_games": len(changed_games),
            "pages": len(pages),
            "source_items": expected_total_items,
        }
        processing_summary = self._processing_summary(changed_games)
        processing_status = self._processing_status(processing_summary)
        completed_at = self.clock().astimezone(timezone.utc).isoformat()
        normalized_watermark_text = normalized_watermark.isoformat()
        if manifest.get("status") == "complete":
            expected_completion = {
                "processing_status": processing_status,
                "processing_summary": processing_summary,
                "summary": summary,
                "watermark_before": normalized_watermark_text,
            }
            if any(
                manifest.get(key) != value for key, value in expected_completion.items()
            ):
                raise GameChangesConflictError(
                    "completed poll manifest conflicts with finalization"
                )
            return summary
        if manifest.get("status") != "open":
            raise GameChangesConflictError("poll manifest has an invalid status")

        manifest["completed_at"] = completed_at
        manifest["processing_status"] = processing_status
        manifest["processing_summary"] = processing_summary
        manifest["status"] = "complete"
        manifest["summary"] = summary
        manifest["updated_at"] = completed_at
        manifest["watermark_before"] = normalized_watermark_text
        atomic_write(manifest_path, encode_json(manifest))
        return summary

    def processable_manifests(self) -> Tuple[Path, ...]:
        """List completed polls with pending or failed changed games.

        Returns:
            Manifest paths ordered by poll partition and run identifier.

        Raises:
            GameChangesConflictError: If a discovered manifest is malformed.
            OSError: If a manifest cannot be read.
        """

        pattern = "raw/mlb_stats_api/game_changes/poll_date=*/run_id=*/manifest.json"
        processable: List[Path] = []
        for manifest_path in sorted(self.data_dir.glob(pattern)):
            manifest = self._read_manifest(manifest_path)
            if manifest.get("status") != "complete":
                continue
            games = self._validated_changed_games(manifest)
            if any(
                game["processing_status"] in {"pending", "failed"} for game in games
            ):
                processable.append(manifest_path)
        return tuple(processable)

    def game_work_items(self, manifest_path: Path) -> Tuple[ChangedGameWorkItem, ...]:
        """Read retriable changed games from a completed poll manifest.

        Args:
            manifest_path: Completed correction-poll manifest.

        Returns:
            Pending and previously failed games in manifest order.

        Raises:
            GameChangesConflictError: If the poll or games are invalid.
            OSError: If the manifest cannot be read.
        """

        manifest = self._read_manifest(manifest_path)
        if manifest.get("status") != "complete":
            raise GameChangesConflictError(
                "changed games can be processed only from a complete poll"
            )
        games = self._validated_changed_games(manifest)
        return tuple(
            ChangedGameWorkItem(
                game_pk=game["game_pk"],
                season=game["season"],
                live_feed_link=game["live_feed_link"],
                processing_status=game["processing_status"],
            )
            for game in games
            if game["processing_status"] in {"pending", "failed"}
        )

    def record_game_outcome(
        self,
        manifest_path: Path,
        game_pk: int,
        status: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Atomically record one corrected game's processing outcome.

        Args:
            manifest_path: Completed correction-poll manifest.
            game_pk: MLB game identifier to update.
            status: One of `skipped`, `succeeded`, or `failed`.
            details: Optional JSON-serializable outcome details.

        Raises:
            ValueError: If the outcome is unsupported.
            GameChangesConflictError: If the manifest or game is invalid.
            OSError: If the manifest cannot be read or written.
        """

        if status not in GAME_CHANGE_PROCESSING_OUTCOMES:
            raise ValueError(f"unsupported changed-game outcome: {status}")
        manifest = self._read_manifest(manifest_path)
        if manifest.get("status") != "complete":
            raise GameChangesConflictError(
                "changed games can be updated only in a complete poll"
            )
        games = self._validated_changed_games(manifest)
        matching_games = [game for game in games if game["game_pk"] == game_pk]
        if len(matching_games) != 1:
            raise GameChangesConflictError(
                f"poll manifest does not contain exactly one game {game_pk}"
            )

        game = matching_games[0]
        recorded_at = self.clock().astimezone(timezone.utc).isoformat()
        outcome = dict(details or {})
        outcome["recorded_at"] = recorded_at
        outcome["status"] = status
        attempts = game.get("processing_attempts", [])
        if not isinstance(attempts, list):
            raise GameChangesConflictError(
                f"poll manifest game {game_pk} has invalid processing attempts"
            )
        attempts.append(outcome)

        outcome_fields = (
            "error_message",
            "error_type",
            "http_attempts",
            "reason",
            "revision_created",
            "revision_id",
            "source_uri",
        )
        for field in outcome_fields:
            game.pop(field, None)
        for field in outcome_fields:
            if field in outcome:
                game[field] = outcome[field]
        game["processing_attempts"] = attempts
        game["processing_status"] = status
        processing_summary = self._processing_summary(games)
        manifest["processing_status"] = self._processing_status(processing_summary)
        manifest["processing_summary"] = processing_summary
        manifest["updated_at"] = recorded_at
        atomic_write(manifest_path, encode_json(manifest))

    def finalize_processing(self, manifest_path: Path) -> Dict[str, int]:
        """Publish a correction manifest's derived processing status.

        Args:
            manifest_path: Completed correction-poll manifest.

        Returns:
            Counts for every changed-game processing status.

        Raises:
            GameChangesConflictError: If the manifest or games are invalid.
            OSError: If the manifest cannot be read or written.
        """

        manifest = self._read_manifest(manifest_path)
        if manifest.get("status") != "complete":
            raise GameChangesConflictError(
                "changed games can be finalized only in a complete poll"
            )
        games = self._validated_changed_games(manifest)
        summary = self._processing_summary(games)
        processing_status = self._processing_status(summary)
        if (
            manifest.get("processing_status") != processing_status
            or manifest.get("processing_summary") != summary
        ):
            updated_at = self.clock().astimezone(timezone.utc).isoformat()
            manifest["processing_status"] = processing_status
            manifest["processing_summary"] = summary
            manifest["updated_at"] = updated_at
            if processing_status == "complete":
                manifest["processing_completed_at"] = updated_at
            else:
                manifest.pop("processing_completed_at", None)
            atomic_write(manifest_path, encode_json(manifest))
        return summary

    @staticmethod
    def _validated_pages(
        page_values: List[Any],
        expected_page_count: int,
    ) -> List[Dict[str, Any]]:
        """Validate the page sequence in a poll manifest.

        Args:
            page_values: Untrusted page entries loaded from the manifest.
            expected_page_count: Required number of landed pages.

        Returns:
            Page entries narrowed to dictionaries.

        Raises:
            GameChangesConflictError: If pages are missing, malformed, or not
                contiguous.
        """

        if len(page_values) != expected_page_count:
            raise GameChangesConflictError(
                "poll manifest does not contain every expected page"
            )
        pages: List[Dict[str, Any]] = []
        expected_offset = 0
        for expected_page_number, value in enumerate(page_values):
            if not isinstance(value, dict):
                raise GameChangesConflictError("poll manifest page is invalid")
            if value.get("page_number") != expected_page_number:
                raise GameChangesConflictError("poll manifest pages are not contiguous")
            limit = value.get("limit")
            offset = value.get("offset")
            if type(limit) is not int or limit <= 0 or offset != expected_offset:
                raise GameChangesConflictError(
                    "poll manifest page pagination is invalid"
                )
            pages.append(value)
            expected_offset += limit
        return pages

    @staticmethod
    def _manifest_timestamp(manifest: Dict[str, Any], key: str) -> datetime:
        """Parse one timezone-aware timestamp from a poll manifest.

        Args:
            manifest: Poll manifest containing the timestamp.
            key: Timestamp field to parse.

        Returns:
            Timestamp normalized to UTC.

        Raises:
            GameChangesConflictError: If the field is missing or invalid.
        """

        value = manifest.get(key)
        if not isinstance(value, str):
            raise GameChangesConflictError(f"poll manifest {key} is invalid")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise GameChangesConflictError(f"poll manifest {key} is invalid") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise GameChangesConflictError(f"poll manifest {key} is invalid")
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _read_manifest(manifest_path: Path) -> Dict[str, Any]:
        """Read a correction manifest and normalize invalid-data failures.

        Args:
            manifest_path: Existing correction manifest path.

        Returns:
            Parsed manifest object.

        Raises:
            GameChangesConflictError: If the file is invalid JSON or not an
                object.
            OSError: If the manifest cannot be read.
        """

        try:
            return read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise GameChangesConflictError("poll manifest is invalid") from exc

    @staticmethod
    def _validated_changed_games(
        manifest: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], ...]:
        """Validate mutable changed-game entries in a poll manifest.

        Args:
            manifest: Parsed correction manifest.

        Returns:
            Validated changed-game objects.

        Raises:
            GameChangesConflictError: If entries, identifiers, seasons, links,
                or processing states are malformed or duplicated.
        """

        games_value = manifest.get("changed_games")
        if not isinstance(games_value, list):
            raise GameChangesConflictError("poll manifest changed games are invalid")
        games: List[Dict[str, Any]] = []
        game_pks: Set[int] = set()
        for value in games_value:
            if not isinstance(value, dict):
                raise GameChangesConflictError("poll manifest changed game is invalid")
            game_pk = value.get("game_pk")
            season = value.get("season")
            live_feed_link = value.get("live_feed_link")
            status = value.get("processing_status")
            if type(game_pk) is not int or game_pk in game_pks:
                raise GameChangesConflictError(
                    "poll manifest changed-game identifiers are invalid"
                )
            if type(season) is not int or season <= 0:
                raise GameChangesConflictError(
                    f"poll manifest game {game_pk} has an invalid season"
                )
            if not isinstance(live_feed_link, str) or not live_feed_link:
                raise GameChangesConflictError(
                    f"poll manifest game {game_pk} has an invalid live-feed link"
                )
            if (
                not isinstance(status, str)
                or status not in GAME_CHANGE_PROCESSING_STATUSES
            ):
                raise GameChangesConflictError(
                    f"poll manifest game {game_pk} has an invalid processing status"
                )
            game_pks.add(game_pk)
            games.append(value)
        return tuple(games)

    @staticmethod
    def _processing_summary(
        games: Tuple[Dict[str, Any], ...],
    ) -> Dict[str, int]:
        """Count changed games by processing status.

        Args:
            games: Validated changed-game manifest entries.

        Returns:
            Count for every supported processing state.
        """

        summary = {status: 0 for status in GAME_CHANGE_PROCESSING_STATUSES}
        for game in games:
            summary[game["processing_status"]] += 1
        return summary

    @staticmethod
    def _processing_status(summary: Dict[str, int]) -> str:
        """Derive overall processing state from per-game counts.

        Args:
            summary: Counts for every changed-game processing state.

        Returns:
            `pending`, `failed`, or `complete`.
        """

        if summary["pending"]:
            return "pending"
        if summary["failed"]:
            return "failed"
        return "complete"

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
                "processing_status": "pending",
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
                raise GameChangesConflictError(f"poll manifest has a conflicting {key}")

        if not isinstance(manifest.get("pages"), list) or not isinstance(
            manifest.get("changed_games"), list
        ):
            raise GameChangesConflictError("poll manifest collections are invalid")
        self._validated_changed_games(manifest)
        if manifest.get("status") not in {"open", "complete"}:
            raise GameChangesConflictError("poll manifest has an invalid status")
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
        if manifest.get("status") == "complete":
            raise GameChangesConflictError(
                "completed poll manifest cannot accept another page"
            )

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
