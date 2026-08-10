"""Local storage for immutable schedule snapshots and run manifests."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple
from uuid import UUID

from zavant.contracts.schedule import ScheduleRequest, ScheduleResponse
from zavant.storage._local_files import (
    atomic_write,
    encode_json,
    read_json_object,
    sha256_bytes,
)


Clock = Callable[[], datetime]
SCHEDULE_GAME_STATUSES = (
    "pending",
    "deferred",
    "skipped",
    "succeeded",
    "failed",
)
SCHEDULE_GAME_OUTCOMES = SCHEDULE_GAME_STATUSES[1:]


def utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        A timezone-aware UTC timestamp.
    """

    return datetime.now(timezone.utc)


class ScheduleConflictError(RuntimeError):
    """Raised when a schedule run conflicts with stored content."""


@dataclass(frozen=True)
class LandedSchedule:
    """Result of landing one bounded schedule snapshot.

    Attributes:
        run_id: Unique identifier for the schedule request.
        request_date: UTC date on which the source request was made.
        response_path: Path containing the unmodified API response.
        metadata_path: Path containing request and provenance metadata.
        manifest_path: Path containing the discovered-game manifest.
        response_sha256: Digest of the exact response bytes.
        scheduled_game_pks: Deduplicated identifiers found in the response.
        created: Whether this call created the response object.
    """

    run_id: UUID
    request_date: str
    response_path: Path
    metadata_path: Path
    manifest_path: Path
    response_sha256: str
    scheduled_game_pks: Tuple[int, ...]
    created: bool

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the result.

        Returns:
            Landing result fields suitable for CLI output.
        """

        return {
            "created": self.created,
            "manifest_path": str(self.manifest_path),
            "metadata_path": str(self.metadata_path),
            "request_date": self.request_date,
            "response_path": str(self.response_path),
            "response_sha256": self.response_sha256,
            "run_id": str(self.run_id),
            "scheduled_game_pks": list(self.scheduled_game_pks),
        }


@dataclass(frozen=True)
class LoadedScheduleRun:
    """Previously landed schedule artifacts used to resume a run.

    Attributes:
        run_id: Unique identifier for the schedule request.
        request_date: UTC request-date partition.
        raw: Exact stored schedule response bytes.
        request: Persisted request provenance.
        response_path: Path containing the stored response.
        metadata_path: Path containing response provenance.
        manifest_path: Path containing per-game processing state.
    """

    run_id: UUID
    request_date: str
    raw: bytes
    request: Dict[str, Any]
    response_path: Path
    metadata_path: Path
    manifest_path: Path


class LocalScheduleStore:
    """Persist immutable schedule responses and discovered-game manifests.

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
        schedule: ScheduleResponse,
        request: ScheduleRequest,
        raw: bytes,
        run_id: UUID,
    ) -> LandedSchedule:
        """Persist one immutable schedule response and its run manifest.

        Args:
            schedule: Validated schedule response.
            request: Request boundaries and provenance.
            raw: Unmodified API response bytes.
            run_id: Unique identifier for the schedule request.

        Returns:
            Paths, identifiers, checksum, scheduled games, and creation status.

        Raises:
            ScheduleConflictError: If the run conflicts with previously stored
                content or provenance.
            OSError: If local persistence fails.
        """

        request_date = request.requested_at.astimezone(timezone.utc).date().isoformat()
        run_directory = self._run_directory(request.requested_at, run_id)
        response_path = run_directory / "response.json"
        metadata_path = run_directory / "metadata.json"
        manifest_path = run_directory / "manifest.json"
        response_checksum = sha256_bytes(raw)
        observed_at = self.clock().astimezone(timezone.utc)
        normalized_request = request.as_dict()

        created = not response_path.exists()
        if response_path.exists():
            existing_checksum = sha256_bytes(response_path.read_bytes())
            if existing_checksum != response_checksum:
                raise ScheduleConflictError(
                    "schedule run already contains a different response"
                )

        metadata = {
            "contract": "mlb-stats-api-schedule-response/v1",
            "observed_at": observed_at.isoformat(),
            "request": normalized_request,
            "response_sha256": response_checksum,
            "run_id": str(run_id),
            "total_games": schedule.total_games,
            "total_items": schedule.total_items,
        }
        self._validate_existing_metadata(metadata_path, metadata)

        manifest = self._load_or_create_manifest(
            manifest_path=manifest_path,
            schedule=schedule,
            request=normalized_request,
            run_id=run_id,
            response_path=response_path,
            metadata_path=metadata_path,
            response_checksum=response_checksum,
            observed_at=observed_at,
        )

        if not response_path.exists():
            atomic_write(response_path, raw)
        if not metadata_path.exists():
            atomic_write(metadata_path, encode_json(metadata))
        if not manifest_path.exists():
            atomic_write(manifest_path, encode_json(manifest))

        return LandedSchedule(
            run_id=run_id,
            request_date=request_date,
            response_path=response_path,
            metadata_path=metadata_path,
            manifest_path=manifest_path,
            response_sha256=response_checksum,
            scheduled_game_pks=schedule.game_pks,
            created=created,
        )

    def load_run(
        self,
        requested_at: datetime,
        run_id: UUID,
    ) -> Optional[LoadedScheduleRun]:
        """Load a complete stored schedule run for safe resumption.

        Args:
            requested_at: Original timezone-aware schedule request time.
            run_id: Original schedule run identifier.

        Returns:
            Stored response and provenance, or `None` if no artifact exists.

        Raises:
            ValueError: If `requested_at` is timezone-naive.
            ScheduleConflictError: If only part of the run exists or its
                provenance is malformed or inconsistent.
            OSError: If stored artifacts cannot be read.
        """

        if requested_at.tzinfo is None or requested_at.utcoffset() is None:
            raise ValueError("requested_at must include a UTC offset")
        request_date = requested_at.astimezone(timezone.utc).date().isoformat()
        run_directory = self._run_directory(requested_at, run_id)
        response_path = run_directory / "response.json"
        metadata_path = run_directory / "metadata.json"
        manifest_path = run_directory / "manifest.json"
        paths = (response_path, metadata_path, manifest_path)
        existing_paths = tuple(path for path in paths if path.exists())
        if not existing_paths:
            return None
        if len(existing_paths) != len(paths):
            raise ScheduleConflictError("schedule run artifacts are incomplete")

        raw = response_path.read_bytes()
        response_checksum = sha256_bytes(raw)
        try:
            metadata = read_json_object(metadata_path)
            manifest = read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise ScheduleConflictError("schedule run artifacts are invalid") from exc

        request = metadata.get("request")
        if not isinstance(request, dict):
            raise ScheduleConflictError("schedule metadata request is invalid")
        expected_metadata = {
            "contract": "mlb-stats-api-schedule-response/v1",
            "response_sha256": response_checksum,
            "run_id": str(run_id),
        }
        if any(metadata.get(key) != value for key, value in expected_metadata.items()):
            raise ScheduleConflictError("schedule metadata conflicts with its response")
        expected_manifest = {
            "contract": "mlb-stats-api-schedule-manifest/v1",
            "request": request,
            "response_path": self._relative_path(response_path),
            "response_sha256": response_checksum,
            "run_id": str(run_id),
        }
        if any(manifest.get(key) != value for key, value in expected_manifest.items()):
            raise ScheduleConflictError("schedule manifest conflicts with its response")
        self._validated_manifest_games(manifest)
        return LoadedScheduleRun(
            run_id=run_id,
            request_date=request_date,
            raw=raw,
            request=request,
            response_path=response_path,
            metadata_path=metadata_path,
            manifest_path=manifest_path,
        )

    def game_statuses(self, manifest_path: Path) -> Dict[int, str]:
        """Read validated per-game processing states from a manifest.

        Args:
            manifest_path: Existing schedule manifest path.

        Returns:
            Mapping from game identifier to processing status.

        Raises:
            ScheduleConflictError: If the manifest or game states are invalid.
            OSError: If the manifest cannot be read.
        """

        manifest = self._read_manifest(manifest_path)
        games = self._validated_manifest_games(manifest)
        return {game["game_pk"]: game["processing_status"] for game in games}

    def record_game_outcome(
        self,
        manifest_path: Path,
        game_pk: int,
        status: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Atomically record one game's latest acquisition outcome.

        Args:
            manifest_path: Existing schedule manifest path.
            game_pk: MLB game identifier to update.
            status: One of `deferred`, `skipped`, `succeeded`, or `failed`.
            details: Optional JSON-serializable outcome details.

        Raises:
            ValueError: If the requested outcome is unsupported.
            ScheduleConflictError: If the manifest or target game is invalid.
            OSError: If the manifest cannot be read or atomically written.
        """

        if status not in SCHEDULE_GAME_OUTCOMES:
            raise ValueError(f"unsupported schedule game outcome: {status}")
        manifest = self._read_manifest(manifest_path)
        games = self._validated_manifest_games(manifest)
        matching_games = [game for game in games if game["game_pk"] == game_pk]
        if len(matching_games) != 1:
            raise ScheduleConflictError(
                f"schedule manifest does not contain exactly one game {game_pk}"
            )

        game = matching_games[0]
        recorded_at = self.clock().astimezone(timezone.utc).isoformat()
        outcome = dict(details or {})
        outcome["recorded_at"] = recorded_at
        outcome["status"] = status
        attempts = game.get("processing_attempts", [])
        if not isinstance(attempts, list):
            raise ScheduleConflictError(
                f"schedule manifest game {game_pk} has invalid attempts"
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
        manifest["status"] = "open"
        manifest["updated_at"] = recorded_at
        manifest.pop("completed_at", None)
        manifest.pop("summary", None)
        atomic_write(manifest_path, encode_json(manifest))

    def finalize_manifest(self, manifest_path: Path) -> Dict[str, int]:
        """Derive and atomically publish a schedule run's final status.

        Args:
            manifest_path: Existing schedule manifest path.

        Returns:
            Counts for every per-game processing status.

        Raises:
            ScheduleConflictError: If the manifest or game states are invalid.
            OSError: If the manifest cannot be read or atomically written.
        """

        manifest = self._read_manifest(manifest_path)
        games = self._validated_manifest_games(manifest)
        summary = {status: 0 for status in SCHEDULE_GAME_STATUSES}
        for game in games:
            summary[game["processing_status"]] += 1

        if summary["pending"]:
            run_status = "incomplete"
        elif summary["failed"]:
            run_status = "failed"
        else:
            run_status = "complete"

        if manifest.get("status") != run_status or manifest.get("summary") != summary:
            finalized_at = self.clock().astimezone(timezone.utc).isoformat()
            manifest["status"] = run_status
            manifest["summary"] = summary
            manifest["updated_at"] = finalized_at
            if run_status == "complete":
                manifest["completed_at"] = finalized_at
            else:
                manifest.pop("completed_at", None)
            atomic_write(manifest_path, encode_json(manifest))
        return summary

    def _validate_existing_metadata(
        self,
        metadata_path: Path,
        expected: Dict[str, Any],
    ) -> None:
        """Validate immutable provenance when metadata already exists.

        Args:
            metadata_path: Path to the schedule response metadata.
            expected: Metadata values derived from the current landing request.

        Raises:
            ScheduleConflictError: If existing metadata is invalid or conflicts.
            OSError: If the metadata cannot be read.
        """

        if not metadata_path.exists():
            return
        try:
            existing = read_json_object(metadata_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise ScheduleConflictError("schedule metadata is invalid") from exc

        immutable_fields = (
            "contract",
            "request",
            "response_sha256",
            "run_id",
            "total_games",
            "total_items",
        )
        if any(existing.get(key) != expected[key] for key in immutable_fields):
            raise ScheduleConflictError("schedule metadata conflicts")

    def _load_or_create_manifest(
        self,
        manifest_path: Path,
        schedule: ScheduleResponse,
        request: Dict[str, Any],
        run_id: UUID,
        response_path: Path,
        metadata_path: Path,
        response_checksum: str,
        observed_at: datetime,
    ) -> Dict[str, Any]:
        """Load a compatible manifest or initialize a new one.

        Args:
            manifest_path: Schedule run manifest path.
            schedule: Validated schedule response.
            request: Normalized request metadata.
            run_id: Unique identifier for the schedule request.
            response_path: Raw response path.
            metadata_path: Response provenance path.
            response_checksum: SHA-256 digest of the response bytes.
            observed_at: Time at which the response was observed.

        Returns:
            A compatible existing manifest or a new manifest.

        Raises:
            ScheduleConflictError: If an existing manifest is malformed or
                describes different source evidence.
            OSError: If the manifest cannot be read.
        """

        if not manifest_path.exists():
            return {
                "contract": "mlb-stats-api-schedule-manifest/v1",
                "created_at": observed_at.isoformat(),
                "games": [game.as_dict() for game in schedule.scheduled_games],
                "metadata_path": self._relative_path(metadata_path),
                "request": request,
                "response_path": self._relative_path(response_path),
                "response_sha256": response_checksum,
                "run_id": str(run_id),
                "status": "open",
                "updated_at": observed_at.isoformat(),
            }

        try:
            manifest = read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise ScheduleConflictError("schedule manifest is invalid") from exc

        expected = {
            "contract": "mlb-stats-api-schedule-manifest/v1",
            "metadata_path": self._relative_path(metadata_path),
            "request": request,
            "response_path": self._relative_path(response_path),
            "response_sha256": response_checksum,
            "run_id": str(run_id),
        }
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise ScheduleConflictError("schedule manifest conflicts")

        games = self._validated_manifest_games(manifest)
        stored_game_pks = sorted(game["game_pk"] for game in games)
        if stored_game_pks != list(schedule.game_pks):
            raise ScheduleConflictError("schedule manifest games conflict")
        return manifest

    @staticmethod
    def _read_manifest(manifest_path: Path) -> Dict[str, Any]:
        """Read a schedule manifest and normalize invalid-data failures.

        Args:
            manifest_path: Existing schedule manifest path.

        Returns:
            Parsed manifest object.

        Raises:
            ScheduleConflictError: If the manifest is invalid JSON or not an
                object.
            OSError: If the manifest cannot be read.
        """

        try:
            return read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise ScheduleConflictError("schedule manifest is invalid") from exc

    @staticmethod
    def _validated_manifest_games(
        manifest: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], ...]:
        """Validate and return a manifest's mutable game entries.

        Args:
            manifest: Parsed schedule manifest.

        Returns:
            Validated game-entry objects.

        Raises:
            ScheduleConflictError: If games, identifiers, or statuses are
                malformed or duplicated.
        """

        games_value = manifest.get("games")
        if not isinstance(games_value, list):
            raise ScheduleConflictError("schedule manifest games are invalid")
        games: List[Dict[str, Any]] = []
        game_pks: Set[int] = set()
        for value in games_value:
            if not isinstance(value, dict):
                raise ScheduleConflictError("schedule manifest game is invalid")
            game_pk = value.get("game_pk")
            status = value.get("processing_status")
            if type(game_pk) is not int or game_pk in game_pks:
                raise ScheduleConflictError(
                    "schedule manifest game identifiers are invalid"
                )
            if not isinstance(status, str) or status not in SCHEDULE_GAME_STATUSES:
                raise ScheduleConflictError(
                    f"schedule manifest game {game_pk} has an invalid status"
                )
            game_pks.add(game_pk)
            games.append(value)
        return tuple(games)

    def _run_directory(self, requested_at: datetime, run_id: UUID) -> Path:
        """Build the local directory for a schedule run.

        Args:
            requested_at: Time at which the schedule request was made.
            run_id: Unique identifier for the schedule request.

        Returns:
            Deterministic run directory below the configured data root.
        """

        request_date = requested_at.astimezone(timezone.utc).date().isoformat()
        return (
            self.data_dir
            / "raw"
            / "mlb_stats_api"
            / "schedules"
            / f"request_date={request_date}"
            / f"run_id={run_id}"
        )

    def _relative_path(self, path: Path) -> str:
        """Return a path relative to the configured data directory.

        Args:
            path: Persisted path under the configured data directory.

        Returns:
            Portable POSIX-style relative path.
        """

        return path.relative_to(self.data_dir).as_posix()
