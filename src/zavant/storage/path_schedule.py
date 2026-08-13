"""Path-backed storage for schedule snapshots and run manifests."""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple
from uuid import UUID

from zavant.contracts.schedule import ScheduleRequest, ScheduleResponse
from zavant.storage._path_io import (
    atomic_write,
    encode_json,
    resolve_artifact_path,
    artifact_reference_for_path,
    read_json_object,
    sha256_bytes,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import ScheduleConflictError
from zavant.storage.models import LandedSchedule, LoadedScheduleRun


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
    return datetime.now(timezone.utc)


class PathScheduleStore:
    """Persist immutable schedule responses and discovered-game manifests.

    Args:
        storage_root: Root path under which raw objects are stored.
        clock: Function returning the current timezone-aware UTC time.
    """

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.storage_root = storage_root
        self.clock = clock

    def land(
        self,
        schedule: ScheduleResponse,
        request: ScheduleRequest,
        raw: bytes,
        run_id: UUID,
    ) -> LandedSchedule:
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
            response_path=artifact_reference_for_path(self.storage_root, response_path),
            metadata_path=artifact_reference_for_path(self.storage_root, metadata_path),
            manifest_path=artifact_reference_for_path(self.storage_root, manifest_path),
            response_sha256=response_checksum,
            scheduled_game_pks=schedule.game_pks,
            created=created,
        )

    def load_run(
        self,
        requested_at: datetime,
        run_id: UUID,
    ) -> Optional[LoadedScheduleRun]:
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
            response_path=artifact_reference_for_path(self.storage_root, response_path),
            metadata_path=artifact_reference_for_path(self.storage_root, metadata_path),
            manifest_path=artifact_reference_for_path(self.storage_root, manifest_path),
        )

    def game_statuses(self, manifest_path: ArtifactReference) -> Dict[int, str]:
        manifest = self._read_manifest(
            resolve_artifact_path(self.storage_root, manifest_path)
        )
        games = self._validated_manifest_games(manifest)
        return {game["game_pk"]: game["processing_status"] for game in games}

    def record_game_outcome(
        self,
        manifest_path: ArtifactReference,
        game_pk: int,
        status: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if status not in SCHEDULE_GAME_OUTCOMES:
            raise ValueError(f"unsupported schedule game outcome: {status}")
        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(resolved_manifest_path)
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
        atomic_write(resolved_manifest_path, encode_json(manifest))

    def finalize_manifest(self, manifest_path: ArtifactReference) -> Dict[str, int]:
        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(resolved_manifest_path)
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
            atomic_write(resolved_manifest_path, encode_json(manifest))
        return summary

    def _validate_existing_metadata(
        self,
        metadata_path: Path,
        expected: Dict[str, Any],
    ) -> None:
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
        try:
            return read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise ScheduleConflictError("schedule manifest is invalid") from exc

    @staticmethod
    def _validated_manifest_games(
        manifest: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], ...]:
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
        request_date = requested_at.astimezone(timezone.utc).date().isoformat()
        return (
            self.storage_root
            / "raw"
            / "mlb_stats_api"
            / "schedules"
            / f"request_date={request_date}"
            / f"run_id={run_id}"
        )

    def _relative_path(self, path: Path) -> str:
        return path.relative_to(self.storage_root).as_posix()
