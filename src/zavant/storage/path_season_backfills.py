"""Path-backed persistence for resumable historical-season reconciliation."""

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

from zavant._time import Clock, as_utc, utc_now
from zavant.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.storage._path_io import (
    atomic_write,
    encode_json,
    resolve_artifact_path,
    artifact_reference_for_path,
    read_json_object,
    sha256_bytes,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import SeasonBackfillConflictError
from zavant.storage.models import (
    LoadedSeasonBackfillChanges,
    SeasonBackfillCheckpoint,
    StartedSeasonBackfillRun,
)


BACKFILL_SEASON_STATUSES = ("pending", "complete", "failed")


class PathSeasonBackfillStore:
    """Persist parent runs, correction evidence, and season checkpoints."""

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.storage_root = storage_root
        self.clock = clock

    def start(
        self,
        run_id: UUID,
        started_at: datetime,
        seasons: Tuple[int, ...],
        mode: str,
        dry_run: bool,
        configuration: Dict[str, Any],
    ) -> StartedSeasonBackfillRun:
        normalized_started_at = as_utc(started_at, "started_at")
        manifest_path = self._manifest_path(normalized_started_at, run_id)
        expected = {
            "configuration": configuration,
            "contract": "zavant-season-backfill-manifest/v1",
            "dry_run": dry_run,
            "mode": mode,
            "run_id": str(run_id),
            "seasons": list(seasons),
            "started_at": normalized_started_at.isoformat(),
        }
        if manifest_path.exists():
            manifest = self._read_manifest(manifest_path)
            if any(manifest.get(key) != value for key, value in expected.items()):
                raise SeasonBackfillConflictError(
                    "season backfill run conflicts with stored configuration"
                )
            return StartedSeasonBackfillRun(
                run_id=run_id,
                started_at=normalized_started_at,
                manifest_path=artifact_reference_for_path(
                    self.storage_root, manifest_path
                ),
                season_statuses=self._season_statuses(manifest),
                resumed=True,
            )

        observed_at = as_utc(self.clock(), "backfill store clock")
        manifest = {
            **expected,
            "created_at": observed_at.isoformat(),
            "season_runs": [
                {"season": season, "status": "pending"} for season in seasons
            ],
            "status": "open",
            "updated_at": observed_at.isoformat(),
        }
        atomic_write(manifest_path, encode_json(manifest))
        return StartedSeasonBackfillRun(
            run_id=run_id,
            started_at=normalized_started_at,
            manifest_path=artifact_reference_for_path(self.storage_root, manifest_path),
            season_statuses={season: "pending" for season in seasons},
            resumed=False,
        )

    def record_season(
        self,
        manifest_path: ArtifactReference,
        season: int,
        status: str,
        details: Dict[str, Any],
    ) -> None:
        if status not in BACKFILL_SEASON_STATUSES[1:]:
            raise ValueError(f"unsupported backfill season status: {status}")
        path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(path)
        entries = self._season_entries(manifest)
        matching = [entry for entry in entries if entry["season"] == season]
        if len(matching) != 1:
            raise SeasonBackfillConflictError(
                f"backfill manifest does not contain season {season}"
            )
        recorded_at = as_utc(self.clock(), "backfill store clock").isoformat()
        entry = matching[0]
        previous_details = entry.get("details")
        merged_details = dict(details)
        if isinstance(previous_details, dict):
            for field in ("downloaded", "revisions_created", "unchanged"):
                previous_value = previous_details.get(field, 0)
                current_value = merged_details.get(field, 0)
                if type(previous_value) is int and type(current_value) is int:
                    merged_details[field] = previous_value + current_value
        entry.clear()
        entry.update(
            {
                "details": merged_details,
                "recorded_at": recorded_at,
                "season": season,
                "status": status,
            }
        )
        manifest["status"] = "open"
        manifest["updated_at"] = recorded_at
        manifest.pop("completed_at", None)
        atomic_write(path, encode_json(manifest))

    def finalize(self, manifest_path: ArtifactReference) -> Dict[int, str]:
        path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(path)
        statuses = self._season_statuses(manifest)
        run_status = (
            "complete"
            if statuses and all(status == "complete" for status in statuses.values())
            else "failed"
            if any(status == "failed" for status in statuses.values())
            else "incomplete"
        )
        finalized_at = as_utc(self.clock(), "backfill store clock").isoformat()
        manifest["status"] = run_status
        manifest["updated_at"] = finalized_at
        if run_status == "complete":
            manifest["completed_at"] = finalized_at
        else:
            manifest.pop("completed_at", None)
        atomic_write(path, encode_json(manifest))
        return statuses

    def land_changes_page(
        self,
        run_id: UUID,
        season: int,
        page_number: int,
        changes: GameChangesResponse,
        request: GameChangesRequest,
        raw: bytes,
    ) -> ArtifactReference:
        page_directory = (
            self.storage_root
            / "raw"
            / "mlb_stats_api"
            / "backfill_game_changes"
            / f"season={season}"
            / f"run_id={run_id}"
            / f"page={page_number:04d}"
        )
        response_path = page_directory / "response.json"
        metadata_path = page_directory / "metadata.json"
        checksum = sha256_bytes(raw)
        metadata = {
            "changed_game_pks": list(changes.game_pks),
            "contract": "mlb-stats-api-backfill-game-changes-page/v1",
            "request": request.as_dict(),
            "response_sha256": checksum,
            "run_id": str(run_id),
            "season": season,
            "total_items": changes.total_items,
        }
        if response_path.exists() and sha256_bytes(response_path.read_bytes()) != checksum:
            raise SeasonBackfillConflictError(
                f"backfill correction page {page_number} conflicts"
            )
        if metadata_path.exists():
            try:
                existing = read_json_object(metadata_path)
            except (ValueError, json.JSONDecodeError) as exc:
                raise SeasonBackfillConflictError(
                    f"backfill correction page {page_number} metadata is invalid"
                ) from exc
            if existing != metadata:
                raise SeasonBackfillConflictError(
                    f"backfill correction page {page_number} metadata conflicts"
                )
        if not response_path.exists():
            atomic_write(response_path, raw)
        if not metadata_path.exists():
            atomic_write(metadata_path, encode_json(metadata))
        return artifact_reference_for_path(self.storage_root, response_path)

    def complete_changes(
        self,
        run_id: UUID,
        season: int,
        updated_since: datetime,
        window_end: datetime,
        total_items: int,
        response_paths: Tuple[ArtifactReference, ...],
        changed_game_pks: Tuple[int, ...],
    ) -> None:
        manifest_path = self._changes_directory(run_id, season) / "manifest.json"
        payload = {
            "changed_game_pks": list(changed_game_pks),
            "contract": "mlb-stats-api-backfill-game-changes-manifest/v1",
            "response_paths": [path.key for path in response_paths],
            "run_id": str(run_id),
            "season": season,
            "status": "complete",
            "total_items": total_items,
            "updated_since": as_utc(updated_since, "updated_since").isoformat(),
            "window_end": as_utc(window_end, "window_end").isoformat(),
        }
        if manifest_path.exists():
            try:
                existing = read_json_object(manifest_path)
            except (ValueError, json.JSONDecodeError) as exc:
                raise SeasonBackfillConflictError(
                    "backfill correction manifest is invalid"
                ) from exc
            if existing != payload:
                raise SeasonBackfillConflictError(
                    "backfill correction manifest conflicts"
                )
            return
        atomic_write(manifest_path, encode_json(payload))

    def load_changes(
        self,
        run_id: UUID,
        season: int,
        updated_since: datetime,
        window_end: datetime,
    ) -> Optional[LoadedSeasonBackfillChanges]:
        manifest_path = self._changes_directory(run_id, season) / "manifest.json"
        if not manifest_path.exists():
            return None
        try:
            payload = read_json_object(manifest_path)
            changed_game_pks = payload["changed_game_pks"]
            response_keys = payload["response_paths"]
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            raise SeasonBackfillConflictError(
                "backfill correction manifest is invalid"
            ) from exc
        expected = {
            "contract": "mlb-stats-api-backfill-game-changes-manifest/v1",
            "run_id": str(run_id),
            "season": season,
            "status": "complete",
            "updated_since": as_utc(updated_since, "updated_since").isoformat(),
            "window_end": as_utc(window_end, "window_end").isoformat(),
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            raise SeasonBackfillConflictError(
                "backfill correction manifest conflicts"
            )
        if (
            not isinstance(changed_game_pks, list)
            or not all(type(game_pk) is int for game_pk in changed_game_pks)
            or not isinstance(response_keys, list)
            or not all(isinstance(key, str) for key in response_keys)
        ):
            raise SeasonBackfillConflictError(
                "backfill correction manifest content is invalid"
            )
        response_paths = tuple(
            artifact_reference_for_path(
                self.storage_root, self.storage_root.joinpath(*key.split("/"))
            )
            for key in response_keys
        )
        if any(
            not resolve_artifact_path(self.storage_root, path).exists()
            for path in response_paths
        ):
            raise SeasonBackfillConflictError(
                "backfill correction response evidence is incomplete"
            )
        return LoadedSeasonBackfillChanges(
            changed_game_pks=tuple(changed_game_pks),
            response_paths=response_paths,
        )

    def read_checkpoint(self, season: int) -> Optional[SeasonBackfillCheckpoint]:
        path = self._checkpoint_path(season)
        if not path.exists():
            return None
        try:
            payload = read_json_object(path)
            if payload.get("contract") != "zavant-season-backfill-checkpoint/v1":
                raise ValueError("unexpected checkpoint contract")
            stored_season = payload["season"]
            updated_since = datetime.fromisoformat(payload["updated_since"])
            run_id = UUID(payload["run_id"])
            manifest_key = payload["manifest_path"]
            updated_at = datetime.fromisoformat(payload["updated_at"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SeasonBackfillConflictError(
                f"season {season} backfill checkpoint is invalid"
            ) from exc
        if stored_season != season or not isinstance(manifest_key, str):
            raise SeasonBackfillConflictError(
                f"season {season} backfill checkpoint conflicts"
            )
        manifest_path = self.storage_root.joinpath(*manifest_key.split("/"))
        return SeasonBackfillCheckpoint(
            season=season,
            updated_since=as_utc(updated_since, "checkpoint updated_since"),
            run_id=run_id,
            manifest_path=artifact_reference_for_path(self.storage_root, manifest_path),
            updated_at=as_utc(updated_at, "checkpoint updated_at"),
        )

    def advance_checkpoint(
        self,
        season: int,
        expected_current: Optional[datetime],
        updated_since: datetime,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> SeasonBackfillCheckpoint:
        normalized_updated_since = as_utc(updated_since, "updated_since")
        current = self.read_checkpoint(season)
        observed_current = current.updated_since if current is not None else None
        normalized_expected = (
            as_utc(expected_current, "expected_current")
            if expected_current is not None
            else None
        )
        if observed_current != normalized_expected:
            raise SeasonBackfillConflictError(
                f"season {season} backfill checkpoint changed concurrently"
            )
        if observed_current is not None and normalized_updated_since < observed_current:
            raise ValueError("updated_since must not move a checkpoint backward")
        updated_at = as_utc(self.clock(), "backfill store clock")
        payload = {
            "contract": "zavant-season-backfill-checkpoint/v1",
            "manifest_path": manifest_path.key,
            "run_id": str(run_id),
            "season": season,
            "updated_at": updated_at.isoformat(),
            "updated_since": normalized_updated_since.isoformat(),
        }
        atomic_write(self._checkpoint_path(season), encode_json(payload))
        return SeasonBackfillCheckpoint(
            season=season,
            updated_since=normalized_updated_since,
            run_id=run_id,
            manifest_path=manifest_path,
            updated_at=updated_at,
        )

    def _manifest_path(self, started_at: datetime, run_id: UUID) -> Path:
        return (
            self.storage_root
            / "runs"
            / "backfill"
            / f"run_date={started_at.date().isoformat()}"
            / f"run_id={run_id}"
            / "manifest.json"
        )

    def _checkpoint_path(self, season: int) -> Path:
        return (
            self.storage_root
            / "state"
            / "mlb_stats_api"
            / "backfills"
            / f"season={season}"
            / "correction_checkpoint.json"
        )

    def _changes_directory(self, run_id: UUID, season: int) -> Path:
        return (
            self.storage_root
            / "raw"
            / "mlb_stats_api"
            / "backfill_game_changes"
            / f"season={season}"
            / f"run_id={run_id}"
        )

    @staticmethod
    def _read_manifest(path: Path) -> Dict[str, Any]:
        try:
            manifest = read_json_object(path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise SeasonBackfillConflictError(
                "season backfill manifest is invalid"
            ) from exc
        if manifest.get("contract") != "zavant-season-backfill-manifest/v1":
            raise SeasonBackfillConflictError(
                "season backfill manifest contract is invalid"
            )
        return manifest

    @staticmethod
    def _season_entries(manifest: Dict[str, Any]) -> Tuple[Dict[str, Any], ...]:
        value = manifest.get("season_runs")
        if not isinstance(value, list) or not all(
            isinstance(entry, dict) for entry in value
        ):
            raise SeasonBackfillConflictError(
                "season backfill manifest entries are invalid"
            )
        entries = tuple(value)
        seasons = [entry.get("season") for entry in entries]
        statuses = [entry.get("status") for entry in entries]
        if (
            any(type(season) is not int for season in seasons)
            or len(set(seasons)) != len(seasons)
            or any(status not in BACKFILL_SEASON_STATUSES for status in statuses)
        ):
            raise SeasonBackfillConflictError(
                "season backfill manifest entries conflict"
            )
        return entries

    @classmethod
    def _season_statuses(cls, manifest: Dict[str, Any]) -> Dict[int, str]:
        return {
            entry["season"]: entry["status"]
            for entry in cls._season_entries(manifest)
        }
