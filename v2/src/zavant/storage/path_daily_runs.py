"""Path-backed manifests for coordinated daily acquisition runs."""

from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict
from uuid import UUID

from zavant.storage._path_io import (
    atomic_write,
    encode_json,
    resolve_artifact_path,
    artifact_reference_for_path,
    read_json_object,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import DailyRunConflictError
from zavant.storage.models import StartedDailyRun


Clock = Callable[[], datetime]
DAILY_BRANCHES = ("correction_discovery", "correction_processing", "schedule_discovery")
DAILY_BRANCH_STATUSES = ("complete", "failed", "skipped")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PathDailyRunStore:
    """Persist coordinator branch outcomes and aggregate run status.

    Args:
        storage_root: Root path containing operational run state.
        clock: Function returning the current timezone-aware UTC time.
    """

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.storage_root = storage_root
        self.clock = clock

    def start(
        self,
        run_id: UUID,
        started_at: datetime,
        through_date: date,
        configuration: Dict[str, Any],
    ) -> StartedDailyRun:
        normalized_started_at = self._normalize_timestamp(started_at, "started_at")
        manifest_path = self._manifest_path(normalized_started_at, run_id)
        if manifest_path.exists():
            raise DailyRunConflictError("daily run already exists")
        manifest = {
            "branches": {},
            "configuration": configuration,
            "contract": "zavant-daily-acquisition-run/v1",
            "created_at": normalized_started_at.isoformat(),
            "run_id": str(run_id),
            "started_at": normalized_started_at.isoformat(),
            "status": "running",
            "through_date": through_date.isoformat(),
            "updated_at": normalized_started_at.isoformat(),
        }
        atomic_write(manifest_path, encode_json(manifest))
        return StartedDailyRun(
            run_id=run_id,
            started_at=normalized_started_at,
            through_date=through_date,
            manifest_path=artifact_reference_for_path(self.storage_root, manifest_path),
        )

    def record_branch(
        self,
        manifest_path: ArtifactReference,
        branch: str,
        status: str,
        details: Dict[str, Any],
    ) -> None:
        if branch not in DAILY_BRANCHES:
            raise ValueError(f"unsupported daily branch: {branch}")
        if status not in DAILY_BRANCH_STATUSES:
            raise ValueError(f"unsupported daily branch status: {status}")
        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(resolved_manifest_path)
        if manifest.get("status") != "running":
            raise DailyRunConflictError("finalized daily run cannot be updated")
        branches = manifest.get("branches")
        if not isinstance(branches, dict):
            raise DailyRunConflictError("daily run branches are invalid")
        if branch in branches:
            raise DailyRunConflictError(f"daily branch already recorded: {branch}")
        recorded_at = self._normalize_timestamp(
            self.clock(), "clock result"
        ).isoformat()
        branches[branch] = {
            "details": details,
            "recorded_at": recorded_at,
            "status": status,
        }
        manifest["updated_at"] = recorded_at
        atomic_write(resolved_manifest_path, encode_json(manifest))

    def finalize(self, manifest_path: ArtifactReference) -> Dict[str, str]:
        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(resolved_manifest_path)
        branches = manifest.get("branches")
        if not isinstance(branches, dict):
            raise DailyRunConflictError("daily run branches are invalid")
        if set(branches) != set(DAILY_BRANCHES):
            raise DailyRunConflictError("daily run does not contain every branch")
        statuses: Dict[str, str] = {}
        for branch in DAILY_BRANCHES:
            entry = branches.get(branch)
            if not isinstance(entry, dict):
                raise DailyRunConflictError(f"daily branch {branch} is invalid")
            status = entry.get("status")
            if not isinstance(status, str) or status not in DAILY_BRANCH_STATUSES:
                raise DailyRunConflictError(
                    f"daily branch {branch} has an invalid status"
                )
            statuses[branch] = status
        run_status = (
            "failed"
            if any(status == "failed" for status in statuses.values())
            else "complete"
        )
        finalized_at = self._normalize_timestamp(
            self.clock(), "clock result"
        ).isoformat()
        manifest["completed_at"] = finalized_at
        manifest["status"] = run_status
        manifest["summary"] = statuses
        manifest["updated_at"] = finalized_at
        atomic_write(resolved_manifest_path, encode_json(manifest))
        return statuses

    @staticmethod
    def _read_manifest(manifest_path: Path) -> Dict[str, Any]:
        try:
            manifest = read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise DailyRunConflictError("daily run manifest is invalid") from exc
        if manifest.get("contract") != "zavant-daily-acquisition-run/v1":
            raise DailyRunConflictError("daily run manifest contract is invalid")
        return manifest

    def _manifest_path(self, started_at: datetime, run_id: UUID) -> Path:
        return (
            self.storage_root
            / "runs"
            / "daily"
            / f"run_date={started_at.date().isoformat()}"
            / f"run_id={run_id}"
            / "manifest.json"
        )

    @staticmethod
    def _normalize_timestamp(value: datetime, name: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include a UTC offset")
        return value.astimezone(timezone.utc)
