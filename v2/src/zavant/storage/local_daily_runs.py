"""Durable local manifests for coordinated daily acquisition runs."""

from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict
from uuid import UUID

from zavant.storage._local_files import (
    atomic_write,
    encode_json,
    local_artifact_path,
    local_artifact_reference,
    read_json_object,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import DailyRunConflictError
from zavant.storage.models import StartedDailyRun


Clock = Callable[[], datetime]
DAILY_BRANCHES = ("correction_discovery", "correction_processing", "schedule_discovery")
DAILY_BRANCH_STATUSES = ("complete", "failed", "skipped")


def utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        A timezone-aware UTC timestamp.
    """

    return datetime.now(timezone.utc)


class LocalDailyRunStore:
    """Persist coordinator branch outcomes and aggregate run status.

    Args:
        data_dir: Root directory containing operational run state.
        clock: Function returning the current timezone-aware UTC time.
    """

    def __init__(self, data_dir: Path, clock: Clock = utc_now) -> None:
        """Initialize the local daily-run store.

        Args:
            data_dir: Root directory containing operational run state.
            clock: Function returning the current timezone-aware UTC time.
        """

        self.data_dir = data_dir
        self.clock = clock

    def start(
        self,
        run_id: UUID,
        started_at: datetime,
        through_date: date,
        configuration: Dict[str, Any],
    ) -> StartedDailyRun:
        """Create an open daily run before executing any branch.

        Args:
            run_id: Unique coordinator run identifier.
            started_at: Timestamp captured before branch work.
            through_date: Inclusive schedule discovery date.
            configuration: JSON-serializable run configuration.

        Returns:
            Stable daily run identity and manifest path.

        Raises:
            ValueError: If `started_at` is timezone-naive.
            DailyRunConflictError: If the run path already exists.
            OSError: If the manifest cannot be written.
        """

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
            manifest_path=local_artifact_reference(self.data_dir, manifest_path),
        )

    def record_branch(
        self,
        manifest_path: ArtifactReference,
        branch: str,
        status: str,
        details: Dict[str, Any],
    ) -> None:
        """Atomically record one coordinator branch outcome.

        Args:
            manifest_path: Existing daily run manifest.
            branch: Supported coordinator branch name.
            status: Complete, failed, or skipped.
            details: JSON-serializable branch result or error details.

        Raises:
            ValueError: If the branch name or status is unsupported.
            DailyRunConflictError: If the manifest is invalid or finalized.
            OSError: If the manifest cannot be read or written.
        """

        if branch not in DAILY_BRANCHES:
            raise ValueError(f"unsupported daily branch: {branch}")
        if status not in DAILY_BRANCH_STATUSES:
            raise ValueError(f"unsupported daily branch status: {status}")
        local_manifest_path = local_artifact_path(self.data_dir, manifest_path)
        manifest = self._read_manifest(local_manifest_path)
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
        atomic_write(local_manifest_path, encode_json(manifest))

    def finalize(self, manifest_path: ArtifactReference) -> Dict[str, str]:
        """Validate all branch outcomes and publish aggregate run status.

        Args:
            manifest_path: Existing daily run manifest.

        Returns:
            Mapping from branch name to recorded status.

        Raises:
            DailyRunConflictError: If branches are missing or malformed.
            OSError: If the manifest cannot be read or written.
        """

        local_manifest_path = local_artifact_path(self.data_dir, manifest_path)
        manifest = self._read_manifest(local_manifest_path)
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
        atomic_write(local_manifest_path, encode_json(manifest))
        return statuses

    @staticmethod
    def _read_manifest(manifest_path: Path) -> Dict[str, Any]:
        """Read and validate the envelope of a daily run manifest.

        Args:
            manifest_path: Existing daily run manifest.

        Returns:
            Parsed manifest object.

        Raises:
            DailyRunConflictError: If the file or contract is invalid.
            OSError: If the manifest cannot be read.
        """

        try:
            manifest = read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise DailyRunConflictError("daily run manifest is invalid") from exc
        if manifest.get("contract") != "zavant-daily-acquisition-run/v1":
            raise DailyRunConflictError("daily run manifest contract is invalid")
        return manifest

    def _manifest_path(self, started_at: datetime, run_id: UUID) -> Path:
        """Build a partitioned daily run manifest path.

        Args:
            started_at: Normalized UTC run timestamp.
            run_id: Unique coordinator run identifier.

        Returns:
            Manifest path below the configured data root.
        """

        return (
            self.data_dir
            / "runs"
            / "daily"
            / f"run_date={started_at.date().isoformat()}"
            / f"run_id={run_id}"
            / "manifest.json"
        )

    @staticmethod
    def _normalize_timestamp(value: datetime, name: str) -> datetime:
        """Validate and normalize one timestamp to UTC.

        Args:
            value: Candidate timestamp.
            name: Field name used in validation errors.

        Returns:
            Timezone-aware UTC timestamp.

        Raises:
            ValueError: If the timestamp is timezone-naive.
        """

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include a UTC offset")
        return value.astimezone(timezone.utc)
