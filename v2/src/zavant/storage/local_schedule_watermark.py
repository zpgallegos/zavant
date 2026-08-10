"""Durable local through-date for incremental schedule discovery."""

from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from uuid import UUID

from zavant.storage._local_files import (
    atomic_write,
    encode_json,
    local_artifact_path,
    local_artifact_reference,
    read_json_object,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import ScheduleWatermarkConflictError
from zavant.storage.models import ScheduleWatermark


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        A timezone-aware UTC timestamp.
    """

    return datetime.now(timezone.utc)


class LocalScheduleWatermarkStore:
    """Persist the schedule through-date as one atomic state document.

    Args:
        data_dir: Root directory containing raw data and operational state.
        clock: Function returning the current timezone-aware UTC time.
    """

    def __init__(self, data_dir: Path, clock: Clock = utc_now) -> None:
        """Initialize the local schedule watermark store.

        Args:
            data_dir: Root directory containing raw data and operational state.
            clock: Function returning the current timezone-aware UTC time.
        """

        self.data_dir = data_dir
        self.clock = clock
        self.path = (
            data_dir / "state" / "mlb_stats_api" / "schedules" / "watermark.json"
        )

    def read(self) -> Optional[ScheduleWatermark]:
        """Read and validate current schedule discovery state.

        Returns:
            Current schedule watermark, or `None` before initialization.

        Raises:
            ScheduleWatermarkConflictError: If stored state is malformed.
            OSError: If stored state cannot be read.
        """

        if not self.path.exists():
            return None
        try:
            payload = read_json_object(self.path)
            return self._from_payload(payload)
        except (ValueError, json.JSONDecodeError) as exc:
            raise ScheduleWatermarkConflictError(
                "schedule watermark is invalid"
            ) from exc

    def advance(
        self,
        expected_current: Optional[date],
        advanced_from: date,
        through_date: date,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> ScheduleWatermark:
        """Compare current state and atomically publish a new through-date.

        Args:
            expected_current: Through-date observed before discovery, or `None`
                during bootstrap.
            advanced_from: Prior through-date or bootstrap start date.
            through_date: Latest successfully covered schedule date.
            run_id: Successful schedule acquisition run.
            manifest_path: Completed manifest supporting the transition.

        Returns:
            Newly persisted schedule watermark.

        Raises:
            ValueError: If date ordering or manifest lineage is invalid.
            ScheduleWatermarkConflictError: If state changed during discovery.
            OSError: If state or manifest files cannot be read or written.
        """

        if advanced_from > through_date:
            raise ValueError("through_date must not be before advanced_from")
        if expected_current is not None and expected_current != advanced_from:
            raise ValueError("advanced_from must equal the expected current date")
        local_manifest_path = local_artifact_path(self.data_dir, manifest_path)
        try:
            manifest = read_json_object(local_manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise ValueError("manifest_path must contain a valid schedule run") from exc
        request = manifest.get("request")
        expected_manifest_fields = {
            "contract": "mlb-stats-api-schedule-manifest/v1",
            "run_id": str(run_id),
            "status": "complete",
        }
        if (
            any(
                manifest.get(key) != value
                for key, value in expected_manifest_fields.items()
            )
            or not isinstance(request, dict)
            or request.get("end_date") != through_date.isoformat()
        ):
            raise ValueError("manifest_path must identify this completed schedule run")

        current = self.read()
        actual_current = current.through_date if current is not None else None
        if actual_current != expected_current:
            raise ScheduleWatermarkConflictError(
                "schedule watermark changed while discovery was running"
            )

        updated_at = self._normalize_timestamp(self.clock(), "clock result")
        watermark = ScheduleWatermark(
            through_date=through_date,
            advanced_from=advanced_from,
            run_id=run_id,
            manifest_path=manifest_path,
            updated_at=updated_at,
        )
        payload = {
            "advanced_from": advanced_from.isoformat(),
            "contract": "mlb-stats-api-schedule-watermark/v1",
            "manifest_path": manifest_path.key,
            "run_id": str(run_id),
            "through_date": through_date.isoformat(),
            "updated_at": updated_at.isoformat(),
        }
        atomic_write(self.path, encode_json(payload))
        return watermark

    def _from_payload(self, payload: Dict[str, Any]) -> ScheduleWatermark:
        """Validate and construct schedule state from stored JSON.

        Args:
            payload: Parsed state document.

        Returns:
            Validated schedule watermark.

        Raises:
            ValueError: If any required field is invalid.
        """

        if payload.get("contract") != "mlb-stats-api-schedule-watermark/v1":
            raise ValueError("schedule watermark contract is unsupported")
        run_id_value = payload.get("run_id")
        manifest_path_value = payload.get("manifest_path")
        if not isinstance(run_id_value, str):
            raise ValueError("schedule watermark run_id is invalid")
        if not isinstance(manifest_path_value, str) or not manifest_path_value:
            raise ValueError("schedule watermark manifest_path is invalid")
        relative_manifest_path = Path(manifest_path_value)
        if relative_manifest_path.is_absolute() or ".." in relative_manifest_path.parts:
            raise ValueError("schedule watermark manifest_path is invalid")
        manifest_path = self.data_dir / relative_manifest_path
        manifest_reference = local_artifact_reference(self.data_dir, manifest_path)
        if not manifest_path.exists():
            raise ValueError("schedule watermark manifest_path does not exist")
        advanced_from = self._parse_date(payload, "advanced_from")
        through_date = self._parse_date(payload, "through_date")
        if advanced_from > through_date:
            raise ValueError("schedule watermark dates are not ordered")
        return ScheduleWatermark(
            through_date=through_date,
            advanced_from=advanced_from,
            run_id=UUID(run_id_value),
            manifest_path=manifest_reference,
            updated_at=self._parse_timestamp(payload, "updated_at"),
        )

    @staticmethod
    def _parse_date(payload: Dict[str, Any], key: str) -> date:
        """Parse one stored ISO date.

        Args:
            payload: State document containing the date.
            key: Date field to parse.

        Returns:
            Parsed calendar date.

        Raises:
            ValueError: If the field is missing or malformed.
        """

        value = payload.get(key)
        if not isinstance(value, str):
            raise ValueError(f"schedule watermark {key} is invalid")
        return date.fromisoformat(value)

    @classmethod
    def _parse_timestamp(cls, payload: Dict[str, Any], key: str) -> datetime:
        """Parse one stored timezone-aware timestamp.

        Args:
            payload: State document containing the timestamp.
            key: Timestamp field to parse.

        Returns:
            Timestamp normalized to UTC.

        Raises:
            ValueError: If the field is missing, malformed, or timezone-naive.
        """

        value = payload.get(key)
        if not isinstance(value, str):
            raise ValueError(f"schedule watermark {key} is invalid")
        return cls._normalize_timestamp(datetime.fromisoformat(value), key)

    @staticmethod
    def _normalize_timestamp(value: datetime, name: str) -> datetime:
        """Validate and normalize a timestamp to UTC.

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
