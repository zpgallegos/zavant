"""Path-backed through-date for incremental schedule discovery."""

from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from uuid import UUID

from zavant.storage._path_io import (
    atomic_write,
    encode_json,
    resolve_artifact_path,
    artifact_reference_for_path,
    read_json_object,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import ScheduleWatermarkConflictError
from zavant.storage.models import ScheduleWatermark


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PathScheduleWatermarkStore:
    """Persist the schedule through-date as one atomic state document.

    Args:
        storage_root: Root path containing raw data and operational state.
        clock: Function returning the current timezone-aware UTC time.
    """

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.storage_root = storage_root
        self.clock = clock
        self.path = (
            storage_root / "state" / "mlb_stats_api" / "schedules" / "watermark.json"
        )

    def read(self) -> Optional[ScheduleWatermark]:
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
        if advanced_from > through_date:
            raise ValueError("through_date must not be before advanced_from")
        if expected_current is not None and expected_current != advanced_from:
            raise ValueError("advanced_from must equal the expected current date")
        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        try:
            manifest = read_json_object(resolved_manifest_path)
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
        manifest_path = self.storage_root / relative_manifest_path
        manifest_reference = artifact_reference_for_path(self.storage_root, manifest_path)
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
        value = payload.get(key)
        if not isinstance(value, str):
            raise ValueError(f"schedule watermark {key} is invalid")
        return date.fromisoformat(value)

    @classmethod
    def _parse_timestamp(cls, payload: Dict[str, Any], key: str) -> datetime:
        value = payload.get(key)
        if not isinstance(value, str):
            raise ValueError(f"schedule watermark {key} is invalid")
        return cls._normalize_timestamp(datetime.fromisoformat(value), key)

    @staticmethod
    def _normalize_timestamp(value: datetime, name: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include a UTC offset")
        return value.astimezone(timezone.utc)
