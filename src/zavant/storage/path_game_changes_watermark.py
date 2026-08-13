"""Path-backed watermark for MLB corrected-game polling."""

from datetime import datetime, timezone
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
from zavant.storage.errors import GameChangesWatermarkConflictError
from zavant.storage.models import GameChangesWatermark


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PathGameChangesWatermarkStore:
    """Persist the correction checkpoint as one atomic state document.

    Args:
        storage_root: Root path containing raw data and operational state.
        clock: Function returning the current timezone-aware UTC time.
    """

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.storage_root = storage_root
        self.clock = clock
        self.path = (
            storage_root / "state" / "mlb_stats_api" / "game_changes" / "watermark.json"
        )

    def read(self) -> Optional[GameChangesWatermark]:
        if not self.path.exists():
            return None
        try:
            payload = read_json_object(self.path)
            return self._from_payload(payload)
        except (ValueError, json.JSONDecodeError) as exc:
            raise GameChangesWatermarkConflictError(
                "game-changes watermark is invalid"
            ) from exc

    def advance(
        self,
        expected_current: Optional[datetime],
        advanced_from: datetime,
        updated_since: datetime,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> GameChangesWatermark:
        normalized_expected = self._normalize_optional_timestamp(
            expected_current, "expected_current"
        )
        normalized_from = self._normalize_timestamp(advanced_from, "advanced_from")
        normalized_updated_since = self._normalize_timestamp(
            updated_since, "updated_since"
        )
        if normalized_from >= normalized_updated_since:
            raise ValueError("updated_since must be after advanced_from")
        if normalized_expected is not None and normalized_expected != normalized_from:
            raise ValueError("advanced_from must equal the expected current watermark")
        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        if not resolved_manifest_path.exists():
            raise ValueError("manifest_path must identify a completed poll manifest")
        try:
            manifest = read_json_object(resolved_manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise ValueError(
                "manifest_path must contain a valid poll manifest"
            ) from exc
        expected_manifest_fields = {
            "contract": "mlb-stats-api-game-changes-manifest/v1",
            "run_id": str(run_id),
            "status": "complete",
            "watermark_before": normalized_from.isoformat(),
            "window_end": normalized_updated_since.isoformat(),
        }
        if any(
            manifest.get(key) != value
            for key, value in expected_manifest_fields.items()
        ):
            raise ValueError("manifest_path must identify this completed poll manifest")

        current = self.read()
        actual_current = current.updated_since if current is not None else None
        if actual_current != normalized_expected:
            raise GameChangesWatermarkConflictError(
                "game-changes watermark changed while the poll was running"
            )

        updated_at = self._normalize_timestamp(self.clock(), "clock result")
        watermark = GameChangesWatermark(
            updated_since=normalized_updated_since,
            advanced_from=normalized_from,
            run_id=run_id,
            manifest_path=manifest_path,
            updated_at=updated_at,
        )
        payload = {
            "advanced_from": normalized_from.isoformat(),
            "contract": "mlb-stats-api-game-changes-watermark/v1",
            "manifest_path": manifest_path.key,
            "run_id": str(run_id),
            "updated_at": updated_at.isoformat(),
            "updated_since": normalized_updated_since.isoformat(),
        }
        atomic_write(self.path, encode_json(payload))
        return watermark

    def _from_payload(self, payload: Dict[str, Any]) -> GameChangesWatermark:
        if payload.get("contract") != "mlb-stats-api-game-changes-watermark/v1":
            raise ValueError("watermark contract is unsupported")
        run_id_value = payload.get("run_id")
        manifest_path_value = payload.get("manifest_path")
        if not isinstance(run_id_value, str):
            raise ValueError("watermark run_id is invalid")
        if not isinstance(manifest_path_value, str) or not manifest_path_value:
            raise ValueError("watermark manifest_path is invalid")
        relative_manifest_path = Path(manifest_path_value)
        if relative_manifest_path.is_absolute() or ".." in relative_manifest_path.parts:
            raise ValueError("watermark manifest_path is invalid")
        advanced_from = self._parse_timestamp(payload, "advanced_from")
        updated_since = self._parse_timestamp(payload, "updated_since")
        if advanced_from >= updated_since:
            raise ValueError("watermark timestamps are not increasing")
        manifest_path = self.storage_root / relative_manifest_path
        manifest_reference = artifact_reference_for_path(self.storage_root, manifest_path)
        if not manifest_path.exists():
            raise ValueError("watermark manifest_path does not exist")
        return GameChangesWatermark(
            updated_since=updated_since,
            advanced_from=advanced_from,
            run_id=UUID(run_id_value),
            manifest_path=manifest_reference,
            updated_at=self._parse_timestamp(payload, "updated_at"),
        )

    @classmethod
    def _parse_timestamp(cls, payload: Dict[str, Any], key: str) -> datetime:
        value = payload.get(key)
        if not isinstance(value, str):
            raise ValueError(f"watermark {key} is invalid")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"watermark {key} is invalid") from exc
        return cls._normalize_timestamp(parsed, key)

    @staticmethod
    def _normalize_timestamp(value: datetime, name: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include a UTC offset")
        return value.astimezone(timezone.utc)

    @classmethod
    def _normalize_optional_timestamp(
        cls,
        value: Optional[datetime],
        name: str,
    ) -> Optional[datetime]:
        return None if value is None else cls._normalize_timestamp(value, name)
