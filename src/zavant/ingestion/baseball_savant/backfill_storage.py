"""Path-backed manifests for resumable Baseball Savant backfills."""

from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Protocol, Tuple
from uuid import UUID

from zavant._time import Clock, as_utc, utc_now
from zavant.storage._path_io import (
    artifact_reference_for_path,
    atomic_write,
    encode_json,
    read_json_object,
    resolve_artifact_path,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.baseball_savant.storage import BaseballSavantStorageError


BACKFILL_DATE_STATUSES = ("pending", "succeeded", "skipped", "failed")


@dataclass(frozen=True)
class StartedBaseballSavantBackfill:
    """Persisted identity and current state of a Savant backfill run."""

    manifest_path: ArtifactReference
    date_statuses: Dict[date, str]
    resumed: bool


class BaseballSavantBackfillStore(Protocol):
    """Persistence required to resume a bounded Savant date backfill."""

    def start(
        self,
        *,
        run_id: UUID,
        started_at: datetime,
        start_date: date,
        end_date: date,
        mode: str,
        dry_run: bool,
        configuration: Mapping[str, Any],
    ) -> StartedBaseballSavantBackfill: ...

    def record_date(
        self,
        manifest_path: ArtifactReference,
        game_date: date,
        status: str,
        details: Mapping[str, Any],
    ) -> None: ...

    def finalize(self, manifest_path: ArtifactReference) -> Dict[str, int]: ...


class PathBaseballSavantBackfillStore:
    """Persist Savant backfill progress independently of daily state.

    The path surface may be a local directory or the conditional S3 facade;
    both backends therefore share the same manifest contract and transitions.
    """

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.storage_root = storage_root
        self.clock = clock

    def start(
        self,
        *,
        run_id: UUID,
        started_at: datetime,
        start_date: date,
        end_date: date,
        mode: str,
        dry_run: bool,
        configuration: Mapping[str, Any],
    ) -> StartedBaseballSavantBackfill:
        normalized_started_at = as_utc(started_at, "started_at")
        manifest_path = self._manifest_path(normalized_started_at, run_id)
        expected = {
            "configuration": dict(configuration),
            "contract": "baseball-savant-backfill-run/v1",
            "dry_run": dry_run,
            "end_date": end_date.isoformat(),
            "mode": mode,
            "run_id": str(run_id),
            "start_date": start_date.isoformat(),
            "started_at": normalized_started_at.isoformat(),
        }
        if manifest_path.exists():
            manifest = self._read_manifest(manifest_path)
            if any(manifest.get(key) != value for key, value in expected.items()):
                raise BaseballSavantStorageError(
                    "Savant backfill run conflicts with stored configuration"
                )
            return StartedBaseballSavantBackfill(
                manifest_path=artifact_reference_for_path(
                    self.storage_root, manifest_path
                ),
                date_statuses=self._date_statuses(manifest),
                resumed=True,
            )

        observed_at = as_utc(self.clock(), "backfill store clock").isoformat()
        planned_dates = self._inclusive_dates(start_date, end_date)
        manifest = {
            **expected,
            "created_at": observed_at,
            "dates": {
                game_date.isoformat(): {"status": "pending"}
                for game_date in planned_dates
            },
            "status": "open",
            "updated_at": observed_at,
        }
        atomic_write(manifest_path, encode_json(manifest))
        return StartedBaseballSavantBackfill(
            manifest_path=artifact_reference_for_path(self.storage_root, manifest_path),
            date_statuses={game_date: "pending" for game_date in planned_dates},
            resumed=False,
        )

    def record_date(
        self,
        manifest_path: ArtifactReference,
        game_date: date,
        status: str,
        details: Mapping[str, Any],
    ) -> None:
        if status not in BACKFILL_DATE_STATUSES[1:]:
            raise ValueError("unsupported Savant backfill date status")
        path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(path)
        dates = self._date_entries(manifest)
        key = game_date.isoformat()
        entry = dates.get(key)
        if entry is None:
            raise BaseballSavantStorageError(
                "Savant backfill date is not planned by this run"
            )
        if entry.get("status") in {"succeeded", "skipped"}:
            raise BaseballSavantStorageError(
                "completed Savant backfill date cannot be updated"
            )
        recorded_at = as_utc(self.clock(), "backfill store clock").isoformat()
        dates[key] = {
            "details": dict(details),
            "recorded_at": recorded_at,
            "status": status,
        }
        manifest["dates"] = dates
        manifest["status"] = "open"
        manifest["updated_at"] = recorded_at
        manifest.pop("completed_at", None)
        manifest.pop("summary", None)
        atomic_write(path, encode_json(manifest))

    def finalize(self, manifest_path: ArtifactReference) -> Dict[str, int]:
        path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(path)
        counts = {status: 0 for status in BACKFILL_DATE_STATUSES}
        for entry in self._date_entries(manifest).values():
            counts[entry["status"]] += 1
        if counts["pending"]:
            status = "incomplete"
        elif counts["failed"]:
            status = "failed"
        else:
            status = "complete"
        finalized_at = as_utc(self.clock(), "backfill store clock").isoformat()
        manifest["status"] = status
        manifest["summary"] = counts
        manifest["updated_at"] = finalized_at
        if status == "complete":
            manifest["completed_at"] = finalized_at
        else:
            manifest.pop("completed_at", None)
        atomic_write(path, encode_json(manifest))
        return counts

    def _manifest_path(self, started_at: datetime, run_id: UUID) -> Path:
        return (
            self.storage_root
            / "runs"
            / "baseball_savant"
            / "backfill"
            / f"run_date={started_at.date().isoformat()}"
            / f"run_id={run_id}"
            / "manifest.json"
        )

    @staticmethod
    def _inclusive_dates(start_date: date, end_date: date) -> Tuple[date, ...]:
        if start_date > end_date:
            raise ValueError("start_date must not be after end_date")
        return tuple(
            date.fromordinal(ordinal)
            for ordinal in range(start_date.toordinal(), end_date.toordinal() + 1)
        )

    @staticmethod
    def _read_manifest(path: Path) -> Dict[str, Any]:
        try:
            manifest = read_json_object(path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise BaseballSavantStorageError(
                "Savant backfill manifest is invalid"
            ) from exc
        if manifest.get("contract") != "baseball-savant-backfill-run/v1":
            raise BaseballSavantStorageError(
                "Savant backfill manifest contract is invalid"
            )
        return manifest

    @staticmethod
    def _date_entries(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        value = manifest.get("dates")
        if not isinstance(value, dict) or not value:
            raise BaseballSavantStorageError(
                "Savant backfill manifest dates are invalid"
            )
        entries: Dict[str, Dict[str, Any]] = {}
        for key, entry in value.items():
            if not isinstance(key, str) or not isinstance(entry, dict):
                raise BaseballSavantStorageError(
                    "Savant backfill manifest dates are invalid"
                )
            try:
                parsed = date.fromisoformat(key)
            except ValueError as exc:
                raise BaseballSavantStorageError(
                    "Savant backfill manifest date is invalid"
                ) from exc
            if parsed.isoformat() != key or entry.get("status") not in (
                BACKFILL_DATE_STATUSES
            ):
                raise BaseballSavantStorageError(
                    "Savant backfill manifest date status is invalid"
                )
            entries[key] = entry
        return entries

    @classmethod
    def _date_statuses(cls, manifest: Dict[str, Any]) -> Dict[date, str]:
        return {
            date.fromisoformat(key): entry["status"]
            for key, entry in cls._date_entries(manifest).items()
        }
