"""Revision-aware storage for Baseball Savant daily CSV acquisition."""

from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Protocol, Tuple, cast
from uuid import UUID

from zavant._time import Clock, as_utc, utc_now
from zavant.ingestion.baseball_savant.contract import StatcastCsvResponse
from zavant.storage._path_io import (
    artifact_reference_for_path,
    atomic_write,
    encode_json,
    read_json_object,
    resolve_artifact_path,
    sha256_bytes,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.s3_objects import S3Client, S3ObjectBackend


DATE_STATUSES = ("pending", "succeeded", "failed")


class BaseballSavantStorageError(RuntimeError):
    """Raised when Savant raw evidence or acquisition state conflicts."""


@dataclass(frozen=True)
class LandedStatcastDate:
    """Artifacts and revision identity for one landed game-date export."""

    game_date: date
    revision_id: str
    previous_revision_id: Optional[str]
    response_path: ArtifactReference
    metadata_path: ArtifactReference
    current_pointer_path: ArtifactReference
    row_count: int
    terminal_row_count: int
    created: bool

    def as_dict(self) -> Dict[str, Any]:
        return {
            "created": self.created,
            "current_pointer_path": str(self.current_pointer_path),
            "game_date": self.game_date.isoformat(),
            "metadata_path": str(self.metadata_path),
            "previous_revision_id": self.previous_revision_id,
            "response_path": str(self.response_path),
            "revision_id": self.revision_id,
            "row_count": self.row_count,
            "terminal_row_count": self.terminal_row_count,
        }


@dataclass(frozen=True)
class BaseballSavantWatermark:
    """Successful through-date for the Savant daily acquisition process."""

    through_date: date
    run_id: UUID
    manifest_path: ArtifactReference
    updated_at: datetime


class BaseballSavantRawStore(Protocol):
    """Revision-aware persistence for exact-date Savant snapshots."""

    def land_date(
        self,
        response: StatcastCsvResponse,
        raw: bytes,
        source_uri: str,
        run_id: UUID,
    ) -> LandedStatcastDate: ...

    def current_revision_id(self, game_date: date) -> Optional[str]: ...


class BaseballSavantStore(BaseballSavantRawStore, Protocol):
    """Persistence contract for Savant snapshots and daily acquisition state."""

    def start_run(
        self,
        run_id: UUID,
        started_at: datetime,
        through_date: date,
        planned_dates: Tuple[date, ...],
        configuration: Mapping[str, Any],
    ) -> ArtifactReference: ...

    def record_date(
        self,
        manifest_path: ArtifactReference,
        game_date: date,
        status: str,
        details: Mapping[str, Any],
    ) -> None: ...

    def finalize_run(self, manifest_path: ArtifactReference) -> Dict[str, int]: ...

    def read_watermark(self) -> Optional[BaseballSavantWatermark]: ...

    def advance_watermark(
        self,
        expected_current: Optional[date],
        through_date: date,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> BaseballSavantWatermark: ...


class PathBaseballSavantStore:
    """Revision and daily-state machine shared by local and S3 paths.

    A revision represents the exact CSV bytes returned for one game date. The
    immutable revisions retain source history and ``current.json`` selects the
    date snapshot that later projection should consume.
    """

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.storage_root = storage_root
        self.clock = clock
        self.watermark_path = (
            storage_root
            / "state"
            / "baseball_savant"
            / "statcast_search"
            / "watermark.json"
        )

    def start_run(
        self,
        run_id: UUID,
        started_at: datetime,
        through_date: date,
        planned_dates: Tuple[date, ...],
        configuration: Mapping[str, Any],
    ) -> ArtifactReference:
        normalized_started_at = as_utc(started_at, "started_at")
        if not planned_dates:
            raise ValueError("planned_dates must not be empty")
        if tuple(sorted(set(planned_dates))) != planned_dates:
            raise ValueError("planned_dates must be unique and ordered")
        manifest_path = (
            self.storage_root
            / "runs"
            / "baseball_savant"
            / "daily"
            / f"run_date={normalized_started_at.date().isoformat()}"
            / f"run_id={run_id}"
            / "manifest.json"
        )
        if manifest_path.exists():
            raise BaseballSavantStorageError("Savant daily run already exists")
        manifest = {
            "configuration": dict(configuration),
            "contract": "baseball-savant-daily-run/v1",
            "dates": {
                value.isoformat(): {"status": "pending"} for value in planned_dates
            },
            "run_id": str(run_id),
            "started_at": normalized_started_at.isoformat(),
            "status": "running",
            "through_date": through_date.isoformat(),
            "updated_at": normalized_started_at.isoformat(),
        }
        atomic_write(manifest_path, encode_json(manifest))
        return artifact_reference_for_path(self.storage_root, manifest_path)

    def land_date(
        self,
        response: StatcastCsvResponse,
        raw: bytes,
        source_uri: str,
        run_id: UUID,
    ) -> LandedStatcastDate:
        # Savant is CSV rather than semantic JSON, so exact response bytes are
        # both the evidence checksum and the revision identity.
        revision_id = sha256_bytes(raw)
        date_directory = (
            self.storage_root
            / "raw"
            / "baseball_savant"
            / "statcast_search"
            / f"game_date={response.game_date.isoformat()}"
        )
        revision_directory = date_directory / f"revision={revision_id}"
        response_path = revision_directory / "response.csv"
        metadata_path = revision_directory / "metadata.json"
        current_pointer_path = date_directory / "current.json"
        previous_revision_id = self._read_current_revision(
            current_pointer_path, response.game_date
        )
        revision_exists = response_path.exists() and metadata_path.exists()

        if response_path.exists() and sha256_bytes(response_path.read_bytes()) != revision_id:
            raise BaseballSavantStorageError(
                f"Savant revision {revision_id} contains different response bytes"
            )

        observed_at = as_utc(self.clock(), "clock result")
        metadata = {
            "columns": list(response.columns),
            "content_length": len(raw),
            "contract": "baseball-savant-statcast-response/v1",
            "game_date": response.game_date.isoformat(),
            "observed_at": observed_at.isoformat(),
            "previous_revision_id": previous_revision_id,
            "response_sha256": revision_id,
            "revision_id": revision_id,
            "row_count": response.row_count,
            "run_id": str(run_id),
            "source_uri": source_uri,
            "terminal_row_count": response.terminal_row_count,
        }
        if metadata_path.exists():
            try:
                existing_metadata = read_json_object(metadata_path)
            except (ValueError, json.JSONDecodeError) as exc:
                raise BaseballSavantStorageError(
                    f"Savant revision {revision_id} metadata is invalid"
                ) from exc
            for key in (
                "contract",
                "game_date",
                "response_sha256",
                "revision_id",
                "row_count",
                "terminal_row_count",
            ):
                if existing_metadata.get(key) != metadata[key]:
                    raise BaseballSavantStorageError(
                        f"Savant revision {revision_id} metadata conflicts"
                    )

        if not response_path.exists():
            atomic_write(response_path, raw)
        if not metadata_path.exists():
            atomic_write(metadata_path, encode_json(metadata))
        created = not revision_exists
        if created or previous_revision_id is None:
            # Do not let replay of a known historical snapshot move the current
            # pointer away from a newer revision.
            pointer = {
                "contract": "baseball-savant-statcast-current/v1",
                "game_date": response.game_date.isoformat(),
                "revision_id": revision_id,
                "updated_at": observed_at.isoformat(),
            }
            atomic_write(current_pointer_path, encode_json(pointer))

        return LandedStatcastDate(
            game_date=response.game_date,
            revision_id=revision_id,
            previous_revision_id=previous_revision_id,
            response_path=artifact_reference_for_path(
                self.storage_root, response_path
            ),
            metadata_path=artifact_reference_for_path(
                self.storage_root, metadata_path
            ),
            current_pointer_path=artifact_reference_for_path(
                self.storage_root, current_pointer_path
            ),
            row_count=response.row_count,
            terminal_row_count=response.terminal_row_count,
            created=created,
        )

    def current_revision_id(self, game_date: date) -> Optional[str]:
        """Return the current revision for a date when one has been landed."""

        if type(game_date) is not date:
            raise ValueError("game_date must be a date")
        date_directory = (
            self.storage_root
            / "raw"
            / "baseball_savant"
            / "statcast_search"
            / f"game_date={game_date.isoformat()}"
        )
        revision_id = self._read_current_revision(
            date_directory / "current.json", game_date
        )
        if revision_id is None:
            return None
        revision_directory = date_directory / f"revision={revision_id}"
        response_path = revision_directory / "response.csv"
        metadata_path = revision_directory / "metadata.json"
        if not response_path.exists() or not metadata_path.exists():
            raise BaseballSavantStorageError(
                "Savant current revision artifacts are incomplete"
            )
        if sha256_bytes(response_path.read_bytes()) != revision_id:
            raise BaseballSavantStorageError(
                "Savant current response checksum conflicts"
            )
        try:
            metadata = read_json_object(metadata_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise BaseballSavantStorageError(
                "Savant current revision metadata is invalid"
            ) from exc
        expected = {
            "contract": "baseball-savant-statcast-response/v1",
            "game_date": game_date.isoformat(),
            "response_sha256": revision_id,
            "revision_id": revision_id,
        }
        if any(metadata.get(key) != value for key, value in expected.items()):
            raise BaseballSavantStorageError(
                "Savant current revision metadata conflicts"
            )
        return revision_id

    def record_date(
        self,
        manifest_path: ArtifactReference,
        game_date: date,
        status: str,
        details: Mapping[str, Any],
    ) -> None:
        if status not in DATE_STATUSES[1:]:
            raise ValueError("Savant date status must be succeeded or failed")
        path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_run(path)
        if manifest.get("status") != "running":
            raise BaseballSavantStorageError("finalized Savant run cannot be updated")
        dates = manifest.get("dates")
        key = game_date.isoformat()
        if not isinstance(dates, dict) or key not in dates:
            raise BaseballSavantStorageError("Savant date is not planned by this run")
        entry = dates.get(key)
        if not isinstance(entry, dict) or entry.get("status") != "pending":
            raise BaseballSavantStorageError("Savant date outcome is already recorded")
        recorded_at = as_utc(self.clock(), "clock result").isoformat()
        dates[key] = {
            "details": dict(details),
            "recorded_at": recorded_at,
            "status": status,
        }
        manifest["updated_at"] = recorded_at
        atomic_write(path, encode_json(manifest))

    def finalize_run(self, manifest_path: ArtifactReference) -> Dict[str, int]:
        path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_run(path)
        dates = manifest.get("dates")
        if not isinstance(dates, dict) or not dates:
            raise BaseballSavantStorageError("Savant run has no planned dates")
        counts = {"succeeded": 0, "failed": 0}
        for entry in dates.values():
            if not isinstance(entry, dict):
                raise BaseballSavantStorageError("Savant date outcome is invalid")
            status = entry.get("status")
            if status not in counts:
                raise BaseballSavantStorageError("Savant run has unfinished dates")
            counts[status] += 1
        completed_at = as_utc(self.clock(), "clock result").isoformat()
        manifest["completed_at"] = completed_at
        manifest["status"] = "failed" if counts["failed"] else "complete"
        manifest["summary"] = counts
        manifest["updated_at"] = completed_at
        atomic_write(path, encode_json(manifest))
        return counts

    def read_watermark(self) -> Optional[BaseballSavantWatermark]:
        if not self.watermark_path.exists():
            return None
        try:
            payload = read_json_object(self.watermark_path)
            if payload.get("contract") != "baseball-savant-watermark/v1":
                raise ValueError("unsupported watermark contract")
            through_date = date.fromisoformat(str(payload["through_date"]))
            run_id = UUID(str(payload["run_id"]))
            updated_at = as_utc(
                datetime.fromisoformat(str(payload["updated_at"])), "updated_at"
            )
            manifest_key = payload["manifest_path"]
            if not isinstance(manifest_key, str):
                raise ValueError("invalid manifest path")
            manifest_path = self.storage_root.joinpath(*manifest_key.split("/"))
            if not manifest_path.exists():
                raise ValueError("watermark manifest does not exist")
        except (
            KeyError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise BaseballSavantStorageError("Savant watermark is invalid") from exc
        return BaseballSavantWatermark(
            through_date=through_date,
            run_id=run_id,
            manifest_path=artifact_reference_for_path(
                self.storage_root, manifest_path
            ),
            updated_at=updated_at,
        )

    def advance_watermark(
        self,
        expected_current: Optional[date],
        through_date: date,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> BaseballSavantWatermark:
        path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_run(path)
        # Completion of this exact run is the proof that every date through the
        # proposed checkpoint has durable evidence.
        if (
            manifest.get("status") != "complete"
            or manifest.get("run_id") != str(run_id)
            or manifest.get("through_date") != through_date.isoformat()
        ):
            raise ValueError("manifest_path must identify this completed Savant run")
        current = self.read_watermark()
        actual_current = current.through_date if current is not None else None
        if actual_current != expected_current:
            raise BaseballSavantStorageError(
                "Savant watermark changed while acquisition was running"
            )
        if expected_current is not None and through_date < expected_current:
            raise ValueError("Savant watermark cannot move backward")
        updated_at = as_utc(self.clock(), "clock result")
        payload = {
            "contract": "baseball-savant-watermark/v1",
            "manifest_path": manifest_path.key,
            "run_id": str(run_id),
            "through_date": through_date.isoformat(),
            "updated_at": updated_at.isoformat(),
        }
        atomic_write(self.watermark_path, encode_json(payload))
        return BaseballSavantWatermark(
            through_date=through_date,
            run_id=run_id,
            manifest_path=manifest_path,
            updated_at=updated_at,
        )

    @staticmethod
    def _read_run(path: Path) -> Dict[str, Any]:
        try:
            manifest = read_json_object(path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise BaseballSavantStorageError("Savant run manifest is invalid") from exc
        if manifest.get("contract") != "baseball-savant-daily-run/v1":
            raise BaseballSavantStorageError("Savant run contract is invalid")
        return manifest

    @staticmethod
    def _read_current_revision(path: Path, game_date: date) -> Optional[str]:
        if not path.exists():
            return None
        try:
            pointer = read_json_object(path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise BaseballSavantStorageError(
                "Savant current pointer is invalid"
            ) from exc
        if (
            pointer.get("contract") != "baseball-savant-statcast-current/v1"
            or pointer.get("game_date") != game_date.isoformat()
        ):
            raise BaseballSavantStorageError("Savant current pointer conflicts")
        revision_id = pointer.get("revision_id")
        if not isinstance(revision_id, str) or not revision_id:
            raise BaseballSavantStorageError("Savant current revision is invalid")
        return revision_id


def local_baseball_savant_store(
    data_dir: Path,
    clock: Clock = utc_now,
) -> BaseballSavantStore:
    """Build the Savant store over a local lake root."""

    return PathBaseballSavantStore(data_dir, clock=clock)


def s3_baseball_savant_store(
    client: S3Client,
    bucket: str,
    prefix: str,
    clock: Clock = utc_now,
) -> BaseballSavantStore:
    """Build the Savant store over the shared conditional S3 backend."""

    root = cast(Path, S3ObjectBackend(client, bucket, prefix).root())
    return PathBaseballSavantStore(root, clock=clock)
