"""S3 discovery and validation for Savant date-revision projection inputs."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Set, Tuple

from zavant._time import as_utc
from zavant.ingestion.baseball_savant.contract import StatcastCsvResponse
from zavant.projection.baseball_savant.models import StatcastProjectionSource
from zavant.projection.contracts import ProjectionContractError
from zavant.storage._path_io import sha256_bytes
from zavant.storage.s3_objects import S3ObjectBackend, S3ObjectSummary


CompletedStatcastProjection = Tuple[date, str]


@dataclass(frozen=True)
class StatcastProjectionRevision:
    """One immutable Savant date revision available for projection."""

    game_date: date
    revision_id: str
    raw_key: str
    metadata_key: str

    @property
    def season(self) -> int:
        return self.game_date.year

    def completed_identity(self) -> CompletedStatcastProjection:
        return self.game_date, self.revision_id


@dataclass(frozen=True)
class CurrentStatcastRevisionCacheEntry:
    """Previously reconciled date revision used to avoid unchanged S3 reads."""

    game_date: date
    revision_id: str
    reconciled_at: datetime


@dataclass(frozen=True)
class CurrentStatcastPointer:
    """Routing identity and modification time for one Savant current pointer."""

    game_date: date
    key: str
    last_modified: datetime


@dataclass(frozen=True)
class StatcastProjectionInventory:
    """Savant revisions and current pointers classified from one S3 listing."""

    revisions: Tuple[StatcastProjectionRevision, ...]
    current_pointers: Tuple[CurrentStatcastPointer, ...]


@dataclass(frozen=True)
class CurrentStatcastRevisionDiscovery:
    """Complete current date state plus pointers requiring validation reads."""

    revisions: Tuple[StatcastProjectionRevision, ...]
    refreshed: Tuple[StatcastProjectionRevision, ...]


def discover_statcast_projection_inventory(
    backend: S3ObjectBackend,
) -> StatcastProjectionInventory:
    """Classify every Savant projection input from one raw-prefix listing."""

    metadata_keys = []
    current_pointers = []
    prefix = "raw/baseball_savant/statcast_search/"
    for summary in backend.list_objects(prefix):
        if summary.key.endswith("/metadata.json") and "/revision=" in summary.key:
            metadata_keys.append(summary.key)
        elif summary.key.endswith("/current.json"):
            current_pointers.append(_current_pointer(summary))
    revisions = _revisions_from_metadata(metadata_keys)
    pointers = tuple(sorted(current_pointers, key=lambda item: item.game_date))
    _validate_unique_current_dates(pointers)
    return StatcastProjectionInventory(revisions, pointers)


def resolve_current_statcast_revisions(
    backend: S3ObjectBackend,
    pointers: Sequence[CurrentStatcastPointer],
    cached: Optional[Mapping[date, CurrentStatcastRevisionCacheEntry]] = None,
    max_workers: int = 16,
    overlap_seconds: float = 300.0,
) -> CurrentStatcastRevisionDiscovery:
    """Resolve current pointers while conservatively reusing reconciled state."""

    if max_workers <= 0:
        raise ValueError("max_workers must be positive")
    if overlap_seconds < 0:
        raise ValueError("overlap_seconds must not be negative")
    overlap = timedelta(seconds=overlap_seconds)
    cached_by_date = cached or {}
    resolved = []
    refresh = []
    for pointer in pointers:
        cached_entry = cached_by_date.get(pointer.game_date)
        if (
            cached_entry is not None
            and pointer.last_modified <= cached_entry.reconciled_at - overlap
        ):
            resolved.append(
                _revision_from_pointer(pointer, cached_entry.revision_id)
            )
        else:
            refresh.append(pointer)

    with ThreadPoolExecutor(max_workers=min(max_workers, max(len(refresh), 1))) as pool:
        refreshed = tuple(pool.map(lambda item: _read_pointer(backend, item), refresh))
    resolved.extend(refreshed)
    return CurrentStatcastRevisionDiscovery(
        revisions=tuple(sorted(resolved, key=lambda item: item.game_date)),
        refreshed=refreshed,
    )


def pending_statcast_revisions(
    revisions: Iterable[StatcastProjectionRevision],
    completed: Set[CompletedStatcastProjection],
) -> Tuple[StatcastProjectionRevision, ...]:
    """Return date revisions absent from the ``statcast_dates`` marker table."""

    return tuple(
        revision
        for revision in revisions
        if revision.completed_identity() not in completed
    )


def validate_current_statcast_revisions(
    revisions: Sequence[StatcastProjectionRevision],
    current: Sequence[StatcastProjectionRevision],
) -> None:
    """Require every current date pointer to reference an inventoried revision."""

    available = {revision.completed_identity() for revision in revisions}
    missing = [
        revision.completed_identity()
        for revision in current
        if revision.completed_identity() not in available
    ]
    if missing:
        raise ProjectionContractError(
            f"Savant current pointers reference missing raw revisions {missing[:10]}"
        )


def load_statcast_projection_source(
    backend: S3ObjectBackend,
    revision: StatcastProjectionRevision,
) -> StatcastProjectionSource:
    """Load and verify one raw Savant date revision and its provenance."""

    raw = backend.read(revision.raw_key)
    if sha256_bytes(raw) != revision.revision_id:
        raise ProjectionContractError(
            f"Savant raw revision hash does not match {backend.uri(revision.raw_key)}"
        )
    response = StatcastCsvResponse.from_bytes(raw, revision.game_date)
    metadata = _json_object(
        backend.read(revision.metadata_key), backend.uri(revision.metadata_key)
    )
    expected = {
        "contract": "baseball-savant-statcast-response/v1",
        "game_date": revision.game_date.isoformat(),
        "response_sha256": revision.revision_id,
        "revision_id": revision.revision_id,
        "row_count": response.row_count,
        "terminal_row_count": response.terminal_row_count,
    }
    if any(metadata.get(key) != value for key, value in expected.items()):
        raise ProjectionContractError(
            f"Savant metadata does not match {backend.uri(revision.metadata_key)}"
        )
    observed_at = _timestamp(metadata, "observed_at", revision.metadata_key)
    source_uri = metadata.get("source_uri")
    if not isinstance(source_uri, str) or not source_uri:
        raise ProjectionContractError(
            f"Savant source_uri is invalid in {backend.uri(revision.metadata_key)}"
        )
    return StatcastProjectionSource(
        game_date=revision.game_date,
        revision_id=revision.revision_id,
        observed_at=observed_at,
        source_uri=source_uri,
        raw_object_uri=backend.uri(revision.raw_key),
        raw=raw,
    )


def _revisions_from_metadata(
    metadata_keys: Iterable[str],
) -> Tuple[StatcastProjectionRevision, ...]:
    revisions = []
    seen: Set[CompletedStatcastProjection] = set()
    for metadata_key in metadata_keys:
        game_date, revision_id = _revision_partitions(metadata_key)
        identity = game_date, revision_id
        if identity in seen:
            raise ProjectionContractError(
                f"Savant raw objects contain duplicate revision {identity}"
            )
        seen.add(identity)
        revision_root = metadata_key.removesuffix("metadata.json")
        revisions.append(
            StatcastProjectionRevision(
                game_date=game_date,
                revision_id=revision_id,
                raw_key=f"{revision_root}response.csv",
                metadata_key=metadata_key,
            )
        )
    return tuple(
        sorted(revisions, key=lambda item: (item.game_date, item.revision_id))
    )


def _current_pointer(summary: S3ObjectSummary) -> CurrentStatcastPointer:
    game_date = _pointer_date(summary.key)
    last_modified = summary.last_modified
    if last_modified is None or last_modified.utcoffset() is None:
        raise ProjectionContractError(
            f"Savant current pointer has no valid modification time {summary.key}"
        )
    return CurrentStatcastPointer(
        game_date=game_date,
        key=summary.key,
        last_modified=last_modified.astimezone(timezone.utc),
    )


def _read_pointer(
    backend: S3ObjectBackend,
    pointer: CurrentStatcastPointer,
) -> StatcastProjectionRevision:
    payload = _json_object(backend.read(pointer.key), backend.uri(pointer.key))
    revision_id = payload.get("revision_id")
    if (
        payload.get("contract") != "baseball-savant-statcast-current/v1"
        or payload.get("game_date") != pointer.game_date.isoformat()
        or not isinstance(revision_id, str)
        or not revision_id
    ):
        raise ProjectionContractError(
            f"invalid Savant current pointer {backend.uri(pointer.key)}"
        )
    return _revision_from_pointer(pointer, revision_id)


def _revision_from_pointer(
    pointer: CurrentStatcastPointer,
    revision_id: str,
) -> StatcastProjectionRevision:
    root = pointer.key.removesuffix("current.json") + f"revision={revision_id}/"
    return StatcastProjectionRevision(
        game_date=pointer.game_date,
        revision_id=revision_id,
        raw_key=f"{root}response.csv",
        metadata_key=f"{root}metadata.json",
    )


def _validate_unique_current_dates(
    pointers: Sequence[CurrentStatcastPointer],
) -> None:
    seen: Set[date] = set()
    for pointer in pointers:
        if pointer.game_date in seen:
            raise ProjectionContractError(
                f"Savant current pointers contain duplicate date {pointer.game_date}"
            )
        seen.add(pointer.game_date)


def _revision_partitions(key: str) -> Tuple[date, str]:
    parts = PurePosixPath(key).parts
    if len(parts) != 6 or parts[:3] != (
        "raw",
        "baseball_savant",
        "statcast_search",
    ):
        raise ProjectionContractError(f"invalid Savant revision key {key}")
    game_date = _partition_date(parts[3], "game_date", key)
    revision_id = _partition_text(parts[4], "revision", key)
    if parts[5] != "metadata.json":
        raise ProjectionContractError(f"invalid Savant revision key {key}")
    return game_date, revision_id


def _pointer_date(key: str) -> date:
    parts = PurePosixPath(key).parts
    if len(parts) != 5 or parts[:3] != (
        "raw",
        "baseball_savant",
        "statcast_search",
    ) or parts[4] != "current.json":
        raise ProjectionContractError(f"invalid Savant current pointer key {key}")
    return _partition_date(parts[3], "game_date", key)


def _partition_date(part: str, name: str, key: str) -> date:
    value = _partition_text(part, name, key)
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ProjectionContractError(f"invalid Savant date partition {key}") from exc


def _partition_text(part: str, name: str, key: str) -> str:
    prefix = f"{name}="
    if not part.startswith(prefix) or not part.removeprefix(prefix):
        raise ProjectionContractError(f"invalid Savant partition {key}")
    return part.removeprefix(prefix)


def _json_object(raw: bytes, uri: str) -> Dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProjectionContractError(f"invalid JSON object {uri}") from exc
    if not isinstance(value, dict):
        raise ProjectionContractError(f"invalid JSON object {uri}")
    return value


def _timestamp(metadata: Dict[str, Any], name: str, key: str) -> datetime:
    value = metadata.get(name)
    if not isinstance(value, str):
        raise ProjectionContractError(f"invalid Savant {name} in {key}")
    try:
        parsed = datetime.fromisoformat(value)
        return as_utc(parsed, name)
    except ValueError as exc:
        raise ProjectionContractError(f"invalid Savant {name} in {key}") from exc
