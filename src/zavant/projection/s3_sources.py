"""S3 discovery and validation for immutable raw-game projection inputs."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Set, Tuple

from zavant.contracts.raw_game import RawGameResponse
from zavant.projection.contracts import ProjectionContractError
from zavant.projection.models import ProjectionSource
from zavant.storage._path_io import canonical_json_sha256
from zavant.storage.s3_objects import S3ObjectBackend, S3ObjectSummary


CompletedProjection = Tuple[int, str]


@dataclass(frozen=True)
class ProjectionRevision:
    """One immutable raw-game revision available for analytical projection."""

    game_pk: int
    season: int
    revision_id: str
    raw_key: str
    metadata_key: str

    def completed_identity(self) -> CompletedProjection:
        return self.game_pk, self.revision_id


@dataclass(frozen=True)
class CurrentRevisionCacheEntry:
    """Previously reconciled current revision used to avoid unchanged S3 reads."""

    game_pk: int
    season: int
    revision_id: str
    reconciled_at: datetime


@dataclass(frozen=True)
class CurrentPointer:
    """Routing identity and modification time for one listed current pointer."""

    game_pk: int
    season: int
    key: str
    last_modified: datetime


@dataclass(frozen=True)
class ProjectionInventory:
    """Revision metadata and current pointers classified from one S3 listing."""

    revisions: Tuple[ProjectionRevision, ...]
    current_pointers: Tuple[CurrentPointer, ...]


@dataclass(frozen=True)
class CurrentRevisionDiscovery:
    """Complete current state plus pointers that required validation reads."""

    revisions: Tuple[ProjectionRevision, ...]
    refreshed: Tuple[ProjectionRevision, ...]


def discover_projection_inventory(
    backend: S3ObjectBackend,
    seasons: Optional[Sequence[int]] = None,
) -> ProjectionInventory:
    """Classify all projection inputs from one raw-game prefix listing."""

    selected_seasons = set(seasons) if seasons is not None else None
    revision_metadata = []
    current_pointers = []
    prefix = "raw/mlb_stats_api/games/"
    for summary in backend.list_objects(prefix):
        if summary.key.endswith("/metadata.json") and "/revision=" in summary.key:
            revision_metadata.append(summary.key)
        elif summary.key.endswith("/current.json"):
            current_pointers.append(_current_pointer(summary))
    revisions = _revisions_from_metadata(revision_metadata, selected_seasons)
    pointers = tuple(
        sorted(
            (
                pointer
                for pointer in current_pointers
                if selected_seasons is None or pointer.season in selected_seasons
            ),
            key=lambda pointer: (pointer.season, pointer.game_pk),
        )
    )
    _validate_unique_current_games(pointers)
    return ProjectionInventory(revisions, pointers)


def discover_revisions(
    backend: S3ObjectBackend,
    seasons: Optional[Sequence[int]] = None,
) -> Tuple[ProjectionRevision, ...]:
    """Enumerate every immutable raw revision available for projection."""

    return discover_projection_inventory(backend, seasons).revisions


def _revisions_from_metadata(
    metadata_keys: Iterable[str],
    selected_seasons: Optional[Set[int]],
) -> Tuple[ProjectionRevision, ...]:
    revisions = []
    seen: Set[CompletedProjection] = set()
    for metadata_key in metadata_keys:
        season, game_pk, revision_id = _revision_partitions(metadata_key)
        if selected_seasons is not None and season not in selected_seasons:
            continue
        identity = game_pk, revision_id
        if identity in seen:
            raise ProjectionContractError(
                f"raw objects contain duplicate revision {game_pk}/{revision_id}"
            )
        seen.add(identity)
        revision_root = metadata_key.removesuffix("metadata.json")
        revisions.append(
            ProjectionRevision(
                game_pk=game_pk,
                season=season,
                revision_id=revision_id,
                raw_key=f"{revision_root}game.json",
                metadata_key=metadata_key,
            )
        )
    return tuple(
        sorted(
            revisions,
            key=lambda revision: (
                revision.season,
                revision.game_pk,
                revision.revision_id,
            ),
        )
    )


def discover_current_revisions(
    backend: S3ObjectBackend,
    seasons: Optional[Sequence[int]] = None,
) -> Tuple[ProjectionRevision, ...]:
    """Enumerate and validate every selected raw-game current pointer in S3."""

    inventory = discover_projection_inventory(backend, seasons)
    return resolve_current_revisions(backend, inventory.current_pointers).revisions


def resolve_current_revisions(
    backend: S3ObjectBackend,
    pointers: Sequence[CurrentPointer],
    cached: Optional[Mapping[int, CurrentRevisionCacheEntry]] = None,
    max_workers: int = 16,
    overlap_seconds: float = 300.0,
) -> CurrentRevisionDiscovery:
    """Resolve pointers, reading only objects newer than their cached mapping."""

    if max_workers <= 0:
        raise ValueError("max_workers must be positive")
    if overlap_seconds < 0:
        raise ValueError("overlap_seconds must not be negative")
    overlap = timedelta(seconds=overlap_seconds)
    cached_by_game = cached or {}
    resolved = []
    refresh = []
    for pointer in pointers:
        cached_entry = cached_by_game.get(pointer.game_pk)
        if (
            cached_entry is not None
            and cached_entry.season == pointer.season
            and pointer.last_modified <= cached_entry.reconciled_at - overlap
        ):
            resolved.append(_revision_from_pointer(pointer, cached_entry.revision_id))
        else:
            refresh.append(pointer)

    with ThreadPoolExecutor(max_workers=min(max_workers, max(len(refresh), 1))) as pool:
        refreshed = tuple(pool.map(lambda item: _read_pointer(backend, item), refresh))
    resolved.extend(refreshed)
    return CurrentRevisionDiscovery(
        revisions=tuple(
            sorted(resolved, key=lambda revision: (revision.season, revision.game_pk))
        ),
        refreshed=refreshed,
    )


def _current_pointer(summary: S3ObjectSummary) -> CurrentPointer:
    season, game_pk = _pointer_partitions(summary.key)
    last_modified = summary.last_modified
    if last_modified is None or last_modified.utcoffset() is None:
        raise ProjectionContractError(
            f"current pointer has no valid modification time {summary.key}"
        )
    return CurrentPointer(
        game_pk=game_pk,
        season=season,
        key=summary.key,
        last_modified=last_modified.astimezone(timezone.utc),
    )


def _validate_unique_current_games(pointers: Sequence[CurrentPointer]) -> None:
    seen_games: Set[int] = set()
    for pointer in pointers:
        if pointer.game_pk in seen_games:
            raise ProjectionContractError(
                f"current pointers contain duplicate game_pk {pointer.game_pk}"
            )
        seen_games.add(pointer.game_pk)


def _read_pointer(
    backend: S3ObjectBackend,
    current: CurrentPointer,
) -> ProjectionRevision:
    pointer = _json_object(backend.read(current.key), backend.uri(current.key))
    pointer_game_pk = pointer.get("game_pk")
    revision_id = pointer.get("revision_id")
    if (
        pointer_game_pk != current.game_pk
        or not isinstance(revision_id, str)
        or not revision_id
    ):
        raise ProjectionContractError(f"invalid current pointer {backend.uri(current.key)}")
    return _revision_from_pointer(current, revision_id)


def _revision_from_pointer(
    pointer: CurrentPointer,
    revision_id: str,
) -> ProjectionRevision:
    revision_root = pointer.key.removesuffix("current.json") + f"revision={revision_id}"
    return ProjectionRevision(
        game_pk=pointer.game_pk,
        season=pointer.season,
        revision_id=revision_id,
        raw_key=f"{revision_root}/game.json",
        metadata_key=f"{revision_root}/metadata.json",
    )


def pending_revisions(
    revisions: Iterable[ProjectionRevision],
    completed: Set[CompletedProjection],
) -> Tuple[ProjectionRevision, ...]:
    """Return raw revisions absent from the terminal analytical table."""

    return tuple(
        revision
        for revision in revisions
        if revision.completed_identity() not in completed
    )


def validate_current_revisions(
    revisions: Sequence[ProjectionRevision],
    current: Sequence[ProjectionRevision],
) -> None:
    """Require every current pointer to reference an inventoried raw revision."""

    available = {revision.completed_identity() for revision in revisions}
    missing = [
        revision.completed_identity()
        for revision in current
        if revision.completed_identity() not in available
    ]
    if missing:
        raise ProjectionContractError(
            f"current pointers reference missing raw revisions {missing[:10]}"
        )


def load_projection_source(
    backend: S3ObjectBackend,
    revision: ProjectionRevision,
) -> ProjectionSource:
    """Load one candidate and verify raw content, routing, and provenance metadata."""

    raw = backend.read(revision.raw_key)
    game = RawGameResponse.from_bytes(raw)
    if game.game_pk != revision.game_pk or game.season != revision.season:
        raise ProjectionContractError(
            f"raw game routing does not match {backend.uri(revision.raw_key)}"
        )
    if canonical_json_sha256(game.payload) != revision.revision_id:
        raise ProjectionContractError(
            f"raw revision hash does not match {backend.uri(revision.raw_key)}"
        )
    metadata = _json_object(
        backend.read(revision.metadata_key), backend.uri(revision.metadata_key)
    )
    if (
        metadata.get("game_pk") != revision.game_pk
        or metadata.get("season") != revision.season
        or metadata.get("revision_id") != revision.revision_id
    ):
        raise ProjectionContractError(
            f"revision metadata does not match {backend.uri(revision.metadata_key)}"
        )
    observed_at = _timestamp(metadata.get("observed_at"), backend.uri(revision.metadata_key))
    source_uri = metadata.get("source_uri")
    if not isinstance(source_uri, str):
        raise ProjectionContractError(
            f"source_uri is invalid in {backend.uri(revision.metadata_key)}"
        )
    return ProjectionSource(
        game=game,
        revision_id=revision.revision_id,
        observed_at=observed_at,
        source_uri=source_uri,
        raw_object_uri=backend.uri(revision.raw_key),
    )


def _pointer_partitions(key: str) -> Tuple[int, int]:
    parts = PurePosixPath(key).parts
    if (
        len(parts) != 6
        or parts[:3] != ("raw", "mlb_stats_api", "games")
        or parts[5] != "current.json"
    ):
        raise ProjectionContractError(f"invalid raw-game pointer key {key}")
    return _partition_integer(parts[3], "season"), _partition_integer(parts[4], "game_pk")


def _revision_partitions(key: str) -> Tuple[int, int, str]:
    parts = PurePosixPath(key).parts
    if (
        len(parts) != 7
        or parts[:3] != ("raw", "mlb_stats_api", "games")
        or parts[6] != "metadata.json"
    ):
        raise ProjectionContractError(f"invalid raw-game revision key {key}")
    revision_prefix = "revision="
    if not parts[5].startswith(revision_prefix):
        raise ProjectionContractError(f"invalid revision partition {parts[5]}")
    revision_id = parts[5].removeprefix(revision_prefix)
    if not revision_id:
        raise ProjectionContractError(f"invalid revision partition {parts[5]}")
    return (
        _partition_integer(parts[3], "season"),
        _partition_integer(parts[4], "game_pk"),
        revision_id,
    )


def _partition_integer(partition: str, name: str) -> int:
    prefix = f"{name}="
    if not partition.startswith(prefix):
        raise ProjectionContractError(f"invalid {name} partition {partition}")
    try:
        value = int(partition.removeprefix(prefix))
    except ValueError as exc:
        raise ProjectionContractError(f"invalid {name} partition {partition}") from exc
    if value <= 0:
        raise ProjectionContractError(f"invalid {name} partition {partition}")
    return value


def _json_object(raw: bytes, uri: str) -> Dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProjectionContractError(f"invalid JSON object {uri}") from exc
    if not isinstance(value, dict):
        raise ProjectionContractError(f"invalid JSON object {uri}")
    return value


def _timestamp(value: Any, uri: str) -> datetime:
    if not isinstance(value, str):
        raise ProjectionContractError(f"observed_at is invalid in {uri}")
    try:
        observed_at = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ProjectionContractError(f"observed_at is invalid in {uri}") from exc
    if observed_at.utcoffset() is None:
        raise ProjectionContractError(f"observed_at is timezone-naive in {uri}")
    return observed_at.astimezone(timezone.utc)
