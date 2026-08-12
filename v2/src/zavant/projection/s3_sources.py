"""S3 discovery and validation for current raw-game projection inputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, Optional, Sequence, Set, Tuple

from zavant.contracts.raw_game import RawGameResponse
from zavant.projection.contracts import (
    PROJECTION_CONTRACT_VERSION,
    ProjectionContractError,
)
from zavant.projection.models import ProjectionSource
from zavant.storage._path_io import canonical_json_sha256
from zavant.storage.s3_objects import S3ObjectBackend


CompletedProjection = Tuple[int, str, str]


@dataclass(frozen=True)
class CurrentProjectionRevision:
    """One raw revision selected by its game's authoritative S3 pointer."""

    game_pk: int
    season: int
    revision_id: str
    pointer_key: str
    raw_key: str
    metadata_key: str

    def completed_identity(
        self, contract_version: str = PROJECTION_CONTRACT_VERSION
    ) -> CompletedProjection:
        return self.game_pk, self.revision_id, contract_version


def discover_current_revisions(
    backend: S3ObjectBackend,
    seasons: Optional[Sequence[int]] = None,
) -> Tuple[CurrentProjectionRevision, ...]:
    """Enumerate and validate every selected raw-game current pointer in S3."""

    selected_seasons = set(seasons) if seasons is not None else None
    revisions = []
    seen_games: Set[int] = set()
    pattern = "raw/mlb_stats_api/games/season=*/game_pk=*/current.json"
    for pointer_key in backend.list(pattern):
        season, game_pk = _pointer_partitions(pointer_key)
        if selected_seasons is not None and season not in selected_seasons:
            continue
        if game_pk in seen_games:
            raise ProjectionContractError(
                f"current pointers contain duplicate game_pk {game_pk}"
            )
        seen_games.add(game_pk)
        pointer = _json_object(backend.read(pointer_key), backend.uri(pointer_key))
        pointer_game_pk = pointer.get("game_pk")
        revision_id = pointer.get("revision_id")
        if pointer_game_pk != game_pk or not isinstance(revision_id, str) or not revision_id:
            raise ProjectionContractError(f"invalid current pointer {backend.uri(pointer_key)}")
        revision_root = pointer_key.removesuffix("current.json") + f"revision={revision_id}"
        revisions.append(
            CurrentProjectionRevision(
                game_pk=game_pk,
                season=season,
                revision_id=revision_id,
                pointer_key=pointer_key,
                raw_key=f"{revision_root}/game.json",
                metadata_key=f"{revision_root}/metadata.json",
            )
        )
    return tuple(sorted(revisions, key=lambda revision: (revision.season, revision.game_pk)))


def pending_current_revisions(
    current: Iterable[CurrentProjectionRevision],
    completed: Set[CompletedProjection],
    contract_version: str = PROJECTION_CONTRACT_VERSION,
) -> Tuple[CurrentProjectionRevision, ...]:
    """Return current revisions absent from the completed projection registry."""

    return tuple(
        revision
        for revision in current
        if revision.completed_identity(contract_version) not in completed
    )


def load_projection_source(
    backend: S3ObjectBackend,
    revision: CurrentProjectionRevision,
) -> ProjectionSource:
    """Load one candidate and verify raw content, routing, and provenance metadata."""

    raw = backend.read(revision.raw_key)
    game = RawGameResponse.from_bytes(raw)
    if game.game_pk != revision.game_pk or game.season != revision.season:
        raise ProjectionContractError(
            f"raw game routing does not match {backend.uri(revision.pointer_key)}"
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
