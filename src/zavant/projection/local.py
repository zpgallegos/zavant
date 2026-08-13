"""Local current-revision discovery and Parquet publication."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
from typing import Any, Dict, Iterable, List, Optional, Sequence
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from zavant.contracts.raw_game import RawGameResponse
from zavant.projection.contracts import (
    PROJECTION_CONTRACT_VERSION,
    Column,
    ProjectionContractError,
    ProjectionRow,
    TABLE_CONTRACTS,
)
from zavant.projection.models import ProjectionSource
from zavant.projection.projector import project_game
from zavant.storage._path_io import canonical_json_sha256, read_json_object


@dataclass(frozen=True)
class LocalProjectionResult:
    """Identity, output paths, and counts for one local projection run."""

    run_id: UUID
    output_dir: Path
    manifest_path: Path
    game_count: int
    ignored_unversioned_game_count: int
    row_counts: Dict[str, int]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "game_count": self.game_count,
            "ignored_unversioned_game_count": self.ignored_unversioned_game_count,
            "manifest_path": str(self.manifest_path),
            "output_dir": str(self.output_dir),
            "row_counts": dict(sorted(self.row_counts.items())),
            "run_id": str(self.run_id),
        }


def current_projection_sources(
    data_dir: Path,
    seasons: Optional[Sequence[int]] = None,
) -> Iterable[ProjectionSource]:
    """Yield validated raw revisions selected by local current pointers."""

    selected_seasons = set(seasons) if seasons is not None else None
    games_root = data_dir / "raw" / "mlb_stats_api" / "games"
    for pointer_path in sorted(games_root.glob("season=*/game_pk=*/current.json")):
        season = _partition_integer(pointer_path.parents[1].name, "season")
        game_pk = _partition_integer(pointer_path.parent.name, "game_pk")
        if selected_seasons is not None and season not in selected_seasons:
            continue
        pointer = read_json_object(pointer_path)
        pointer_game_pk = pointer.get("game_pk")
        revision_id = pointer.get("revision_id")
        if pointer_game_pk != game_pk or not isinstance(revision_id, str):
            raise ProjectionContractError(f"invalid current pointer {pointer_path}")

        revision_dir = pointer_path.parent / f"revision={revision_id}"
        game_path = revision_dir / "game.json"
        metadata_path = revision_dir / "metadata.json"
        metadata = read_json_object(metadata_path)
        raw = game_path.read_bytes()
        game = RawGameResponse.from_bytes(raw)
        if game.game_pk != game_pk or game.season != season:
            raise ProjectionContractError(
                f"raw game routing does not match {pointer_path.parent}"
            )
        if canonical_json_sha256(game.payload) != revision_id:
            raise ProjectionContractError(f"raw revision hash does not match {game_path}")
        if metadata.get("revision_id") != revision_id:
            raise ProjectionContractError(
                f"revision metadata does not match {metadata_path}"
            )
        observed_at = _metadata_timestamp(metadata, metadata_path)
        source_uri = metadata.get("source_uri")
        if not isinstance(source_uri, str):
            raise ProjectionContractError(f"source_uri is invalid in {metadata_path}")
        yield ProjectionSource(
            game=game,
            revision_id=revision_id,
            observed_at=observed_at,
            source_uri=source_uri,
            raw_object_uri=game_path.resolve().as_uri(),
        )


def run_local_projection(
    data_dir: Path,
    output_dir: Path,
    run_id: Optional[UUID] = None,
    projected_at: Optional[datetime] = None,
    seasons: Optional[Sequence[int]] = None,
) -> LocalProjectionResult:
    """Project current local revisions into an atomically published Parquet run."""

    resolved_run_id = run_id or uuid4()
    resolved_projected_at = (projected_at or datetime.now(timezone.utc)).astimezone(
        timezone.utc
    )
    if output_dir.exists():
        raise FileExistsError(f"projection output already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_dir.with_name(
        f".{output_dir.name}.{resolved_run_id.hex}.tmp"
    )
    if temporary_dir.exists():
        raise FileExistsError(f"projection temporary output exists: {temporary_dir}")

    row_counts: Counter[str] = Counter()
    event_kind_counts: Counter[str] = Counter()
    game_count = 0
    source_revisions: List[Dict[str, Any]] = []
    sample_rows: Dict[str, List[ProjectionRow]] = {
        name: [] for name in TABLE_CONTRACTS
    }
    ignored_unversioned_games = _unversioned_game_files(data_dir, seasons)
    try:
        temporary_dir.mkdir(parents=True)
        with _ParquetRunWriter(temporary_dir) as writer:
            for source in current_projection_sources(data_dir, seasons):
                projection = project_game(
                    source,
                    run_id=resolved_run_id,
                    projected_at=resolved_projected_at,
                )
                game_count += 1
                source_revisions.append(
                    {
                        "game_pk": source.game.game_pk,
                        "official_date": source.game.official_date.isoformat(),
                        "revision_id": source.revision_id,
                        "season": source.game.season,
                    }
                )
                event_kind_counts.update(projection.event_kind_counts)
                for name, rows in projection.tables().items():
                    writer.write(name, rows)
                    row_counts[name] += len(rows)
                    remaining = 5 - len(sample_rows[name])
                    if remaining > 0:
                        sample_rows[name].extend(rows[:remaining])
        if game_count == 0:
            raise ProjectionContractError("no current raw-game revisions were found")

        _write_json(temporary_dir / "schemas.json", _schemas_document())
        samples_dir = temporary_dir / "samples"
        samples_dir.mkdir()
        for name, rows in sample_rows.items():
            _write_json(samples_dir / f"{name}.json", {"rows": rows})
        manifest = {
            "contract": "zavant-local-analytical-projection-run/v1",
            "event_kind_counts": dict(sorted(event_kind_counts.items())),
            "game_count": game_count,
            "ignored_unversioned_game_files": [
                str(path) for path in ignored_unversioned_games
            ],
            "output_tables": {
                name: f"{name}/data.parquet" for name in TABLE_CONTRACTS
            },
            "projected_at": resolved_projected_at.isoformat(),
            "projection_contract_version": PROJECTION_CONTRACT_VERSION,
            "row_counts": dict(sorted(row_counts.items())),
            "run_id": str(resolved_run_id),
            "seasons": sorted(set(seasons)) if seasons is not None else None,
            "source_data_dir": str(data_dir.resolve()),
            "source_revisions": source_revisions,
            "status": "complete",
        }
        _write_json(temporary_dir / "manifest.json", manifest)
        os.replace(temporary_dir, output_dir)
    except Exception:
        if temporary_dir.exists():
            shutil.rmtree(temporary_dir)
        raise

    return LocalProjectionResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        manifest_path=output_dir / "manifest.json",
        game_count=game_count,
        ignored_unversioned_game_count=len(ignored_unversioned_games),
        row_counts=dict(row_counts),
    )


class _ParquetRunWriter:
    _ROW_GROUP_SIZE = 50_000

    def __init__(self, root: Path) -> None:
        self.root = root
        self._writers: Dict[str, pq.ParquetWriter] = {}
        self._buffers: Dict[str, List[ProjectionRow]] = {
            name: [] for name in TABLE_CONTRACTS
        }
        self._wrote_rows: Dict[str, bool] = {
            name: False for name in TABLE_CONTRACTS
        }

    def __enter__(self) -> "_ParquetRunWriter":
        for name, contract in TABLE_CONTRACTS.items():
            table_dir = self.root / name
            table_dir.mkdir()
            self._writers[name] = pq.ParquetWriter(
                table_dir / "data.parquet",
                _arrow_schema(contract.columns),
                compression="zstd",
                use_dictionary=True,
            )
        return self

    def write(self, name: str, rows: Sequence[ProjectionRow]) -> None:
        self._buffers[name].extend(rows)
        while len(self._buffers[name]) >= self._ROW_GROUP_SIZE:
            batch = self._buffers[name][: self._ROW_GROUP_SIZE]
            del self._buffers[name][: self._ROW_GROUP_SIZE]
            self._write_batch(name, batch)

    def _write_batch(self, name: str, rows: Sequence[ProjectionRow]) -> None:
        contract = TABLE_CONTRACTS[name]
        table = pa.Table.from_pylist(list(rows), schema=_arrow_schema(contract.columns))
        self._writers[name].write_table(table)
        self._wrote_rows[name] = True

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc, traceback
        for name, writer in self._writers.items():
            if exc_type is None and self._buffers[name]:
                self._write_batch(name, self._buffers[name])
            if exc_type is None and not self._wrote_rows[name]:
                writer.write_table(
                    pa.Table.from_pylist([], schema=_arrow_schema(TABLE_CONTRACTS[name].columns))
                )
            writer.close()


def _arrow_schema(columns: Sequence[Column]) -> pa.Schema:
    return pa.schema(
        [
            pa.field(column.name, _arrow_type(column), nullable=column.nullable)
            for column in columns
        ]
    )


def _arrow_type(column: Column) -> pa.DataType:
    if column.kind == "boolean":
        return pa.bool_()
    if column.kind == "date":
        return pa.date32()
    if column.kind == "float64":
        return pa.float64()
    if column.kind == "int32":
        return pa.int32()
    if column.kind == "int64":
        return pa.int64()
    if column.kind == "string":
        return pa.string()
    if column.kind == "timestamp":
        return pa.timestamp("us", tz="UTC")
    raise AssertionError(f"unsupported column kind: {column.kind}")


def _schemas_document() -> Dict[str, Any]:
    return {
        "projection_contract_version": PROJECTION_CONTRACT_VERSION,
        "tables": {
            name: {
                "columns": [
                    {
                        "kind": column.kind,
                        "name": column.name,
                        "nullable": column.nullable,
                    }
                    for column in contract.columns
                ],
                "primary_key": list(contract.primary_key),
            }
            for name, contract in TABLE_CONTRACTS.items()
        },
    }


def _partition_integer(partition: str, name: str) -> int:
    prefix = f"{name}="
    if not partition.startswith(prefix):
        raise ProjectionContractError(f"invalid {name} partition: {partition}")
    try:
        return int(partition.removeprefix(prefix))
    except ValueError as exc:
        raise ProjectionContractError(
            f"invalid {name} partition: {partition}"
        ) from exc


def _unversioned_game_files(
    data_dir: Path,
    seasons: Optional[Sequence[int]],
) -> List[Path]:
    selected_seasons = set(seasons) if seasons is not None else None
    games_root = data_dir / "raw" / "mlb_stats_api" / "games"
    paths = []
    for path in games_root.glob("season=*/game_pk=*/game.json"):
        season = _partition_integer(path.parents[1].name, "season")
        if selected_seasons is None or season in selected_seasons:
            paths.append(path)
    return sorted(paths)


def _metadata_timestamp(metadata: Dict[str, Any], path: Path) -> datetime:
    value = metadata.get("observed_at")
    if not isinstance(value, str):
        raise ProjectionContractError(f"observed_at is invalid in {path}")
    try:
        observed_at = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ProjectionContractError(f"observed_at is invalid in {path}") from exc
    if observed_at.utcoffset() is None:
        raise ProjectionContractError(f"observed_at is timezone-naive in {path}")
    return observed_at.astimezone(timezone.utc)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, default=_json_default, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        result = value.isoformat()
        if isinstance(result, str):
            return result
    raise TypeError(f"cannot encode {type(value).__name__}")
