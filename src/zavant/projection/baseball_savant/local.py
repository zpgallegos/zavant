"""Local discovery and Parquet publication for Savant date revisions."""

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import shutil
from typing import Any, Dict, Iterable, List, Optional, Sequence
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from zavant._time import as_utc
from zavant.ingestion.baseball_savant.contract import StatcastCsvResponse
from zavant.projection.baseball_savant.contracts import (
    STATCAST_HISTORY_CONTRACTS,
    STATCAST_PROJECTION_CONTRACT_VERSION,
)
from zavant.projection.baseball_savant.models import StatcastProjectionSource
from zavant.projection.baseball_savant.projector import project_statcast_date
from zavant.projection.contracts import Column, ProjectionContractError, ProjectionRow
from zavant.storage._path_io import read_json_object, sha256_bytes


@dataclass(frozen=True)
class LocalStatcastProjectionResult:
    """Identity, output paths, and counts for one local Savant projection."""

    run_id: UUID
    output_dir: Path
    manifest_path: Path
    date_revision_count: int
    row_counts: Dict[str, int]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "date_revision_count": self.date_revision_count,
            "manifest_path": str(self.manifest_path),
            "output_dir": str(self.output_dir),
            "row_counts": dict(sorted(self.row_counts.items())),
            "run_id": str(self.run_id),
        }


def statcast_projection_sources(
    data_dir: Path,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Iterable[StatcastProjectionSource]:
    """Yield validated immutable Savant revisions from the local lake."""

    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    root = data_dir / "raw" / "baseball_savant" / "statcast_search"
    for metadata_path in sorted(
        root.glob("game_date=*/revision=*/metadata.json")
    ):
        revision_dir = metadata_path.parent
        game_date = _date_partition(revision_dir.parent.name)
        if start_date is not None and game_date < start_date:
            continue
        if end_date is not None and game_date > end_date:
            continue
        revision_id = _text_partition(revision_dir.name, "revision")
        raw_path = revision_dir / "response.csv"
        raw = raw_path.read_bytes()
        if sha256_bytes(raw) != revision_id:
            raise ProjectionContractError(
                f"Savant raw revision hash does not match {raw_path}"
            )
        response = StatcastCsvResponse.from_bytes(raw, game_date)
        metadata = read_json_object(metadata_path)
        expected = {
            "contract": "baseball-savant-statcast-response/v1",
            "game_date": game_date.isoformat(),
            "response_sha256": revision_id,
            "revision_id": revision_id,
            "row_count": response.row_count,
            "terminal_row_count": response.terminal_row_count,
        }
        if any(metadata.get(key) != value for key, value in expected.items()):
            raise ProjectionContractError(
                f"Savant metadata does not match {metadata_path}"
            )
        source_uri = metadata.get("source_uri")
        if not isinstance(source_uri, str) or not source_uri:
            raise ProjectionContractError(
                f"Savant source_uri is invalid in {metadata_path}"
            )
        observed_at = _metadata_timestamp(metadata, metadata_path)
        yield StatcastProjectionSource(
            game_date=game_date,
            revision_id=revision_id,
            observed_at=observed_at,
            source_uri=source_uri,
            raw_object_uri=raw_path.resolve().as_uri(),
            raw=raw,
        )


def run_local_statcast_projection(
    data_dir: Path,
    output_dir: Path,
    run_id: Optional[UUID] = None,
    projected_at: Optional[datetime] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> LocalStatcastProjectionResult:
    """Publish a local-only Parquet mirror of the Savant projection contract.

    Every immutable revision in the selected date range is included. The
    result is a history dataset for inspection and validation; production
    current-state selection remains the responsibility of Glue and Iceberg.
    """

    resolved_run_id = run_id or uuid4()
    resolved_projected_at = as_utc(
        projected_at or datetime.now(timezone.utc), "projected_at"
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
    date_revision_count = 0
    source_revisions: List[Dict[str, Any]] = []
    samples: Dict[str, List[ProjectionRow]] = {
        contract.name: [] for contract in STATCAST_HISTORY_CONTRACTS
    }
    try:
        temporary_dir.mkdir(parents=True)
        with _StatcastParquetWriter(temporary_dir) as writer:
            for source in statcast_projection_sources(
                data_dir, start_date=start_date, end_date=end_date
            ):
                projection = project_statcast_date(
                    source,
                    run_id=resolved_run_id,
                    projected_at=resolved_projected_at,
                )
                date_revision_count += 1
                source_revisions.append(
                    {
                        "game_date": source.game_date.isoformat(),
                        "revision_id": source.revision_id,
                    }
                )
                for name, rows in projection.table_rows.items():
                    writer.write(name, rows)
                    row_counts[name] += len(rows)
                    remaining = 5 - len(samples[name])
                    if remaining > 0:
                        samples[name].extend(rows[:remaining])
        if date_revision_count == 0:
            raise ProjectionContractError("no Savant date revisions were found")

        _write_json(temporary_dir / "schemas.json", _schemas_document())
        samples_dir = temporary_dir / "samples"
        samples_dir.mkdir()
        for name, rows in samples.items():
            _write_json(samples_dir / f"{name}.json", {"rows": rows})
        manifest = {
            "contract": "zavant-local-statcast-projection-run/v1",
            "date_revision_count": date_revision_count,
            "end_date": end_date.isoformat() if end_date is not None else None,
            "output_tables": {
                contract.name: f"{contract.name}/data.parquet"
                for contract in STATCAST_HISTORY_CONTRACTS
            },
            "projected_at": resolved_projected_at.isoformat(),
            "projection_contract_version": STATCAST_PROJECTION_CONTRACT_VERSION,
            "row_counts": dict(sorted(row_counts.items())),
            "run_id": str(resolved_run_id),
            "source_data_dir": str(data_dir.resolve()),
            "source_revisions": source_revisions,
            "start_date": start_date.isoformat() if start_date is not None else None,
            "status": "complete",
        }
        _write_json(temporary_dir / "manifest.json", manifest)
        # Publish the directory only after both Parquet tables and the run
        # manifest are complete.
        os.replace(temporary_dir, output_dir)
    except Exception:
        if temporary_dir.exists():
            shutil.rmtree(temporary_dir)
        raise
    return LocalStatcastProjectionResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        manifest_path=output_dir / "manifest.json",
        date_revision_count=date_revision_count,
        row_counts=dict(row_counts),
    )


class _StatcastParquetWriter:
    def __init__(self, root: Path) -> None:
        self._writers: Dict[str, pq.ParquetWriter] = {}
        self._wrote_rows = {
            contract.name: False for contract in STATCAST_HISTORY_CONTRACTS
        }
        for contract in STATCAST_HISTORY_CONTRACTS:
            table_dir = root / contract.name
            table_dir.mkdir()
            self._writers[contract.name] = pq.ParquetWriter(
                table_dir / "data.parquet",
                _arrow_schema(contract.columns),
                compression="zstd",
                use_dictionary=True,
            )

    def __enter__(self) -> "_StatcastParquetWriter":
        return self

    def write(self, name: str, rows: Sequence[ProjectionRow]) -> None:
        if not rows:
            return
        contract = next(
            contract for contract in STATCAST_HISTORY_CONTRACTS if contract.name == name
        )
        table = pa.Table.from_pylist(
            list(rows), schema=_arrow_schema(contract.columns)
        )
        self._writers[name].write_table(table)
        self._wrote_rows[name] = True

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc, traceback
        for contract in STATCAST_HISTORY_CONTRACTS:
            writer = self._writers[contract.name]
            if exc_type is None and not self._wrote_rows[contract.name]:
                writer.write_table(
                    pa.Table.from_pylist([], schema=_arrow_schema(contract.columns))
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
    return {
        "boolean": pa.bool_,
        "date": pa.date32,
        "float64": pa.float64,
        "int32": pa.int32,
        "int64": pa.int64,
        "string": pa.string,
        "timestamp": lambda: pa.timestamp("us", tz="UTC"),
    }[column.kind]()


def _schemas_document() -> Dict[str, Any]:
    return {
        "projection_contract_version": STATCAST_PROJECTION_CONTRACT_VERSION,
        "tables": {
            contract.name: {
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
            for contract in STATCAST_HISTORY_CONTRACTS
        },
    }


def _date_partition(partition: str) -> date:
    value = _text_partition(partition, "game_date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ProjectionContractError(
            f"invalid Savant game_date partition: {partition}"
        ) from exc


def _text_partition(partition: str, name: str) -> str:
    prefix = f"{name}="
    if not partition.startswith(prefix) or not partition.removeprefix(prefix):
        raise ProjectionContractError(f"invalid Savant {name} partition: {partition}")
    return partition.removeprefix(prefix)


def _metadata_timestamp(metadata: Dict[str, Any], path: Path) -> datetime:
    value = metadata.get("observed_at")
    if not isinstance(value, str):
        raise ProjectionContractError(f"observed_at is invalid in {path}")
    try:
        return as_utc(datetime.fromisoformat(value), "observed_at")
    except ValueError as exc:
        raise ProjectionContractError(f"observed_at is invalid in {path}") from exc


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, default=_json_default, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        result = value.isoformat()
        if isinstance(result, str):
            return result
    raise TypeError(f"cannot encode {type(value).__name__}")
