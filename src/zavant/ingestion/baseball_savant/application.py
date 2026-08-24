"""Composition surface for Baseball Savant acquisition applications."""

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from zavant._time import Clock, utc_now
from zavant.ingestion.baseball_savant.backfill_storage import (
    BaseballSavantBackfillStore,
    PathBaseballSavantBackfillStore,
)
from zavant.ingestion.baseball_savant.storage import (
    BaseballSavantRawStore,
    PathBaseballSavantStore,
)
from zavant.storage.s3_objects import S3Client, S3ObjectBackend


@dataclass(frozen=True)
class BaseballSavantBackfillStorage:
    """Raw evidence and resumable run state required by Savant backfills."""

    raw: BaseballSavantRawStore
    runs: BaseballSavantBackfillStore


def local_backfill_storage(
    data_dir: Path,
    clock: Clock = utc_now,
) -> BaseballSavantBackfillStorage:
    """Build Savant backfill storage over a local lake root."""

    return _backfill_storage(data_dir, clock)


def s3_backfill_storage(
    client: S3Client,
    bucket: str,
    prefix: str,
    clock: Clock = utc_now,
) -> BaseballSavantBackfillStorage:
    """Build Savant backfill storage over one conditional S3 backend."""

    root = cast(Path, S3ObjectBackend(client, bucket, prefix).root())
    return _backfill_storage(root, clock)


def _backfill_storage(
    root: Path,
    clock: Clock,
) -> BaseballSavantBackfillStorage:
    # Raw revisions and their resumable run manifest must use the same backend
    # so every artifact reference resolves inside one configured lake root.
    return BaseballSavantBackfillStorage(
        raw=PathBaseballSavantStore(root, clock=clock),
        runs=PathBaseballSavantBackfillStore(root, clock=clock),
    )
