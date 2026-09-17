"""Read acquisition completion evidence; never invoke an acquisition."""

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import cast

import boto3
import dagster as dg

from zavant.orchestration.sources import PublicationSource
from zavant.storage.s3_objects import S3Client, S3ObjectBackend


@dataclass(frozen=True)
class AcquisitionEvidence:
    """Latest external attempt, retained even when unsuccessful or unfinished."""

    run_id: str
    started_at: datetime
    completed_at: datetime | None
    status: str
    manifest_uri: str


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Acquisition manifest timestamp must be a string.")
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("Acquisition manifest timestamp must include its timezone.")
    return parsed


def latest_acquisition(
    backend: S3ObjectBackend,
    source: PublicationSource,
    since: datetime,
) -> AcquisitionEvidence | None:
    """Read the latest attempt in this cycle, including failed/incomplete attempts.

    Never fall back to an older success when a newer attempt failed. The legacy
    Stats API manifest prefix intentionally remains unchanged in the AWS producer.
    """

    attempts: list[AcquisitionEvidence] = []
    for summary in backend.list_objects(source.manifest_prefix):
        if not summary.key.endswith("/manifest.json"):
            continue
        if summary.last_modified is not None and summary.last_modified < since:
            continue
        manifest = json.loads(backend.read(summary.key))
        if (
            not isinstance(manifest, dict)
            or manifest.get("contract") != source.manifest_contract
        ):
            raise ValueError(f"Unexpected acquisition manifest contract: {summary.key}")
        started_at = _timestamp(manifest.get("started_at"))
        if started_at < since:
            continue
        run_id = manifest.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError(f"Missing acquisition run ID: {summary.key}")
        completed = manifest.get("completed_at")
        attempts.append(
            AcquisitionEvidence(
                run_id=run_id,
                started_at=started_at,
                completed_at=_timestamp(completed) if completed is not None else None,
                status=str(manifest.get("status", "unknown")),
                manifest_uri=backend.uri(summary.key),
            )
        )
    return max(attempts, key=lambda attempt: attempt.started_at, default=None)


class AcquisitionEvidenceResource(dg.ConfigurableResource):
    """Read daily coordinator manifests using the normal AWS credential chain."""

    bucket: str
    prefix: str = "lake"
    region_name: str = "us-east-1"

    def latest(
        self, source: PublicationSource, since: datetime
    ) -> AcquisitionEvidence | None:
        if not self.bucket:
            raise ValueError("ZAVANT_S3_BUCKET is not configured.")
        client = cast(S3Client, boto3.client("s3", region_name=self.region_name))
        return latest_acquisition(
            S3ObjectBackend(client, self.bucket, self.prefix), source, since
        )


ACQUISITION_EVIDENCE_RESOURCE = AcquisitionEvidenceResource(
    bucket=os.environ.get("ZAVANT_S3_BUCKET", ""),
    prefix=os.environ.get("ZAVANT_S3_PREFIX", "lake"),
    region_name=os.environ.get("ZAVANT_AWS_REGION", "us-east-1"),
)
