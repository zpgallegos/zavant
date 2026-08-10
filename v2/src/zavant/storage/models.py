"""Storage-neutral values exchanged by acquisition and storage adapters."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

from zavant.storage.artifacts import ArtifactReference


@dataclass(frozen=True)
class LandedRawGame:
    """Result of landing a raw-game revision.

    Attributes:
        game_pk: MLB's primary game identifier.
        season: Season partition containing the revision.
        revision_id: Canonical content hash identifying the revision.
        previous_revision_id: Revision current before this landing.
        object_path: Raw response artifact reference.
        metadata_path: Revision metadata artifact reference.
        current_pointer_path: Current-revision pointer artifact reference.
        raw_sha256: Digest of the exact source bytes.
        canonical_sha256: Digest of canonicalized JSON content.
        created: Whether the call created a revision.
    """

    game_pk: int
    season: int
    revision_id: str
    previous_revision_id: Optional[str]
    object_path: ArtifactReference
    metadata_path: ArtifactReference
    current_pointer_path: ArtifactReference
    raw_sha256: str
    canonical_sha256: str
    created: bool

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable landing result.

        Returns:
            Landing fields suitable for CLI output and run metadata.
        """

        return {
            "canonical_sha256": self.canonical_sha256,
            "created": self.created,
            "current_pointer_path": str(self.current_pointer_path),
            "game_pk": self.game_pk,
            "metadata_path": str(self.metadata_path),
            "object_path": str(self.object_path),
            "previous_revision_id": self.previous_revision_id,
            "raw_sha256": self.raw_sha256,
            "revision_id": self.revision_id,
            "season": self.season,
        }


@dataclass(frozen=True)
class LandedSchedule:
    """Result of landing one bounded schedule snapshot.

    Attributes:
        run_id: Unique schedule request identifier.
        request_date: UTC date on which the request was made.
        response_path: Exact response artifact reference.
        metadata_path: Request metadata artifact reference.
        manifest_path: Discovered-game manifest artifact reference.
        response_sha256: Digest of the exact response bytes.
        scheduled_game_pks: Deduplicated discovered game identifiers.
        created: Whether the call created the response artifact.
    """

    run_id: UUID
    request_date: str
    response_path: ArtifactReference
    metadata_path: ArtifactReference
    manifest_path: ArtifactReference
    response_sha256: str
    scheduled_game_pks: Tuple[int, ...]
    created: bool

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable landing result.

        Returns:
            Landing fields suitable for CLI output.
        """

        return {
            "created": self.created,
            "manifest_path": str(self.manifest_path),
            "metadata_path": str(self.metadata_path),
            "request_date": self.request_date,
            "response_path": str(self.response_path),
            "response_sha256": self.response_sha256,
            "run_id": str(self.run_id),
            "scheduled_game_pks": list(self.scheduled_game_pks),
        }


@dataclass(frozen=True)
class LoadedScheduleRun:
    """Previously landed schedule artifacts used to resume a run.

    Attributes:
        run_id: Unique schedule request identifier.
        request_date: UTC request-date partition.
        raw: Exact stored schedule response bytes.
        request: Persisted request provenance.
        response_path: Stored response artifact reference.
        metadata_path: Response metadata artifact reference.
        manifest_path: Processing manifest artifact reference.
    """

    run_id: UUID
    request_date: str
    raw: bytes
    request: Dict[str, Any]
    response_path: ArtifactReference
    metadata_path: ArtifactReference
    manifest_path: ArtifactReference


@dataclass(frozen=True)
class ChangedGameWorkItem:
    """One changed game awaiting or retrying live-feed retrieval.

    Attributes:
        game_pk: MLB's primary game identifier.
        season: MLB season partition containing the raw game.
        live_feed_link: Relative complete-game feed reported by MLB.
        processing_status: Current manifest processing state.
    """

    game_pk: int
    season: int
    live_feed_link: str
    processing_status: str


@dataclass(frozen=True)
class LandedGameChangesPage:
    """Result of landing one page from a corrected-game poll.

    Attributes:
        run_id: Identifier shared by every poll page.
        poll_date: UTC date partition for the poll.
        page_number: Zero-based logical page number.
        response_path: Exact response artifact reference.
        metadata_path: Page metadata artifact reference.
        manifest_path: Merged poll manifest artifact reference.
        response_sha256: Digest of the exact response bytes.
        changed_game_pks: Deduplicated game identifiers on the page.
        created: Whether the call created the response artifact.
    """

    run_id: UUID
    poll_date: str
    page_number: int
    response_path: ArtifactReference
    metadata_path: ArtifactReference
    manifest_path: ArtifactReference
    response_sha256: str
    changed_game_pks: Tuple[int, ...]
    created: bool

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable landing result.

        Returns:
            Landing fields suitable for CLI output.
        """

        return {
            "changed_game_pks": list(self.changed_game_pks),
            "created": self.created,
            "manifest_path": str(self.manifest_path),
            "metadata_path": str(self.metadata_path),
            "page_number": self.page_number,
            "poll_date": self.poll_date,
            "response_path": str(self.response_path),
            "response_sha256": self.response_sha256,
            "run_id": str(self.run_id),
        }


@dataclass(frozen=True)
class ScheduleWatermark:
    """Current durable through-date for schedule discovery.

    Attributes:
        through_date: Latest date covered successfully.
        advanced_from: Prior through-date or bootstrap start date.
        run_id: Schedule run supporting the advancement.
        manifest_path: Completed schedule manifest artifact reference.
        updated_at: UTC time at which the watermark was published.
    """

    through_date: date
    advanced_from: date
    run_id: UUID
    manifest_path: ArtifactReference
    updated_at: datetime

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable schedule watermark.

        Returns:
            Through-date, lineage, and update metadata.
        """

        return {
            "advanced_from": self.advanced_from.isoformat(),
            "manifest_path": str(self.manifest_path),
            "run_id": str(self.run_id),
            "through_date": self.through_date.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True)
class GameChangesWatermark:
    """Current durable checkpoint for corrected-game polling.

    Attributes:
        updated_since: Logical lower checkpoint for the next poll.
        advanced_from: Logical checkpoint used by the successful poll.
        run_id: Poll run supporting the advancement.
        manifest_path: Completed poll manifest artifact reference.
        updated_at: UTC time at which the watermark was published.
    """

    updated_since: datetime
    advanced_from: datetime
    run_id: UUID
    manifest_path: ArtifactReference
    updated_at: datetime

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable correction watermark.

        Returns:
            Checkpoint, lineage, and update metadata.
        """

        return {
            "advanced_from": self.advanced_from.isoformat(),
            "manifest_path": str(self.manifest_path),
            "run_id": str(self.run_id),
            "updated_at": self.updated_at.isoformat(),
            "updated_since": self.updated_since.isoformat(),
        }


@dataclass(frozen=True)
class StartedDailyRun:
    """Identity and manifest reference of a newly created daily run.

    Attributes:
        run_id: Unique daily coordinator run identifier.
        started_at: UTC time captured before branch work.
        through_date: Inclusive schedule discovery date.
        manifest_path: Coordinator manifest artifact reference.
    """

    run_id: UUID
    started_at: datetime
    through_date: date
    manifest_path: ArtifactReference
