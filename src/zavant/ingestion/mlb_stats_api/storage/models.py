"""Values exchanged by Stats API acquisition and storage adapters."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

from zavant.storage.artifacts import ArtifactReference


@dataclass(frozen=True)
class LandedRawGame:
    """Result of landing a raw-game revision."""

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
class CurrentRawGameRevision:
    """Current persisted revision used to plan reconciliation work."""

    game_pk: int
    season: int
    revision_id: str
    observed_at: datetime


@dataclass(frozen=True)
class DeferredScheduledGame:
    """Regular-season game retained until it reaches a terminal state."""

    game_pk: int
    season: int
    official_date: date
    live_feed_link: str
    first_deferred_at: datetime
    last_evaluated_at: datetime


@dataclass(frozen=True)
class LandedSchedule:
    """Result of landing one bounded schedule snapshot."""

    run_id: UUID
    request_date: str
    response_path: ArtifactReference
    metadata_path: ArtifactReference
    manifest_path: ArtifactReference
    response_sha256: str
    scheduled_game_pks: Tuple[int, ...]
    created: bool

    def as_dict(self) -> Dict[str, Any]:
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
    """Previously landed schedule artifacts used to resume a run."""

    run_id: UUID
    request_date: str
    raw: bytes
    request: Dict[str, Any]
    response_path: ArtifactReference
    metadata_path: ArtifactReference
    manifest_path: ArtifactReference


@dataclass(frozen=True)
class ChangedGameWorkItem:
    """One changed game awaiting or retrying live-feed retrieval."""

    game_pk: int
    season: int
    live_feed_link: str
    processing_status: str


@dataclass(frozen=True)
class LandedGameChangesPage:
    """Result of landing one page from a corrected-game poll."""

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
    """Current durable through-date for schedule discovery."""

    through_date: date
    advanced_from: date
    run_id: UUID
    manifest_path: ArtifactReference
    updated_at: datetime

    def as_dict(self) -> Dict[str, Any]:
        return {
            "advanced_from": self.advanced_from.isoformat(),
            "manifest_path": str(self.manifest_path),
            "run_id": str(self.run_id),
            "through_date": self.through_date.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True)
class GameChangesWatermark:
    """Current durable checkpoint for corrected-game polling."""

    updated_since: datetime
    advanced_from: datetime
    run_id: UUID
    manifest_path: ArtifactReference
    updated_at: datetime

    def as_dict(self) -> Dict[str, Any]:
        return {
            "advanced_from": self.advanced_from.isoformat(),
            "manifest_path": str(self.manifest_path),
            "run_id": str(self.run_id),
            "updated_at": self.updated_at.isoformat(),
            "updated_since": self.updated_since.isoformat(),
        }


@dataclass(frozen=True)
class StartedDailyRun:
    """Identity and manifest reference of a newly created daily run."""

    run_id: UUID
    started_at: datetime
    through_date: date
    manifest_path: ArtifactReference


@dataclass(frozen=True)
class SeasonBackfillCheckpoint:
    """Successful public-correction checkpoint for one historical season."""

    season: int
    updated_since: datetime
    run_id: UUID
    manifest_path: ArtifactReference
    updated_at: datetime


@dataclass(frozen=True)
class StartedSeasonBackfillRun:
    """Identity and persisted state of a season-backfill run."""

    run_id: UUID
    started_at: datetime
    manifest_path: ArtifactReference
    season_statuses: Dict[int, str]
    resumed: bool


@dataclass(frozen=True)
class LoadedSeasonBackfillChanges:
    """Completed correction evidence reusable by a resumed backfill."""

    changed_game_pks: Tuple[int, ...]
    response_paths: Tuple[ArtifactReference, ...]
