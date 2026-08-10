"""Behavioral interfaces implemented by acquisition persistence stores."""

from datetime import date, datetime
from typing import Any, Dict, Mapping, Optional, Protocol, Tuple, runtime_checkable
from uuid import UUID

from zavant.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.contracts.raw_game import RawGameResponse
from zavant.contracts.schedule import ScheduleRequest, ScheduleResponse
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.models import (
    ChangedGameWorkItem,
    CurrentRawGameRevision,
    GameChangesWatermark,
    LandedGameChangesPage,
    LandedRawGame,
    LandedSchedule,
    LoadedSeasonBackfillChanges,
    LoadedScheduleRun,
    ScheduleWatermark,
    StartedDailyRun,
    StartedSeasonBackfillRun,
    SeasonBackfillCheckpoint,
)


@runtime_checkable
class RawGameStore(Protocol):
    """Revision-aware raw-game persistence required by acquisition."""

    def land(
        self,
        game: RawGameResponse,
        raw: bytes,
        source_uri: str,
        trigger: str = "manual",
    ) -> LandedRawGame:
        """Persist one validated raw-game revision.

        Args:
            game: Validated raw-game response.
            raw: Exact source response bytes.
            source_uri: Source location recorded as provenance.
            trigger: Reason the response was acquired.

        Returns:
            Revision identity, artifact references, hashes, and creation state.

        Notes:
            Callers must supply a current live-endpoint observation. A newly
            observed content revision becomes current by observation order;
            the store cannot infer source chronology from the response body.
        """

        ...

    def current_revision_id(self, season: int, game_pk: int) -> Optional[str]:
        """Return the current revision ID for a game when one exists.

        Args:
            season: MLB season partition.
            game_pk: MLB's primary game identifier.

        Returns:
            Current revision ID, or `None` when the game is not landed.
        """

        ...

    def current_revisions(self, season: int) -> Tuple[CurrentRawGameRevision, ...]:
        """List current persisted revisions for one season."""

        ...


@runtime_checkable
class ScheduleStore(Protocol):
    """Schedule evidence and processing-manifest persistence."""

    def land(
        self,
        schedule: ScheduleResponse,
        request: ScheduleRequest,
        raw: bytes,
        run_id: UUID,
    ) -> LandedSchedule:
        """Persist one immutable schedule response and its manifest.

        Args:
            schedule: Validated schedule response.
            request: Schedule request boundaries and provenance.
            raw: Exact source response bytes.
            run_id: Unique schedule run identifier.

        Returns:
            Schedule artifact references, checksum, and discovered games.
        """

        ...

    def load_run(
        self, requested_at: datetime, run_id: UUID
    ) -> Optional[LoadedScheduleRun]:
        """Load a previously landed schedule run for resumption.

        Args:
            requested_at: Original schedule request time.
            run_id: Original schedule run identifier.

        Returns:
            Stored response and provenance, or `None` when absent.
        """

        ...

    def game_statuses(self, manifest_path: ArtifactReference) -> Dict[int, str]:
        """Return the validated game processing states in a manifest.

        Args:
            manifest_path: Schedule manifest artifact reference.

        Returns:
            Processing status keyed by game identifier.
        """

        ...

    def record_game_outcome(
        self,
        manifest_path: ArtifactReference,
        game_pk: int,
        status: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Record one game outcome in a schedule manifest.

        Args:
            manifest_path: Schedule manifest artifact reference.
            game_pk: MLB game identifier to update.
            status: Terminal or retriable processing status.
            details: Optional JSON-serializable outcome details.
        """

        ...

    def finalize_manifest(self, manifest_path: ArtifactReference) -> Dict[str, int]:
        """Finalize and return a schedule manifest summary.

        Args:
            manifest_path: Schedule manifest artifact reference.

        Returns:
            Counts grouped by schedule processing status.
        """

        ...


@runtime_checkable
class GameChangesStore(Protocol):
    """Corrected-game response pages and processing-manifest persistence."""

    def land_page(
        self,
        changes: GameChangesResponse,
        request: GameChangesRequest,
        raw: bytes,
        run_id: UUID,
    ) -> LandedGameChangesPage:
        """Persist one immutable corrected-game response page.

        Args:
            changes: Validated corrected-game response.
            request: Poll window and pagination provenance.
            raw: Exact source response bytes.
            run_id: Identifier shared by every poll page.

        Returns:
            Page artifact references, checksum, and changed games.
        """

        ...

    def finalize_manifest(
        self,
        manifest_path: ArtifactReference,
        expected_page_count: int,
        expected_total_items: int,
        watermark_before: datetime,
    ) -> Dict[str, int]:
        """Validate and complete a correction-poll manifest.

        Args:
            manifest_path: Poll manifest artifact reference.
            expected_page_count: Page count derived from source metadata.
            expected_total_items: Item count reported by the source.
            watermark_before: Logical checkpoint used by the poll.

        Returns:
            Counts for pages, source items, and changed games.
        """

        ...

    def processable_manifests(self) -> Tuple[ArtifactReference, ...]:
        """List completed correction manifests with outstanding work.

        Returns:
            Ordered artifact references for processable manifests.
        """

        ...

    def game_work_items(
        self, manifest_path: ArtifactReference
    ) -> Tuple[ChangedGameWorkItem, ...]:
        """Return pending and failed games from a correction manifest.

        Args:
            manifest_path: Poll manifest artifact reference.

        Returns:
            Retriable changed-game work items.
        """

        ...

    def record_game_outcome(
        self,
        manifest_path: ArtifactReference,
        game_pk: int,
        status: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Record one corrected-game processing outcome.

        Args:
            manifest_path: Poll manifest artifact reference.
            game_pk: MLB game identifier to update.
            status: Terminal or retriable processing status.
            details: Optional JSON-serializable outcome details.
        """

        ...

    def finalize_processing(self, manifest_path: ArtifactReference) -> Dict[str, int]:
        """Finalize and return correction processing counts.

        Args:
            manifest_path: Poll manifest artifact reference.

        Returns:
            Counts grouped by changed-game processing status.
        """

        ...


@runtime_checkable
class ScheduleWatermarkStore(Protocol):
    """Success-only schedule discovery state."""

    def read(self) -> Optional[ScheduleWatermark]:
        """Read the current schedule watermark when initialized.

        Returns:
            Current watermark, or `None` before bootstrap.
        """

        ...

    def advance(
        self,
        expected_current: Optional[date],
        advanced_from: date,
        through_date: date,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> ScheduleWatermark:
        """Compare and advance the schedule watermark.

        Args:
            expected_current: State observed before discovery.
            advanced_from: Prior date or bootstrap boundary.
            through_date: Latest successfully covered date.
            run_id: Schedule run supporting the advancement.
            manifest_path: Completed schedule manifest reference.

        Returns:
            Newly published schedule watermark.
        """

        ...


@runtime_checkable
class GameChangesWatermarkStore(Protocol):
    """Success-only corrected-game discovery state."""

    def read(self) -> Optional[GameChangesWatermark]:
        """Read the current correction watermark when initialized.

        Returns:
            Current watermark, or `None` before bootstrap.
        """

        ...

    def advance(
        self,
        expected_current: Optional[datetime],
        advanced_from: datetime,
        updated_since: datetime,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> GameChangesWatermark:
        """Compare and advance the correction watermark.

        Args:
            expected_current: State observed before polling.
            advanced_from: Logical checkpoint used by the poll.
            updated_since: New checkpoint captured before polling.
            run_id: Poll run supporting the advancement.
            manifest_path: Completed poll manifest reference.

        Returns:
            Newly published correction watermark.
        """

        ...


@runtime_checkable
class DailyRunStore(Protocol):
    """Daily coordinator manifest persistence."""

    def start(
        self,
        run_id: UUID,
        started_at: datetime,
        through_date: date,
        configuration: Dict[str, Any],
    ) -> StartedDailyRun:
        """Create one open daily run manifest.

        Args:
            run_id: Unique daily run identifier.
            started_at: UTC time captured before branch work.
            through_date: Inclusive schedule discovery date.
            configuration: JSON-serializable run configuration.

        Returns:
            Started run identity and manifest reference.
        """

        ...

    def record_branch(
        self,
        manifest_path: ArtifactReference,
        branch: str,
        status: str,
        details: Dict[str, Any],
    ) -> None:
        """Record one daily branch outcome."""

        ...

    def finalize(self, manifest_path: ArtifactReference) -> Dict[str, str]:
        """Finalize a daily run and return its branch statuses."""

        ...


@runtime_checkable
class SeasonBackfillStore(Protocol):
    """Backfill run evidence and season-scoped reconciliation state."""

    def start(
        self,
        run_id: UUID,
        started_at: datetime,
        seasons: Tuple[int, ...],
        mode: str,
        dry_run: bool,
        configuration: Dict[str, Any],
    ) -> StartedSeasonBackfillRun:
        ...

    def record_season(
        self,
        manifest_path: ArtifactReference,
        season: int,
        status: str,
        details: Dict[str, Any],
    ) -> None:
        ...

    def finalize(self, manifest_path: ArtifactReference) -> Dict[int, str]:
        ...

    def land_changes_page(
        self,
        run_id: UUID,
        season: int,
        page_number: int,
        changes: GameChangesResponse,
        request: GameChangesRequest,
        raw: bytes,
    ) -> ArtifactReference:
        ...

    def complete_changes(
        self,
        run_id: UUID,
        season: int,
        updated_since: datetime,
        window_end: datetime,
        total_items: int,
        response_paths: Tuple[ArtifactReference, ...],
        changed_game_pks: Tuple[int, ...],
    ) -> None:
        ...

    def load_changes(
        self,
        run_id: UUID,
        season: int,
        updated_since: datetime,
        window_end: datetime,
    ) -> Optional[LoadedSeasonBackfillChanges]:
        ...

    def read_checkpoint(self, season: int) -> Optional[SeasonBackfillCheckpoint]:
        ...

    def advance_checkpoint(
        self,
        season: int,
        expected_current: Optional[datetime],
        updated_since: datetime,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> SeasonBackfillCheckpoint:
        ...
