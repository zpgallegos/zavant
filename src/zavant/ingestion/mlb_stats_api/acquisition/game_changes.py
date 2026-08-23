"""Paginated corrected-game polling with success-only watermark advancement."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional
from uuid import UUID, uuid4

from zavant._time import Clock, as_utc, utc_now
from zavant.ingestion.mlb_stats_api.acquisition.correction_pagination import (
    CorrectionPaginationGuard,
    GameChangesPollingError,
)
from zavant.ingestion.mlb_stats_api.acquisition.protocols import GameChangesApi
from zavant.ingestion.mlb_stats_api.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.mlb_stats_api.storage.protocols import GameChangesStore, GameChangesWatermarkStore


RunIdFactory = Callable[[], UUID]


class GameChangesWatermarkNotInitializedError(GameChangesPollingError):
    """Raised when the first poll omits an explicit initial watermark."""


@dataclass(frozen=True)
class GameChangesPollingResult:
    """Summary of one completed correction poll."""

    run_id: UUID
    watermark_before: datetime
    query_updated_since: datetime
    watermark_after: datetime
    manifest_path: ArtifactReference
    page_count: int
    source_item_count: int
    changed_game_count: int
    http_attempts: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "changed_game_count": self.changed_game_count,
            "http_attempts": self.http_attempts,
            "manifest_path": str(self.manifest_path),
            "page_count": self.page_count,
            "query_updated_since": self.query_updated_since.isoformat(),
            "run_id": str(self.run_id),
            "source_item_count": self.source_item_count,
            "watermark_after": self.watermark_after.isoformat(),
            "watermark_before": self.watermark_before.isoformat(),
        }


class GameChangesPoller:
    """Land a complete correction poll before advancing its watermark.

    Args:
        api: MLB client supporting corrected-game retrieval.
        changes_store: Store for immutable response pages and the run manifest.
        watermark_store: Store for the durable success checkpoint.
        clock: Function capturing the poll's upper checkpoint before requests.
        run_id_factory: Function generating a poll run identifier.
    """

    def __init__(
        self,
        api: GameChangesApi,
        changes_store: GameChangesStore,
        watermark_store: GameChangesWatermarkStore,
        clock: Clock = utc_now,
        run_id_factory: RunIdFactory = uuid4,
    ) -> None:
        self.api = api
        self.changes_store = changes_store
        self.watermark_store = watermark_store
        self.clock = clock
        self.run_id_factory = run_id_factory

    def poll(
        self,
        initial_watermark: Optional[datetime] = None,
        sport_id: int = 1,
        limit: int = 1000,
        overlap: timedelta = timedelta(minutes=5),
        max_pages: int = 100,
    ) -> GameChangesPollingResult:
        """Poll every page and advance state only after durable completion.

        The first invocation requires `initial_watermark`. Later invocations
        read their lower checkpoint from the watermark store and reject a new
        bootstrap value. The source query begins before that checkpoint by the
        configured overlap, while successful state advances to the timestamp
        captured before any network request.

        Args:
            initial_watermark: Explicit first-run checkpoint. Omit after the
                durable watermark has been initialized.
            sport_id: MLB sport identifier, with `1` representing MLB.
            limit: Maximum number of source items requested per page.
            overlap: Safety interval subtracted from the logical checkpoint.
            max_pages: Guard against unexpectedly large or unstable results.

        Returns:
            Completed poll boundaries, evidence path, and result counts.

        Raises:
            ValueError: If arguments or timestamps are invalid.
            GameChangesWatermarkNotInitializedError: If first-run state has no
                explicit initial checkpoint.
            GameChangesPollingError: If source pagination exceeds the guard.
            MlbStatsApiError: If any response page cannot be retrieved.
            GameChangesContractError: If any response page is invalid.
            GameChangesConflictError: If immutable poll evidence conflicts.
            GameChangesWatermarkConflictError: If another poll advanced state.
            OSError: If evidence or state cannot be read or written.
        """

        # Capture the upper checkpoint before I/O. The API has only a lower
        # bound, so the overlap on the next poll safely replays any items that
        # arrive while this request is in flight.
        watermark_after = as_utc(self.clock(), "poll clock result")
        self._validate_options(sport_id, limit, overlap, max_pages)
        current = self.watermark_store.read()
        if current is None:
            if initial_watermark is None:
                raise GameChangesWatermarkNotInitializedError(
                    "the first correction poll requires --initial-watermark"
                )
            watermark_before = as_utc(initial_watermark, "initial_watermark")
            expected_current: Optional[datetime] = None
        else:
            if initial_watermark is not None:
                raise ValueError(
                    "initial_watermark must be omitted after initialization"
                )
            watermark_before = current.updated_since
            expected_current = current.updated_since

        if watermark_before >= watermark_after:
            raise ValueError("poll clock must be after the current watermark")
        try:
            query_updated_since = watermark_before - overlap
        except OverflowError as exc:
            raise ValueError("overlap moves the query timestamp out of range") from exc

        run_id = self.run_id_factory()
        page_number = 0
        pagination = CorrectionPaginationGuard(limit, max_pages, "correction poll")
        http_attempts = 0
        manifest_path: Optional[ArtifactReference] = None
        while page_number < pagination.expected_page_count:
            offset = page_number * limit
            retrieved = self.api.get_game_changes(
                updated_since=query_updated_since,
                sport_id=sport_id,
                limit=limit,
                offset=offset,
            )
            http_attempts += retrieved.attempts
            changes = GameChangesResponse.from_bytes(retrieved.body)
            pagination.accept(page_number, changes)

            request = GameChangesRequest(
                updated_since=query_updated_since,
                window_end=watermark_after,
                page_number=page_number,
                limit=limit,
                offset=offset,
                source_uri=retrieved.source_uri,
            )
            landed = self.changes_store.land_page(
                changes=changes,
                request=request,
                raw=retrieved.body,
                run_id=run_id,
            )
            manifest_path = landed.manifest_path
            page_number += 1

        if manifest_path is None:
            raise AssertionError("correction poll completed without landing a page")
        pagination.validate_complete()
        summary = self.changes_store.finalize_manifest(
            manifest_path=manifest_path,
            expected_page_count=pagination.expected_page_count,
            expected_total_items=pagination.total_items,
            watermark_before=watermark_before,
        )
        # The completed manifest is the durable proof that authorizes moving
        # the watermark. Never reverse this order.
        self.watermark_store.advance(
            expected_current=expected_current,
            advanced_from=watermark_before,
            updated_since=watermark_after,
            run_id=run_id,
            manifest_path=manifest_path,
        )
        return GameChangesPollingResult(
            run_id=run_id,
            watermark_before=watermark_before,
            query_updated_since=query_updated_since,
            watermark_after=watermark_after,
            manifest_path=manifest_path,
            page_count=summary["pages"],
            source_item_count=pagination.total_items,
            changed_game_count=summary["changed_games"],
            http_attempts=http_attempts,
        )

    @staticmethod
    def _validate_options(
        sport_id: int,
        limit: int,
        overlap: timedelta,
        max_pages: int,
    ) -> None:
        if type(sport_id) is not int or sport_id <= 0:
            raise ValueError("sport_id must be a positive integer")
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be a positive integer")
        if overlap < timedelta(0):
            raise ValueError("overlap must not be negative")
        if type(max_pages) is not int or max_pages <= 0:
            raise ValueError("max_pages must be a positive integer")
