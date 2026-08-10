"""Paginated corrected-game polling with success-only watermark advancement."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, Protocol
from uuid import UUID, uuid4

from zavant.clients.mlb_stats_api import RetrievedResource
from zavant.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.protocols import GameChangesStore, GameChangesWatermarkStore


Clock = Callable[[], datetime]
RunIdFactory = Callable[[], UUID]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MlbGameChangesApi(Protocol):
    """MLB client operation required by corrected-game polling."""

    def get_game_changes(
        self,
        updated_since: datetime,
        sport_id: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> RetrievedResource:
        """Retrieve one corrected-game response page.

        Args:
            updated_since: Inclusive lower query boundary.
            sport_id: MLB sport identifier.
            limit: Maximum results requested from this page.
            offset: Result offset for this page.

        Returns:
            Exact response bytes and HTTP provenance.
        """

        ...


class GameChangesPollingError(RuntimeError):
    """Raised when a correction poll cannot establish a complete page set."""


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
        api: MlbGameChangesApi,
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

        watermark_after = self._normalize_timestamp(self.clock(), "poll clock result")
        self._validate_options(sport_id, limit, overlap, max_pages)
        current = self.watermark_store.read()
        if current is None:
            if initial_watermark is None:
                raise GameChangesWatermarkNotInitializedError(
                    "the first correction poll requires --initial-watermark"
                )
            watermark_before = self._normalize_timestamp(
                initial_watermark, "initial_watermark"
            )
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
        expected_page_count = 1
        source_item_count = 0
        http_attempts = 0
        manifest_path: Optional[ArtifactReference] = None
        while page_number < expected_page_count:
            offset = page_number * limit
            retrieved = self.api.get_game_changes(
                updated_since=query_updated_since,
                sport_id=sport_id,
                limit=limit,
                offset=offset,
            )
            http_attempts += retrieved.attempts
            changes = GameChangesResponse.from_bytes(retrieved.body)
            if page_number == 0:
                source_item_count = changes.total_items
                expected_page_count = max(
                    1,
                    (source_item_count + limit - 1) // limit,
                )
                if expected_page_count > max_pages:
                    raise GameChangesPollingError(
                        f"poll requires {expected_page_count} pages, exceeding "
                        f"max_pages={max_pages}"
                    )
            expected_items_on_page = min(
                limit,
                max(0, source_item_count - offset),
            )
            if len(changes.changed_games) != expected_items_on_page:
                raise GameChangesPollingError(
                    f"page {page_number} contains {len(changes.changed_games)} "
                    f"deduplicated games; expected {expected_items_on_page}"
                )

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
        summary = self.changes_store.finalize_manifest(
            manifest_path=manifest_path,
            expected_page_count=expected_page_count,
            expected_total_items=source_item_count,
            watermark_before=watermark_before,
        )
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
            source_item_count=summary["source_items"],
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

    @staticmethod
    def _normalize_timestamp(value: datetime, name: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include a UTC offset")
        return value.astimezone(timezone.utc)
