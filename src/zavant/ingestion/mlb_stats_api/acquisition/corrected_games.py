"""Process durable corrected-game manifests into raw-game revisions."""

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from zavant.ingestion.mlb_stats_api.acquisition.live_games import GameIdentityError, retrieve_live_game
from zavant.ingestion.mlb_stats_api.acquisition.protocols import LiveGameApi
from zavant.ingestion.mlb_stats_api.client import MlbStatsApiError
from zavant.ingestion.mlb_stats_api.contracts.raw_game import RawGameContractError
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.mlb_stats_api.storage.errors import RawGameConflictError
from zavant.ingestion.mlb_stats_api.storage.protocols import GameChangesStore, RawGameStore


@dataclass(frozen=True)
class CorrectionManifestProcessingResult:
    """Processing result for one completed correction manifest."""

    manifest_path: ArtifactReference
    status: str
    summary: Dict[str, int]

    @property
    def successful(self) -> bool:
        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "manifest_path": str(self.manifest_path),
            "status": self.status,
            "summary": self.summary,
        }


@dataclass(frozen=True)
class CorrectedGameProcessingResult:
    """Aggregate result for all outstanding correction manifests."""

    manifests: Tuple[CorrectionManifestProcessingResult, ...]
    status: str
    summary: Dict[str, int]

    @property
    def successful(self) -> bool:
        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "manifests": [manifest.as_dict() for manifest in self.manifests],
            "status": self.status,
            "summary": self.summary,
        }


class CorrectedGameProcessor:
    """Retrieve changed games already present in the managed portfolio.

    Args:
        api: MLB client supporting complete live-game retrieval.
        changes_store: Store containing completed correction manifests.
        game_store: Revision-aware raw-game store.
    """

    def __init__(
        self,
        api: LiveGameApi,
        changes_store: GameChangesStore,
        game_store: RawGameStore,
    ) -> None:
        self.api = api
        self.changes_store = changes_store
        self.game_store = game_store

    def process_all(self) -> CorrectedGameProcessingResult:
        """Process every completed manifest with pending or failed games.

        Returns:
            Aggregate and per-manifest processing results.

        Raises:
            GameChangesConflictError: If correction state is malformed.
            OSError: If manifests cannot be read or updated.
        """

        results: List[CorrectionManifestProcessingResult] = []
        aggregate = {
            status: 0 for status in ("pending", "skipped", "succeeded", "failed")
        }
        for manifest_path in self.changes_store.processable_manifests():
            result = self.process_manifest(manifest_path)
            results.append(result)
            for status, count in result.summary.items():
                aggregate[status] += count
        overall_status = (
            "failed" if aggregate["failed"] or aggregate["pending"] else "complete"
        )
        return CorrectedGameProcessingResult(
            manifests=tuple(results),
            status=overall_status,
            summary=aggregate,
        )

    def process_manifest(
        self,
        manifest_path: ArtifactReference,
    ) -> CorrectionManifestProcessingResult:
        """Process pending and failed games from one completed poll.

        Games without an existing raw revision are skipped because schedule
        discovery owns initial portfolio inclusion. Failed games are retried
        on the next invocation; succeeded and skipped entries are terminal.

        Args:
            manifest_path: Completed correction-poll manifest.

        Returns:
            Final processing state and per-game counts.

        Raises:
            GameChangesConflictError: If correction state is malformed.
            OSError: If manifest outcomes cannot be persisted.
        """

        for item in self.changes_store.game_work_items(manifest_path):
            try:
                current_revision_id = self.game_store.current_revision_id(
                    season=item.season,
                    game_pk=item.game_pk,
                )
            except (OSError, RawGameConflictError, ValueError) as exc:
                self._record_failure(manifest_path, item.game_pk, exc)
                continue
            if current_revision_id is None:
                self.changes_store.record_game_outcome(
                    manifest_path=manifest_path,
                    game_pk=item.game_pk,
                    status="skipped",
                    details={"reason": "game_not_previously_landed"},
                )
                continue

            try:
                retrieved, game = retrieve_live_game(
                    self.api, item.game_pk, item.season
                )
                landed = self.game_store.land(
                    game=game,
                    raw=retrieved.body,
                    source_uri=retrieved.source_uri,
                    trigger="game_changes",
                )
            except (
                GameIdentityError,
                MlbStatsApiError,
                OSError,
                RawGameConflictError,
                RawGameContractError,
            ) as exc:
                self._record_failure(manifest_path, item.game_pk, exc)
                continue

            self.changes_store.record_game_outcome(
                manifest_path=manifest_path,
                game_pk=item.game_pk,
                status="succeeded",
                details={
                    "http_attempts": retrieved.attempts,
                    "revision_created": landed.created,
                    "revision_id": landed.revision_id,
                    "source_uri": retrieved.source_uri,
                },
            )

        summary = self.changes_store.finalize_processing(manifest_path)
        status = "failed" if summary["failed"] or summary["pending"] else "complete"
        return CorrectionManifestProcessingResult(
            manifest_path=manifest_path,
            status=status,
            summary=summary,
        )

    def _record_failure(
        self, manifest_path: ArtifactReference, game_pk: int, exc: Exception
    ) -> None:
        self.changes_store.record_game_outcome(
            manifest_path=manifest_path,
            game_pk=game_pk,
            status="failed",
            details={
                "error_message": str(exc)[:500],
                "error_type": type(exc).__name__,
            },
        )
