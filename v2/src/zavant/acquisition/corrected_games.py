"""Process durable corrected-game manifests into raw-game revisions."""

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Tuple

from zavant.clients.mlb_stats_api import MlbStatsApiError, RetrievedResource
from zavant.contracts.raw_game import RawGameContractError, RawGameResponse
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import RawGameConflictError
from zavant.storage.protocols import GameChangesStore, RawGameStore


class MlbCorrectedGameApi(Protocol):
    """MLB client operation required by corrected-game processing."""

    def get_live_game(self, game_pk: int) -> RetrievedResource:
        """Retrieve one complete live-game response.

        Args:
            game_pk: MLB's primary game identifier.

        Returns:
            Exact live-game response and HTTP provenance.
        """

        ...


class CorrectedGameIdentityError(ValueError):
    """Raised when a correction response identifies a different game."""


@dataclass(frozen=True)
class CorrectionManifestProcessingResult:
    """Processing result for one completed correction manifest.

    Attributes:
        manifest_path: Correction manifest that supplied the work.
        status: Derived processing status: complete or failed.
        summary: Counts grouped by changed-game processing status.
    """

    manifest_path: ArtifactReference
    status: str
    summary: Dict[str, int]

    @property
    def successful(self) -> bool:
        """Return whether every changed game reached a terminal success state.

        Returns:
            `True` when no game is pending or failed.
        """

        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable manifest result.

        Returns:
            Manifest location, status, and processing counts.
        """

        return {
            "manifest_path": str(self.manifest_path),
            "status": self.status,
            "summary": self.summary,
        }


@dataclass(frozen=True)
class CorrectedGameProcessingResult:
    """Aggregate result for all outstanding correction manifests.

    Attributes:
        manifests: Per-manifest processing results.
        status: Derived aggregate status: complete or failed.
        summary: Counts across every processed manifest.
    """

    manifests: Tuple[CorrectionManifestProcessingResult, ...]
    status: str
    summary: Dict[str, int]

    @property
    def successful(self) -> bool:
        """Return whether every processed manifest completed.

        Returns:
            `True` when the aggregate status is complete.
        """

        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable aggregate result.

        Returns:
            Aggregate status, counts, and per-manifest outcomes.
        """

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
        api: MlbCorrectedGameApi,
        changes_store: GameChangesStore,
        game_store: RawGameStore,
    ) -> None:
        """Initialize the corrected-game processor.

        Args:
            api: MLB client supporting complete live-game retrieval.
            changes_store: Store containing completed correction manifests.
            game_store: Revision-aware raw-game store.
        """

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
                retrieved = self.api.get_live_game(item.game_pk)
                game = RawGameResponse.from_bytes(retrieved.body)
                if game.game_pk != item.game_pk or game.season != item.season:
                    raise CorrectedGameIdentityError(
                        f"expected gamePk {item.game_pk} in season {item.season}, "
                        f"received gamePk {game.game_pk} in season {game.season}"
                    )
                landed = self.game_store.land(
                    game=game,
                    raw=retrieved.body,
                    source_uri=retrieved.source_uri,
                    trigger="game_changes",
                )
            except (
                CorrectedGameIdentityError,
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
        """Record one corrected-game failure in its source manifest.

        Args:
            manifest_path: Correction manifest receiving the outcome.
            game_pk: MLB game identifier that failed.
            exc: Failure raised during lookup, retrieval, validation, or landing.

        Raises:
            GameChangesConflictError: If correction state is malformed.
            OSError: If the failure cannot be persisted.
        """

        self.changes_store.record_game_outcome(
            manifest_path=manifest_path,
            game_pk=game_pk,
            status="failed",
            details={
                "error_message": str(exc)[:500],
                "error_type": type(exc).__name__,
            },
        )
