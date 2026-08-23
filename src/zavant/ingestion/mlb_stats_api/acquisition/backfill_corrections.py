"""Public correction discovery scoped to a historical backfill run."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Set, Tuple
from uuid import UUID

from zavant.ingestion.mlb_stats_api.acquisition.correction_pagination import (
    CorrectionPaginationGuard,
)
from zavant.ingestion.mlb_stats_api.acquisition.protocols import GameChangesApi
from zavant.ingestion.mlb_stats_api.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.mlb_stats_api.storage.protocols import SeasonBackfillStore


@dataclass(frozen=True)
class BackfillCorrectionDiscovery:
    game_pks: Set[int]
    response_paths: Tuple[ArtifactReference, ...]


class SeasonCorrectionDiscoverer:
    """Retrieve and persist a complete correction page set for one season."""

    def __init__(
        self,
        api: GameChangesApi,
        store: SeasonBackfillStore,
    ) -> None:
        self.api = api
        self.store = store

    def discover(
        self,
        season: int,
        updated_since: datetime,
        window_end: datetime,
        sport_id: int,
        limit: int,
        max_pages: int,
        run_id: UUID,
    ) -> BackfillCorrectionDiscovery:
        completed = self.store.load_changes(
            run_id, season, updated_since, window_end
        )
        if completed is not None:
            return BackfillCorrectionDiscovery(
                game_pks=set(completed.changed_game_pks),
                response_paths=completed.response_paths,
            )

        changed_game_pks: Set[int] = set()
        evidence: List[ArtifactReference] = []
        page_number = 0
        pagination = CorrectionPaginationGuard(
            limit,
            max_pages,
            f"season {season} reconciliation",
        )
        while page_number < pagination.expected_page_count:
            offset = page_number * limit
            retrieved = self.api.get_game_changes(
                updated_since=updated_since,
                sport_id=sport_id,
                limit=limit,
                offset=offset,
            )
            changes = GameChangesResponse.from_bytes(retrieved.body)
            pagination.accept(page_number, changes)
            request = GameChangesRequest(
                updated_since=updated_since,
                window_end=window_end,
                page_number=page_number,
                limit=limit,
                offset=offset,
                source_uri=retrieved.source_uri,
            )
            evidence.append(
                self.store.land_changes_page(
                    run_id,
                    season,
                    page_number,
                    changes,
                    request,
                    retrieved.body,
                )
            )
            changed_game_pks.update(
                game.game_pk for game in changes.changed_games if game.season == season
            )
            page_number += 1

        pagination.validate_complete()
        response_paths = tuple(evidence)
        normalized_game_pks = tuple(sorted(changed_game_pks))
        self.store.complete_changes(
            run_id=run_id,
            season=season,
            updated_since=updated_since,
            window_end=window_end,
            total_items=pagination.total_items,
            response_paths=response_paths,
            changed_game_pks=normalized_game_pks,
        )
        return BackfillCorrectionDiscovery(
            game_pks=set(normalized_game_pks),
            response_paths=response_paths,
        )
