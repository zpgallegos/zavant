"""Public correction discovery scoped to a historical backfill run."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Protocol, Set, Tuple
from uuid import UUID

from zavant.acquisition.game_changes import GameChangesPollingError
from zavant.clients.mlb_stats_api import RetrievedResource
from zavant.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.protocols import SeasonBackfillStore


class MlbBackfillCorrectionsApi(Protocol):
    def get_game_changes(
        self,
        updated_since: datetime,
        sport_id: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> RetrievedResource:
        ...


@dataclass(frozen=True)
class BackfillCorrectionDiscovery:
    game_pks: Set[int]
    response_paths: Tuple[ArtifactReference, ...]


class SeasonCorrectionDiscoverer:
    """Retrieve and persist a complete correction page set for one season."""

    def __init__(
        self,
        api: MlbBackfillCorrectionsApi,
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
        expected_pages = 1
        total_items = 0
        while page_number < expected_pages:
            offset = page_number * limit
            retrieved = self.api.get_game_changes(
                updated_since=updated_since,
                sport_id=sport_id,
                limit=limit,
                offset=offset,
            )
            changes = GameChangesResponse.from_bytes(retrieved.body)
            if page_number == 0:
                total_items = changes.total_items
                expected_pages = max(1, (total_items + limit - 1) // limit)
                if expected_pages > max_pages:
                    raise GameChangesPollingError(
                        f"season {season} reconciliation requires {expected_pages} "
                        f"pages, exceeding max_pages={max_pages}"
                    )
            expected_items = min(limit, max(0, total_items - offset))
            if len(changes.changed_games) != expected_items:
                raise GameChangesPollingError(
                    f"correction page {page_number} contains "
                    f"{len(changes.changed_games)} games; expected {expected_items}"
                )
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

        response_paths = tuple(evidence)
        normalized_game_pks = tuple(sorted(changed_game_pks))
        self.store.complete_changes(
            run_id=run_id,
            season=season,
            updated_since=updated_since,
            window_end=window_end,
            total_items=total_items,
            response_paths=response_paths,
            changed_game_pks=normalized_game_pks,
        )
        return BackfillCorrectionDiscovery(
            game_pks=set(normalized_game_pks),
            response_paths=response_paths,
        )
