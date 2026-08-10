from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from typing import Dict, List
import unittest
from uuid import UUID

from zavant.acquisition.corrected_games import CorrectedGameProcessor
from zavant.clients.mlb_stats_api import (
    MlbStatsApiError,
    MlbStatsApiResponseError,
    RetrievedResource,
)
from zavant.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.contracts.raw_game import RawGameResponse
from zavant.storage.local_game_changes import LocalGameChangesStore
from zavant.storage.local_raw import LocalRawGameStore


UPDATED_SINCE = datetime(2026, 8, 8, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 8, 9, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 9, 0, 1, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000020")


def raw_game(game_pk: int, marker: str) -> bytes:
    """Build a minimal valid live-game response revision.

    Args:
        game_pk: MLB game identifier embedded in the response.
        marker: Meaningful source value distinguishing revisions.

    Returns:
        UTF-8 JSON bytes satisfying the raw-game contract.
    """

    return json.dumps(
        {
            "gamePk": game_pk,
            "metaData": {"timeStamp": marker},
            "gameData": {"datetime": {"officialDate": "2026-08-08"}},
            "liveData": {"marker": marker},
        }
    ).encode()


def changes_raw(game_pks: List[int]) -> bytes:
    """Build a valid corrected-game response.

    Args:
        game_pks: Changed game identifiers included in the response.

    Returns:
        UTF-8 JSON bytes satisfying the correction contract.
    """

    games = [
        {
            "gamePk": game_pk,
            "officialDate": "2026-08-08",
            "season": "2026",
            "link": f"/api/v1.1/game/{game_pk}/feed/live",
            "status": {"codedGameState": "F", "detailedState": "Final"},
        }
        for game_pk in game_pks
    ]
    return json.dumps(
        {
            "totalItems": len(games),
            "totalGames": len(games),
            "dates": [{"date": "2026-08-08", "games": games}],
        }
    ).encode()


def retrieved(body: bytes, game_pk: int) -> RetrievedResource:
    """Build a successful fake live-game resource.

    Args:
        body: Exact response bytes.
        game_pk: Game identifier included in source provenance.

    Returns:
        Successful retrieved resource.
    """

    uri = f"https://statsapi.example.test/api/v1.1/game/{game_pk}/feed/live"
    return RetrievedResource(
        body=body,
        request_url=uri,
        response_url=uri,
        status_code=200,
        headers={},
        attempts=1,
    )


class FakeCorrectedGameApi:
    """Queue live-game responses and failures by game identifier."""

    def __init__(self, outcomes: Dict[int, List[object]]) -> None:
        """Initialize queued game outcomes.

        Args:
            outcomes: Responses or MLB client errors queued by game ID.
        """

        self.outcomes = {key: list(value) for key, value in outcomes.items()}
        self.calls: List[int] = []

    def get_live_game(self, game_pk: int) -> RetrievedResource:
        """Return or raise the next configured game outcome.

        Args:
            game_pk: Requested MLB game identifier.

        Returns:
            Configured successful response.

        Raises:
            MlbStatsApiError: If the next outcome is a configured client error.
            AssertionError: If no valid outcome remains.
        """

        self.calls.append(game_pk)
        outcomes = self.outcomes.get(game_pk)
        if not outcomes:
            raise AssertionError(f"no queued response for game {game_pk}")
        outcome = outcomes.pop(0)
        if isinstance(outcome, MlbStatsApiError):
            raise outcome
        if not isinstance(outcome, RetrievedResource):
            raise AssertionError(f"invalid outcome for game {game_pk}")
        return outcome


class CorrectedGameProcessorTests(unittest.TestCase):
    """Tests for durable correction-manifest processing."""

    def setUp(self) -> None:
        """Create isolated raw and correction stores."""

        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.changes_store = LocalGameChangesStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.game_store = LocalRawGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def correction_manifest(self, game_pks: List[int]) -> Path:
        """Land and finalize one correction response manifest.

        Args:
            game_pks: Changed games included in the poll.

        Returns:
            Completed correction manifest path.
        """

        raw = changes_raw(game_pks)
        changes = GameChangesResponse.from_bytes(raw)
        landed = self.changes_store.land_page(
            changes=changes,
            request=GameChangesRequest(
                updated_since=UPDATED_SINCE,
                window_end=WINDOW_END,
                page_number=0,
                limit=1000,
                offset=0,
                source_uri="fixture://changes",
            ),
            raw=raw,
            run_id=RUN_ID,
        )
        self.changes_store.finalize_manifest(
            manifest_path=landed.manifest_path,
            expected_page_count=1,
            expected_total_items=len(game_pks),
            watermark_before=UPDATED_SINCE,
        )
        return landed.manifest_path

    def land_existing_game(self, game_pk: int, marker: str = "old") -> None:
        """Seed a current raw revision for correction eligibility.

        Args:
            game_pk: MLB game identifier to seed.
            marker: Content marker for the seeded revision.
        """

        raw = raw_game(game_pk, marker)
        self.game_store.land(
            RawGameResponse.from_bytes(raw),
            raw,
            source_uri="fixture://existing",
            trigger="initial",
        )

    def test_lands_new_revision_and_completes_manifest(self) -> None:
        """Retrieve an existing changed game into a new raw revision."""

        self.land_existing_game(823426)
        manifest_path = self.correction_manifest([823426])
        api = FakeCorrectedGameApi(
            {823426: [retrieved(raw_game(823426, "corrected"), 823426)]}
        )

        result = CorrectedGameProcessor(
            api, self.changes_store, self.game_store
        ).process_all()

        self.assertTrue(result.successful)
        self.assertEqual(result.summary["succeeded"], 1)
        self.assertEqual(api.calls, [823426])
        manifest = json.loads(manifest_path.read_text())
        self.assertEqual(manifest["processing_status"], "complete")
        self.assertTrue(manifest["changed_games"][0]["revision_created"])
        self.assertEqual(
            len(list(self.data_dir.rglob("game_pk=823426/revision=*/game.json"))),
            2,
        )

    def test_skips_game_not_previously_landed(self) -> None:
        """Leave initial portfolio inclusion to schedule discovery."""

        manifest_path = self.correction_manifest([823426])
        api = FakeCorrectedGameApi({})

        result = CorrectedGameProcessor(
            api, self.changes_store, self.game_store
        ).process_all()

        self.assertTrue(result.successful)
        self.assertEqual(result.summary["skipped"], 1)
        self.assertEqual(api.calls, [])
        manifest = json.loads(manifest_path.read_text())
        self.assertEqual(
            manifest["changed_games"][0]["reason"],
            "game_not_previously_landed",
        )

    def test_retries_failed_game_on_next_invocation(self) -> None:
        """Retry only failed correction work and preserve attempt history."""

        self.land_existing_game(823426)
        manifest_path = self.correction_manifest([823426])
        api = FakeCorrectedGameApi(
            {
                823426: [
                    MlbStatsApiResponseError(
                        status_code=503,
                        url="https://statsapi.example.test/game/823426",
                        attempts=3,
                    ),
                    retrieved(raw_game(823426, "corrected"), 823426),
                ]
            }
        )
        processor = CorrectedGameProcessor(api, self.changes_store, self.game_store)

        first = processor.process_all()
        second = processor.process_all()

        self.assertFalse(first.successful)
        self.assertTrue(second.successful)
        self.assertEqual(api.calls, [823426, 823426])
        manifest = json.loads(manifest_path.read_text())
        self.assertEqual(len(manifest["changed_games"][0]["processing_attempts"]), 2)
