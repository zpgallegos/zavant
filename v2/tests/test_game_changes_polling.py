from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
from typing import Dict, List, Tuple
import unittest
from uuid import UUID

from zavant.acquisition.game_changes import (
    GameChangesPoller,
    GameChangesPollingError,
    GameChangesWatermarkNotInitializedError,
)
from zavant.clients.mlb_stats_api import RetrievedResource
from zavant.storage.path_game_changes import PathGameChangesStore
from zavant.storage._path_io import artifact_reference_for_path
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.path_game_changes_watermark import (
    GameChangesWatermarkConflictError,
    PathGameChangesWatermarkStore,
)


INITIAL_WATERMARK = datetime(2026, 8, 8, tzinfo=timezone.utc)
POLL_STARTED_AT = datetime(2026, 8, 9, tzinfo=timezone.utc)
NEXT_POLL_STARTED_AT = datetime(2026, 8, 10, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 9, 0, 1, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000010")
NEXT_RUN_ID = UUID("00000000-0000-0000-0000-000000000011")


def game_changes_raw(game_pks: List[int], total_items: int) -> bytes:
    games = [
        {
            "gamePk": game_pk,
            "officialDate": "2026-08-08",
            "season": "2026",
            "link": f"/api/v1.1/game/{game_pk}/feed/live",
            "status": {
                "codedGameState": "F",
                "detailedState": "Final",
            },
        }
        for game_pk in game_pks
    ]
    return json.dumps(
        {
            "totalItems": total_items,
            "totalGames": total_items,
            "dates": ([{"date": "2026-08-08", "games": games}] if games else []),
        }
    ).encode()


def retrieved(body: bytes, offset: int, attempts: int = 1) -> RetrievedResource:
    source_uri = f"https://statsapi.example.test/api/v1/game/changes?offset={offset}"
    return RetrievedResource(
        body=body,
        request_url=source_uri,
        response_url=source_uri,
        status_code=200,
        headers={"Content-Type": "application/json"},
        attempts=attempts,
    )


class FakeGameChangesApi:
    def __init__(self, outcomes: Dict[int, object]) -> None:
        self.outcomes = outcomes
        self.calls: List[Tuple[datetime, int, int, int]] = []

    def get_game_changes(
        self,
        updated_since: datetime,
        sport_id: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> RetrievedResource:
        self.calls.append((updated_since, sport_id, limit, offset))
        outcome = self.outcomes.get(offset)
        if isinstance(outcome, RuntimeError):
            raise outcome
        if not isinstance(outcome, RetrievedResource):
            raise AssertionError(f"no valid outcome for offset {offset}")
        return outcome


class GameChangesPollerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.changes_store = PathGameChangesStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.watermark_store = PathGameChangesWatermarkStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def poller(
        self,
        api: FakeGameChangesApi,
        poll_started_at: datetime = POLL_STARTED_AT,
        run_id: UUID = RUN_ID,
    ) -> GameChangesPoller:
        return GameChangesPoller(
            api=api,
            changes_store=self.changes_store,
            watermark_store=self.watermark_store,
            clock=lambda: poll_started_at,
            run_id_factory=lambda: run_id,
        )

    def test_lands_all_pages_then_advances_watermark(self) -> None:
        api = FakeGameChangesApi(
            {
                0: retrieved(game_changes_raw([823426], total_items=2), 0),
                1: retrieved(
                    game_changes_raw([823427], total_items=2),
                    1,
                    attempts=2,
                ),
            }
        )

        result = self.poller(api).poll(
            initial_watermark=INITIAL_WATERMARK,
            limit=1,
            overlap=timedelta(minutes=5),
        )

        self.assertEqual(result.page_count, 2)
        self.assertEqual(result.changed_game_count, 2)
        self.assertEqual(result.source_item_count, 2)
        self.assertEqual(result.http_attempts, 3)
        self.assertEqual(
            [call[3] for call in api.calls],
            [0, 1],
        )
        self.assertTrue(
            all(
                call[0] == INITIAL_WATERMARK - timedelta(minutes=5)
                for call in api.calls
            )
        )

        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["watermark_before"], INITIAL_WATERMARK.isoformat())
        self.assertEqual(manifest["summary"]["pages"], 2)
        self.assertEqual(
            [game["game_pk"] for game in manifest["changed_games"]],
            [823426, 823427],
        )
        self.assertTrue(
            all(
                game["processing_status"] == "pending"
                for game in manifest["changed_games"]
            )
        )

        watermark = self.watermark_store.read()
        self.assertIsNotNone(watermark)
        assert watermark is not None
        self.assertEqual(watermark.advanced_from, INITIAL_WATERMARK)
        self.assertEqual(watermark.updated_since, POLL_STARTED_AT)
        self.assertEqual(watermark.manifest_path, result.manifest_path)

    def test_empty_poll_still_advances_watermark(self) -> None:
        api = FakeGameChangesApi({0: retrieved(game_changes_raw([], total_items=0), 0)})

        result = self.poller(api).poll(initial_watermark=INITIAL_WATERMARK)

        self.assertEqual(result.page_count, 1)
        self.assertEqual(result.changed_game_count, 0)
        self.assertEqual(result.source_item_count, 0)
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(manifest["status"], "complete")

    def test_subsequent_poll_reads_stored_watermark(self) -> None:
        first_api = FakeGameChangesApi(
            {0: retrieved(game_changes_raw([], total_items=0), 0)}
        )
        self.poller(first_api).poll(initial_watermark=INITIAL_WATERMARK)
        next_api = FakeGameChangesApi(
            {0: retrieved(game_changes_raw([], total_items=0), 0)}
        )

        result = self.poller(
            next_api,
            poll_started_at=NEXT_POLL_STARTED_AT,
            run_id=NEXT_RUN_ID,
        ).poll()

        self.assertEqual(result.watermark_before, POLL_STARTED_AT)
        self.assertEqual(
            next_api.calls[0][0],
            POLL_STARTED_AT - timedelta(minutes=5),
        )
        watermark = self.watermark_store.read()
        assert watermark is not None
        self.assertEqual(watermark.updated_since, NEXT_POLL_STARTED_AT)

    def test_page_failure_leaves_watermark_unchanged(self) -> None:
        initial_api = FakeGameChangesApi(
            {0: retrieved(game_changes_raw([], total_items=0), 0)}
        )
        self.poller(initial_api).poll(initial_watermark=INITIAL_WATERMARK)
        failing_api = FakeGameChangesApi(
            {
                0: retrieved(game_changes_raw([823426], total_items=2), 0),
                1: RuntimeError("source failed"),
            }
        )

        with self.assertRaisesRegex(RuntimeError, "source failed"):
            self.poller(
                failing_api,
                poll_started_at=NEXT_POLL_STARTED_AT,
                run_id=NEXT_RUN_ID,
            ).poll(limit=1)

        watermark = self.watermark_store.read()
        assert watermark is not None
        self.assertEqual(watermark.updated_since, POLL_STARTED_AT)
        failed_manifests = list(
            self.data_dir.glob(
                "raw/mlb_stats_api/game_changes/poll_date=2026-08-10/"
                "run_id=*/manifest.json"
            )
        )
        self.assertEqual(len(failed_manifests), 1)
        failed_manifest = json.loads(failed_manifests[0].read_text())
        self.assertEqual(failed_manifest["status"], "open")
        self.assertEqual(len(failed_manifest["pages"]), 1)

    def test_requires_initial_watermark_for_first_poll(self) -> None:
        api = FakeGameChangesApi({})

        with self.assertRaises(GameChangesWatermarkNotInitializedError):
            self.poller(api).poll()

        self.assertEqual(api.calls, [])

    def test_rejects_initial_watermark_after_initialization(self) -> None:
        api = FakeGameChangesApi({0: retrieved(game_changes_raw([], total_items=0), 0)})
        self.poller(api).poll(initial_watermark=INITIAL_WATERMARK)

        with self.assertRaisesRegex(ValueError, "must be omitted"):
            self.poller(api, poll_started_at=NEXT_POLL_STARTED_AT).poll(
                initial_watermark=INITIAL_WATERMARK
            )

    def test_max_pages_guard_does_not_advance_watermark(self) -> None:
        api = FakeGameChangesApi(
            {0: retrieved(game_changes_raw([823426], total_items=3), 0)}
        )

        with self.assertRaisesRegex(GameChangesPollingError, "exceeding"):
            self.poller(api).poll(
                initial_watermark=INITIAL_WATERMARK,
                limit=1,
                max_pages=2,
            )

        self.assertIsNone(self.watermark_store.read())

    def test_short_page_does_not_advance_watermark(self) -> None:
        api = FakeGameChangesApi({0: retrieved(game_changes_raw([], total_items=1), 0)})

        with self.assertRaisesRegex(GameChangesPollingError, "expected 1"):
            self.poller(api).poll(initial_watermark=INITIAL_WATERMARK)

        self.assertIsNone(self.watermark_store.read())


class PathGameChangesWatermarkStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.store = PathGameChangesWatermarkStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def completed_manifest(
        self,
        run_id: UUID,
        watermark_before: datetime,
        window_end: datetime,
    ) -> ArtifactReference:
        path = self.data_dir / "raw" / str(run_id) / "manifest.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps(
                {
                    "contract": "mlb-stats-api-game-changes-manifest/v1",
                    "run_id": str(run_id),
                    "status": "complete",
                    "watermark_before": watermark_before.isoformat(),
                    "window_end": window_end.isoformat(),
                }
            )
        )
        return artifact_reference_for_path(self.data_dir, path)

    def test_compare_and_set_rejects_stale_expected_state(self) -> None:
        first_manifest = self.completed_manifest(
            RUN_ID,
            INITIAL_WATERMARK,
            POLL_STARTED_AT,
        )
        self.store.advance(
            expected_current=None,
            advanced_from=INITIAL_WATERMARK,
            updated_since=POLL_STARTED_AT,
            run_id=RUN_ID,
            manifest_path=first_manifest,
        )
        stale_manifest = self.completed_manifest(
            NEXT_RUN_ID,
            INITIAL_WATERMARK,
            NEXT_POLL_STARTED_AT,
        )

        with self.assertRaises(GameChangesWatermarkConflictError):
            self.store.advance(
                expected_current=None,
                advanced_from=INITIAL_WATERMARK,
                updated_since=NEXT_POLL_STARTED_AT,
                run_id=NEXT_RUN_ID,
                manifest_path=stale_manifest,
            )
