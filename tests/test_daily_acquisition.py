from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from uuid import UUID

from zavant.ingestion.mlb_stats_api.acquisition.bounded_games import BoundedGameAcquirer
from zavant.ingestion.mlb_stats_api.acquisition.corrected_games import CorrectedGameProcessor
from zavant.ingestion.mlb_stats_api.acquisition.daily import DailyAcquisitionCoordinator
from zavant.ingestion.mlb_stats_api.acquisition.deferred_games import DeferredGameProcessor
from zavant.ingestion.mlb_stats_api.acquisition.game_changes import GameChangesPoller
from zavant.ingestion.mlb_stats_api.acquisition.schedule_discovery import ScheduleDiscoverer
from zavant.ingestion.mlb_stats_api.storage.path_daily_runs import PathDailyRunStore
from zavant.ingestion.mlb_stats_api.storage.path_deferred_games import PathDeferredGameStore
from zavant.ingestion.mlb_stats_api.storage.path_game_changes import PathGameChangesStore
from zavant.ingestion.mlb_stats_api.storage.path_game_changes_watermark import (
    PathGameChangesWatermarkStore,
)
from zavant.ingestion.mlb_stats_api.storage.path_raw import PathRawGameStore
from zavant.ingestion.mlb_stats_api.storage.path_schedule import PathScheduleStore
from zavant.ingestion.mlb_stats_api.storage.path_schedule_watermark import PathScheduleWatermarkStore
from tests.test_bounded_game_acquisition import (
    FakeMlbGameAcquisitionApi,
    raw_game,
    retrieved as retrieved_game,
)
from tests.test_game_changes_polling import (
    FakeGameChangesApi,
    game_changes_raw,
    retrieved as retrieved_changes,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SCHEDULE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-schedule.json"
INITIAL_CORRECTION_WATERMARK = datetime(2026, 8, 7, tzinfo=timezone.utc)
FIRST_STARTED_AT = datetime(2026, 8, 9, tzinfo=timezone.utc)
SECOND_STARTED_AT = datetime(2026, 8, 10, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 10, 0, 1, tzinfo=timezone.utc)
FIRST_DAILY_RUN_ID = UUID("00000000-0000-0000-0000-000000000040")
SECOND_DAILY_RUN_ID = UUID("00000000-0000-0000-0000-000000000041")
FIRST_POLL_RUN_ID = UUID("00000000-0000-0000-0000-000000000042")
SECOND_POLL_RUN_ID = UUID("00000000-0000-0000-0000-000000000043")
FIRST_SCHEDULE_RUN_ID = UUID("00000000-0000-0000-0000-000000000044")
SECOND_SCHEDULE_RUN_ID = UUID("00000000-0000-0000-0000-000000000045")


def corrected_raw_game(game_pk: int) -> bytes:
    payload = json.loads(raw_game(game_pk))
    payload["liveData"]["correction"] = "published"
    return json.dumps(payload).encode()


class DailyAcquisitionCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.schedule_store = PathScheduleStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.changes_store = PathGameChangesStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.game_store = PathRawGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.schedule_watermark_store = PathScheduleWatermarkStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.changes_watermark_store = PathGameChangesWatermarkStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.daily_run_store = PathDailyRunStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.deferred_game_store = PathDeferredGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def coordinator(
        self,
        changes_api: FakeGameChangesApi,
        game_api: FakeMlbGameAcquisitionApi,
        started_at: datetime,
        daily_run_id: UUID,
        poll_run_id: UUID,
        schedule_run_id: UUID,
    ) -> DailyAcquisitionCoordinator:
        return DailyAcquisitionCoordinator(
            changes_poller=GameChangesPoller(
                api=changes_api,
                changes_store=self.changes_store,
                watermark_store=self.changes_watermark_store,
                clock=lambda: started_at,
                run_id_factory=lambda: poll_run_id,
            ),
            corrected_game_processor=CorrectedGameProcessor(
                api=game_api,
                changes_store=self.changes_store,
                game_store=self.game_store,
            ),
            deferred_game_processor=DeferredGameProcessor(
                api=game_api,
                deferred_game_store=self.deferred_game_store,
                game_store=self.game_store,
            ),
            schedule_discoverer=ScheduleDiscoverer(
                acquirer=BoundedGameAcquirer(
                    api=game_api,
                    schedule_store=self.schedule_store,
                    game_store=self.game_store,
                    deferred_game_store=self.deferred_game_store,
                    clock=lambda: started_at,
                ),
                watermark_store=self.schedule_watermark_store,
                clock=lambda: started_at,
                run_id_factory=lambda: schedule_run_id,
            ),
            run_store=self.daily_run_store,
            clock=lambda: started_at,
            run_id_factory=lambda: daily_run_id,
        )

    def test_consecutive_runs_acquire_new_games_then_correct_a_revision(self) -> None:
        first_changes_api = FakeGameChangesApi(
            {0: retrieved_changes(game_changes_raw([], total_items=0), 0)}
        )
        first_game_api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [retrieved_game(raw_game(823514), "fixture://game/823514")],
                824726: [retrieved_game(raw_game(824726), "fixture://game/824726")],
            },
        )

        first = self.coordinator(
            first_changes_api,
            first_game_api,
            FIRST_STARTED_AT,
            FIRST_DAILY_RUN_ID,
            FIRST_POLL_RUN_ID,
            FIRST_SCHEDULE_RUN_ID,
        ).run(
            initial_schedule_date=date(2026, 8, 8),
            initial_correction_watermark=INITIAL_CORRECTION_WATERMARK,
            through_date=date(2026, 8, 8),
        )

        self.assertTrue(first.successful)
        self.assertEqual(first_game_api.game_calls, [823514, 824726])
        self.assertEqual(len(list(self.data_dir.rglob("game.json"))), 2)

        second_changes_api = FakeGameChangesApi(
            {0: retrieved_changes(game_changes_raw([823514], total_items=1), 0)}
        )
        second_game_api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [
                    retrieved_game(
                        corrected_raw_game(823514),
                        "fixture://game/823514/corrected",
                    )
                ]
            },
        )

        second = self.coordinator(
            second_changes_api,
            second_game_api,
            SECOND_STARTED_AT,
            SECOND_DAILY_RUN_ID,
            SECOND_POLL_RUN_ID,
            SECOND_SCHEDULE_RUN_ID,
        ).run(through_date=date(2026, 8, 9))

        self.assertTrue(second.successful)
        self.assertEqual(
            second.branch_statuses,
            {
                "correction_discovery": "complete",
                "correction_processing": "complete",
                "deferred_game_processing": "complete",
                "schedule_discovery": "complete",
            },
        )
        self.assertEqual(second_game_api.game_calls, [823514])
        self.assertEqual(
            len(list(self.data_dir.rglob("game_pk=823514/revision=*/game.json"))),
            2,
        )
        schedule_watermark = self.schedule_watermark_store.read()
        changes_watermark = self.changes_watermark_store.read()
        assert schedule_watermark is not None
        assert changes_watermark is not None
        self.assertEqual(schedule_watermark.through_date, date(2026, 8, 9))
        self.assertEqual(changes_watermark.updated_since, SECOND_STARTED_AT)
        daily_manifest = json.loads(Path(second.manifest_path.uri).read_text())
        self.assertEqual(daily_manifest["status"], "complete")

    def test_branch_failure_does_not_prevent_other_discovery(self) -> None:
        changes_api = FakeGameChangesApi({})
        game_api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [retrieved_game(raw_game(823514), "fixture://game/823514")],
                824726: [retrieved_game(raw_game(824726), "fixture://game/824726")],
            },
        )

        result = self.coordinator(
            changes_api,
            game_api,
            FIRST_STARTED_AT,
            FIRST_DAILY_RUN_ID,
            FIRST_POLL_RUN_ID,
            FIRST_SCHEDULE_RUN_ID,
        ).run(
            initial_schedule_date=date(2026, 8, 8),
            through_date=date(2026, 8, 8),
        )

        self.assertFalse(result.successful)
        self.assertEqual(result.branch_statuses["correction_discovery"], "failed")
        self.assertEqual(result.branch_statuses["correction_processing"], "complete")
        self.assertEqual(result.branch_statuses["deferred_game_processing"], "complete")
        self.assertEqual(result.branch_statuses["schedule_discovery"], "complete")
        self.assertEqual(game_api.game_calls, [823514, 824726])
