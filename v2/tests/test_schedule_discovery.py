from datetime import date, datetime, timezone
from pathlib import Path
import tempfile
import unittest
from uuid import UUID

from zavant.acquisition.bounded_games import BoundedGameAcquirer
from zavant.acquisition.schedule_discovery import (
    ScheduleDiscoverer,
    ScheduleWatermarkNotInitializedError,
)
from zavant.clients.mlb_stats_api import MlbStatsApiResponseError
from zavant.storage.local_raw import LocalRawGameStore
from zavant.storage.local_schedule import LocalScheduleStore
from zavant.storage.local_schedule_watermark import LocalScheduleWatermarkStore
from tests.test_bounded_game_acquisition import (
    FakeMlbGameAcquisitionApi,
    raw_game,
    retrieved,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SCHEDULE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-schedule.json"
INITIAL_DATE = date(2026, 8, 8)
NEXT_DATE = date(2026, 8, 9)
FIRST_STARTED_AT = datetime(2026, 8, 8, 23, tzinfo=timezone.utc)
NEXT_STARTED_AT = datetime(2026, 8, 9, 23, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 9, 23, 1, tzinfo=timezone.utc)
FIRST_RUN_ID = UUID("00000000-0000-0000-0000-000000000030")
NEXT_RUN_ID = UUID("00000000-0000-0000-0000-000000000031")


class ScheduleDiscovererTests(unittest.TestCase):
    """Tests for rolling schedule discovery and its success watermark."""

    def setUp(self) -> None:
        """Create isolated schedule, raw-game, and watermark stores."""

        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.schedule_store = LocalScheduleStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.game_store = LocalRawGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.watermark_store = LocalScheduleWatermarkStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def discoverer(
        self,
        api: FakeMlbGameAcquisitionApi,
        started_at: datetime,
        run_id: UUID,
    ) -> ScheduleDiscoverer:
        """Build a deterministic schedule discovery service.

        Args:
            api: Fake schedule and live-game source.
            started_at: Schedule request timestamp.
            run_id: Schedule acquisition run identifier.

        Returns:
            Configured incremental schedule discoverer.
        """

        acquirer = BoundedGameAcquirer(
            api=api,
            schedule_store=self.schedule_store,
            game_store=self.game_store,
            clock=lambda: started_at,
        )
        return ScheduleDiscoverer(
            acquirer=acquirer,
            watermark_store=self.watermark_store,
            clock=lambda: started_at,
            run_id_factory=lambda: run_id,
        )

    @staticmethod
    def successful_api() -> FakeMlbGameAcquisitionApi:
        """Build a source that can acquire both schedule fixture games.

        Returns:
            Configured successful schedule/game source.
        """

        return FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [retrieved(raw_game(823514), "fixture://game/823514")],
                824726: [retrieved(raw_game(824726), "fixture://game/824726")],
            },
        )

    def test_bootstrap_and_rolling_lookback_skip_existing_downloads(self) -> None:
        """Advance dates while reconsidering recent schedule evidence cheaply."""

        first_api = self.successful_api()
        first = self.discoverer(first_api, FIRST_STARTED_AT, FIRST_RUN_ID).discover(
            initial_start_date=INITIAL_DATE,
            through_date=INITIAL_DATE,
        )
        next_api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={},
        )

        second = self.discoverer(next_api, NEXT_STARTED_AT, NEXT_RUN_ID).discover(
            through_date=NEXT_DATE,
            lookback_days=7,
        )

        self.assertTrue(first.successful)
        self.assertTrue(second.successful)
        self.assertEqual(second.start_date, date(2026, 8, 2))
        self.assertEqual(next_api.schedule_calls, [(date(2026, 8, 2), NEXT_DATE, 1)])
        self.assertEqual(next_api.game_calls, [])
        assert second.acquisition is not None
        self.assertEqual(second.acquisition.summary["succeeded"], 2)
        watermark = self.watermark_store.read()
        assert watermark is not None
        self.assertEqual(watermark.through_date, NEXT_DATE)

    def test_same_through_date_is_a_noop(self) -> None:
        """Avoid another schedule request when discovery is already current."""

        first_api = self.successful_api()
        self.discoverer(first_api, FIRST_STARTED_AT, FIRST_RUN_ID).discover(
            initial_start_date=INITIAL_DATE,
            through_date=INITIAL_DATE,
        )
        next_api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={},
        )

        result = self.discoverer(next_api, NEXT_STARTED_AT, NEXT_RUN_ID).discover(
            through_date=INITIAL_DATE
        )

        self.assertEqual(result.status, "skipped")
        self.assertEqual(next_api.schedule_calls, [])

    def test_failed_acquisition_does_not_advance_watermark(self) -> None:
        """Retain the old boundary when a newly eligible game fails."""

        api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [
                    MlbStatsApiResponseError(
                        status_code=503,
                        url="fixture://game/823514",
                        attempts=3,
                    )
                ],
                824726: [retrieved(raw_game(824726), "fixture://game/824726")],
            },
        )

        result = self.discoverer(api, FIRST_STARTED_AT, FIRST_RUN_ID).discover(
            initial_start_date=INITIAL_DATE,
            through_date=INITIAL_DATE,
        )

        self.assertEqual(result.status, "failed")
        self.assertIsNone(self.watermark_store.read())

    def test_requires_bootstrap_date(self) -> None:
        """Refuse to invent the first schedule coverage boundary."""

        api = FakeMlbGameAcquisitionApi(SAMPLE_SCHEDULE.read_bytes(), {})

        with self.assertRaises(ScheduleWatermarkNotInitializedError):
            self.discoverer(api, FIRST_STARTED_AT, FIRST_RUN_ID).discover(
                through_date=INITIAL_DATE
            )

        self.assertEqual(api.schedule_calls, [])
