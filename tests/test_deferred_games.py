import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from zavant.acquisition.deferred_games import DeferredGameProcessor
from zavant.contracts.schedule import ScheduleResponse
from zavant.storage.path_deferred_games import PathDeferredGameStore
from zavant.storage.path_raw import PathRawGameStore
from tests.test_bounded_game_acquisition import (
    FakeMlbGameAcquisitionApi,
    raw_game,
    retrieved,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SCHEDULE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-schedule.json"
OBSERVED_AT = datetime(2026, 8, 10, tzinfo=timezone.utc)


def game_with_status(game_pk: int, status_code: str) -> bytes:
    payload = json.loads(raw_game(game_pk))
    payload["gameData"]["status"]["codedGameState"] = status_code
    return json.dumps(payload).encode()


class DeferredGameProcessingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.store = PathDeferredGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.raw_store = PathRawGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        scheduled = ScheduleResponse.from_bytes(
            SAMPLE_SCHEDULE.read_bytes()
        ).scheduled_games[0]
        self.game = replace(
            scheduled,
            status_code="I",
            detailed_state="In Progress",
        )

    def test_state_survives_store_recomposition(self) -> None:
        self.store.defer(self.game)

        pending = PathDeferredGameStore(self.data_dir).pending()

        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].game_pk, self.game.game_pk)
        self.assertEqual(pending[0].official_date, self.game.official_date)

    def test_non_final_game_remains_until_a_later_final_response(self) -> None:
        self.store.defer(self.game)
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=b"{}",
            game_outcomes={
                self.game.game_pk: [
                    retrieved(
                        game_with_status(self.game.game_pk, "I"),
                        "fixture://deferred/in-progress",
                    ),
                    retrieved(
                        game_with_status(self.game.game_pk, "F"),
                        "fixture://deferred/final",
                    ),
                ]
            },
        )
        processor = DeferredGameProcessor(api, self.store, self.raw_store)

        first = processor.process_all()
        second = processor.process_all()

        self.assertTrue(first.successful)
        self.assertEqual(first.summary["deferred"], 1)
        self.assertTrue(second.successful)
        self.assertEqual(second.summary["succeeded"], 1)
        self.assertEqual(self.store.pending(), ())
        self.assertIsNotNone(
            self.raw_store.current_revision_id(self.game.season, self.game.game_pk)
        )

    def test_cancelled_game_is_terminally_removed(self) -> None:
        self.store.defer(self.game)
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=b"{}",
            game_outcomes={
                self.game.game_pk: [
                    retrieved(
                        game_with_status(self.game.game_pk, "C"),
                        "fixture://deferred/cancelled",
                    )
                ]
            },
        )

        result = DeferredGameProcessor(api, self.store, self.raw_store).process_all()

        self.assertTrue(result.successful)
        self.assertEqual(result.summary["skipped"], 1)
        self.assertEqual(self.store.pending(), ())


if __name__ == "__main__":
    unittest.main()
