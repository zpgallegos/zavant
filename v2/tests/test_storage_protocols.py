"""Shared storage-boundary tests for path-backed stores."""

from pathlib import Path
import tempfile
import unittest

from zavant.storage.artifacts import ArtifactReference
from zavant.storage.path_daily_runs import PathDailyRunStore
from zavant.storage.path_game_changes import PathGameChangesStore
from zavant.storage.path_game_changes_watermark import (
    PathGameChangesWatermarkStore,
)
from zavant.storage.path_raw import PathRawGameStore
from zavant.storage.path_schedule import PathScheduleStore
from zavant.storage.path_schedule_watermark import PathScheduleWatermarkStore
from zavant.storage.path_season_backfills import PathSeasonBackfillStore
from zavant.storage.protocols import (
    DailyRunStore,
    GameChangesStore,
    GameChangesWatermarkStore,
    RawGameStore,
    ScheduleStore,
    ScheduleWatermarkStore,
    SeasonBackfillStore,
)


class ArtifactReferenceTests(unittest.TestCase):
    def test_preserves_portable_key_and_backend_uri(self) -> None:
        reference = ArtifactReference(
            key="raw/mlb_stats_api/games/season=2026/game_pk=1/game.json",
            uri="s3://example/lake/raw/mlb_stats_api/games/season=2026/"
            "game_pk=1/game.json",
        )

        self.assertEqual(
            reference.key,
            "raw/mlb_stats_api/games/season=2026/game_pk=1/game.json",
        )
        self.assertEqual(str(reference), reference.uri)

    def test_rejects_unsafe_or_backend_specific_keys(self) -> None:
        invalid_keys = ("", "/raw/game.json", "../raw/game.json", "raw\\game.json")
        for key in invalid_keys:
            with self.subTest(key=key), self.assertRaises(ValueError):
                ArtifactReference(key=key, uri="fixture://artifact")


class PathStorageProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)

    def test_path_stores_implement_storage_protocols(self) -> None:
        raw_store: RawGameStore = PathRawGameStore(self.data_dir)
        schedule_store: ScheduleStore = PathScheduleStore(self.data_dir)
        changes_store: GameChangesStore = PathGameChangesStore(self.data_dir)
        schedule_watermark_store: ScheduleWatermarkStore = PathScheduleWatermarkStore(
            self.data_dir
        )
        changes_watermark_store: GameChangesWatermarkStore = (
            PathGameChangesWatermarkStore(self.data_dir)
        )
        daily_run_store: DailyRunStore = PathDailyRunStore(self.data_dir)
        backfill_store: SeasonBackfillStore = PathSeasonBackfillStore(self.data_dir)

        self.assertIsInstance(raw_store, RawGameStore)
        self.assertIsInstance(schedule_store, ScheduleStore)
        self.assertIsInstance(changes_store, GameChangesStore)
        self.assertIsInstance(schedule_watermark_store, ScheduleWatermarkStore)
        self.assertIsInstance(changes_watermark_store, GameChangesWatermarkStore)
        self.assertIsInstance(daily_run_store, DailyRunStore)
        self.assertIsInstance(backfill_store, SeasonBackfillStore)


if __name__ == "__main__":
    unittest.main()
