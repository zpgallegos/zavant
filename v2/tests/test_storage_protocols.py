"""Shared storage-boundary tests for local and future cloud adapters."""

from pathlib import Path
import tempfile
import unittest

from zavant.storage.artifacts import ArtifactReference
from zavant.storage.local_daily_runs import LocalDailyRunStore
from zavant.storage.local_game_changes import LocalGameChangesStore
from zavant.storage.local_game_changes_watermark import (
    LocalGameChangesWatermarkStore,
)
from zavant.storage.local_raw import LocalRawGameStore
from zavant.storage.local_schedule import LocalScheduleStore
from zavant.storage.local_schedule_watermark import LocalScheduleWatermarkStore
from zavant.storage.protocols import (
    DailyRunStore,
    GameChangesStore,
    GameChangesWatermarkStore,
    RawGameStore,
    ScheduleStore,
    ScheduleWatermarkStore,
)


class ArtifactReferenceTests(unittest.TestCase):
    """Tests for portable artifact identity and operator locations."""

    def test_preserves_portable_key_and_backend_uri(self) -> None:
        """Keep logical identity separate from the backend location."""

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
        """Reject absolute, traversing, and platform-specific logical keys."""

        invalid_keys = ("", "/raw/game.json", "../raw/game.json", "raw\\game.json")
        for key in invalid_keys:
            with self.subTest(key=key), self.assertRaises(ValueError):
                ArtifactReference(key=key, uri="fixture://artifact")


class LocalStorageProtocolTests(unittest.TestCase):
    """Verify local adapters conform to the shared storage interfaces."""

    def setUp(self) -> None:
        """Create isolated local adapters for structural contract checks."""

        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)

    def test_local_adapters_implement_storage_protocols(self) -> None:
        """Keep acquisition dependencies free of concrete local store types."""

        raw_store: RawGameStore = LocalRawGameStore(self.data_dir)
        schedule_store: ScheduleStore = LocalScheduleStore(self.data_dir)
        changes_store: GameChangesStore = LocalGameChangesStore(self.data_dir)
        schedule_watermark_store: ScheduleWatermarkStore = LocalScheduleWatermarkStore(
            self.data_dir
        )
        changes_watermark_store: GameChangesWatermarkStore = (
            LocalGameChangesWatermarkStore(self.data_dir)
        )
        daily_run_store: DailyRunStore = LocalDailyRunStore(self.data_dir)

        self.assertIsInstance(raw_store, RawGameStore)
        self.assertIsInstance(schedule_store, ScheduleStore)
        self.assertIsInstance(changes_store, GameChangesStore)
        self.assertIsInstance(schedule_watermark_store, ScheduleWatermarkStore)
        self.assertIsInstance(changes_watermark_store, GameChangesWatermarkStore)
        self.assertIsInstance(daily_run_store, DailyRunStore)


if __name__ == "__main__":
    unittest.main()
