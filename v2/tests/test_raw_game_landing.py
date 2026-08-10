from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from zavant.contracts.raw_game import RawGameContractError, RawGameResponse
from zavant.storage.local_raw import LocalRawGameStore, RawGameConflictError


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_GAME = REPOSITORY_ROOT / "tests" / "fixtures" / "example-game-raw.json"
OBSERVED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)


class RawGameContractTests(unittest.TestCase):
    """Tests for validation at the MLB raw-game boundary."""

    def test_extracts_routing_fields_from_real_fixture(self) -> None:
        """Extract routing fields from the representative source fixture."""

        game = RawGameResponse.from_bytes(SAMPLE_GAME.read_bytes())

        self.assertEqual(game.game_pk, 744863)
        self.assertEqual(game.season, 2024)
        self.assertEqual(game.feed_timecode, "20240424_015511")

    def test_rejects_payload_without_live_data(self) -> None:
        """Reject a payload missing a required top-level source object."""

        raw = json.dumps(
            {
                "gamePk": 1,
                "gameData": {"datetime": {"officialDate": "2024-04-23"}},
            }
        ).encode()

        with self.assertRaisesRegex(RawGameContractError, "gameData and liveData"):
            RawGameResponse.from_bytes(raw)


class LocalRawGameStoreTests(unittest.TestCase):
    """Tests for revision-aware local raw-game persistence."""

    def setUp(self) -> None:
        """Create an isolated local store for each test."""

        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.raw = SAMPLE_GAME.read_bytes()
        self.game = RawGameResponse.from_bytes(self.raw)
        self.store = LocalRawGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def test_lands_source_revision_metadata_and_current_pointer(self) -> None:
        """Persist exact source bytes with revision and provenance metadata."""

        result = self.store.land(
            self.game,
            self.raw,
            "fixture://sample-game",
            trigger="initial",
        )

        self.assertTrue(result.created)
        self.assertEqual(result.object_path.read_bytes(), self.raw)
        self.assertIn(
            f"season=2024/game_pk=744863/revision={result.revision_id}",
            result.object_path.as_posix(),
        )

        metadata = json.loads(result.metadata_path.read_text())
        self.assertEqual(metadata["contract"], "mlb-stats-api-raw-game/v2")
        self.assertEqual(metadata["canonical_sha256"], result.canonical_sha256)
        self.assertEqual(metadata["raw_sha256"], result.raw_sha256)
        self.assertEqual(metadata["source_uri"], "fixture://sample-game")
        self.assertEqual(metadata["trigger"], "initial")
        self.assertIsNone(metadata["previous_revision_id"])

        current = json.loads(result.current_pointer_path.read_text())
        self.assertEqual(current["revision_id"], result.revision_id)

    def test_same_payload_is_idempotent(self) -> None:
        """Report an existing identical revision without rewriting it."""

        first_result = self.store.land(
            self.game,
            self.raw,
            "fixture://sample-game",
        )

        second_result = self.store.land(
            self.game,
            self.raw,
            "fixture://sample-game",
        )

        self.assertFalse(second_result.created)
        self.assertEqual(second_result.revision_id, first_result.revision_id)
        self.assertIsNone(second_result.previous_revision_id)

    def test_equivalent_json_is_the_same_revision(self) -> None:
        """Ignore insignificant JSON formatting and key-order differences."""

        first_result = self.store.land(
            self.game,
            self.raw,
            "fixture://sample-game",
        )
        reformatted = json.dumps(self.game.payload, sort_keys=True).encode()
        reformatted_game = RawGameResponse.from_bytes(reformatted)

        second_result = self.store.land(
            reformatted_game,
            reformatted,
            "fixture://reformatted-game",
        )

        self.assertFalse(second_result.created)
        self.assertEqual(second_result.revision_id, first_result.revision_id)
        self.assertEqual(second_result.raw_sha256, first_result.raw_sha256)

    def test_changed_game_creates_a_new_revision(self) -> None:
        """Preserve both revisions when meaningful game content changes."""

        first_result = self.store.land(
            self.game,
            self.raw,
            "fixture://sample-game",
            trigger="initial",
        )
        changed_payload = json.loads(self.raw)
        changed_payload["gameData"]["status"]["detailedState"] = "Final: Corrected"
        changed_raw = json.dumps(changed_payload).encode()
        changed_game = RawGameResponse.from_bytes(changed_raw)

        second_result = self.store.land(
            changed_game,
            changed_raw,
            "fixture://corrected-game",
            trigger="game_changes",
        )

        self.assertTrue(second_result.created)
        self.assertNotEqual(second_result.revision_id, first_result.revision_id)
        self.assertNotEqual(second_result.object_path, first_result.object_path)
        self.assertEqual(
            second_result.previous_revision_id,
            first_result.revision_id,
        )
        self.assertTrue(first_result.object_path.exists())
        current = json.loads(second_result.current_pointer_path.read_text())
        self.assertEqual(current["revision_id"], second_result.revision_id)

        old_result = self.store.land(
            self.game,
            self.raw,
            "fixture://sample-game",
        )
        current_after_old_revision = json.loads(
            old_result.current_pointer_path.read_text()
        )
        self.assertFalse(old_result.created)
        self.assertEqual(
            current_after_old_revision["revision_id"],
            second_result.revision_id,
        )

    def test_detects_corrupted_revision_content(self) -> None:
        """Fail when stored content no longer matches its revision identifier."""

        result = self.store.land(self.game, self.raw, "fixture://sample-game")
        result.object_path.write_bytes(b"different source bytes")

        with self.assertRaisesRegex(RawGameConflictError, "invalid JSON"):
            self.store.land(self.game, self.raw, "fixture://sample-game")
