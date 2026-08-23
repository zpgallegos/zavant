from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from uuid import UUID

from zavant.ingestion.mlb_stats_api.contracts.game_changes import (
    GameChangesContractError,
    GameChangesRequest,
    GameChangesResponse,
)
from zavant.ingestion.mlb_stats_api.storage.path_game_changes import (
    GameChangesConflictError,
    PathGameChangesStore,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_CHANGES = REPOSITORY_ROOT / "tests" / "fixtures" / "example-game-changes.json"
UPDATED_SINCE = datetime(2026, 8, 8, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 8, 9, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 9, 0, 1, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000001")


class GameChangesContractTests(unittest.TestCase):
    def test_extracts_deduplicated_changed_games(self) -> None:
        changes = GameChangesResponse.from_bytes(SAMPLE_CHANGES.read_bytes())

        self.assertEqual(changes.total_items, 2)
        self.assertEqual(changes.total_games, 2)
        self.assertEqual(changes.game_pks, (822863, 823426))
        self.assertEqual(changes.changed_games[1].season, 2026)
        self.assertEqual(
            changes.changed_games[1].live_feed_link,
            "/api/v1.1/game/823426/feed/live",
        )

    def test_rejects_game_without_live_feed_link(self) -> None:
        payload = json.loads(SAMPLE_CHANGES.read_bytes())
        del payload["dates"][0]["games"][0]["link"]

        with self.assertRaisesRegex(GameChangesContractError, "link must be"):
            GameChangesResponse.from_bytes(json.dumps(payload).encode())

    def test_request_requires_timezone_aware_boundaries(self) -> None:
        with self.assertRaisesRegex(ValueError, "UTC offset"):
            GameChangesRequest(
                updated_since=datetime(2026, 8, 8),
                window_end=WINDOW_END,
                page_number=0,
                limit=1000,
                offset=0,
                source_uri="fixture://game-changes",
            )


class PathGameChangesStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.raw = SAMPLE_CHANGES.read_bytes()
        self.changes = GameChangesResponse.from_bytes(self.raw)
        self.store = PathGameChangesStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def request(self, page_number: int = 0, offset: int = 0) -> GameChangesRequest:
        return GameChangesRequest(
            updated_since=UPDATED_SINCE,
            window_end=WINDOW_END,
            page_number=page_number,
            limit=1000,
            offset=offset,
            source_uri="fixture://game-changes",
        )

    def test_lands_page_metadata_and_deduplicated_manifest(self) -> None:
        result = self.store.land_page(
            changes=self.changes,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )

        self.assertTrue(result.created)
        self.assertEqual(Path(result.response_path.uri).read_bytes(), self.raw)
        self.assertIn(
            "game_changes/poll_date=2026-08-09/"
            "run_id=00000000-0000-0000-0000-000000000001/page=0000",
            result.response_path.key,
        )

        metadata = json.loads(Path(result.metadata_path.uri).read_text())
        self.assertEqual(
            metadata["contract"],
            "mlb-stats-api-game-changes-page/v1",
        )
        self.assertEqual(
            metadata["request"]["updated_since"],
            "2026-08-08T00:00:00+00:00",
        )

        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(
            manifest["contract"],
            "mlb-stats-api-game-changes-manifest/v1",
        )
        self.assertEqual(len(manifest["pages"]), 1)
        self.assertEqual(
            [game["game_pk"] for game in manifest["changed_games"]],
            [822863, 823426],
        )
        self.assertTrue(
            all(
                game["processing_status"] == "pending"
                for game in manifest["changed_games"]
            )
        )

    def test_same_page_is_idempotent(self) -> None:
        self.store.land_page(
            changes=self.changes,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )

        result = self.store.land_page(
            changes=self.changes,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )

        self.assertFalse(result.created)
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(len(manifest["pages"]), 1)
        self.assertEqual(len(manifest["changed_games"]), 2)

    def test_multiple_pages_merge_and_deduplicate_games(self) -> None:
        first_result = self.store.land_page(
            changes=self.changes,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )
        self.store.land_page(
            changes=self.changes,
            request=self.request(page_number=1, offset=1000),
            raw=self.raw,
            run_id=RUN_ID,
        )

        manifest = json.loads(Path(first_result.manifest_path.uri).read_text())
        self.assertEqual(
            [page["page_number"] for page in manifest["pages"]],
            [0, 1],
        )
        self.assertEqual(len(manifest["changed_games"]), 2)

    def test_refuses_different_content_for_the_same_page(self) -> None:
        self.store.land_page(
            changes=self.changes,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )
        changed_payload = json.loads(self.raw)
        changed_payload["copyright"] = "different"
        changed_raw = json.dumps(changed_payload).encode()
        changed_response = GameChangesResponse.from_bytes(changed_raw)

        with self.assertRaisesRegex(GameChangesConflictError, "different content"):
            self.store.land_page(
                changes=changed_response,
                request=self.request(),
                raw=changed_raw,
                run_id=RUN_ID,
            )

    def test_refuses_conflicting_request_for_the_same_page(self) -> None:
        self.store.land_page(
            changes=self.changes,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )
        conflicting_request = GameChangesRequest(
            updated_since=UPDATED_SINCE,
            window_end=WINDOW_END,
            page_number=0,
            limit=1000,
            offset=1000,
            source_uri="fixture://game-changes",
        )

        with self.assertRaisesRegex(GameChangesConflictError, "metadata conflicts"):
            self.store.land_page(
                changes=self.changes,
                request=conflicting_request,
                raw=self.raw,
                run_id=RUN_ID,
            )

    def test_finalize_requires_every_expected_page(self) -> None:
        result = self.store.land_page(
            changes=self.changes,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )

        with self.assertRaisesRegex(GameChangesConflictError, "every expected page"):
            self.store.finalize_manifest(
                manifest_path=result.manifest_path,
                expected_page_count=2,
                expected_total_items=2,
                watermark_before=UPDATED_SINCE,
            )

        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(manifest["status"], "open")
