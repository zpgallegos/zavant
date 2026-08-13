from datetime import datetime, timezone
from pathlib import Path
import unittest
from uuid import UUID

from zavant.contracts.raw_game import RawGameResponse
from zavant.contracts.schedule import ScheduleResponse
from zavant.storage.s3_objects import (
    S3ObjectBackend,
    S3ObjectWriteConflictError,
)
from zavant.storage.bundles import s3_acquisition_storage
from tests.fake_s3 import FakeS3Client


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RAW_GAME_FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-game-raw.json"
SCHEDULE_FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-schedule.json"
OBSERVED_AT = datetime(2026, 8, 9, tzinfo=timezone.utc)


class S3ObjectBackendTests(unittest.TestCase):
    def test_conditional_update_rejects_a_stale_writer(self) -> None:
        client = FakeS3Client()
        first = S3ObjectBackend(client, "example-bucket", "lake")
        second = S3ObjectBackend(client, "example-bucket", "lake")
        first.write("state/watermark.json", b"first")
        self.assertEqual(second.read("state/watermark.json"), b"first")
        self.assertEqual(first.read("state/watermark.json"), b"first")

        first.write("state/watermark.json", b"advanced")

        with self.assertRaises(S3ObjectWriteConflictError):
            second.write("state/watermark.json", b"stale")
        self.assertEqual(first.read("state/watermark.json"), b"advanced")

    def test_concurrent_identical_creation_is_idempotent(self) -> None:
        client = FakeS3Client()
        first = S3ObjectBackend(client, "example-bucket", "lake")
        second = S3ObjectBackend(client, "example-bucket", "lake")
        self.assertFalse(first.exists("raw/response.json"))
        self.assertFalse(second.exists("raw/response.json"))

        first.write("raw/response.json", b"same")
        second.write("raw/response.json", b"same")

        self.assertEqual(second.read("raw/response.json"), b"same")

    def test_list_follows_pagination_and_matches_logical_keys(self) -> None:
        client = FakeS3Client(page_size=1)
        backend = S3ObjectBackend(client, "example-bucket", "lake")
        backend.write("runs/a/manifest.json", b"a")
        backend.write("runs/b/manifest.json", b"b")
        backend.write("runs/c/other.json", b"c")

        self.assertEqual(
            backend.list("runs/*/manifest.json"),
            ("runs/a/manifest.json", "runs/b/manifest.json"),
        )


class S3AcquisitionStorageTests(unittest.TestCase):
    def test_deferred_game_state_reuses_s3_state_machine(self) -> None:
        client = FakeS3Client()
        first = s3_acquisition_storage(
            client=client,
            bucket="example-bucket",
            prefix="portfolio/lake",
            clock=lambda: OBSERVED_AT,
        )
        game = ScheduleResponse.from_bytes(
            SCHEDULE_FIXTURE.read_bytes()
        ).scheduled_games[0]

        first.deferred_games.defer(game)
        second = s3_acquisition_storage(
            client=client,
            bucket="example-bucket",
            prefix="portfolio/lake",
            clock=lambda: OBSERVED_AT,
        )

        self.assertEqual(second.deferred_games.pending()[0].game_pk, game.game_pk)
        second.deferred_games.resolve(game.game_pk)
        self.assertEqual(first.deferred_games.pending(), ())

    def test_raw_game_landing_reuses_revision_state_machine(self) -> None:
        client = FakeS3Client()
        storage = s3_acquisition_storage(
            client=client,
            bucket="example-bucket",
            prefix="portfolio/lake",
            clock=lambda: OBSERVED_AT,
        )
        raw = RAW_GAME_FIXTURE.read_bytes()
        game = RawGameResponse.from_bytes(raw)

        first = storage.raw_games.land(
            game=game,
            raw=raw,
            source_uri="https://statsapi.example.test/game",
            trigger="initial",
        )
        replay = storage.raw_games.land(
            game=game,
            raw=raw,
            source_uri="https://statsapi.example.test/game",
            trigger="initial",
        )

        self.assertTrue(first.created)
        self.assertFalse(replay.created)
        self.assertTrue(
            first.object_path.uri.startswith("s3://example-bucket/portfolio/lake/")
        )
        self.assertEqual(
            client.content(
                "example-bucket", f"portfolio/lake/{first.object_path.key}"
            ),
            raw,
        )
        self.assertEqual(
            storage.raw_games.current_revision_id(game.season, game.game_pk),
            first.revision_id,
        )
        revisions = storage.raw_games.current_revisions(game.season)
        self.assertEqual(len(revisions), 1)
        self.assertEqual(revisions[0].game_pk, game.game_pk)

    def test_backfill_run_and_checkpoint_reuse_s3_state_machine(self) -> None:
        storage = s3_acquisition_storage(
            client=FakeS3Client(),
            bucket="example-bucket",
            prefix="portfolio/lake",
            clock=lambda: OBSERVED_AT,
        )
        run_id = UUID("00000000-0000-0000-0000-000000000032")
        started = storage.season_backfills.start(
            run_id=run_id,
            started_at=OBSERVED_AT,
            seasons=(2024,),
            mode="reconcile",
            dry_run=False,
            configuration={"sport_id": 1},
        )
        storage.season_backfills.record_season(
            started.manifest_path, 2024, "complete", {"selected": 0}
        )
        statuses = storage.season_backfills.finalize(started.manifest_path)
        storage.season_backfills.advance_checkpoint(
            season=2024,
            expected_current=None,
            updated_since=OBSERVED_AT,
            run_id=run_id,
            manifest_path=started.manifest_path,
        )

        self.assertEqual(statuses, {2024: "complete"})
        checkpoint = storage.season_backfills.read_checkpoint(2024)
        self.assertIsNotNone(checkpoint)
        assert checkpoint is not None
        self.assertEqual(checkpoint.updated_since, OBSERVED_AT)


if __name__ == "__main__":
    unittest.main()
