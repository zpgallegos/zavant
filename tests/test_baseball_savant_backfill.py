from datetime import date, datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Set
import unittest
from uuid import UUID

from zavant.ingestion.baseball_savant.backfill import (
    BaseballSavantBackfillCoordinator,
    BaseballSavantBackfillMode,
)
from zavant.ingestion.baseball_savant.application import s3_backfill_storage
from zavant.ingestion.http import RetrievedResource
from zavant.ingestion.baseball_savant.storage import PathBaseballSavantStore
from zavant.ingestion.baseball_savant.backfill_storage import (
    PathBaseballSavantBackfillStore,
)
from tests.fake_s3 import FakeS3Client


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-baseball-savant.csv"
STARTED_AT = datetime(2026, 8, 10, 13, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000051")


def csv_for(game_date: date) -> bytes:
    return FIXTURE.read_text().replace("2026-08-08", game_date.isoformat()).encode()


class FakeSavantApi:
    def __init__(self, invalid_dates: Set[date] | None = None) -> None:
        self.invalid_dates = invalid_dates or set()
        self.calls: List[date] = []

    def get_statcast_date(self, game_date: date) -> RetrievedResource:
        self.calls.append(game_date)
        response_date = (
            date(2026, 8, 1) if game_date in self.invalid_dates else game_date
        )
        url = f"https://baseballsavant.example.test/csv?date={game_date}"
        return RetrievedResource(csv_for(response_date), url, url, 200, {}, 1)


class BaseballSavantBackfillCoordinatorTests(unittest.TestCase):
    def stores(
        self, root: Path
    ) -> tuple[PathBaseballSavantStore, PathBaseballSavantBackfillStore]:
        return (
            PathBaseballSavantStore(root, clock=lambda: STARTED_AT),
            PathBaseballSavantBackfillStore(root, clock=lambda: STARTED_AT),
        )

    def coordinator(
        self,
        api: FakeSavantApi,
        root: Path,
        delays: List[float] | None = None,
    ) -> BaseballSavantBackfillCoordinator:
        raw_store, backfill_store = self.stores(root)
        return BaseballSavantBackfillCoordinator(
            api=api,
            raw_store=raw_store,
            backfill_store=backfill_store,
            clock=lambda: STARTED_AT,
            run_id_factory=lambda: RUN_ID,
            sleeper=(delays if delays is not None else []).append,
        )

    def test_backfills_inclusive_dates_and_leaves_daily_watermark_untouched(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            api = FakeSavantApi()
            delays: List[float] = []

            result = self.coordinator(api, root, delays).run(
                start_date=date(2026, 8, 7),
                end_date=date(2026, 8, 9),
                request_delay_seconds=0.25,
            )

            self.assertTrue(result.successful)
            self.assertEqual(
                api.calls,
                [date(2026, 8, 7), date(2026, 8, 8), date(2026, 8, 9)],
            )
            self.assertEqual(delays, [0.25, 0.25])
            self.assertEqual(result.succeeded, 3)
            self.assertFalse(
                (
                    root
                    / "state"
                    / "baseball_savant"
                    / "statcast_search"
                    / "watermark.json"
                ).exists()
            )
            manifest = json.loads(Path(result.manifest_path.uri).read_text())
            self.assertEqual(manifest["contract"], "baseball-savant-backfill-run/v1")
            self.assertIn("/baseball_savant/backfill/", result.manifest_path.uri)

    def test_missing_skips_an_existing_date_while_verify_reacquires_it(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            first_api = FakeSavantApi()
            self.coordinator(first_api, root).run(
                start_date=date(2026, 8, 8),
                end_date=date(2026, 8, 8),
                request_delay_seconds=0,
            )
            missing_api = FakeSavantApi()
            raw_store, backfill_store = self.stores(root)
            missing = BaseballSavantBackfillCoordinator(
                missing_api,
                raw_store,
                backfill_store,
                clock=lambda: STARTED_AT,
                run_id_factory=lambda: UUID(
                    "00000000-0000-0000-0000-000000000052"
                ),
                sleeper=lambda _: None,
            ).run(
                start_date=date(2026, 8, 8),
                end_date=date(2026, 8, 8),
                request_delay_seconds=0,
            )
            verify_api = FakeSavantApi()
            verify = BaseballSavantBackfillCoordinator(
                verify_api,
                raw_store,
                backfill_store,
                clock=lambda: STARTED_AT,
                run_id_factory=lambda: UUID(
                    "00000000-0000-0000-0000-000000000053"
                ),
                sleeper=lambda _: None,
            ).run(
                start_date=date(2026, 8, 8),
                end_date=date(2026, 8, 8),
                mode=BaseballSavantBackfillMode.VERIFY,
                request_delay_seconds=0,
            )

            self.assertEqual(missing_api.calls, [])
            self.assertEqual(missing.skipped, 1)
            self.assertEqual(verify_api.calls, [date(2026, 8, 8)])
            self.assertEqual(verify.succeeded, 1)

    def test_resume_retries_only_failed_dates(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            failed_date = date(2026, 8, 8)
            first_api = FakeSavantApi({failed_date})
            with self.assertLogs(
                "zavant.ingestion.baseball_savant.backfill", level="ERROR"
            ):
                first = self.coordinator(first_api, root).run(
                    start_date=date(2026, 8, 7),
                    end_date=date(2026, 8, 9),
                    request_delay_seconds=0,
                    run_id=RUN_ID,
                    started_at=STARTED_AT,
                )
            resumed_api = FakeSavantApi()
            resumed = self.coordinator(resumed_api, root).run(
                start_date=date(2026, 8, 7),
                end_date=date(2026, 8, 9),
                request_delay_seconds=0,
                run_id=RUN_ID,
                started_at=STARTED_AT,
            )

            self.assertFalse(first.successful)
            self.assertTrue(resumed.successful)
            self.assertTrue(resumed.resumed)
            self.assertEqual(resumed_api.calls, [failed_date])
            self.assertEqual(resumed.succeeded, 3)

    def test_dry_run_records_plan_without_source_requests(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            api = FakeSavantApi()

            result = self.coordinator(api, root).run(
                start_date=date(2026, 8, 7),
                end_date=date(2026, 8, 9),
                dry_run=True,
                request_delay_seconds=0,
            )

            self.assertTrue(result.successful)
            self.assertEqual(api.calls, [])
            self.assertEqual(result.skipped, 3)
            self.assertEqual(list(root.rglob("response.csv")), [])

    def test_rejects_today_before_creating_a_manifest(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            api = FakeSavantApi()

            with self.assertRaisesRegex(ValueError, "before today"):
                self.coordinator(api, root).run(
                    start_date=STARTED_AT.date(),
                    end_date=STARTED_AT.date(),
                    request_delay_seconds=0,
                )

            self.assertEqual(api.calls, [])
            self.assertEqual(list(root.rglob("manifest.json")), [])

    def test_s3_storage_persists_raw_dates_and_resumable_run_state(self) -> None:
        client = FakeS3Client()
        storage = s3_backfill_storage(
            client,
            "example-bucket",
            "portfolio/lake",
            clock=lambda: STARTED_AT,
        )
        api = FakeSavantApi()

        result = BaseballSavantBackfillCoordinator(
            api=api,
            raw_store=storage.raw,
            backfill_store=storage.runs,
            clock=lambda: STARTED_AT,
            run_id_factory=lambda: RUN_ID,
            sleeper=lambda _: None,
        ).run(
            start_date=date(2026, 8, 8),
            end_date=date(2026, 8, 9),
            request_delay_seconds=0,
        )

        keys = {
            key
            for bucket, key in client.objects
            if bucket == "example-bucket"
        }
        self.assertTrue(result.successful)
        self.assertEqual(result.succeeded, 2)
        self.assertTrue(result.manifest_path.uri.startswith("s3://example-bucket/"))
        self.assertTrue(
            any(
                key.startswith(
                    "portfolio/lake/raw/baseball_savant/statcast_search/"
                )
                for key in keys
            )
        )
        self.assertIn(
            "portfolio/lake/runs/baseball_savant/backfill/"
            "run_date=2026-08-10/"
            f"run_id={RUN_ID}/manifest.json",
            keys,
        )
        self.assertFalse(
            any("state/baseball_savant" in key for key in keys)
        )


if __name__ == "__main__":
    unittest.main()
