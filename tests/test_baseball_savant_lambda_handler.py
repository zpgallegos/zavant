from datetime import date, datetime, timezone
from pathlib import Path
from typing import List
import unittest
from uuid import UUID

from zavant.ingestion.baseball_savant.daily import BaseballSavantDailyAcquirer
from zavant.ingestion.baseball_savant.lambda_handler import (
    BaseballSavantLambdaApplication,
    BaseballSavantLambdaConfiguration,
)
from zavant.ingestion.http import RetrievedResource
from zavant.ingestion.baseball_savant.storage import s3_baseball_savant_store
from tests.fake_s3 import FakeS3Client


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-baseball-savant.csv"
STARTED_AT = datetime(2026, 8, 10, 13, tzinfo=timezone.utc)


class FakeSavantApi:
    def __init__(self) -> None:
        self.calls: List[date] = []

    def get_statcast_date(self, game_date: date) -> RetrievedResource:
        self.calls.append(game_date)
        body = FIXTURE.read_text().replace(
            "2026-08-08", game_date.isoformat()
        ).encode()
        url = f"https://baseballsavant.example.test/csv?date={game_date}"
        return RetrievedResource(body, url, url, 200, {}, 1)


class BaseballSavantLambdaConfigurationTests(unittest.TestCase):
    def test_requires_an_explicit_initial_date(self) -> None:
        with self.assertRaisesRegex(ValueError, "SAVANT_INITIAL_DATE"):
            BaseballSavantLambdaConfiguration.from_environment(
                {"ZAVANT_S3_BUCKET": "example-bucket"}
            )


class BaseballSavantLambdaApplicationTests(unittest.TestCase):
    def test_runs_in_an_isolated_s3_namespace(self) -> None:
        client = FakeS3Client()
        api = FakeSavantApi()
        configuration = BaseballSavantLambdaConfiguration.from_environment(
            {
                "ZAVANT_S3_BUCKET": "example-bucket",
                "ZAVANT_S3_PREFIX": "portfolio/lake",
                "ZAVANT_SAVANT_INITIAL_DATE": "2026-08-08",
            }
        )
        store = s3_baseball_savant_store(
            client,
            configuration.bucket,
            configuration.prefix,
            clock=lambda: STARTED_AT,
        )
        application = BaseballSavantLambdaApplication(
            acquirer=BaseballSavantDailyAcquirer(
                api,
                store,
                clock=lambda: STARTED_AT,
                run_id_factory=lambda: UUID(
                    "00000000-0000-0000-0000-000000000043"
                ),
            ),
            store=store,
            configuration=configuration,
        )

        result = application.run({"savant_through_date": "2026-08-09"})

        self.assertEqual(result["status"], "complete")
        self.assertEqual(api.calls, [date(2026, 8, 8), date(2026, 8, 9)])
        keys = {key for bucket, key in client.objects if bucket == "example-bucket"}
        self.assertTrue(
            any(
                key.startswith("portfolio/lake/raw/baseball_savant/")
                for key in keys
            )
        )
        self.assertIn(
            "portfolio/lake/state/baseball_savant/statcast_search/watermark.json",
            keys,
        )
        self.assertFalse(any("mlb_stats_api" in key for key in keys))


if __name__ == "__main__":
    unittest.main()
