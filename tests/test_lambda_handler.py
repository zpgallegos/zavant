from datetime import date, datetime, timezone
import json
from typing import List
import unittest

from zavant.clients.mlb_stats_api import RetrievedResource
from zavant.lambda_handler import (
    LambdaConfiguration,
    build_lambda_application,
)
from tests.fake_s3 import FakeS3Client


STARTED_AT = datetime(2026, 8, 9, tzinfo=timezone.utc)


def retrieved(body: bytes, resource: str) -> RetrievedResource:
    uri = f"https://statsapi.example.test/{resource}"
    return RetrievedResource(
        body=body,
        request_url=uri,
        response_url=uri,
        status_code=200,
        headers={"Content-Type": "application/json"},
        attempts=1,
    )


class EmptyDailyApi:
    def __init__(self) -> None:
        self.schedule_calls: List[date] = []

    def get_schedule(
        self, start_date: date, end_date: date, sport_id: int = 1
    ) -> RetrievedResource:
        del end_date, sport_id
        self.schedule_calls.append(start_date)
        return retrieved(
            json.dumps({"totalItems": 0, "totalGames": 0, "dates": []}).encode(),
            "schedule",
        )

    def get_game_changes(
        self,
        updated_since: datetime,
        sport_id: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> RetrievedResource:
        del updated_since, sport_id, limit, offset
        return retrieved(
            json.dumps({"totalItems": 0, "totalGames": 0, "dates": []}).encode(),
            "changes",
        )

    def get_live_game(self, game_pk: int) -> RetrievedResource:
        raise AssertionError(f"unexpected game request: {game_pk}")


class LambdaConfigurationTests(unittest.TestCase):
    def test_requires_bucket_and_explicit_bootstrap_boundaries(self) -> None:
        with self.assertRaises(ValueError):
            LambdaConfiguration.from_environment({})

        with self.assertRaisesRegex(ValueError, "INITIAL_SCHEDULE_DATE"):
            LambdaConfiguration.from_environment(
                {"ZAVANT_S3_BUCKET": "example-bucket"}
            )

        configuration = LambdaConfiguration.from_environment(
            {
                "ZAVANT_S3_BUCKET": "example-bucket",
                "ZAVANT_INITIAL_SCHEDULE_DATE": "2026-08-03",
                "ZAVANT_INITIAL_CORRECTION_WATERMARK": "2026-08-03T00:00:00Z",
            }
        )

        self.assertEqual(configuration.initial_schedule_date, date(2026, 8, 3))


class LambdaApplicationTests(unittest.TestCase):
    def test_runs_complete_daily_workflow_and_returns_s3_manifest(self) -> None:
        client = FakeS3Client()
        api = EmptyDailyApi()
        application = build_lambda_application(
            environ={
                "ZAVANT_S3_BUCKET": "example-bucket",
                "ZAVANT_S3_PREFIX": "portfolio/lake",
                "ZAVANT_INITIAL_SCHEDULE_DATE": "2026-08-03",
                "ZAVANT_INITIAL_CORRECTION_WATERMARK": "2026-08-08T00:00:00Z",
            },
            s3_client=client,
            api=api,
            clock=lambda: STARTED_AT,
        )

        result = application.run({"through_date": "2026-08-03"})

        self.assertEqual(result["status"], "complete")
        self.assertTrue(result["manifest_path"].startswith("s3://example-bucket/"))
        self.assertEqual(api.schedule_calls, [date(2026, 8, 3)])
        keys = {key for bucket, key in client.objects if bucket == "example-bucket"}
        self.assertIn(
            "portfolio/lake/state/mlb_stats_api/schedules/watermark.json",
            keys,
        )
        self.assertIn(
            "portfolio/lake/state/mlb_stats_api/game_changes/watermark.json",
            keys,
        )
        self.assertTrue(
            any(key.startswith("portfolio/lake/runs/daily/") for key in keys)
        )


if __name__ == "__main__":
    unittest.main()
