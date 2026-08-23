from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
import unittest
from uuid import UUID

from zavant.ingestion.baseball_savant.daily import BaseballSavantDailyAcquirer
from zavant.ingestion.http import RetrievedResource
from zavant.ingestion.baseball_savant.contract import (
    BaseballSavantContractError,
    StatcastCsvResponse,
)
from zavant.ingestion.baseball_savant.storage import PathBaseballSavantStore


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-baseball-savant.csv"
STARTED_AT = datetime(2026, 8, 10, 13, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000041")


def csv_for(game_date: date) -> bytes:
    return FIXTURE.read_text().replace("2026-08-08", game_date.isoformat()).encode()


class FakeSavantApi:
    def __init__(self) -> None:
        self.calls: List[date] = []

    def get_statcast_date(self, game_date: date) -> RetrievedResource:
        self.calls.append(game_date)
        url = f"https://baseballsavant.example.test/csv?date={game_date}"
        return RetrievedResource(
            body=csv_for(game_date),
            request_url=url,
            response_url=url,
            status_code=200,
            headers={"Content-Type": "text/csv"},
            attempts=1,
        )


class InvalidDateSavantApi(FakeSavantApi):
    def get_statcast_date(self, game_date: date) -> RetrievedResource:
        retrieved = super().get_statcast_date(game_date)
        if game_date == date(2026, 8, 8):
            return RetrievedResource(
                body=csv_for(date(2026, 8, 7)),
                request_url=retrieved.request_url,
                response_url=retrieved.response_url,
                status_code=200,
                headers=retrieved.headers,
                attempts=1,
            )
        return retrieved


class StatcastCsvContractTests(unittest.TestCase):
    def test_validates_expected_columns_and_counts_terminal_rows(self) -> None:
        response = StatcastCsvResponse.from_bytes(
            FIXTURE.read_bytes(), date(2026, 8, 8)
        )

        self.assertEqual(response.row_count, 3)
        self.assertEqual(response.terminal_row_count, 2)
        self.assertIn("estimated_slg_using_speedangle", response.columns)

    def test_rejects_rows_outside_the_exact_requested_date(self) -> None:
        with self.assertRaisesRegex(BaseballSavantContractError, "outside"):
            StatcastCsvResponse.from_bytes(
                FIXTURE.read_bytes(), date(2026, 8, 9)
            )

    def test_rejects_a_missing_expected_statistics_column(self) -> None:
        raw = FIXTURE.read_bytes().replace(
            b"estimated_slg_using_speedangle",
            b"unexpected_column",
        )

        with self.assertRaisesRegex(BaseballSavantContractError, "estimated_slg"):
            StatcastCsvResponse.from_bytes(raw, date(2026, 8, 8))


class BaseballSavantDailyAcquirerTests(unittest.TestCase):
    def test_lands_each_date_and_advances_watermark_after_complete_run(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            api = FakeSavantApi()
            store = PathBaseballSavantStore(root, clock=lambda: STARTED_AT)
            acquirer = BaseballSavantDailyAcquirer(
                api,
                store,
                clock=lambda: STARTED_AT,
                run_id_factory=lambda: RUN_ID,
            )

            result = acquirer.run(
                initial_date=date(2026, 8, 7),
                through_date=date(2026, 8, 9),
            )

            self.assertTrue(result.successful)
            self.assertEqual(
                api.calls,
                [date(2026, 8, 7), date(2026, 8, 8), date(2026, 8, 9)],
            )
            watermark = store.read_watermark()
            self.assertIsNotNone(watermark)
            assert watermark is not None
            self.assertEqual(watermark.through_date, date(2026, 8, 9))
            for game_date in api.calls:
                current = (
                    root
                    / "raw"
                    / "baseball_savant"
                    / "statcast_search"
                    / f"game_date={game_date}"
                    / "current.json"
                )
                self.assertTrue(current.exists())

    def test_reacquires_rolling_dates_without_sharing_stats_api_state(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            store = PathBaseballSavantStore(root, clock=lambda: STARTED_AT)
            first_api = FakeSavantApi()
            BaseballSavantDailyAcquirer(
                first_api,
                store,
                clock=lambda: STARTED_AT,
                run_id_factory=lambda: RUN_ID,
            ).run(
                initial_date=date(2026, 8, 7),
                through_date=date(2026, 8, 9),
                lookback_days=2,
            )
            second_api = FakeSavantApi()
            second = BaseballSavantDailyAcquirer(
                second_api,
                store,
                clock=lambda: STARTED_AT,
                run_id_factory=lambda: UUID(
                    "00000000-0000-0000-0000-000000000042"
                ),
            )

            second.run(
                initial_date=date(2026, 8, 7),
                through_date=date(2026, 8, 10),
                lookback_days=2,
            )

            self.assertEqual(second_api.calls, [date(2026, 8, 9), date(2026, 8, 10)])
            self.assertFalse((root / "state" / "mlb_stats_api").exists())

    def test_does_not_advance_watermark_after_a_contract_failure(self) -> None:
        with TemporaryDirectory() as directory:
            store = PathBaseballSavantStore(
                Path(directory), clock=lambda: STARTED_AT
            )
            with self.assertLogs(
                "zavant.ingestion.baseball_savant.daily", level="ERROR"
            ):
                result = BaseballSavantDailyAcquirer(
                    InvalidDateSavantApi(),
                    store,
                    clock=lambda: STARTED_AT,
                    run_id_factory=lambda: RUN_ID,
                ).run(
                    initial_date=date(2026, 8, 7),
                    through_date=date(2026, 8, 9),
                )

            self.assertFalse(result.successful)
            self.assertEqual(result.failed, 1)
            self.assertIsNone(store.read_watermark())


if __name__ == "__main__":
    unittest.main()
