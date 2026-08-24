from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Mapping
import unittest
from urllib.parse import parse_qs, urlsplit

from zavant.ingestion.baseball_savant.client import BaseballSavantClient
from zavant.ingestion.http import HttpResponse


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-baseball-savant.csv"
TEST_BASE_URL = "https://baseballsavant.example.test"


@dataclass(frozen=True)
class TransportCall:
    url: str
    headers: Mapping[str, str]
    timeout_seconds: float


class FakeTransport:
    def __init__(self, *responses: HttpResponse) -> None:
        self.responses = list(responses)
        self.calls: List[TransportCall] = []

    def get(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        self.calls.append(TransportCall(url, dict(headers), timeout_seconds))
        return self.responses.pop(0)


class BaseballSavantClientTests(unittest.TestCase):
    def test_requests_all_batters_for_exactly_one_regular_season_date(self) -> None:
        transport = FakeTransport(
            HttpResponse(200, FIXTURE.read_bytes(), {}, TEST_BASE_URL)
        )
        client = BaseballSavantClient(
            base_url=TEST_BASE_URL,
            timeout_seconds=12.5,
            transport=transport,
        )

        retrieved = client.get_statcast_date(date(2026, 8, 8))

        call = transport.calls[0]
        parsed = urlsplit(call.url)
        self.assertEqual(parsed.path, "/statcast_search/csv")
        self.assertEqual(
            parse_qs(parsed.query),
            {
                "all": ["true"],
                "game_date_gt": ["2026-08-08"],
                "game_date_lt": ["2026-08-08"],
                "hfGT": ["R|"],
                "player_type": ["batter"],
                "type": ["details"],
            },
        )
        self.assertEqual(call.headers["Accept"], "text/csv")
        self.assertEqual(call.timeout_seconds, 12.5)
        self.assertEqual(retrieved.body, FIXTURE.read_bytes())

    def test_retries_a_transient_savant_response(self) -> None:
        transport = FakeTransport(
            HttpResponse(503, b"unavailable", {}, TEST_BASE_URL),
            HttpResponse(200, FIXTURE.read_bytes(), {}, TEST_BASE_URL),
        )
        delays: List[float] = []
        client = BaseballSavantClient(
            base_url=TEST_BASE_URL,
            transport=transport,
            sleeper=delays.append,
        )

        retrieved = client.get_statcast_date(date(2026, 8, 8))

        self.assertEqual(retrieved.attempts, 2)
        self.assertEqual(delays, [0.5])


if __name__ == "__main__":
    unittest.main()
