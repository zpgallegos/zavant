from dataclasses import replace
from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile
from typing import Dict, List, Tuple
import unittest
from uuid import UUID

from zavant.acquisition.bounded_games import BoundedGameAcquirer
from zavant.acquisition.game_eligibility import (
    EligibilityDisposition,
    FinalRegularSeasonGamePolicy,
)
from zavant.clients.mlb_stats_api import (
    MlbStatsApiError,
    MlbStatsApiResponseError,
    RetrievedResource,
)
from zavant.contracts.schedule import ScheduleResponse
from zavant.storage.path_raw import PathRawGameStore
from zavant.storage.path_deferred_games import PathDeferredGameStore
from zavant.storage.path_schedule import PathScheduleStore, ScheduleConflictError


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SCHEDULE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-schedule.json"
START_DATE = date(2026, 8, 8)
END_DATE = date(2026, 8, 8)
REQUESTED_AT = datetime(2026, 8, 9, 0, 0, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 9, 0, 1, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000003")


def retrieved(body: bytes, source_uri: str, attempts: int = 1) -> RetrievedResource:
    return RetrievedResource(
        body=body,
        request_url=source_uri,
        response_url=source_uri,
        status_code=200,
        headers={"Content-Type": "application/json"},
        attempts=attempts,
    )


def raw_game(game_pk: int, official_date: date = START_DATE) -> bytes:
    return json.dumps(
        {
            "gamePk": game_pk,
            "metaData": {"timeStamp": "20260809_000000"},
            "gameData": {
                "datetime": {"officialDate": official_date.isoformat()},
                "status": {"codedGameState": "F", "detailedState": "Final"},
            },
            "liveData": {},
        }
    ).encode()


class FakeMlbGameAcquisitionApi:
    def __init__(
        self,
        schedule_raw: bytes,
        game_outcomes: Dict[int, List[object]],
    ) -> None:
        self.schedule_raw = schedule_raw
        self.game_outcomes = {
            game_pk: list(outcomes) for game_pk, outcomes in game_outcomes.items()
        }
        self.schedule_calls: List[Tuple[date, date, int]] = []
        self.game_calls: List[int] = []

    def get_schedule(
        self,
        start_date: date,
        end_date: date,
        sport_id: int = 1,
    ) -> RetrievedResource:
        self.schedule_calls.append((start_date, end_date, sport_id))
        return retrieved(
            self.schedule_raw,
            "https://statsapi.example.test/api/v1/schedule"
            "?sportId=1&startDate=2026-08-08&endDate=2026-08-08",
        )

    def get_live_game(self, game_pk: int) -> RetrievedResource:
        self.game_calls.append(game_pk)
        outcomes = self.game_outcomes.get(game_pk)
        if not outcomes:
            raise AssertionError(f"no queued response for game {game_pk}")
        outcome = outcomes.pop(0)
        if isinstance(outcome, MlbStatsApiError):
            raise outcome
        if not isinstance(outcome, RetrievedResource):
            raise AssertionError(f"invalid queued response for game {game_pk}")
        return outcome


class FinalRegularSeasonGamePolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        schedule = ScheduleResponse.from_bytes(SAMPLE_SCHEDULE.read_bytes())
        self.game = schedule.scheduled_games[0]
        self.policy = FinalRegularSeasonGamePolicy()

    def test_acquires_final_regular_season_game(self) -> None:
        decision = self.policy.evaluate(self.game)

        self.assertEqual(decision.disposition, EligibilityDisposition.ELIGIBLE)
        self.assertEqual(decision.reason, "final_regular_season_game")

    def test_defers_unfinished_regular_season_game(self) -> None:
        decision = self.policy.evaluate(
            replace(self.game, status_code="I", detailed_state="In Progress")
        )

        self.assertEqual(decision.disposition, EligibilityDisposition.DEFERRED)
        self.assertEqual(decision.reason, "game_not_final")

    def test_skips_canceled_regular_season_game(self) -> None:
        decision = self.policy.evaluate(
            replace(self.game, status_code="C", detailed_state="Cancelled")
        )

        self.assertEqual(decision.disposition, EligibilityDisposition.SKIPPED)
        self.assertEqual(decision.reason, "canceled_game")

    def test_skips_non_regular_season_game(self) -> None:
        decision = self.policy.evaluate(replace(self.game, game_type="S"))

        self.assertEqual(decision.disposition, EligibilityDisposition.SKIPPED)
        self.assertEqual(decision.reason, "unsupported_game_type")


class BoundedGameAcquirerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.schedule_store = PathScheduleStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.game_store = PathRawGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.deferred_game_store = PathDeferredGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def acquirer(self, api: FakeMlbGameAcquisitionApi) -> BoundedGameAcquirer:
        return BoundedGameAcquirer(
            api=api,
            schedule_store=self.schedule_store,
            game_store=self.game_store,
            deferred_game_store=self.deferred_game_store,
            clock=lambda: REQUESTED_AT,
            run_id_factory=lambda: RUN_ID,
        )

    def test_lands_schedule_and_every_eligible_game(self) -> None:
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [
                    retrieved(
                        raw_game(823514),
                        "https://statsapi.example.test/api/v1.1/game/823514/feed/live",
                    )
                ],
                824726: [
                    retrieved(
                        raw_game(824726),
                        "https://statsapi.example.test/api/v1.1/game/824726/feed/live",
                        attempts=2,
                    )
                ],
            },
        )

        result = self.acquirer(api).acquire(START_DATE, END_DATE)

        self.assertTrue(result.successful)
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.summary["succeeded"], 2)
        self.assertEqual(result.summary["failed"], 0)
        self.assertEqual(api.schedule_calls, [(START_DATE, END_DATE, 1)])
        self.assertEqual(api.game_calls, [823514, 824726])

        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["summary"]["succeeded"], 2)
        self.assertTrue(manifest["games"][0]["revision_id"])
        self.assertEqual(manifest["games"][1]["http_attempts"], 2)
        self.assertEqual(
            len(list(self.data_dir.rglob("game.json"))),
            2,
        )

    def test_records_skipped_and_deferred_games_without_downloading(self) -> None:
        payload = json.loads(SAMPLE_SCHEDULE.read_bytes())
        payload["dates"][0]["games"][0]["gameType"] = "S"
        payload["dates"][0]["games"][1]["status"]["codedGameState"] = "I"
        payload["dates"][0]["games"][1]["status"]["detailedState"] = "In Progress"
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=json.dumps(payload).encode(),
            game_outcomes={},
        )

        result = self.acquirer(api).acquire(START_DATE, END_DATE)

        self.assertTrue(result.successful)
        self.assertEqual(result.summary["skipped"], 1)
        self.assertEqual(result.summary["deferred"], 1)
        self.assertEqual(api.game_calls, [])
        pending = self.deferred_game_store.pending()
        self.assertEqual([game.game_pk for game in pending], [824726])
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(manifest["games"][0]["reason"], "unsupported_game_type")
        self.assertEqual(manifest["games"][1]["reason"], "game_not_final")

    def test_records_failure_and_continues_with_remaining_games(self) -> None:
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [
                    MlbStatsApiResponseError(
                        status_code=503,
                        url="https://statsapi.example.test/game/823514",
                        attempts=3,
                    )
                ],
                824726: [
                    retrieved(
                        raw_game(824726),
                        "https://statsapi.example.test/game/824726",
                    )
                ],
            },
        )

        result = self.acquirer(api).acquire(START_DATE, END_DATE)

        self.assertFalse(result.successful)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.summary["failed"], 1)
        self.assertEqual(result.summary["succeeded"], 1)
        self.assertEqual(api.game_calls, [823514, 824726])
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(
            manifest["games"][0]["error_type"],
            "MlbStatsApiResponseError",
        )

    def test_resume_retries_failure_without_refetching_completed_work(self) -> None:
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [
                    MlbStatsApiResponseError(
                        status_code=503,
                        url="https://statsapi.example.test/game/823514",
                        attempts=3,
                    ),
                    retrieved(
                        raw_game(823514),
                        "https://statsapi.example.test/game/823514",
                    ),
                ],
                824726: [
                    retrieved(
                        raw_game(824726),
                        "https://statsapi.example.test/game/824726",
                    )
                ],
            },
        )
        acquirer = self.acquirer(api)
        first_result = acquirer.acquire(
            START_DATE,
            END_DATE,
            run_id=RUN_ID,
            requested_at=REQUESTED_AT,
        )

        second_result = acquirer.acquire(
            START_DATE,
            END_DATE,
            run_id=RUN_ID,
            requested_at=REQUESTED_AT,
        )

        self.assertEqual(first_result.status, "failed")
        self.assertTrue(second_result.successful)
        self.assertTrue(second_result.resumed)
        self.assertEqual(second_result.schedule_http_attempts, 0)
        self.assertEqual(len(api.schedule_calls), 1)
        self.assertEqual(api.game_calls, [823514, 824726, 823514])
        manifest = json.loads(Path(second_result.manifest_path.uri).read_text())
        self.assertEqual(len(manifest["games"][0]["processing_attempts"]), 2)
        self.assertEqual(len(manifest["games"][1]["processing_attempts"]), 1)

    def test_rejects_live_feed_for_a_different_game(self) -> None:
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [
                    retrieved(
                        raw_game(999999),
                        "https://statsapi.example.test/game/823514",
                    )
                ],
                824726: [
                    retrieved(
                        raw_game(824726),
                        "https://statsapi.example.test/game/824726",
                    )
                ],
            },
        )

        result = self.acquirer(api).acquire(START_DATE, END_DATE)

        self.assertEqual(result.summary["failed"], 1)
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(manifest["games"][0]["error_type"], "GameIdentityError")
        self.assertFalse(
            list(self.data_dir.rglob("game_pk=999999")),
        )

    def test_rejects_live_feed_for_a_different_season(self) -> None:
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [
                    retrieved(
                        raw_game(823514, date(2025, 8, 8)),
                        "https://statsapi.example.test/game/823514",
                    )
                ],
                824726: [
                    retrieved(
                        raw_game(824726),
                        "https://statsapi.example.test/game/824726",
                    )
                ],
            },
        )

        result = self.acquirer(api).acquire(START_DATE, END_DATE)

        self.assertEqual(result.summary["failed"], 1)
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(manifest["games"][0]["error_type"], "GameIdentityError")
        self.assertFalse(list(self.data_dir.rglob("season=2025")))

    def test_rejects_resume_arguments_that_conflict_with_stored_request(self) -> None:
        api = FakeMlbGameAcquisitionApi(
            schedule_raw=SAMPLE_SCHEDULE.read_bytes(),
            game_outcomes={
                823514: [
                    retrieved(
                        raw_game(823514),
                        "https://statsapi.example.test/game/823514",
                    )
                ],
                824726: [
                    retrieved(
                        raw_game(824726),
                        "https://statsapi.example.test/game/824726",
                    )
                ],
            },
        )
        acquirer = self.acquirer(api)
        acquirer.acquire(
            START_DATE,
            END_DATE,
            run_id=RUN_ID,
            requested_at=REQUESTED_AT,
        )

        with self.assertRaisesRegex(ScheduleConflictError, "start_date"):
            acquirer.acquire(
                date(2026, 8, 7),
                END_DATE,
                run_id=RUN_ID,
                requested_at=REQUESTED_AT,
            )

        self.assertEqual(len(api.schedule_calls), 1)
