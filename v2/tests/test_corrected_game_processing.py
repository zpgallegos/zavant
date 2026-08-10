from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from typing import Dict, List
import unittest
from uuid import UUID

from zavant.acquisition.corrected_games import CorrectedGameProcessor
from zavant.clients.mlb_stats_api import (
    MlbStatsApiError,
    MlbStatsApiResponseError,
    RetrievedResource,
)
from zavant.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.contracts.raw_game import RawGameResponse
from zavant.storage.path_game_changes import PathGameChangesStore
from zavant.storage.path_raw import PathRawGameStore
from zavant.storage.artifacts import ArtifactReference


UPDATED_SINCE = datetime(2026, 8, 8, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 8, 9, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 9, 0, 1, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000020")


def raw_game(game_pk: int, marker: str) -> bytes:
    return json.dumps(
        {
            "gamePk": game_pk,
            "metaData": {"timeStamp": marker},
            "gameData": {"datetime": {"officialDate": "2026-08-08"}},
            "liveData": {"marker": marker},
        }
    ).encode()


def changes_raw(game_pks: List[int]) -> bytes:
    games = [
        {
            "gamePk": game_pk,
            "officialDate": "2026-08-08",
            "season": "2026",
            "link": f"/api/v1.1/game/{game_pk}/feed/live",
            "status": {"codedGameState": "F", "detailedState": "Final"},
        }
        for game_pk in game_pks
    ]
    return json.dumps(
        {
            "totalItems": len(games),
            "totalGames": len(games),
            "dates": [{"date": "2026-08-08", "games": games}],
        }
    ).encode()


def retrieved(body: bytes, game_pk: int) -> RetrievedResource:
    uri = f"https://statsapi.example.test/api/v1.1/game/{game_pk}/feed/live"
    return RetrievedResource(
        body=body,
        request_url=uri,
        response_url=uri,
        status_code=200,
        headers={},
        attempts=1,
    )


class FakeCorrectedGameApi:
    def __init__(self, outcomes: Dict[int, List[object]]) -> None:
        self.outcomes = {key: list(value) for key, value in outcomes.items()}
        self.calls: List[int] = []

    def get_live_game(self, game_pk: int) -> RetrievedResource:
        self.calls.append(game_pk)
        outcomes = self.outcomes.get(game_pk)
        if not outcomes:
            raise AssertionError(f"no queued response for game {game_pk}")
        outcome = outcomes.pop(0)
        if isinstance(outcome, MlbStatsApiError):
            raise outcome
        if not isinstance(outcome, RetrievedResource):
            raise AssertionError(f"invalid outcome for game {game_pk}")
        return outcome


class CorrectedGameProcessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.changes_store = PathGameChangesStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )
        self.game_store = PathRawGameStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def correction_manifest(self, game_pks: List[int]) -> ArtifactReference:
        raw = changes_raw(game_pks)
        changes = GameChangesResponse.from_bytes(raw)
        landed = self.changes_store.land_page(
            changes=changes,
            request=GameChangesRequest(
                updated_since=UPDATED_SINCE,
                window_end=WINDOW_END,
                page_number=0,
                limit=1000,
                offset=0,
                source_uri="fixture://changes",
            ),
            raw=raw,
            run_id=RUN_ID,
        )
        self.changes_store.finalize_manifest(
            manifest_path=landed.manifest_path,
            expected_page_count=1,
            expected_total_items=len(game_pks),
            watermark_before=UPDATED_SINCE,
        )
        return landed.manifest_path

    def land_existing_game(self, game_pk: int, marker: str = "old") -> None:
        raw = raw_game(game_pk, marker)
        self.game_store.land(
            RawGameResponse.from_bytes(raw),
            raw,
            source_uri="fixture://existing",
            trigger="initial",
        )

    def test_lands_new_revision_and_completes_manifest(self) -> None:
        self.land_existing_game(823426)
        manifest_path = self.correction_manifest([823426])
        api = FakeCorrectedGameApi(
            {823426: [retrieved(raw_game(823426, "corrected"), 823426)]}
        )

        result = CorrectedGameProcessor(
            api, self.changes_store, self.game_store
        ).process_all()

        self.assertTrue(result.successful)
        self.assertEqual(result.summary["succeeded"], 1)
        self.assertEqual(api.calls, [823426])
        manifest = json.loads(Path(manifest_path.uri).read_text())
        self.assertEqual(manifest["processing_status"], "complete")
        self.assertTrue(manifest["changed_games"][0]["revision_created"])
        self.assertEqual(
            len(list(self.data_dir.rglob("game_pk=823426/revision=*/game.json"))),
            2,
        )

    def test_skips_game_not_previously_landed(self) -> None:
        manifest_path = self.correction_manifest([823426])
        api = FakeCorrectedGameApi({})

        result = CorrectedGameProcessor(
            api, self.changes_store, self.game_store
        ).process_all()

        self.assertTrue(result.successful)
        self.assertEqual(result.summary["skipped"], 1)
        self.assertEqual(api.calls, [])
        manifest = json.loads(Path(manifest_path.uri).read_text())
        self.assertEqual(
            manifest["changed_games"][0]["reason"],
            "game_not_previously_landed",
        )

    def test_retries_failed_game_on_next_invocation(self) -> None:
        self.land_existing_game(823426)
        manifest_path = self.correction_manifest([823426])
        api = FakeCorrectedGameApi(
            {
                823426: [
                    MlbStatsApiResponseError(
                        status_code=503,
                        url="https://statsapi.example.test/game/823426",
                        attempts=3,
                    ),
                    retrieved(raw_game(823426, "corrected"), 823426),
                ]
            }
        )
        processor = CorrectedGameProcessor(api, self.changes_store, self.game_store)

        first = processor.process_all()
        second = processor.process_all()

        self.assertFalse(first.successful)
        self.assertTrue(second.successful)
        self.assertEqual(api.calls, [823426, 823426])
        manifest = json.loads(Path(manifest_path.uri).read_text())
        self.assertEqual(len(manifest["changed_games"][0]["processing_attempts"]), 2)
