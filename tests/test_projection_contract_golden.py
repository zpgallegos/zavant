import hashlib
import json
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Sequence
from uuid import UUID

from zavant.ingestion.mlb_stats_api.contracts.raw_game import RawGameResponse
from zavant.projection.contracts import ProjectionRow
from zavant.projection.mlb_stats_api.contracts import PROJECTION_CONTRACT_VERSION
from zavant.projection.mlb_stats_api.models import ProjectionSource
from zavant.projection.mlb_stats_api.projector import project_game
from zavant.storage._path_io import canonical_json_sha256, sha256_bytes


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RAW_GAME_FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-game-raw.json"
GOLDEN_FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "projection-contract-v1.json"
OBSERVED_AT = datetime(2026, 8, 9, 12, tzinfo=timezone.utc)
PROJECTED_AT = datetime(2026, 8, 10, 12, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000015")


def build_fixture() -> Dict[str, Any]:
    raw = RAW_GAME_FIXTURE.read_bytes()
    game = RawGameResponse.from_bytes(raw)
    projection = project_game(
        ProjectionSource(
            game=game,
            revision_id=canonical_json_sha256(game.payload),
            observed_at=OBSERVED_AT,
            source_uri="fixture://example-game",
            raw_object_uri="file:///example-game.json",
        ),
        RUN_ID,
        PROJECTED_AT,
    )
    rows_by_table = projection.tables()
    return {
        "contract": "zavant-projection-golden-fixture/v1",
        "projection_contract_version": PROJECTION_CONTRACT_VERSION,
        "source_sha256": sha256_bytes(raw),
        "tables": {
            name: {
                "row_count": len(rows),
                "rows_sha256": _rows_sha256(rows),
            }
            for name, rows in sorted(rows_by_table.items())
        },
    }


class ProjectionContractGoldenTests(unittest.TestCase):
    def test_observable_projection_matches_versioned_golden_fixture(self) -> None:
        expected = json.loads(GOLDEN_FIXTURE.read_bytes())

        self.assertEqual(build_fixture(), expected)


def _rows_sha256(rows: Sequence[ProjectionRow]) -> str:
    encoded = json.dumps(
        rows,
        default=_json_value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _json_value(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"unsupported golden-fixture value: {type(value).__name__}")


if __name__ == "__main__":
    unittest.main()
