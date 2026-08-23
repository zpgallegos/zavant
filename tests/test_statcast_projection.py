from datetime import date, datetime, timezone
from pathlib import Path
import tempfile
import unittest
from uuid import UUID

import pyarrow.parquet as pq

from zavant.ingestion.baseball_savant.contract import StatcastCsvResponse
from zavant.ingestion.baseball_savant.storage import (
    PathBaseballSavantStore,
    s3_baseball_savant_store,
)
from zavant.projection.baseball_savant.contracts import (
    STATCAST_BATTING_EVENTS_CONTRACT,
    STATCAST_PROJECTION_CONTRACT_VERSION,
)
from zavant.projection.baseball_savant.local import (
    run_local_statcast_projection,
    statcast_projection_sources,
)
from zavant.projection.baseball_savant.models import StatcastProjectionSource
from zavant.projection.baseball_savant.projector import project_statcast_date
from zavant.projection.baseball_savant.s3_sources import (
    CurrentStatcastRevisionCacheEntry,
    discover_statcast_projection_inventory,
    load_statcast_projection_source,
    pending_statcast_revisions,
    resolve_current_statcast_revisions,
    validate_current_statcast_revisions,
)
from zavant.projection.contracts import ProjectionContractError
from zavant.storage._path_io import sha256_bytes
from zavant.storage.s3_objects import S3ObjectBackend
from tests.fake_s3 import FakeS3Client


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-baseball-savant.csv"
GAME_DATE = date(2026, 8, 8)
OBSERVED_AT = datetime(2026, 8, 9, tzinfo=timezone.utc)
PROJECTED_AT = datetime(2026, 8, 10, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000017")


class StatcastProjectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = FIXTURE.read_bytes()
        self.source = StatcastProjectionSource(
            game_date=GAME_DATE,
            revision_id=sha256_bytes(self.raw),
            observed_at=OBSERVED_AT,
            source_uri="https://baseballsavant.example.test/csv",
            raw_object_uri="file:///example-baseball-savant.csv",
            raw=self.raw,
        )

    def test_projects_only_terminal_batting_events(self) -> None:
        projection = project_statcast_date(self.source, RUN_ID, PROJECTED_AT)

        self.assertEqual(len(projection.batting_events), 2)
        self.assertEqual(len(projection.dates), 1)
        batted_ball = projection.batting_events[0]
        self.assertEqual(batted_ball["game_pk"], 823514)
        self.assertEqual(batted_ball["at_bat_number"], 1)
        self.assertEqual(batted_ball["pitch_number"], 2)
        self.assertEqual(batted_ball["launch_speed"], 88.8)
        self.assertEqual(batted_ball["estimated_ba_using_speedangle"], 0.01)
        self.assertEqual(batted_ball["estimated_slg_using_speedangle"], 0.017)
        self.assertEqual(batted_ball["estimated_woba_using_speedangle"], 0.013)
        walk = projection.batting_events[1]
        self.assertEqual(walk["estimated_woba_using_speedangle"], 0.689)
        self.assertEqual(walk["woba_value"], 0.689)
        self.assertEqual(
            walk["projection_contract_version"],
            STATCAST_PROJECTION_CONTRACT_VERSION,
        )
        self.assertEqual(projection.dates[0]["row_count"], 3)
        self.assertEqual(projection.dates[0]["terminal_row_count"], 2)

    def test_rejects_invalid_expected_stat_value(self) -> None:
        raw = self.raw.replace(b'"0.010"', b'"not-a-number"')
        source = StatcastProjectionSource(
            game_date=GAME_DATE,
            revision_id=sha256_bytes(raw),
            observed_at=OBSERVED_AT,
            source_uri=self.source.source_uri,
            raw_object_uri=self.source.raw_object_uri,
            raw=raw,
        )

        with self.assertRaisesRegex(
            ProjectionContractError, "invalid estimated_ba_using_speedangle"
        ):
            project_statcast_date(source, RUN_ID, PROJECTED_AT)


class StatcastS3SourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeS3Client(page_size=1)
        self.bucket = "example-bucket"
        self.prefix = "portfolio/lake"
        self.raw = FIXTURE.read_bytes()
        response = StatcastCsvResponse.from_bytes(self.raw, GAME_DATE)
        self.landed = s3_baseball_savant_store(
            self.client,
            self.bucket,
            self.prefix,
            clock=lambda: OBSERVED_AT,
        ).land_date(
            response,
            self.raw,
            "https://baseballsavant.example.test/csv",
            RUN_ID,
        )
        self.backend = S3ObjectBackend(self.client, self.bucket, self.prefix)

    def test_discovers_resolves_and_loads_date_revision(self) -> None:
        inventory = discover_statcast_projection_inventory(self.backend)

        self.assertEqual(len(inventory.revisions), 1)
        self.assertEqual(len(inventory.current_pointers), 1)
        discovery = resolve_current_statcast_revisions(
            self.backend, inventory.current_pointers
        )
        validate_current_statcast_revisions(
            inventory.revisions, discovery.revisions
        )
        revision = discovery.revisions[0]
        source = load_statcast_projection_source(self.backend, revision)
        self.assertEqual(revision.game_date, GAME_DATE)
        self.assertEqual(revision.revision_id, self.landed.revision_id)
        self.assertEqual(source.observed_at, OBSERVED_AT)
        self.assertEqual(source.raw, self.raw)
        self.assertEqual(
            pending_statcast_revisions(inventory.revisions, set()),
            inventory.revisions,
        )
        self.assertEqual(
            pending_statcast_revisions(
                inventory.revisions, {revision.completed_identity()}
            ),
            (),
        )

    def test_current_pointer_cache_avoids_unchanged_object_read(self) -> None:
        inventory = discover_statcast_projection_inventory(self.backend)
        pointer = inventory.current_pointers[0]
        cache = {
            GAME_DATE: CurrentStatcastRevisionCacheEntry(
                game_date=GAME_DATE,
                revision_id=self.landed.revision_id,
                reconciled_at=pointer.last_modified.replace(year=2027),
            )
        }
        reads_before = self.client.get_object_calls

        discovery = resolve_current_statcast_revisions(
            self.backend,
            inventory.current_pointers,
            cache,
        )

        self.assertEqual(discovery.refreshed, ())
        self.assertEqual(discovery.revisions[0].revision_id, self.landed.revision_id)
        self.assertEqual(self.client.get_object_calls, reads_before)


class LocalStatcastProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.data_dir = self.root / "lake"
        self.raw = FIXTURE.read_bytes()
        response = StatcastCsvResponse.from_bytes(self.raw, GAME_DATE)
        PathBaseballSavantStore(
            self.data_dir, clock=lambda: OBSERVED_AT
        ).land_date(
            response,
            self.raw,
            "https://baseballsavant.example.test/csv",
            RUN_ID,
        )

    def test_discovers_and_publishes_local_date_revisions(self) -> None:
        sources = list(statcast_projection_sources(self.data_dir))
        output_dir = self.root / "statcast-projection"

        result = run_local_statcast_projection(
            self.data_dir,
            output_dir,
            run_id=RUN_ID,
            projected_at=PROJECTED_AT,
        )

        self.assertEqual(len(sources), 1)
        self.assertEqual(result.date_revision_count, 1)
        self.assertEqual(result.row_counts["statcast_batting_events"], 2)
        table = pq.read_table(
            output_dir / "statcast_batting_events" / "data.parquet"
        )
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(
            table.schema.names,
            [
                column.name
                for column in STATCAST_BATTING_EVENTS_CONTRACT.columns
            ],
        )
        self.assertTrue((output_dir / "manifest.json").exists())
        self.assertTrue((output_dir / "schemas.json").exists())

    def test_rejects_existing_output_without_overwriting_it(self) -> None:
        output_dir = self.root / "statcast-projection"
        output_dir.mkdir()
        marker = output_dir / "owned-by-user.txt"
        marker.write_text("preserve")

        with self.assertRaisesRegex(FileExistsError, "already exists"):
            run_local_statcast_projection(self.data_dir, output_dir)

        self.assertEqual(marker.read_text(), "preserve")


if __name__ == "__main__":
    unittest.main()
