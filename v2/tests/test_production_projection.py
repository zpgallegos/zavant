from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import UUID

from zavant.contracts.raw_game import RawGameResponse
from zavant.projection.contracts import TABLE_CONTRACTS
from zavant.projection.glue_job import (
    GlueProjectionConfiguration,
    run_glue_projection,
)
from zavant.projection.iceberg import create_table_sql, merge_table_sql
from zavant.projection.s3_sources import (
    CurrentProjectionRevision,
    discover_current_revisions,
    load_projection_source,
    pending_current_revisions,
)
from zavant.projection.models import GameProjection
from zavant.storage.bundles import s3_acquisition_storage
from zavant.storage.s3_objects import S3ObjectBackend
from tests.fake_s3 import FakeS3Client


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RAW_GAME_FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-game-raw.json"
OBSERVED_AT = datetime(2026, 8, 9, tzinfo=timezone.utc)


class S3ProjectionSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeS3Client(page_size=1)
        self.bucket = "example-bucket"
        self.prefix = "portfolio/lake"
        self.raw = RAW_GAME_FIXTURE.read_bytes()
        self.game = RawGameResponse.from_bytes(self.raw)
        self.landed = s3_acquisition_storage(
            self.client,
            self.bucket,
            self.prefix,
            clock=lambda: OBSERVED_AT,
        ).raw_games.land(
            self.game,
            self.raw,
            "https://statsapi.example.test/game",
            "backfill",
        )
        self.backend = S3ObjectBackend(self.client, self.bucket, self.prefix)

    def test_discovers_and_loads_validated_current_revision(self) -> None:
        revisions = discover_current_revisions(self.backend)

        self.assertEqual(len(revisions), 1)
        revision = revisions[0]
        self.assertEqual(revision.game_pk, self.game.game_pk)
        self.assertEqual(revision.season, self.game.season)
        self.assertEqual(revision.revision_id, self.landed.revision_id)
        source = load_projection_source(self.backend, revision)
        self.assertEqual(source.game.game_pk, self.game.game_pk)
        self.assertEqual(source.revision_id, self.landed.revision_id)
        self.assertEqual(source.observed_at, OBSERVED_AT)
        self.assertTrue(source.raw_object_uri.startswith("s3://example-bucket/"))

    def test_reconciliation_selects_only_unregistered_contract_revision(self) -> None:
        revisions = discover_current_revisions(self.backend)
        completed = {revisions[0].completed_identity()}

        self.assertEqual(pending_current_revisions(revisions, set()), revisions)
        self.assertEqual(pending_current_revisions(revisions, completed), ())
        self.assertEqual(
            pending_current_revisions(revisions, completed, "future-contract/v2"),
            revisions,
        )

    def test_rejects_pointer_whose_routing_identity_was_changed(self) -> None:
        pointer_key = f"{self.prefix}/{self.landed.current_pointer_path.key}"
        pointer = json.loads(self.client.objects[(self.bucket, pointer_key)])
        pointer["game_pk"] = self.game.game_pk + 1
        self.client.objects[(self.bucket, pointer_key)] = json.dumps(pointer).encode()

        with self.assertRaisesRegex(ValueError, "invalid current pointer"):
            discover_current_revisions(self.backend)


class IcebergDefinitionTests(unittest.TestCase):
    def test_creates_format_v2_partitioned_table_from_contract(self) -> None:
        sql = create_table_sql(
            "glue_catalog",
            "zavant_analytical_prod",
            TABLE_CONTRACTS["pitches"],
            "s3://example-bucket/lake/analytical/iceberg",
        )

        self.assertIn("CREATE TABLE IF NOT EXISTS", sql)
        self.assertIn("`game_pk` BIGINT NOT NULL", sql)
        self.assertIn("PARTITIONED BY (season)", sql)
        self.assertIn("'format-version' = '2'", sql)
        self.assertIn("/iceberg/pitches'", sql)

    def test_merge_uses_every_natural_key_column(self) -> None:
        contract = TABLE_CONTRACTS["runner_movements"]
        sql = merge_table_sql(
            "glue_catalog",
            "zavant_analytical_prod",
            contract,
            "zavant_stage_runner_movements",
        )

        for column in contract.primary_key:
            self.assertIn(f"target.`{column}` = source.`{column}`", sql)
        self.assertIn("WHEN MATCHED THEN UPDATE", sql)
        self.assertIn("WHEN NOT MATCHED THEN INSERT", sql)

    def test_configuration_places_warehouse_below_analytical_prefix(self) -> None:
        configuration = GlueProjectionConfiguration(
            bucket="example-bucket",
            prefix="portfolio/lake",
            database="zavant_analytical_prod",
        )

        self.assertEqual(
            configuration.warehouse_uri,
            "s3://example-bucket/portfolio/lake/analytical/iceberg",
        )


class GlueProjectionCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.configuration = GlueProjectionConfiguration(
            bucket="example-bucket",
            database="zavant_analytical_prod",
        )
        self.revision = CurrentProjectionRevision(
            game_pk=744863,
            season=2024,
            revision_id="revision-one",
            pointer_key="raw/mlb_stats_api/games/season=2024/game_pk=744863/current.json",
            raw_key="raw/mlb_stats_api/games/season=2024/game_pk=744863/revision=revision-one/game.json",
            metadata_key="raw/mlb_stats_api/games/season=2024/game_pk=744863/revision=revision-one/metadata.json",
        )
        self.run_id = UUID("00000000-0000-0000-0000-000000000016")
        self.projection = GameProjection(
            table_rows={name: () for name in TABLE_CONTRACTS},
            event_kind_counts={},
        )

    def test_registers_completion_only_after_every_analytical_merge(self) -> None:
        spark = _FakeSpark()
        events = []

        with (
            patch("zavant.projection.glue_job._ensure_tables"),
            patch(
                "zavant.projection.glue_job._completed_projections",
                return_value=set(),
            ),
            patch(
                "zavant.projection.glue_job.discover_current_revisions",
                return_value=(self.revision,),
            ),
            patch(
                "zavant.projection.glue_job._project_partition",
                return_value=iter((self.projection,)),
            ),
            patch("zavant.projection.glue_job._spark_schema", return_value=object()),
            patch(
                "zavant.projection.glue_job._merge_dataframe",
                side_effect=lambda _spark, _configuration, contract, _frame: events.append(
                    contract.name
                ),
            ),
            patch(
                "zavant.projection.glue_job._merge_rows",
                side_effect=lambda _spark, _configuration, contract, _rows: events.append(
                    contract.name
                ),
            ),
            patch(
                "zavant.projection.glue_job.import_module",
                return_value=SimpleNamespace(
                    StorageLevel=SimpleNamespace(MEMORY_AND_DISK="memory-and-disk")
                ),
            ),
        ):
            result = run_glue_projection(
                spark,
                FakeS3Client(),
                self.configuration,
                self.run_id,
                OBSERVED_AT,
            )

        self.assertEqual(events[: len(TABLE_CONTRACTS)], list(TABLE_CONTRACTS))
        self.assertEqual(events[-2:], ["projection_revisions", "current_game_revisions"])
        self.assertEqual(result.projected_revision_count, 1)
        self.assertTrue(spark.last_rdd.unpersisted)

    def test_failed_table_merge_does_not_advance_either_registry(self) -> None:
        spark = _FakeSpark()

        with (
            patch("zavant.projection.glue_job._ensure_tables"),
            patch(
                "zavant.projection.glue_job._completed_projections",
                return_value=set(),
            ),
            patch(
                "zavant.projection.glue_job.discover_current_revisions",
                return_value=(self.revision,),
            ),
            patch(
                "zavant.projection.glue_job._project_partition",
                return_value=iter((self.projection,)),
            ),
            patch("zavant.projection.glue_job._spark_schema", return_value=object()),
            patch(
                "zavant.projection.glue_job._merge_dataframe",
                side_effect=RuntimeError("Iceberg commit failed"),
            ),
            patch("zavant.projection.glue_job._merge_rows") as merge_rows,
            patch(
                "zavant.projection.glue_job.import_module",
                return_value=SimpleNamespace(
                    StorageLevel=SimpleNamespace(MEMORY_AND_DISK="memory-and-disk")
                ),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "Iceberg commit failed"):
                run_glue_projection(
                    spark,
                    FakeS3Client(),
                    self.configuration,
                    self.run_id,
                    OBSERVED_AT,
                )

        merge_rows.assert_not_called()
        self.assertTrue(spark.last_rdd.unpersisted)


class _FakeConfiguration:
    def set(self, _name: str, _value: str) -> None:
        pass


class _FakeRDD:
    def __init__(self, values: list[object], owner: "_FakeSpark") -> None:
        self.values = values
        self.owner = owner
        self.unpersisted = False

    def mapPartitions(self, function):
        result = _FakeRDD(list(function(iter(self.values))), self.owner)
        self.owner.last_rdd = result
        return result

    def persist(self, _storage_level: object) -> "_FakeRDD":
        return self

    def count(self) -> int:
        return len(self.values)

    def flatMap(self, function):
        flattened = []
        for value in self.values:
            flattened.extend(function(value))
        return _FakeRDD(flattened, self.owner)

    def unpersist(self) -> None:
        self.unpersisted = True


class _FakeSparkContext:
    def __init__(self, owner: "_FakeSpark") -> None:
        self.owner = owner

    def parallelize(self, values, _partitions: int) -> _FakeRDD:
        rdd = _FakeRDD(list(values), self.owner)
        self.owner.last_rdd = rdd
        return rdd


class _FakeSpark:
    def __init__(self) -> None:
        self.conf = _FakeConfiguration()
        self.sparkContext = _FakeSparkContext(self)
        self.last_rdd = _FakeRDD([], self)

    def createDataFrame(self, rows, _schema):
        return tuple(rows.values)


if __name__ == "__main__":
    unittest.main()
