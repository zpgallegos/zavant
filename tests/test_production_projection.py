from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
from uuid import UUID

from zavant.contracts.raw_game import RawGameResponse
from zavant.projection.contracts import TABLE_CONTRACTS
from zavant.projection.current_views import (
    PRIVATE_COLUMNS,
    all_current_views,
    create_current_view_sql,
    current_views_need_publication,
    publish_current_views,
    record_current_view_publication,
)
from zavant.projection.glue_job import (
    GlueProjectionConfiguration,
    _analytical_merge_contracts,
    _completed_projections,
    _ensure_tables,
    _schema_drift_details,
    run_glue_projection,
)
from zavant.projection.iceberg import create_table_sql, merge_table_sql
from zavant.projection.s3_sources import (
    CurrentRevisionCacheEntry,
    CurrentRevisionDiscovery,
    ProjectionInventory,
    ProjectionRevision,
    discover_current_revisions,
    discover_projection_inventory,
    discover_revisions,
    load_projection_source,
    pending_revisions,
    resolve_current_revisions,
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

    def test_discovers_and_loads_validated_revision(self) -> None:
        revisions = discover_revisions(self.backend)

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

    def test_reconciliation_selects_only_unprojected_revision(self) -> None:
        revisions = discover_revisions(self.backend)
        completed = {revisions[0].completed_identity()}

        self.assertEqual(pending_revisions(revisions, set()), revisions)
        self.assertEqual(pending_revisions(revisions, completed), ())

    def test_discovers_superseded_and_current_revisions(self) -> None:
        changed_payload = json.loads(self.raw)
        changed_payload["gameData"]["status"]["detailedState"] = "Final: Corrected"
        changed_raw = json.dumps(changed_payload).encode()
        changed_game = RawGameResponse.from_bytes(changed_raw)
        second = s3_acquisition_storage(
            self.client,
            self.bucket,
            self.prefix,
            clock=lambda: OBSERVED_AT,
        ).raw_games.land(
            changed_game,
            changed_raw,
            "https://statsapi.example.test/game",
            "game_changes",
        )

        revisions = discover_revisions(self.backend)
        current = discover_current_revisions(self.backend)

        self.assertEqual(
            {revision.revision_id for revision in revisions},
            {self.landed.revision_id, second.revision_id},
        )
        self.assertEqual(
            tuple(revision.revision_id for revision in current),
            (second.revision_id,),
        )

    def test_current_discovery_selects_pointer_revision(self) -> None:
        revisions = discover_current_revisions(self.backend)

        self.assertEqual(len(revisions), 1)
        self.assertEqual(revisions[0].revision_id, self.landed.revision_id)

    def test_rejects_pointer_whose_routing_identity_was_changed(self) -> None:
        pointer_key = f"{self.prefix}/{self.landed.current_pointer_path.key}"
        pointer = json.loads(self.client.objects[(self.bucket, pointer_key)])
        pointer["game_pk"] = self.game.game_pk + 1
        self.client.objects[(self.bucket, pointer_key)] = json.dumps(pointer).encode()

        with self.assertRaisesRegex(ValueError, "invalid current pointer"):
            discover_current_revisions(self.backend)

    def test_single_inventory_classifies_revisions_and_current_pointers(self) -> None:
        inventory = discover_projection_inventory(self.backend)

        self.assertEqual(len(inventory.revisions), 1)
        self.assertEqual(len(inventory.current_pointers), 1)
        self.assertEqual(inventory.current_pointers[0].game_pk, self.game.game_pk)

    def test_current_pointer_cache_avoids_unchanged_object_read(self) -> None:
        inventory = discover_projection_inventory(self.backend)
        pointer = inventory.current_pointers[0]
        cache = {
            self.game.game_pk: CurrentRevisionCacheEntry(
                game_pk=self.game.game_pk,
                season=self.game.season,
                revision_id=self.landed.revision_id,
                reconciled_at=pointer.last_modified + timedelta(minutes=10),
            )
        }
        reads_before = self.client.get_object_calls

        discovery = resolve_current_revisions(
            self.backend,
            inventory.current_pointers,
            cache,
        )

        self.assertEqual(discovery.refreshed, ())
        self.assertEqual(discovery.revisions[0].revision_id, self.landed.revision_id)
        self.assertEqual(self.client.get_object_calls, reads_before)


class IcebergDefinitionTests(unittest.TestCase):
    def test_completed_projection_scan_rejects_another_projection_release(
        self,
    ) -> None:
        spark = MagicMock()
        spark.table.return_value.select.return_value.collect.return_value = [
            {
                "game_pk": 744863,
                "source_revision_id": "revision-one",
                "projection_contract_version": (
                    "zavant-analytical-game-projection/v2"
                ),
            }
        ]
        configuration = GlueProjectionConfiguration(
            bucket="example-bucket",
            database="zavant_analytical_prod",
        )

        with self.assertRaisesRegex(RuntimeError, "rebuild every analytical table"):
            _completed_projections(spark, configuration)

    def test_schema_drift_reports_actionable_column_differences(self) -> None:
        details = _schema_drift_details(
            (("game_pk", "bigint", False), ("old_name", "string", True)),
            (("game_pk", "bigint", False), ("new_name", "string", True)),
        )

        self.assertIn("missing=['new_name']", details)
        self.assertIn("unexpected=['old_name']", details)
        self.assertIn("incompatible={}", details)

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
        self.assertIn("target.`season` = source.`season`", sql)
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
        self.assertEqual(
            configuration.athena_output_uri,
            "s3://example-bucket/portfolio/lake/analytical/athena-results/"
            "projection-views/",
        )

    def test_existing_tables_skip_repeated_create_ddl(self) -> None:
        spark = MagicMock()
        configuration = GlueProjectionConfiguration(
            bucket="example-bucket",
            database="zavant_analytical_prod",
        )
        existing = {
            contract.name
            for contract in (*TABLE_CONTRACTS.values(),)
        } | {"current_game_revisions"}

        with patch("zavant.projection.glue_job._validate_table_schema"):
            _ensure_tables(spark, configuration, existing)

        spark.sql.assert_not_called()


class CurrentViewTests(unittest.TestCase):
    def test_builds_one_current_view_for_every_analytical_table(self) -> None:
        views = all_current_views("zavant_analytical_prod")

        self.assertEqual(
            tuple(view.name for view in views),
            tuple(f"current_{name}" for name in TABLE_CONTRACTS),
        )

    def test_current_view_resolves_revision_without_exposing_revision_keys(self) -> None:
        sql = create_current_view_sql(
            "zavant_analytical_prod",
            TABLE_CONTRACTS["plays"],
        )

        self.assertIn('CREATE OR REPLACE VIEW "current_plays"', sql)
        self.assertIn('history."game_pk" = current_revision."game_pk"', sql)
        self.assertIn(
            'history."source_revision_id" = current_revision."source_revision_id"',
            sql,
        )
        selection = sql.split("FROM", maxsplit=1)[0]
        for private_column in PRIVATE_COLUMNS:
            self.assertNotIn(f'history."{private_column}"', selection)

    def test_current_games_exposes_freshness_time_and_source_revision(self) -> None:
        sql = create_current_view_sql(
            "zavant_analytical_prod",
            TABLE_CONTRACTS["games"],
        )

        self.assertIn('current_revision."reconciled_at"', sql)
        self.assertIn('current_revision."source_revision_id"', sql)

    def test_publisher_waits_for_each_athena_ddl(self) -> None:
        client = _FakeAthenaClient()

        publish_current_views(
            client,
            "zavant_analytical_prod",
            "primary",
            "s3://example-bucket/results/",
            poll_interval_seconds=0,
            wait=lambda _seconds: None,
        )

        self.assertEqual(len(client.started), len(TABLE_CONTRACTS))
        self.assertEqual(len(client.inspected), len(TABLE_CONTRACTS))
        first = client.started[0]
        self.assertEqual(first["WorkGroup"], "primary")
        self.assertEqual(
            first["ResultConfiguration"],
            {"OutputLocation": "s3://example-bucket/results/"},
        )

    def test_publication_marker_skips_unchanged_complete_view_set(self) -> None:
        client = FakeS3Client()
        backend = S3ObjectBackend(client, "example-bucket", "lake")
        existing = {view.name for view in all_current_views("analytics")}

        self.assertTrue(current_views_need_publication(backend, "analytics", existing))
        record_current_view_publication(backend, "analytics")

        self.assertFalse(current_views_need_publication(backend, "analytics", existing))


class GlueProjectionCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.configuration = GlueProjectionConfiguration(
            bucket="example-bucket",
            database="zavant_analytical_prod",
        )
        self.revision = ProjectionRevision(
            game_pk=744863,
            season=2024,
            revision_id="revision-one",
            raw_key="raw/mlb_stats_api/games/season=2024/game_pk=744863/revision=revision-one/game.json",
            metadata_key="raw/mlb_stats_api/games/season=2024/game_pk=744863/revision=revision-one/metadata.json",
        )
        self.run_id = UUID("00000000-0000-0000-0000-000000000016")
        self.projection = GameProjection(
            table_rows={name: () for name in TABLE_CONTRACTS},
            event_kind_counts={},
        )

    def test_merges_completion_marker_after_every_analytical_table(self) -> None:
        spark = _FakeSpark()
        events = []

        with (
            patch("zavant.projection.glue_job._ensure_tables"),
            patch("zavant.projection.glue_job._existing_catalog_tables", return_value=set()),
            patch("zavant.projection.glue_job._current_revision_cache", return_value={}),
            patch("zavant.projection.glue_job.publish_current_views"),
            patch(
                "zavant.projection.glue_job._completed_projections",
                return_value=set(),
            ),
            patch(
                "zavant.projection.glue_job.discover_projection_inventory",
                return_value=ProjectionInventory((self.revision,), ()),
            ),
            patch(
                "zavant.projection.glue_job.resolve_current_revisions",
                return_value=CurrentRevisionDiscovery(
                    (self.revision,),
                    (self.revision,),
                ),
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
                _FakeAthenaClient(),
                _FakeGlueClient(),
                self.configuration,
                self.run_id,
                OBSERVED_AT,
            )

        self.assertEqual(
            events[: len(TABLE_CONTRACTS)],
            [contract.name for contract in _analytical_merge_contracts()],
        )
        self.assertEqual(events[-2:], ["games", "current_game_revisions"])
        self.assertEqual(result.projected_revision_count, 1)
        self.assertTrue(spark.last_rdd.unpersisted)

    def test_failed_table_merge_does_not_advance_current_mapping(self) -> None:
        spark = _FakeSpark()

        with (
            patch("zavant.projection.glue_job._ensure_tables"),
            patch("zavant.projection.glue_job._existing_catalog_tables", return_value=set()),
            patch("zavant.projection.glue_job._current_revision_cache", return_value={}),
            patch("zavant.projection.glue_job.publish_current_views"),
            patch(
                "zavant.projection.glue_job._completed_projections",
                return_value=set(),
            ),
            patch(
                "zavant.projection.glue_job.discover_projection_inventory",
                return_value=ProjectionInventory((self.revision,), ()),
            ),
            patch(
                "zavant.projection.glue_job.resolve_current_revisions",
                return_value=CurrentRevisionDiscovery(
                    (self.revision,),
                    (self.revision,),
                ),
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
                    _FakeAthenaClient(),
                    _FakeGlueClient(),
                    self.configuration,
                    self.run_id,
                    OBSERVED_AT,
                )

        merge_rows.assert_not_called()
        self.assertTrue(spark.last_rdd.unpersisted)

    def test_failed_view_publication_does_not_advance_current_mapping(self) -> None:
        spark = _FakeSpark()

        with (
            patch("zavant.projection.glue_job._ensure_tables"),
            patch("zavant.projection.glue_job._existing_catalog_tables", return_value=set()),
            patch("zavant.projection.glue_job._current_revision_cache", return_value={}),
            patch(
                "zavant.projection.glue_job._completed_projections",
                return_value={self.revision.completed_identity()},
            ),
            patch(
                "zavant.projection.glue_job.discover_projection_inventory",
                return_value=ProjectionInventory((self.revision,), ()),
            ),
            patch(
                "zavant.projection.glue_job.resolve_current_revisions",
                return_value=CurrentRevisionDiscovery(
                    (self.revision,),
                    (self.revision,),
                ),
            ),
            patch(
                "zavant.projection.glue_job.publish_current_views",
                side_effect=RuntimeError("Athena DDL failed"),
            ),
            patch("zavant.projection.glue_job._merge_rows") as merge_rows,
        ):
            with self.assertRaisesRegex(RuntimeError, "Athena DDL failed"):
                run_glue_projection(
                    spark,
                    FakeS3Client(),
                    _FakeAthenaClient(),
                    _FakeGlueClient(),
                    self.configuration,
                    self.run_id,
                    OBSERVED_AT,
                )

        merge_rows.assert_not_called()


class _FakeConfiguration:
    def set(self, _name: str, _value: str) -> None:
        pass


class _FakeAthenaClient:
    def __init__(self) -> None:
        self.started: list[dict[str, object]] = []
        self.inspected: list[str] = []

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {"QueryExecutionId": f"query-{len(self.started)}"}

    def get_query_execution(self, **kwargs):
        self.inspected.append(kwargs["QueryExecutionId"])
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}


class _FakeGlueClient:
    def get_tables(self, **_kwargs):
        return {"TableList": []}


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
