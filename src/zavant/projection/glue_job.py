"""AWS Glue composition for reconciling raw revisions into Iceberg tables."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
import logging
import sys
from time import monotonic
from typing import Any, Dict, Iterable, Iterator, Mapping, Protocol, Sequence, Set, cast
from uuid import UUID, uuid4

from zavant.projection.contracts import (
    PROJECTION_CONTRACT_VERSION,
    Column,
    TableContract,
    TABLE_CONTRACTS,
)
from zavant.projection.current_views import (
    AthenaClient,
    current_views_need_publication,
    publish_current_views,
    record_current_view_publication,
)
from zavant.projection.iceberg import (
    CURRENT_REVISION_CONTRACT,
    all_iceberg_contracts,
    create_table_sql,
    merge_table_sql,
    qualified_table,
)
from zavant.projection.models import GameProjection
from zavant.projection.projector import project_game
from zavant.projection.s3_sources import (
    CompletedProjection,
    CurrentRevisionCacheEntry,
    ProjectionRevision,
    discover_projection_inventory,
    load_projection_source,
    pending_revisions,
    resolve_current_revisions,
    validate_current_revisions,
)
from zavant.storage.s3_objects import S3Client, S3ObjectBackend


LOGGER = logging.getLogger(__name__)


class GlueCatalogClient(Protocol):
    """Subset of the Boto3 Glue client used to inspect catalog tables."""

    def get_tables(self, **kwargs: Any) -> Dict[str, Any]:
        ...


@dataclass(frozen=True)
class GlueProjectionConfiguration:
    """Runtime-owned names and scaling limits for one Glue reconciliation."""

    bucket: str
    database: str
    prefix: str = "lake"
    catalog: str = "glue_catalog"
    athena_workgroup: str = "primary"
    max_projection_partitions: int = 64

    def __post_init__(self) -> None:
        if not self.bucket or "/" in self.bucket:
            raise ValueError("bucket must be a non-empty S3 bucket name")
        if not self.database:
            raise ValueError("database must not be empty")
        if not self.athena_workgroup:
            raise ValueError("athena_workgroup must not be empty")
        if self.max_projection_partitions <= 0:
            raise ValueError("max_projection_partitions must be positive")

    @property
    def warehouse_uri(self) -> str:
        suffix = "/".join(
            part for part in (self.prefix.strip("/"), "analytical", "iceberg") if part
        )
        return f"s3://{self.bucket}/{suffix}"

    @property
    def athena_output_uri(self) -> str:
        suffix = "/".join(
            part
            for part in (
                self.prefix.strip("/"),
                "analytical",
                "athena-results",
                "projection-views",
            )
            if part
        )
        return f"s3://{self.bucket}/{suffix}/"


@dataclass(frozen=True)
class GlueProjectionResult:
    """Summary emitted after a complete production reconciliation."""

    run_id: UUID
    current_revision_count: int
    projected_revision_count: int
    contract_version: str = PROJECTION_CONTRACT_VERSION

    def as_dict(self) -> Dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "current_revision_count": self.current_revision_count,
            "projected_revision_count": self.projected_revision_count,
            "run_id": str(self.run_id),
        }


def run_glue_projection(
    spark: Any,
    s3_client: S3Client,
    athena_client: AthenaClient,
    glue_client: GlueCatalogClient,
    configuration: GlueProjectionConfiguration,
    run_id: UUID,
    projected_at: datetime,
) -> GlueProjectionResult:
    """Reconcile all immutable S3 revisions and publish missing ones to Iceberg.

    The one-row-per-revision ``games`` table is merged last and acts as the
    completion marker. A retry therefore reprocesses partially committed
    revisions and repairs them through deterministic natural-key merges.
    """

    if projected_at.utcoffset() is None:
        raise ValueError("projected_at must be timezone-aware")
    projected_at_utc = projected_at.astimezone(timezone.utc)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    with _timed_phase("catalog_inventory"):
        existing_tables = _existing_catalog_tables(
            glue_client,
            configuration.database,
        )
    with _timed_phase("table_ensure_and_schema_validation"):
        _ensure_tables(spark, configuration, existing_tables)

    backend = S3ObjectBackend(
        s3_client,
        configuration.bucket,
        configuration.prefix,
    )
    with _timed_phase("current_revision_cache"):
        current_cache = _current_revision_cache(spark, configuration)
    with _timed_phase("s3_projection_inventory"):
        inventory = discover_projection_inventory(backend)
    with _timed_phase("current_pointer_resolution"):
        current_discovery = resolve_current_revisions(
            backend,
            inventory.current_pointers,
            current_cache,
        )
    with _timed_phase("completed_revision_scan"):
        completed = _completed_projections(spark, configuration)
    revisions = inventory.revisions
    current = current_discovery.revisions
    validate_current_revisions(revisions, current)
    pending = pending_revisions(revisions, completed)
    LOGGER.info(
        "projection reconciliation discovered revisions=%d current=%d "
        "pointers_read=%d pending=%d",
        len(revisions),
        len(current),
        len(current_discovery.refreshed),
        len(pending),
    )

    projection_rdd: Any = None
    if pending:
        partitions = min(len(pending), configuration.max_projection_partitions)
        candidates = spark.sparkContext.parallelize(list(pending), partitions)
        projection_rdd = candidates.mapPartitions(
            lambda values: _project_partition(
                values,
                configuration.bucket,
                configuration.prefix,
                run_id,
                projected_at_utc,
            )
        )
        storage_level = import_module("pyspark").StorageLevel.MEMORY_AND_DISK
        projection_rdd.persist(storage_level)
        try:
            projected_count = projection_rdd.count()
            if projected_count != len(pending):
                raise RuntimeError(
                    "projected game count does not match pending revision count"
                )
            for contract in _analytical_merge_contracts():
                rows = projection_rdd.flatMap(
                    lambda projection, name=contract.name: projection.table_rows[name]
                )
                dataframe = spark.createDataFrame(rows, _spark_schema(contract.columns))
                with _timed_phase(f"merge_{contract.name}"):
                    _merge_dataframe(spark, configuration, contract, dataframe)
        finally:
            projection_rdd.unpersist()

    with _timed_phase("current_view_publication"):
        expected_history_tables = {
            contract.name
            for contract in all_iceberg_contracts(TABLE_CONTRACTS.values())
        }
        view_catalog = (
            existing_tables
            if expected_history_tables.issubset(existing_tables)
            else set()
        )
        if current_views_need_publication(
            backend,
            configuration.database,
            view_catalog,
        ):
            publish_current_views(
                athena_client,
                configuration.database,
                configuration.athena_workgroup,
                configuration.athena_output_uri,
            )
            record_current_view_publication(backend, configuration.database)
        else:
            LOGGER.info("current analytical view definitions are unchanged")

    current_rows = [
        {
            "game_pk": revision.game_pk,
            "season": revision.season,
            "source_revision_id": revision.revision_id,
            "projection_contract_version": PROJECTION_CONTRACT_VERSION,
            "projection_run_id": str(run_id),
            "reconciled_at": projected_at_utc,
            "raw_object_uri": backend.uri(revision.raw_key),
        }
        for revision in current
    ]
    if current_rows:
        with _timed_phase("current_revision_merge"):
            _merge_rows(
                spark,
                configuration,
                CURRENT_REVISION_CONTRACT,
                current_rows,
            )
    return GlueProjectionResult(
        run_id=run_id,
        current_revision_count=len(current),
        projected_revision_count=len(pending),
    )


def main(argv: Sequence[str] = sys.argv[1:]) -> None:
    """Compose the production job from Glue arguments and managed services."""

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--JOB_NAME", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="lake")
    parser.add_argument("--database", required=True)
    parser.add_argument("--catalog", default="glue_catalog")
    parser.add_argument("--athena-workgroup", default="primary")
    parser.add_argument("--max-projection-partitions", type=int, default=64)
    arguments, _ = parser.parse_known_args(argv)

    spark_session = import_module("pyspark.sql").SparkSession.builder.getOrCreate()
    boto3 = import_module("boto3")
    configuration = GlueProjectionConfiguration(
        bucket=arguments.bucket,
        prefix=arguments.prefix,
        database=arguments.database,
        catalog=arguments.catalog,
        athena_workgroup=arguments.athena_workgroup,
        max_projection_partitions=arguments.max_projection_partitions,
    )
    result = run_glue_projection(
        spark=spark_session,
        s3_client=cast(S3Client, boto3.client("s3")),
        athena_client=cast(AthenaClient, boto3.client("athena")),
        glue_client=cast(GlueCatalogClient, boto3.client("glue")),
        configuration=configuration,
        run_id=uuid4(),
        projected_at=datetime.now(timezone.utc),
    )
    LOGGER.info("projection reconciliation complete: %s", result.as_dict())


def _project_partition(
    revisions: Iterable[ProjectionRevision],
    bucket: str,
    prefix: str,
    run_id: UUID,
    projected_at: datetime,
) -> Iterator[GameProjection]:
    boto3 = import_module("boto3")
    backend = S3ObjectBackend(cast(S3Client, boto3.client("s3")), bucket, prefix)
    for revision in revisions:
        yield project_game(
            load_projection_source(backend, revision),
            run_id=run_id,
            projected_at=projected_at,
        )


def _ensure_tables(
    spark: Any,
    configuration: GlueProjectionConfiguration,
    existing_tables: Set[str],
) -> None:
    contracts = all_iceberg_contracts(TABLE_CONTRACTS.values())
    for contract in contracts:
        if contract.name not in existing_tables:
            spark.sql(
                create_table_sql(
                    configuration.catalog,
                    configuration.database,
                    contract,
                    configuration.warehouse_uri,
                )
            )
        _validate_table_schema(spark, configuration, contract)


def _existing_catalog_tables(
    client: GlueCatalogClient,
    database: str,
) -> Set[str]:
    names = set()
    next_token: str | None = None
    while True:
        request: Dict[str, Any] = {"DatabaseName": database}
        if next_token is not None:
            request["NextToken"] = next_token
        response = client.get_tables(**request)
        tables = response.get("TableList")
        if not isinstance(tables, list):
            raise RuntimeError("Glue returned no catalog table list")
        for table in tables:
            if not isinstance(table, dict) or not isinstance(table.get("Name"), str):
                raise RuntimeError("Glue returned invalid catalog table metadata")
            names.add(table["Name"])
        token = response.get("NextToken")
        if token is None:
            return names
        if not isinstance(token, str) or not token:
            raise RuntimeError("Glue returned an invalid catalog continuation token")
        next_token = token


def _validate_table_schema(
    spark: Any,
    configuration: GlueProjectionConfiguration,
    contract: TableContract,
) -> None:
    table = spark.table(
        qualified_table(configuration.catalog, configuration.database, contract.name)
    )
    observed = tuple(
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in table.schema.fields
    )
    expected = tuple(
        (column.name, _spark_type_name(column), column.nullable)
        for column in contract.columns
    )
    if observed != expected:
        details = _schema_drift_details(observed, expected)
        raise RuntimeError(
            f"Iceberg table {contract.name} does not match its projection contract: "
            f"{details}. Rebuild every analytical table before deploying this "
            "projection contract"
        )


def _schema_drift_details(
    observed: Sequence[tuple[str, str, bool]],
    expected: Sequence[tuple[str, str, bool]],
) -> str:
    observed_by_name = {name: (kind, nullable) for name, kind, nullable in observed}
    expected_by_name = {name: (kind, nullable) for name, kind, nullable in expected}
    missing = sorted(set(expected_by_name) - set(observed_by_name))
    unexpected = sorted(set(observed_by_name) - set(expected_by_name))
    incompatible = {
        name: {
            "observed": observed_by_name[name],
            "expected": expected_by_name[name],
        }
        for name in sorted(set(observed_by_name).intersection(expected_by_name))
        if observed_by_name[name] != expected_by_name[name]
    }
    details = [
        f"missing={missing}",
        f"unexpected={unexpected}",
        f"incompatible={incompatible}",
    ]
    if not missing and not unexpected and not incompatible and observed != expected:
        details.append(
            "column_order_changed="
            f"observed={[name for name, _, _ in observed]} "
            f"expected={[name for name, _, _ in expected]}"
        )
    return " ".join(details)


def _completed_projections(
    spark: Any,
    configuration: GlueProjectionConfiguration,
) -> Set[CompletedProjection]:
    table = spark.table(
        qualified_table(
            configuration.catalog,
            configuration.database,
            TABLE_CONTRACTS["games"].name,
        )
    )
    completed = set()
    versions = set()
    for row in table.select(
        "game_pk",
        "source_revision_id",
        "projection_contract_version",
    ).collect():
        completed.add((int(row["game_pk"]), str(row["source_revision_id"])))
        versions.add(str(row["projection_contract_version"]))
    _validate_projection_versions(versions)
    return completed


def _current_revision_cache(
    spark: Any,
    configuration: GlueProjectionConfiguration,
) -> Dict[int, CurrentRevisionCacheEntry]:
    table = spark.table(
        qualified_table(
            configuration.catalog,
            configuration.database,
            CURRENT_REVISION_CONTRACT.name,
        )
    )
    cache = {}
    for row in table.select(
        "game_pk",
        "season",
        "source_revision_id",
        "reconciled_at",
    ).collect():
        reconciled_at = cast(datetime, row["reconciled_at"])
        if reconciled_at.utcoffset() is None:
            reconciled_at = reconciled_at.replace(tzinfo=timezone.utc)
        game_pk = int(row["game_pk"])
        cache[game_pk] = CurrentRevisionCacheEntry(
            game_pk=game_pk,
            season=int(row["season"]),
            revision_id=str(row["source_revision_id"]),
            reconciled_at=reconciled_at.astimezone(timezone.utc),
        )
    return cache


def _validate_projection_release(
    spark: Any,
    configuration: GlueProjectionConfiguration,
) -> None:
    """Require a table rebuild before publishing a new projection contract."""

    table = spark.table(
        qualified_table(
            configuration.catalog,
            configuration.database,
            TABLE_CONTRACTS["games"].name,
        )
    )
    observed = {
        str(row["projection_contract_version"])
        for row in table.select("projection_contract_version").distinct().collect()
    }
    _validate_projection_versions(observed)


def _validate_projection_versions(observed: Set[str]) -> None:
    incompatible = observed - {PROJECTION_CONTRACT_VERSION}
    if incompatible:
        raise RuntimeError(
            "analytical tables contain projection contract version(s) "
            f"{sorted(incompatible)} but this job publishes "
            f"{PROJECTION_CONTRACT_VERSION}; rebuild every analytical table "
            "before deploying a new projection release"
        )


def _analytical_merge_contracts() -> tuple[TableContract, ...]:
    """Order analytical merges so ``games`` is the completion marker."""

    games = TABLE_CONTRACTS["games"]
    return (
        *(contract for contract in TABLE_CONTRACTS.values() if contract is not games),
        games,
    )


def _merge_rows(
    spark: Any,
    configuration: GlueProjectionConfiguration,
    contract: TableContract,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    dataframe = spark.createDataFrame(list(rows), _spark_schema(contract.columns))
    _merge_dataframe(spark, configuration, contract, dataframe)


def _merge_dataframe(
    spark: Any,
    configuration: GlueProjectionConfiguration,
    contract: TableContract,
    dataframe: Any,
) -> None:
    source_view = f"zavant_stage_{contract.name}"
    dataframe.createOrReplaceTempView(source_view)
    spark.sql(
        merge_table_sql(
            configuration.catalog,
            configuration.database,
            contract,
            source_view,
        )
    )
    spark.catalog.dropTempView(source_view)


def _spark_schema(columns: Sequence[Column]) -> Any:
    types = import_module("pyspark.sql.types")
    return types.StructType(
        [
            types.StructField(
                column.name,
                _spark_data_type(types, column),
                column.nullable,
            )
            for column in columns
        ]
    )


def _spark_data_type(types: Any, column: Column) -> Any:
    return {
        "boolean": types.BooleanType,
        "date": types.DateType,
        "float64": types.DoubleType,
        "int32": types.IntegerType,
        "int64": types.LongType,
        "string": types.StringType,
        "timestamp": types.TimestampType,
    }[column.kind]()


def _spark_type_name(column: Column) -> str:
    return {
        "boolean": "boolean",
        "date": "date",
        "float64": "double",
        "int32": "int",
        "int64": "bigint",
        "string": "string",
        "timestamp": "timestamp",
    }[column.kind]


@contextmanager
def _timed_phase(name: str) -> Iterator[None]:
    started = monotonic()
    try:
        yield
    finally:
        LOGGER.info(
            "projection phase=%s elapsed_seconds=%.3f",
            name,
            monotonic() - started,
        )


if __name__ == "__main__":
    main()
