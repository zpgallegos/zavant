"""AWS Glue composition for reconciling raw revisions into Iceberg tables."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
import logging
import sys
from typing import Any, Dict, Iterable, Iterator, Mapping, Sequence, Set, cast
from uuid import UUID, uuid4

from zavant.projection.contracts import (
    PROJECTION_CONTRACT_VERSION,
    Column,
    TableContract,
    TABLE_CONTRACTS,
)
from zavant.projection.iceberg import (
    CURRENT_REVISION_CONTRACT,
    PROJECTION_REGISTRY_CONTRACT,
    all_iceberg_contracts,
    create_database_sql,
    create_table_sql,
    merge_table_sql,
    qualified_table,
)
from zavant.projection.models import GameProjection
from zavant.projection.projector import project_game
from zavant.projection.s3_sources import (
    CompletedProjection,
    CurrentProjectionRevision,
    discover_current_revisions,
    load_projection_source,
    pending_current_revisions,
)
from zavant.storage.s3_objects import S3Client, S3ObjectBackend


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class GlueProjectionConfiguration:
    """Runtime-owned names and scaling limits for one Glue reconciliation."""

    bucket: str
    database: str
    prefix: str = "lake"
    catalog: str = "glue_catalog"
    max_projection_partitions: int = 64

    def __post_init__(self) -> None:
        if not self.bucket or "/" in self.bucket:
            raise ValueError("bucket must be a non-empty S3 bucket name")
        if not self.database:
            raise ValueError("database must not be empty")
        if self.max_projection_partitions <= 0:
            raise ValueError("max_projection_partitions must be positive")

    @property
    def warehouse_uri(self) -> str:
        suffix = "/".join(
            part for part in (self.prefix.strip("/"), "analytical", "iceberg") if part
        )
        return f"s3://{self.bucket}/{suffix}"


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
    configuration: GlueProjectionConfiguration,
    run_id: UUID,
    projected_at: datetime,
) -> GlueProjectionResult:
    """Reconcile all current S3 revisions and publish missing ones to Iceberg.

    The completion registry is updated only after every analytical merge
    succeeds. A retry therefore reprocesses partially committed revisions and
    repairs them through deterministic natural-key merges.
    """

    if projected_at.utcoffset() is None:
        raise ValueError("projected_at must be timezone-aware")
    projected_at_utc = projected_at.astimezone(timezone.utc)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    _ensure_tables(spark, configuration)

    backend = S3ObjectBackend(
        s3_client,
        configuration.bucket,
        configuration.prefix,
    )
    current = discover_current_revisions(backend)
    completed = _completed_projections(spark, configuration)
    pending = pending_current_revisions(current, completed)
    LOGGER.info(
        "projection reconciliation discovered current=%d pending=%d",
        len(current),
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
        storage_level = getattr(
            import_module("pyspark"), "StorageLevel"
        ).MEMORY_AND_DISK
        projection_rdd.persist(storage_level)
        try:
            projected_count = projection_rdd.count()
            if projected_count != len(pending):
                raise RuntimeError(
                    "projected game count does not match pending revision count"
                )
            for contract in TABLE_CONTRACTS.values():
                rows = projection_rdd.flatMap(
                    lambda projection, name=contract.name: projection.table_rows[name]
                )
                dataframe = spark.createDataFrame(rows, _spark_schema(contract.columns))
                _merge_dataframe(spark, configuration, contract, dataframe)
            registry_rows = [
                {
                    "game_pk": revision.game_pk,
                    "season": revision.season,
                    "source_revision_id": revision.revision_id,
                    "projection_contract_version": PROJECTION_CONTRACT_VERSION,
                    "projection_run_id": str(run_id),
                    "projected_at": projected_at_utc,
                    "raw_object_uri": backend.uri(revision.raw_key),
                }
                for revision in pending
            ]
            _merge_rows(
                spark,
                configuration,
                PROJECTION_REGISTRY_CONTRACT,
                registry_rows,
            )
        finally:
            projection_rdd.unpersist()

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
    parser.add_argument("--max-projection-partitions", type=int, default=64)
    arguments, _ = parser.parse_known_args(argv)

    spark_session = getattr(
        import_module("pyspark.sql"), "SparkSession"
    ).builder.getOrCreate()
    boto3 = import_module("boto3")
    configuration = GlueProjectionConfiguration(
        bucket=arguments.bucket,
        prefix=arguments.prefix,
        database=arguments.database,
        catalog=arguments.catalog,
        max_projection_partitions=arguments.max_projection_partitions,
    )
    result = run_glue_projection(
        spark=spark_session,
        s3_client=cast(S3Client, boto3.client("s3")),
        configuration=configuration,
        run_id=uuid4(),
        projected_at=datetime.now(timezone.utc),
    )
    LOGGER.info("projection reconciliation complete: %s", result.as_dict())


def _project_partition(
    revisions: Iterable[CurrentProjectionRevision],
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


def _ensure_tables(spark: Any, configuration: GlueProjectionConfiguration) -> None:
    spark.sql(create_database_sql(configuration.catalog, configuration.database))
    contracts = all_iceberg_contracts(TABLE_CONTRACTS.values())
    for contract in contracts:
        spark.sql(
            create_table_sql(
                configuration.catalog,
                configuration.database,
                contract,
                configuration.warehouse_uri,
            )
        )
        _validate_table_schema(spark, configuration, contract)


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
        raise RuntimeError(
            f"Iceberg table {contract.name} does not match its projection contract"
        )


def _completed_projections(
    spark: Any,
    configuration: GlueProjectionConfiguration,
) -> Set[CompletedProjection]:
    table = spark.table(
        qualified_table(
            configuration.catalog,
            configuration.database,
            PROJECTION_REGISTRY_CONTRACT.name,
        )
    )
    return {
        (int(row["game_pk"]), str(row["source_revision_id"]), str(row["projection_contract_version"]))
        for row in table.select(
            "game_pk", "source_revision_id", "projection_contract_version"
        ).collect()
    }


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


if __name__ == "__main__":
    main()
