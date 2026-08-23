"""AWS Glue composition for reconciling raw revisions into Iceberg tables."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from importlib import import_module
import logging
import sys
from time import monotonic
from typing import Any, Dict, Iterable, Iterator, Mapping, Protocol, Sequence, Set, cast
from uuid import UUID, uuid4

from zavant.projection.baseball_savant.contracts import (
    CURRENT_STATCAST_DATE_REVISIONS_CONTRACT,
    STATCAST_DATES_CONTRACT,
    STATCAST_HISTORY_CONTRACTS,
    STATCAST_ICEBERG_CONTRACTS,
    STATCAST_PROJECTION_CONTRACT_VERSION,
)
from zavant.projection.baseball_savant.models import StatcastDateProjection
from zavant.projection.baseball_savant.projector import project_statcast_date
from zavant.projection.baseball_savant.s3_sources import (
    CompletedStatcastProjection,
    CurrentStatcastRevisionCacheEntry,
    StatcastProjectionRevision,
    discover_statcast_projection_inventory,
    load_statcast_projection_source,
    pending_statcast_revisions,
    resolve_current_statcast_revisions,
    validate_current_statcast_revisions,
)
from zavant.projection.contracts import (
    Column,
    TableContract,
)
from zavant.projection.current_views import (
    AthenaClient,
    current_views_need_publication,
    publish_current_views,
    record_current_view_publication,
)
from zavant.projection.iceberg import (
    create_table_sql,
    merge_table_sql,
    qualified_table,
)
from zavant.projection.mlb_stats_api.contracts import (
    CURRENT_REVISION_CONTRACT,
    PROJECTION_CONTRACT_VERSION,
    TABLE_CONTRACTS,
)
from zavant.projection.mlb_stats_api.models import GameProjection
from zavant.projection.mlb_stats_api.projector import project_game
from zavant.projection.mlb_stats_api.s3_sources import (
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
    current_statcast_date_revision_count: int
    projected_statcast_date_revision_count: int
    contract_version: str = PROJECTION_CONTRACT_VERSION
    statcast_contract_version: str = STATCAST_PROJECTION_CONTRACT_VERSION

    def as_dict(self) -> Dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "current_revision_count": self.current_revision_count,
            "current_statcast_date_revision_count": (
                self.current_statcast_date_revision_count
            ),
            "projected_revision_count": self.projected_revision_count,
            "projected_statcast_date_revision_count": (
                self.projected_statcast_date_revision_count
            ),
            "run_id": str(self.run_id),
            "statcast_contract_version": self.statcast_contract_version,
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
    """Reconcile Stats API and Savant raw revisions into Iceberg history.

    ``games`` and ``statcast_dates`` are the source-specific completion marker
    tables. A retry therefore reprocesses partially committed revisions and
    repairs them through deterministic natural-key merges before either source's
    current mapping is advanced.
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
        current_statcast_cache = _current_statcast_revision_cache(
            spark, configuration
        )
    with _timed_phase("s3_projection_inventory"):
        inventory = discover_projection_inventory(backend)
        statcast_inventory = discover_statcast_projection_inventory(backend)
    with _timed_phase("current_pointer_resolution"):
        current_discovery = resolve_current_revisions(
            backend,
            inventory.current_pointers,
            current_cache,
        )
        current_statcast_discovery = resolve_current_statcast_revisions(
            backend,
            statcast_inventory.current_pointers,
            current_statcast_cache,
        )
    with _timed_phase("completed_revision_scan"):
        completed = _completed_projections(spark, configuration)
        completed_statcast = _completed_statcast_projections(spark, configuration)
    revisions = inventory.revisions
    current = current_discovery.revisions
    validate_current_revisions(revisions, current)
    pending = pending_revisions(revisions, completed)
    statcast_revisions = statcast_inventory.revisions
    current_statcast = current_statcast_discovery.revisions
    validate_current_statcast_revisions(statcast_revisions, current_statcast)
    pending_statcast = pending_statcast_revisions(
        statcast_revisions, completed_statcast
    )
    LOGGER.info(
        "projection reconciliation discovered revisions=%d current=%d "
        "pointers_read=%d pending=%d",
        len(revisions),
        len(current),
        len(current_discovery.refreshed),
        len(pending),
    )
    LOGGER.info(
        "Statcast projection reconciliation discovered revisions=%d current=%d "
        "pointers_read=%d pending=%d",
        len(statcast_revisions),
        len(current_statcast),
        len(current_statcast_discovery.refreshed),
        len(pending_statcast),
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
            # games is intentionally last: its row declares that every table
            # for a revision has been merged successfully.
            for contract in _analytical_merge_contracts():
                rows = projection_rdd.flatMap(
                    lambda projection, name=contract.name: projection.table_rows[name]
                )
                dataframe = spark.createDataFrame(rows, _spark_schema(contract.columns))
                with _timed_phase(f"merge_{contract.name}"):
                    _merge_dataframe(spark, configuration, contract, dataframe)
        finally:
            projection_rdd.unpersist()

    statcast_projection_rdd: Any = None
    if pending_statcast:
        partitions = min(
            len(pending_statcast), configuration.max_projection_partitions
        )
        candidates = spark.sparkContext.parallelize(
            list(pending_statcast), partitions
        )
        statcast_projection_rdd = candidates.mapPartitions(
            lambda values: _project_statcast_partition(
                values,
                configuration.bucket,
                configuration.prefix,
                run_id,
                projected_at_utc,
            )
        )
        storage_level = import_module("pyspark").StorageLevel.MEMORY_AND_DISK
        statcast_projection_rdd.persist(storage_level)
        try:
            projected_count = statcast_projection_rdd.count()
            if projected_count != len(pending_statcast):
                raise RuntimeError(
                    "projected Statcast date count does not match pending revision count"
                )
            for contract in STATCAST_HISTORY_CONTRACTS:
                rows = statcast_projection_rdd.flatMap(
                    lambda projection, name=contract.name: projection.table_rows[name]
                )
                dataframe = spark.createDataFrame(
                    rows, _spark_schema(contract.columns)
                )
                with _timed_phase(f"merge_{contract.name}"):
                    _merge_dataframe(spark, configuration, contract, dataframe)
        finally:
            statcast_projection_rdd.unpersist()

    with _timed_phase("current_view_publication"):
        expected_history_tables = {
            contract.name
            for contract in _all_projection_contracts()
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

    # History publication and current-state selection are separate concerns.
    # Updating these rows is what makes a newly projected revision visible via
    # the public current_* views.
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
    current_statcast_rows = [
        {
            "game_date": revision.game_date,
            "season": revision.season,
            "source_revision_id": revision.revision_id,
            "projection_contract_version": STATCAST_PROJECTION_CONTRACT_VERSION,
            "projection_run_id": str(run_id),
            "reconciled_at": projected_at_utc,
            "raw_object_uri": backend.uri(revision.raw_key),
        }
        for revision in current_statcast
    ]
    if current_statcast_rows:
        with _timed_phase("current_statcast_revision_merge"):
            _merge_rows(
                spark,
                configuration,
                CURRENT_STATCAST_DATE_REVISIONS_CONTRACT,
                current_statcast_rows,
            )
    return GlueProjectionResult(
        run_id=run_id,
        current_revision_count=len(current),
        projected_revision_count=len(pending),
        current_statcast_date_revision_count=len(current_statcast),
        projected_statcast_date_revision_count=len(pending_statcast),
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


def _project_statcast_partition(
    revisions: Iterable[StatcastProjectionRevision],
    bucket: str,
    prefix: str,
    run_id: UUID,
    projected_at: datetime,
) -> Iterator[StatcastDateProjection]:
    """Project Savant date revisions inside one Spark worker partition."""

    boto3 = import_module("boto3")
    backend = S3ObjectBackend(cast(S3Client, boto3.client("s3")), bucket, prefix)
    for revision in revisions:
        yield project_statcast_date(
            load_statcast_projection_source(backend, revision),
            run_id=run_id,
            projected_at=projected_at,
        )


def _all_projection_contracts() -> tuple[TableContract, ...]:
    """Return every Iceberg history and control contract owned by this job."""

    return (
        *TABLE_CONTRACTS.values(),
        CURRENT_REVISION_CONTRACT,
        *STATCAST_ICEBERG_CONTRACTS,
    )


def _ensure_tables(
    spark: Any,
    configuration: GlueProjectionConfiguration,
    existing_tables: Set[str],
) -> None:
    for contract in _all_projection_contracts():
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


def _completed_statcast_projections(
    spark: Any,
    configuration: GlueProjectionConfiguration,
) -> Set[CompletedStatcastProjection]:
    """Read terminal Savant date revisions and reject incompatible releases."""

    table = spark.table(
        qualified_table(
            configuration.catalog,
            configuration.database,
            STATCAST_DATES_CONTRACT.name,
        )
    )
    completed = set()
    versions = set()
    for row in table.select(
        "game_date",
        "source_revision_id",
        "projection_contract_version",
    ).collect():
        completed.add((cast(date, row["game_date"]), str(row["source_revision_id"])))
        versions.add(str(row["projection_contract_version"]))
    _validate_statcast_projection_versions(versions)
    return completed


def _current_statcast_revision_cache(
    spark: Any,
    configuration: GlueProjectionConfiguration,
) -> Dict[date, CurrentStatcastRevisionCacheEntry]:
    """Load the current Savant mapping used to skip old pointer reads."""

    table = spark.table(
        qualified_table(
            configuration.catalog,
            configuration.database,
            CURRENT_STATCAST_DATE_REVISIONS_CONTRACT.name,
        )
    )
    cache = {}
    for row in table.select(
        "game_date",
        "source_revision_id",
        "reconciled_at",
    ).collect():
        game_date = cast(date, row["game_date"])
        reconciled_at = cast(datetime, row["reconciled_at"])
        if reconciled_at.utcoffset() is None:
            reconciled_at = reconciled_at.replace(tzinfo=timezone.utc)
        cache[game_date] = CurrentStatcastRevisionCacheEntry(
            game_date=game_date,
            revision_id=str(row["source_revision_id"]),
            reconciled_at=reconciled_at.astimezone(timezone.utc),
        )
    return cache


def _validate_projection_versions(observed: Set[str]) -> None:
    incompatible = observed - {PROJECTION_CONTRACT_VERSION}
    if incompatible:
        raise RuntimeError(
            "analytical tables contain projection contract version(s) "
            f"{sorted(incompatible)} but this job publishes "
            f"{PROJECTION_CONTRACT_VERSION}; rebuild every analytical table "
            "before deploying a new projection release"
        )


def _validate_statcast_projection_versions(observed: Set[str]) -> None:
    incompatible = observed - {STATCAST_PROJECTION_CONTRACT_VERSION}
    if incompatible:
        raise RuntimeError(
            "Statcast analytical tables contain projection contract version(s) "
            f"{sorted(incompatible)} but this job publishes "
            f"{STATCAST_PROJECTION_CONTRACT_VERSION}; rebuild the Statcast "
            "analytical tables before deploying this projection release"
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
