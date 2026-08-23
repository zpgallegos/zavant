"""Iceberg table definitions and deterministic Spark SQL generation."""

from __future__ import annotations

import re
from zavant.projection.contracts import Column, TableContract


def qualified_table(catalog: str, database: str, table: str) -> str:
    """Return a safely quoted three-part Spark table identifier."""

    return ".".join(f"`{_identifier(part)}`" for part in (catalog, database, table))


def create_database_sql(catalog: str, database: str) -> str:
    return f"CREATE DATABASE IF NOT EXISTS {qualified_namespace(catalog, database)}"


def qualified_namespace(catalog: str, database: str) -> str:
    return ".".join(f"`{_identifier(part)}`" for part in (catalog, database))


def create_table_sql(
    catalog: str,
    database: str,
    contract: TableContract,
    warehouse_uri: str,
) -> str:
    """Create one format-v2 Iceberg table with an explicit schema and location."""

    location = f"{warehouse_uri.rstrip('/')}/{contract.name}"
    columns = ",\n  ".join(_column_definition(column) for column in contract.columns)
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_table(catalog, database, contract.name)} (\n"
        f"  {columns}\n"
        ")\n"
        "USING iceberg\n"
        "PARTITIONED BY (season)\n"
        f"LOCATION '{_sql_string(location)}'\n"
        "TBLPROPERTIES (\n"
        "  'format-version' = '2',\n"
        "  'write.parquet.compression-codec' = 'zstd'\n"
        ")"
    )


def merge_table_sql(
    catalog: str,
    database: str,
    contract: TableContract,
    source_view: str,
) -> str:
    """Build an idempotent Iceberg merge using a contract's natural key."""

    target = qualified_table(catalog, database, contract.name)
    source = f"`{_identifier(source_view)}`"
    predicate_columns = (
        *contract.primary_key,
        *(("season",) if "season" not in contract.primary_key else ()),
    )
    predicate = " AND ".join(
        f"target.`{column}` = source.`{column}`" for column in predicate_columns
    )
    assignments = ",\n  ".join(
        f"target.`{column.name}` = source.`{column.name}`"
        for column in contract.columns
    )
    column_names = ", ".join(f"`{column.name}`" for column in contract.columns)
    source_columns = ", ".join(
        f"source.`{column.name}`" for column in contract.columns
    )
    return (
        f"MERGE INTO {target} AS target\n"
        f"USING {source} AS source\n"
        f"ON {predicate}\n"
        "WHEN MATCHED THEN UPDATE SET\n"
        f"  {assignments}\n"
        f"WHEN NOT MATCHED THEN INSERT ({column_names})\n"
        f"VALUES ({source_columns})"
    )


def _column_definition(column: Column) -> str:
    required = " NOT NULL" if not column.nullable else ""
    return f"`{column.name}` {_spark_sql_type(column)}{required}"


def _spark_sql_type(column: Column) -> str:
    return {
        "boolean": "BOOLEAN",
        "date": "DATE",
        "float64": "DOUBLE",
        "int32": "INT",
        "int64": "BIGINT",
        "string": "STRING",
        "timestamp": "TIMESTAMP",
    }[column.kind]


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"invalid SQL identifier: {value}")
    return value


def _sql_string(value: str) -> str:
    if "'" in value or not value.startswith("s3://"):
        raise ValueError(f"invalid Iceberg warehouse URI: {value}")
    return value
