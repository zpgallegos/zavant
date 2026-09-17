"""External lake assets: ownership and lineage, with no AWS execution logic."""

import dagster as dg

from zavant.orchestration.sources import ANALYTICAL_DATABASE, PUBLICATION_SOURCES
from zavant.projection.current_views import all_current_views


def athena_relation_asset_key(database: str, relation: str) -> dg.AssetKey:
    return dg.AssetKey(["aws", "athena", database, relation])


def _relation_key(relation: str) -> dg.AssetKey:
    return athena_relation_asset_key(ANALYTICAL_DATABASE, relation)


RAW_ASSET_SPECS = tuple(
    dg.AssetSpec(
        key=dg.AssetKey(["aws", "s3", source.raw_asset_name]),
        group_name="acquisition",
        kinds={"s3"},
        description="Raw dataset loaded externally by the Step Functions acquisition workflow.",
        metadata={"source_family": source.name, "producer": "Step Functions"},
    )
    for source in PUBLICATION_SOURCES
)
SOURCE_BY_TABLE = {
    contract.name: source
    for source in PUBLICATION_SOURCES
    for contract in source.tables
}


def _table_specs() -> list[dg.AssetSpec]:
    specs = []
    for source, raw_spec in zip(PUBLICATION_SOURCES, RAW_ASSET_SPECS, strict=True):
        for contract in source.tables:
            specs.append(
                dg.AssetSpec(
                    key=_relation_key(contract.name),
                    deps=[raw_spec.key],
                    group_name="athena_tables",
                    kinds={"athena", "iceberg"},
                    description="Iceberg history/control table published externally by Glue.",
                    metadata={
                        "source_family": source.name,
                        "dagster/table_name": f"awsdatacatalog.{ANALYTICAL_DATABASE}.{contract.name}",
                        "dagster/column_schema": dg.TableSchema(
                            columns=[
                                dg.TableColumn(
                                    name=column.name,
                                    type=column.kind,
                                    constraints=dg.TableColumnConstraints(
                                        nullable=column.nullable
                                    ),
                                )
                                for column in contract.columns
                            ]
                        ),
                    },
                )
            )
    return specs


def _view_specs() -> list[dg.AssetSpec]:
    specs = []
    for view in all_current_views(ANALYTICAL_DATABASE):
        history_name = view.name.removeprefix("current_")
        source = SOURCE_BY_TABLE[history_name]
        specs.append(
            dg.AssetSpec(
                key=_relation_key(view.name),
                deps=[_relation_key(history_name), _relation_key(source.mapping_table)],
                group_name="athena_views",
                kinds={"athena"},
                description="Current-state view over history and its source's revision mapping.",
                metadata={
                    "source_family": source.name,
                    "dagster/table_name": f"awsdatacatalog.{ANALYTICAL_DATABASE}.{view.name}",
                    "view_sql": dg.MetadataValue.text(view.sql),
                },
            )
        )
    return specs


# Inventory comes from the producer, including relations not consumed by dbt.
ATHENA_ASSET_SPECS = (*_table_specs(), *_view_specs())
EXTERNAL_ASSET_SPECS = (*RAW_ASSET_SPECS, *ATHENA_ASSET_SPECS)
SOURCE_BY_ASSET_KEY = {
    spec.key: str(spec.metadata["source_family"]) for spec in EXTERNAL_ASSET_SPECS
}
