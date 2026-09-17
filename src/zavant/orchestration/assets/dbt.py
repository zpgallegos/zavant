"""Dagster definitions for executing Zavant's dbt project."""

import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, cast

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    dbt_assets,
)

from zavant.orchestration.assets.athena import (
    SOURCE_BY_ASSET_KEY,
    athena_relation_asset_key,
)
from zavant.orchestration.publication import PublicationConfig, require_publications
from zavant.orchestration.resources.acquisition import AcquisitionEvidenceResource
from zavant.orchestration.resources.athena import AthenaQueryResource
from zavant.orchestration.resources.dbt import ZAVANT_DBT_PROJECT
from zavant.orchestration.sources import SOURCES_BY_NAME, cycle_start, utc_now

_DBT_MODEL_GROUPS_BY_DIRECTORY = {
    "intermediate": "dbt_intermediate",
    "marts": "dbt_marts",
    "semantic": "dbt_semantic",
    "staging": "dbt_staging",
}


def _load_manifest(manifest_path: Path) -> Mapping[str, Any]:
    """Load a generated dbt manifest whose top-level value must be an object."""

    with manifest_path.open(encoding="utf-8") as manifest_file:
        manifest: object = json.load(manifest_file)

    if not isinstance(manifest, dict):
        raise ValueError(f"Expected a JSON object in dbt manifest: {manifest_path}")
    return cast(Mapping[str, Any], manifest)


class ZavantDagsterDbtTranslator(DagsterDbtTranslator):
    """Namespace dbt-built assets while preserving their physical source keys."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        """Keep source identities intact and prefix dbt-built assets with analytics."""

        if (
            dbt_resource_props.get("resource_type") == "source"
            and str(dbt_resource_props.get("database", "")).lower() == "awsdatacatalog"
        ):
            # dbt calls the Athena catalog "database" and the Athena database
            # "schema". Its source identifier is the actual table/view name.
            return athena_relation_asset_key(
                database=dbt_resource_props["schema"],
                relation=dbt_resource_props.get("identifier")
                or dbt_resource_props["name"],
            )
        asset_key = super().get_asset_key(dbt_resource_props)
        if dbt_resource_props.get("resource_type") in {"model", "seed", "snapshot"}:
            # Apply the same translation to outputs and ref() dependencies;
            # source() dependencies must still match their producer's keys.
            return asset_key.with_prefix("analytics")
        return asset_key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """Derive model groups from directories; Glue owns source groups."""

        original_file_path = dbt_resource_props.get("original_file_path")
        if isinstance(original_file_path, str):
            path_parts = Path(original_file_path).parts
            if len(path_parts) >= 2 and path_parts[0] == "models":
                group_name = _DBT_MODEL_GROUPS_BY_DIRECTORY.get(path_parts[1])
                if group_name is not None:
                    return group_name

        return super().get_group_name(dbt_resource_props)


DBT_MANIFEST = _load_manifest(ZAVANT_DBT_PROJECT.manifest_path)
DBT_TRANSLATOR = ZavantDagsterDbtTranslator()


def _model_sources(manifest: Mapping[str, Any]) -> dict[dg.AssetKey, frozenset[str]]:
    """Derive each model's source families from dbt's full transitive lineage."""

    nodes = {**manifest["nodes"], **manifest["sources"]}
    cache: dict[str, frozenset[str]] = {}

    def visit(unique_id: str) -> frozenset[str]:
        if unique_id in cache:
            return cache[unique_id]
        node = nodes[unique_id]
        if node["resource_type"] == "source":
            key = DBT_TRANSLATOR.get_asset_key(node)
            if key not in SOURCE_BY_ASSET_KEY:
                raise ValueError(f"Unregistered external dbt source: {key}")
            result = frozenset({SOURCE_BY_ASSET_KEY[key]})
        else:
            result = frozenset().union(
                *(
                    visit(parent)
                    for parent in node.get("depends_on", {}).get("nodes", [])
                )
            )
        cache[unique_id] = result
        return result

    return {
        DBT_TRANSLATOR.get_asset_key(node): visit(unique_id)
        for unique_id, node in manifest["nodes"].items()
        if node["resource_type"] in {"model", "seed", "snapshot"}
        and node.get("config", {}).get("materialized") != "ephemeral"
    }


DBT_MODEL_SOURCES = _model_sources(DBT_MANIFEST)


@dbt_assets(
    manifest=DBT_MANIFEST,
    dagster_dbt_translator=DBT_TRANSLATOR,
    project=ZAVANT_DBT_PROJECT,
)
def zavant_dbt_assets(
    context: dg.AssetExecutionContext,
    config: PublicationConfig,
    dbt: DbtCliResource,
    analytical_readiness_athena: AthenaQueryResource,
    acquisition_evidence: AcquisitionEvidenceResource,
) -> Iterator[
    dg.Output[Any]
    | dg.AssetMaterialization
    | dg.AssetObservation
    | dg.AssetCheckResult
    | dg.AssetCheckEvaluation
]:
    """Revalidate selected inputs, then build only the requested dbt nodes.

    Preflight also protects direct UI materializations, which may omit external
    asset checks. It is intentionally read-only against the source lake.
    """

    now = utc_now()
    source_names = frozenset().union(
        *(DBT_MODEL_SOURCES[key] for key in context.selected_asset_keys)
    )
    states = require_publications(
        tuple(SOURCES_BY_NAME[name] for name in sorted(source_names)),
        config,
        analytical_readiness_athena,
        acquisition_evidence,
        now,
    )
    for key in context.selected_asset_keys:
        context.add_asset_metadata(
            {
                "processing_cycle": cycle_start(now).date().isoformat(),
                "source_publications": {
                    name: state.publication_id for name, state in states.items()
                },
            },
            asset_key=key,
        )
    yield from dbt.cli(["build"], context=context).stream()
