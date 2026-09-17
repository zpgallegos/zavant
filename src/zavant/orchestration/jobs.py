"""Selectable dbt work; the external producer has no executable job here."""

from dataclasses import dataclass

import dagster as dg

from zavant.orchestration.assets.athena import ATHENA_ASSET_SPECS, SOURCE_BY_ASSET_KEY
from zavant.orchestration.assets.dbt import DBT_MODEL_SOURCES, zavant_dbt_assets
from zavant.orchestration.publication import READINESS_CHECK_NAME


@dataclass(frozen=True)
class DbtBranch:
    """Models sharing external prerequisites, not a manually maintained model list."""

    name: str
    sources: frozenset[str]
    keys: frozenset[dg.AssetKey]
    upstream_branches: frozenset[str]


def _branch_name(sources: frozenset[str]) -> str:
    return "_and_".join(sorted(sources)) or "independent"


def _dbt_branches() -> tuple[DbtBranch, ...]:
    groups: dict[frozenset[str], set[dg.AssetKey]] = {}
    for key in zavant_dbt_assets.keys:
        groups.setdefault(DBT_MODEL_SOURCES[key], set()).add(key)
    return tuple(
        DbtBranch(
            name=_branch_name(sources),
            sources=sources,
            keys=frozenset(keys),
            upstream_branches=frozenset(
                _branch_name(DBT_MODEL_SOURCES[parent])
                for key in keys
                for parent in zavant_dbt_assets.asset_deps[key]
                if parent in zavant_dbt_assets.keys and parent not in keys
            ),
        )
        for sources, keys in sorted(
            groups.items(), key=lambda item: (len(item[0]), sorted(item[0]))
        )
    )


def source_check_keys(sources: frozenset[str]) -> list[dg.AssetCheckKey]:
    return [
        dg.AssetCheckKey(spec.key, READINESS_CHECK_NAME)
        for spec in ATHENA_ASSET_SPECS
        if SOURCE_BY_ASSET_KEY[spec.key] in sources
    ]


DBT_BRANCHES = _dbt_branches()
DBT_BUILD_JOB = dg.define_asset_job(
    name="build_dbt",
    selection=(
        dg.AssetSelection.assets(zavant_dbt_assets)
        | dg.AssetSelection.checks_for_assets(
            *(spec.key for spec in ATHENA_ASSET_SPECS)
        )
    ),
    description="Build selected dbt branches after validating their externally published inputs.",
)
