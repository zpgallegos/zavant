"""Dagster owns dbt; Step Functions owns acquisition and Glue publication."""

import dagster as dg

from zavant.orchestration.assets.athena import EXTERNAL_ASSET_SPECS
from zavant.orchestration.assets.dbt import zavant_dbt_assets
from zavant.orchestration.checks.athena import athena_sources_ready
from zavant.orchestration.jobs import DBT_BUILD_JOB
from zavant.orchestration.resources.acquisition import ACQUISITION_EVIDENCE_RESOURCE
from zavant.orchestration.resources.athena import ANALYTICAL_READINESS_ATHENA_RESOURCE
from zavant.orchestration.resources.dbt import ZAVANT_DBT_RESOURCE
from zavant.orchestration.resources.notifications import DAGSTER_NOTIFICATION_RESOURCE
from zavant.orchestration.sensors.athena import monitor_athena_publications
from zavant.orchestration.sensors.failures import notify_run_failure


defs = dg.Definitions(
    assets=[*EXTERNAL_ASSET_SPECS, zavant_dbt_assets],
    asset_checks=[athena_sources_ready],
    jobs=[DBT_BUILD_JOB],
    sensors=[monitor_athena_publications, notify_run_failure],
    resources={
        "analytical_readiness_athena": ANALYTICAL_READINESS_ATHENA_RESOURCE,
        "acquisition_evidence": ACQUISITION_EVIDENCE_RESOURCE,
        "dbt": ZAVANT_DBT_RESOURCE,
        "notifications": DAGSTER_NOTIFICATION_RESOURCE,
    },
)
