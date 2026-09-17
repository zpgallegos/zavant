"""Monitor external publications and request deduplicated, source-ready dbt branches."""

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime

import dagster as dg

from zavant.orchestration.assets.athena import EXTERNAL_ASSET_SPECS, SOURCE_BY_ASSET_KEY
from zavant.orchestration.assets.dbt import zavant_dbt_assets
from zavant.orchestration.jobs import (
    DBT_BRANCHES,
    DBT_BUILD_JOB,
    DbtBranch,
    source_check_keys,
)
from zavant.orchestration.publication import PublicationState, read_publication
from zavant.orchestration.resources.acquisition import AcquisitionEvidenceResource
from zavant.orchestration.resources.athena import AthenaQueryResource
from zavant.orchestration.sources import (
    BRANCH_TAG,
    CYCLE_TAG,
    PUBLICATION_SOURCES,
    PUBLICATION_TAG,
    cycle_start,
    utc_now,
)


def publication_token(
    branch: DbtBranch, states: Mapping[str, PublicationState], cycle: str
) -> str:
    """Deduplicate attempts by cycle and exact source publication identities."""

    identity = [
        cycle,
        branch.name,
        *[
            [
                name,
                states[name].publication_id,
                states[name].metadata.get("acquisition_run_id"),
            ]
            for name in sorted(branch.sources)
        ],
    ]
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def plan_dbt_runs(
    states: Mapping[str, PublicationState],
    runs: Sequence[dg.DagsterRun],
    cycle: str,
) -> list[dg.RunRequest]:
    """Request ready source branches using lineage derived from the dbt manifest.

    Shared parents must have a successful run for the current publication before
    a combining branch is requested. Failed attempts are not silently retried;
    reexecute the failed run in the UI. A genuinely new publication has a new key.
    """

    if any(not run.is_finished for run in runs):
        return []
    ready = {name for name, state in states.items() if state.ready}
    if not ready:
        return []
    eligible = {
        branch.name: branch
        for branch in DBT_BRANCHES
        if branch.sources <= ready
        and len({states[name].publication_id for name in branch.sources}) <= 1
    }
    tokens = {
        name: publication_token(branch, states, cycle)
        for name, branch in eligible.items()
    }
    successful = {
        name
        for name, token in tokens.items()
        if any(
            run.tags.get(PUBLICATION_TAG) == token
            and run.status == dg.DagsterRunStatus.SUCCESS
            for run in runs
        )
    }
    requests = []
    for name, branch in eligible.items():
        token = tokens[name]
        if not branch.upstream_branches <= successful:
            continue
        if any(run.tags.get(PUBLICATION_TAG) == token for run in runs):
            continue
        config = {
            "processing_cycle": cycle,
            "expected_publications": {
                source: states[source].publication_id
                for source in sorted(branch.sources)
            },
        }
        model_checks = [
            key for key in zavant_dbt_assets.check_keys if key.asset_key in branch.keys
        ]
        requests.append(
            dg.RunRequest(
                run_key=f"dbt:{token}",
                asset_selection=sorted(
                    branch.keys, key=lambda key: key.to_user_string()
                ),
                asset_check_keys=[*source_check_keys(branch.sources), *model_checks],
                tags={CYCLE_TAG: cycle, BRANCH_TAG: name, PUBLICATION_TAG: token},
                run_config={
                    "ops": {
                        "zavant_dbt_assets": {"config": config},
                        **(
                            {"athena_sources_ready": {"config": config}}
                            if branch.sources
                            else {}
                        ),
                    }
                },
            )
        )
    return requests


def evaluate_publications(
    states: Mapping[str, PublicationState],
    runs: Sequence[dg.DagsterRun],
    cursor: str | None,
    now: datetime,
) -> dg.SensorResult:
    """Keep view-level events separate even when a family publishes together."""

    previous = json.loads(cursor) if cursor else {}
    if not isinstance(previous, dict) or previous.get("version", 1) != 1:
        raise ValueError("Unsupported Athena publication sensor cursor.")
    seen = dict(previous.get("publications", {}))
    events: list[dg.AssetMaterialization] = []
    cycle = cycle_start(now).date().isoformat()
    for name, state in states.items():
        identity = f"{cycle}:{state.publication_id}:{state.metadata.get('acquisition_run_id', '')}"
        if state.ready and seen.get(name) != identity:
            events.extend(
                dg.AssetMaterialization(
                    asset_key=spec.key,
                    description="Observed data published by the external AWS workflow.",
                    metadata=state.metadata,
                )
                for spec in EXTERNAL_ASSET_SPECS
                if SOURCE_BY_ASSET_KEY[spec.key] == name
            )
            seen[name] = identity
    requests = plan_dbt_runs(states, runs, cycle)
    return dg.SensorResult(
        run_requests=requests,
        asset_events=events,
        cursor=json.dumps({"version": 1, "publications": seen}, sort_keys=True),
        skip_reason=None
        if requests
        else "; ".join(f"{name}: {state.reason}" for name, state in states.items())
        + "; no new runnable branch (already attempted, waiting on upstream dbt, or a run is active).",
    )


@dg.sensor(
    job=DBT_BUILD_JOB,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Observe source-specific Athena publications and build eligible dbt branches.",
)
def monitor_athena_publications(
    context: dg.SensorEvaluationContext,
    analytical_readiness_athena: AthenaQueryResource,
    acquisition_evidence: AcquisitionEvidenceResource,
) -> dg.SensorResult:
    """Poll read-only evidence; preview never launches AWS producer work.

    Athena SELECT queries still incur normal query charges. Only committing or
    enabling the sensor can request dbt writes. Each source fails independently.
    """

    now = utc_now()
    states = {}
    for source in PUBLICATION_SOURCES:
        try:
            states[source.name] = read_publication(
                source, analytical_readiness_athena, acquisition_evidence, now
            )
        except Exception as error:
            context.log.exception(
                "Cannot determine %s publication readiness", source.name
            )
            states[source.name] = PublicationState(
                source.name, False, f"Readiness query failed: {error}"
            )
    runs = context.instance.get_runs(filters=dg.RunsFilter(job_name=DBT_BUILD_JOB.name))
    cycle = cycle_start(now).date().isoformat()
    # Active previous-day work also blocks new submissions. Completed history is
    # only relevant for this cycle's deduplication/dependency decisions.
    relevant = [
        run for run in runs if not run.is_finished or run.tags.get(CYCLE_TAG) == cycle
    ]
    return evaluate_publications(states, relevant, context.cursor, now)
