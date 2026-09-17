"""Per-relation blocking checks backed by source-specific publication evidence."""

from collections.abc import Iterator
from typing import Any

import dagster as dg

from zavant.orchestration.assets.athena import ATHENA_ASSET_SPECS, SOURCE_BY_ASSET_KEY
from zavant.orchestration.publication import (
    PublicationConfig,
    READINESS_CHECK_NAME,
    require_publications,
)
from zavant.orchestration.resources.acquisition import AcquisitionEvidenceResource
from zavant.orchestration.resources.athena import AthenaQueryResource
from zavant.orchestration.sources import SOURCES_BY_NAME, utc_now


@dg.multi_asset_check(
    specs=[
        dg.AssetCheckSpec(name=READINESS_CHECK_NAME, asset=spec.key, blocking=True)
        for spec in ATHENA_ASSET_SPECS
    ],
    can_subset=True,
)
def athena_sources_ready(
    context: dg.AssetCheckExecutionContext,
    config: PublicationConfig,
    analytical_readiness_athena: AthenaQueryResource,
    acquisition_evidence: AcquisitionEvidenceResource,
) -> Iterator[dg.AssetCheckResult]:
    """Validate each selected family once and report its own relation checks.

    A Savant failure cannot fail a Stats-only selection. Check failures block
    downstream dbt in the same run; direct UI builds also perform preflight.
    """

    now = utc_now()
    names = {
        SOURCE_BY_ASSET_KEY[key.asset_key] for key in context.selected_asset_check_keys
    }
    metadata: dict[str, Any]
    for name in sorted(names):
        try:
            states = require_publications(
                (SOURCES_BY_NAME[name],),
                config,
                analytical_readiness_athena,
                acquisition_evidence,
                now,
            )
            passed, description, metadata = (
                True,
                states[name].reason,
                states[name].metadata,
            )
        except Exception as error:
            passed, description = False, str(error)
            metadata = {"source_family": name}
            if isinstance(error, dg.Failure):
                metadata.update(error.metadata or {})
        for key in sorted(
            context.selected_asset_check_keys, key=lambda key: key.to_user_string()
        ):
            if SOURCE_BY_ASSET_KEY[key.asset_key] == name:
                yield dg.AssetCheckResult(
                    asset_key=key.asset_key,
                    check_name=key.name,
                    passed=passed,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=description,
                    metadata=metadata,
                )
