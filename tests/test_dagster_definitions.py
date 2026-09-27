"""Verify the ownership boundary, lineage, and real Dagster subset execution."""

import unittest
from collections import Counter
from collections.abc import Iterator
from graphlib import TopologicalSorter
from types import SimpleNamespace
from unittest.mock import patch

import dagster as dg
from dagster_dbt import DbtCliResource

from tests.test_publication_monitoring import (
    ACQUISITION,
    NOW,
    VALID_ROW,
    finished_run,
    states,
)
from zavant.orchestration.assets.athena import (
    ATHENA_ASSET_SPECS,
    EXTERNAL_ASSET_SPECS,
    athena_relation_asset_key,
)
from zavant.orchestration.assets.dbt import (
    DBT_MANIFEST,
    DBT_MODEL_SOURCES,
    DBT_TRANSLATOR,
    zavant_dbt_assets,
)
from zavant.orchestration.definitions import defs
from zavant.orchestration.jobs import DBT_BRANCHES
from zavant.orchestration.resources.acquisition import AcquisitionEvidenceResource
from zavant.orchestration.resources.athena import AthenaQueryResource, AthenaQueryResult
from zavant.orchestration.sensors.athena import (
    evaluate_publications,
    monitor_athena_publications,
)
from zavant.projection.catalog import all_projection_contracts


def _relation(name: str) -> dg.AssetKey:
    return athena_relation_asset_key("zavant_analytical_prod", name)


def _fake_dbt_stream(
    context: dg.AssetExecutionContext,
) -> Iterator[dg.Output | dg.AssetCheckResult]:
    """Simulate dbt events, not SQL, while exercising actual Dagster execution."""

    keys = context.selected_asset_keys
    order = TopologicalSorter(
        {key: zavant_dbt_assets.asset_deps[key] & keys for key in keys}
    ).static_order()
    for key in order:
        yield dg.Output(
            None, output_name=context.assets_def.get_output_name_for_asset_key(key)
        )
    for check in context.selected_asset_check_keys:
        yield dg.AssetCheckResult(
            asset_key=check.asset_key, check_name=check.name, passed=True
        )


class DagsterDefinitionsTests(unittest.TestCase):
    def test_only_dbt_is_materializable_and_no_acquisition_job_or_schedule_remains(
        self,
    ) -> None:
        dg.Definitions.validate_loadable(defs)
        graph = defs.resolve_asset_graph()
        self.assertEqual(len(EXTERNAL_ASSET_SPECS), 57)
        self.assertEqual(len(zavant_dbt_assets.keys), 38)
        self.assertEqual(
            {
                key
                for key in graph.get_all_asset_keys()
                if graph.get(key).is_materializable
            },
            zavant_dbt_assets.keys,
        )
        self.assertEqual(
            Counter(key.path[0] for key in graph.get_all_asset_keys()),
            {"aws": 57, "analytics": 38},
        )
        self.assertFalse(defs.schedules)
        self.assertEqual([job.name for job in defs.jobs or []], ["build_dbt"])
        self.assertEqual(
            set(defs.resources or {}),
            {
                "dbt",
                "analytical_readiness_athena",
                "acquisition_evidence",
                "notifications",
            },
        )
        self.assertEqual(
            {sensor.name for sensor in defs.sensors or []},
            {"monitor_athena_publications", "notify_run_failure"},
        )
        self.assertTrue(
            all(
                sensor.default_status == dg.DefaultSensorStatus.STOPPED
                for sensor in defs.sensors or []
            )
        )

    def test_source_specific_table_view_and_dbt_edges(self) -> None:
        graph = defs.resolve_asset_graph()
        self.assertEqual(
            graph.get(_relation("pitches")).parent_keys,
            {dg.AssetKey(["aws", "s3", "mlb_stats_api_raw"])},
        )
        self.assertEqual(
            graph.get(_relation("statcast_batting_events")).parent_keys,
            {dg.AssetKey(["aws", "s3", "baseball_savant_raw"])},
        )
        self.assertEqual(
            graph.get(_relation("current_pitches")).parent_keys,
            {_relation("pitches"), _relation("current_game_revisions")},
        )
        self.assertEqual(
            graph.get(_relation("current_statcast_batting_events")).parent_keys,
            {
                _relation("statcast_batting_events"),
                _relation("current_statcast_date_revisions"),
            },
        )
        self.assertEqual(
            graph.get(dg.AssetKey(["analytics", "stg_pitches"])).parent_keys,
            {_relation("current_pitches")},
        )
        source_keys = {
            DBT_TRANSLATOR.get_asset_key(source)
            for source in DBT_MANIFEST["sources"].values()
        }
        # Most staging models read one source directly. Historical batting
        # normalization additionally needs final-game and runner evidence.
        normalization_inputs = {
            "stg_plays": {"stg_games", "stg_runner_movements"},
            "stg_boxscore_player_batting": {"stg_plays"},
            "stg_boxscore_team_batting": {"stg_plays"},
        }
        for key in zavant_dbt_assets.keys:
            if graph.get(key).group_name == "dbt_staging":
                parents = graph.get(key).parent_keys
                self.assertEqual(len(parents & source_keys), 1)
                self.assertEqual(
                    parents - source_keys,
                    {
                        dg.AssetKey(["analytics", name])
                        for name in normalization_inputs.get(key.path[-1], set())
                    },
                )
        self.assertEqual(
            len({spec.key for spec in ATHENA_ASSET_SPECS} - source_keys), 28
        )

    def test_keeps_useful_groups_and_derives_four_source_branches(self) -> None:
        self.assertEqual(
            {
                spec.key.path[-1]
                for spec in ATHENA_ASSET_SPECS
                if spec.group_name == "athena_tables"
            },
            {contract.name for contract in all_projection_contracts()},
        )
        self.assertEqual(
            Counter(spec.group_name for spec in ATHENA_ASSET_SPECS),
            {"athena_tables": 29, "athena_views": 26},
        )
        self.assertEqual(
            {branch.name: len(branch.keys) for branch in DBT_BRANCHES},
            {"independent": 1, "savant": 2, "stats_api": 33, "savant_and_stats_api": 2},
        )
        self.assertEqual(
            DBT_MODEL_SOURCES[dg.AssetKey(["analytics", "fct_pitches"])], {"stats_api"}
        )
        self.assertEqual(
            DBT_MODEL_SOURCES[dg.AssetKey(["analytics", "fct_batted_balls"])],
            {"stats_api", "savant"},
        )
        self.assertEqual(
            DBT_MODEL_SOURCES[dg.AssetKey(["analytics", "metricflow_time_spine"])],
            set(),
        )

    def test_alias_maps_to_physical_source_and_dbt_outputs_keep_analytics_prefix(
        self,
    ) -> None:
        self.assertEqual(
            DBT_TRANSLATOR.get_asset_key(
                {
                    "resource_type": "source",
                    "database": "awsdatacatalog",
                    "schema": "zavant_analytical_prod",
                    "source_name": "anything",
                    "name": "pitches",
                    "identifier": "current_pitches",
                }
            ),
            _relation("current_pitches"),
        )
        for kind in ("model", "seed", "snapshot"):
            self.assertEqual(
                DBT_TRANSLATOR.get_asset_key(
                    {"resource_type": kind, "name": "example", "config": {}}
                ),
                dg.AssetKey(["analytics", "example"]),
            )


class DagsterPublicationExecutionTests(unittest.TestCase):
    def test_every_requested_branch_executes_only_selected_models_and_checks(
        self,
    ) -> None:
        job = defs.resolve_job_def("build_dbt")
        runs: list[dg.DagsterRun] = []
        seen: set[dg.AssetKey] = set()
        with (
            patch("zavant.orchestration.checks.athena.utc_now", return_value=NOW),
            patch("zavant.orchestration.assets.dbt.utc_now", return_value=NOW),
            patch.object(
                AcquisitionEvidenceResource, "latest", return_value=ACQUISITION
            ),
            patch.object(
                AthenaQueryResource,
                "query_one",
                return_value=AthenaQueryResult("query", VALID_ROW),
            ),
            patch.object(
                DbtCliResource,
                "cli",
                side_effect=lambda args, context: SimpleNamespace(
                    stream=lambda: _fake_dbt_stream(context)
                ),
            ) as dbt,
        ):
            for _ in range(2):
                requests = (
                    evaluate_publications(states(), runs, None, NOW).run_requests or []
                )
                for request in requests:
                    selection = set(request.asset_selection or [])
                    subset = job.get_subset(
                        asset_selection=selection,
                        asset_check_selection=set(request.asset_check_keys or []),
                    )
                    result = subset.execute_in_process(
                        run_config=request.run_config, tags=request.tags
                    )
                    self.assertTrue(result.success)
                    self.assertEqual(
                        {
                            event.asset_key
                            for event in result.get_asset_materialization_events()
                        },
                        selection,
                    )
                    seen.update(selection)
                    runs.append(finished_run(request))
            self.assertEqual(dbt.call_count, 4)
            self.assertEqual(seen, zavant_dbt_assets.keys)

    def test_failed_source_check_prevents_dbt_invocation(self) -> None:
        request = next(
            r
            for r in evaluate_publications(states(), [], None, NOW).run_requests or []
            if r.tags["zavant/dbt_branch"] == "stats_api"
        )
        subset = defs.resolve_job_def("build_dbt").get_subset(
            asset_selection=set(request.asset_selection or []),
            asset_check_selection=set(request.asset_check_keys or []),
        )
        with (
            patch("zavant.orchestration.checks.athena.utc_now", return_value=NOW),
            patch.object(AcquisitionEvidenceResource, "latest", return_value=None),
            patch.object(DbtCliResource, "cli") as dbt,
        ):
            result = subset.execute_in_process(
                run_config=request.run_config, raise_on_error=False
            )
            self.assertFalse(result.success)
            dbt.assert_not_called()

    def test_manual_materialization_without_checks_still_validates_sources(
        self,
    ) -> None:
        subset = defs.resolve_job_def("build_dbt").get_subset(
            asset_selection={dg.AssetKey(["analytics", "stg_pitches"])},
            asset_check_selection=set(),
        )
        with (
            patch("zavant.orchestration.assets.dbt.utc_now", return_value=NOW),
            patch.object(AcquisitionEvidenceResource, "latest", return_value=None),
            patch.object(DbtCliResource, "cli") as dbt,
        ):
            result = subset.execute_in_process(raise_on_error=False)
            self.assertFalse(result.success)
            dbt.assert_not_called()

    def test_real_sensor_evaluation_keeps_healthy_source_when_other_query_fails(
        self,
    ) -> None:
        current = states()
        with (
            dg.DagsterInstance.ephemeral() as instance,
            dg.build_sensor_context(
                instance=instance, definitions=defs, resources=defs.resources
            ) as context,
            patch("zavant.orchestration.sensors.athena.utc_now", return_value=NOW),
            patch(
                "zavant.orchestration.sensors.athena.read_publication",
                side_effect=[current["stats_api"], RuntimeError("Savant unavailable")],
            ),
        ):
            result = monitor_athena_publications.evaluate_tick(context)
        self.assertEqual(
            {r.tags["zavant/dbt_branch"] for r in result.run_requests or []},
            {"stats_api", "independent"},
        )
        self.assertTrue(result.asset_events)
        self.assertIsNotNone(result.cursor)
