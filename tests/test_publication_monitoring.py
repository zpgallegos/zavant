"""Exercise external readiness and branch planning without AWS or dbt writes."""

import json
import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import dagster as dg

from tests.fake_s3 import FakeS3Client
from zavant.orchestration.jobs import DBT_BRANCHES
from zavant.orchestration.publication import (
    PublicationConfig,
    PublicationState,
    publication_sql,
    read_publication,
    require_publications,
)
from zavant.orchestration.resources.acquisition import (
    AcquisitionEvidence,
    AcquisitionEvidenceResource,
    latest_acquisition,
)
from zavant.orchestration.resources.athena import AthenaQueryResource, AthenaQueryResult
from zavant.orchestration.sensors.athena import evaluate_publications, plan_dbt_runs
from zavant.orchestration.sources import (
    BRANCH_TAG,
    PUBLICATION_SOURCES,
    SAVANT,
    STATS_API,
    cycle_start,
)
from zavant.storage.s3_objects import S3ObjectBackend


NOW = datetime(2026, 9, 16, 16, tzinfo=timezone.utc)
CYCLE = "2026-09-16"
ACQUISITION = AcquisitionEvidence(
    "acquisition-1",
    NOW - timedelta(hours=3),
    NOW - timedelta(hours=2),
    "complete",
    "s3://test/lake/runs/daily/run_date=2026-09-16/run_id=acquisition-1/manifest.json",
)
VALID_ROW: dict[str, str | None] = {
    "mapping_rows": "2",
    "distinct_entities": "2",
    "publication_count": "1",
    "publication_id": "projection-1",
    "reconciled_at": "2026-09-16 14:10:00.000",
    "invalid_mappings": "0",
}
ATHENA = AthenaQueryResource(
    region_name="us-east-1",
    database="zavant_analytical_prod",
    workgroup="test",
    output_location="s3://test/results",
)
EVIDENCE = AcquisitionEvidenceResource(bucket="test")


def states(*, savant_ready: bool = True) -> dict[str, PublicationState]:
    return {
        source.name: PublicationState(
            source.name,
            source != SAVANT or savant_ready,
            "Ready" if source != SAVANT or savant_ready else "Waiting",
            "projection-1",
            {
                "acquisition_run_id": f"{source.name}-1",
                "processing_cycle": CYCLE,
                "source_family": source.name,
            },
        )
        for source in PUBLICATION_SOURCES
    }


def finished_run(
    request: dg.RunRequest, status: dg.DagsterRunStatus = dg.DagsterRunStatus.SUCCESS
) -> dg.DagsterRun:
    return dg.DagsterRun(job_name="build_dbt", tags=request.tags, status=status)


class AcquisitionEvidenceTests(unittest.TestCase):
    def test_all_pages_and_both_prefixes_use_latest_attempt_not_latest_success(
        self,
    ) -> None:
        for source in PUBLICATION_SOURCES:
            with self.subTest(source=source.name):
                client = FakeS3Client(page_size=1)
                backend = S3ObjectBackend(client, "test", "lake")
                # Lexical UUID order is deliberately not chronological order.
                for run_id, offset, status in (
                    ("a-new", 1, "failed"),
                    ("z-old", 2, "complete"),
                ):
                    key = f"lake/{source.manifest_prefix}run_date={CYCLE}/run_id={run_id}/manifest.json"
                    client.put_object(
                        Bucket="test",
                        Key=key,
                        Body=json.dumps(
                            {
                                "contract": source.manifest_contract,
                                "run_id": run_id,
                                "started_at": (
                                    NOW - timedelta(hours=offset)
                                ).isoformat(),
                                "completed_at": NOW.isoformat(),
                                "status": status,
                            }
                        ).encode(),
                    )
                    client.modified_at[("test", key)] = NOW
                latest = latest_acquisition(backend, source, cycle_start(NOW))
                assert latest is not None
                self.assertEqual((latest.run_id, latest.status), ("a-new", "failed"))
                self.assertEqual(client.list_objects_v2_calls, 2)
                self.assertIn(source.manifest_prefix, latest.manifest_uri)

    def test_old_attempt_is_not_promoted_by_recent_s3_modification(self) -> None:
        client = FakeS3Client()
        key = f"lake/{STATS_API.manifest_prefix}old/manifest.json"
        client.put_object(
            Bucket="test",
            Key=key,
            Body=json.dumps(
                {
                    "contract": STATS_API.manifest_contract,
                    "started_at": (NOW - timedelta(days=1)).isoformat(),
                }
            ).encode(),
        )
        client.modified_at[("test", key)] = NOW
        self.assertIsNone(
            latest_acquisition(
                S3ObjectBackend(client, "test", "lake"), STATS_API, cycle_start(NOW)
            )
        )

    def test_malformed_manifest_fails_closed(self) -> None:
        client = FakeS3Client()
        key = f"lake/{STATS_API.manifest_prefix}bad/manifest.json"
        client.put_object(Bucket="test", Key=key, Body=b"{}")
        client.modified_at[("test", key)] = NOW
        with self.assertRaisesRegex(ValueError, "contract"):
            latest_acquisition(
                S3ObjectBackend(client, "test", "lake"), STATS_API, cycle_start(NOW)
            )


class PublicationReadinessTests(unittest.TestCase):
    def test_registry_query_detects_missing_markers_and_duplicate_mapping_keys(
        self,
    ) -> None:
        # SQLite exercises the relational logic locally; live Athena dialect/IAM
        # validation remains an explicit deployment acceptance step.
        for source in PUBLICATION_SOURCES:
            with (
                self.subTest(source=source.name),
                closing(sqlite3.connect(":memory:")) as database,
            ):
                database.row_factory = sqlite3.Row
                database.execute("ATTACH DATABASE ':memory:' AS test")
                database.execute(
                    f'CREATE TABLE test."{source.mapping_table}" ({source.entity_key} TEXT, source_revision_id TEXT, projection_contract_version TEXT, projection_run_id TEXT, reconciled_at TEXT)'
                )
                database.execute(
                    f'CREATE TABLE test."{source.marker_table}" ({source.entity_key} TEXT, source_revision_id TEXT, projection_contract_version TEXT)'
                )
                mapping = (
                    "entity-1",
                    "revision-1",
                    source.contract_version,
                    "projection-1",
                    "2026-09-16 14:10:00",
                )
                database.execute(
                    f'INSERT INTO test."{source.mapping_table}" VALUES (?, ?, ?, ?, ?)',
                    mapping,
                )
                sql = publication_sql("test", source)
                self.assertEqual(
                    database.execute(sql).fetchone()["invalid_mappings"], 1
                )
                database.execute(
                    f'INSERT INTO test."{source.marker_table}" VALUES (?, ?, ?)',
                    mapping[:3],
                )
                valid = database.execute(sql).fetchone()
                self.assertEqual(
                    (
                        valid["mapping_rows"],
                        valid["distinct_entities"],
                        valid["invalid_mappings"],
                    ),
                    (1, 1, 0),
                )
                database.execute(
                    f'INSERT INTO test."{source.mapping_table}" VALUES (?, ?, ?, ?, ?)',
                    mapping,
                )
                duplicate = database.execute(sql).fetchone()
                self.assertEqual(
                    (duplicate["mapping_rows"], duplicate["distinct_entities"]), (2, 1)
                )

    def test_ready_for_each_source_including_unchanged_history_content(self) -> None:
        with (
            patch.object(
                AcquisitionEvidenceResource, "latest", return_value=ACQUISITION
            ),
            patch.object(
                AthenaQueryResource,
                "query_one",
                return_value=AthenaQueryResult("query-1", VALID_ROW),
            ),
        ):
            for source in PUBLICATION_SOURCES:
                state = read_publication(source, ATHENA, EVIDENCE, NOW)
                self.assertTrue(state.ready)
                self.assertEqual(state.publication_id, "projection-1")
                self.assertEqual(state.metadata["processing_cycle"], CYCLE)

    def test_reconciliation_accepts_athena_utc_suffix_and_iso_timestamps(self) -> None:
        for timestamp in (
            "2026-09-16 14:10:00.422515 UTC",
            "2026-09-16 14:10:00.422515",
            "2026-09-16T14:10:00.422515Z",
            "2026-09-16T14:10:00.422515+00:00",
            "2026-09-16T07:10:00.422515-07:00",
        ):
            for source in PUBLICATION_SOURCES:
                with (
                    self.subTest(timestamp=timestamp, source=source.name),
                    patch.object(
                        AcquisitionEvidenceResource,
                        "latest",
                        return_value=ACQUISITION,
                    ),
                    patch.object(
                        AthenaQueryResource,
                        "query_one",
                        return_value=AthenaQueryResult(
                            "query-1", {**VALID_ROW, "reconciled_at": timestamp}
                        ),
                    ),
                ):
                    state = read_publication(source, ATHENA, EVIDENCE, NOW)
                    self.assertTrue(state.ready)
                    parsed = datetime.fromisoformat(state.metadata["reconciled_at"])
                    self.assertEqual(
                        parsed,
                        datetime(2026, 9, 16, 14, 10, 0, 422515, tzinfo=timezone.utc),
                    )

    def test_malformed_reconciliation_timestamp_fails_closed(self) -> None:
        for timestamp in (None, "", "not-a-timestamp", "2026-09-16 14:10:00 XYZ"):
            with (
                self.subTest(timestamp=timestamp),
                patch.object(
                    AcquisitionEvidenceResource, "latest", return_value=ACQUISITION
                ),
                patch.object(
                    AthenaQueryResource,
                    "query_one",
                    return_value=AthenaQueryResult(
                        "query-1", {**VALID_ROW, "reconciled_at": timestamp}
                    ),
                ),
                self.assertRaises(ValueError),
            ):
                read_publication(STATS_API, ATHENA, EVIDENCE, NOW)

    def test_no_current_success_does_not_query_athena(self) -> None:
        for evidence in (
            None,
            replace(ACQUISITION, status="failed"),
            replace(ACQUISITION, status="running", completed_at=None),
            replace(ACQUISITION, completed_at=NOW + timedelta(hours=1)),
        ):
            with (
                self.subTest(evidence=evidence),
                patch.object(
                    AcquisitionEvidenceResource, "latest", return_value=evidence
                ),
                patch.object(AthenaQueryResource, "query_one") as query,
            ):
                self.assertFalse(
                    read_publication(STATS_API, ATHENA, EVIDENCE, NOW).ready
                )
                query.assert_not_called()

    def test_invalid_mapping_and_stale_publication_block(self) -> None:
        overrides = [
            {"mapping_rows": "0"},
            {"distinct_entities": "1"},
            {"publication_count": "2"},
            {"invalid_mappings": "1"},
            {"publication_id": None},
            {"reconciled_at": "2026-09-15 14:00:00"},
            {"reconciled_at": "2026-09-16 13:30:00"},
            {"reconciled_at": "2026-09-16 17:00:00"},
            {"reconciled_at": "2026-09-16 13:30:00 UTC"},
            {"reconciled_at": "2026-09-16 17:00:00 UTC"},
        ]
        for override in overrides:
            with (
                self.subTest(override=override),
                patch.object(
                    AcquisitionEvidenceResource, "latest", return_value=ACQUISITION
                ),
                patch.object(
                    AthenaQueryResource,
                    "query_one",
                    return_value=AthenaQueryResult("query", {**VALID_ROW, **override}),
                ),
            ):
                self.assertFalse(
                    read_publication(STATS_API, ATHENA, EVIDENCE, NOW).ready
                )

    def test_queries_use_own_mapping_and_completion_marker(self) -> None:
        for source in PUBLICATION_SOURCES:
            sql = publication_sql("test", source)
            self.assertIn(f'"test"."{source.mapping_table}"', sql)
            self.assertIn(f'"test"."{source.marker_table}"', sql)
            self.assertIn("source_revision_id", sql)
            self.assertNotIn("MAX(projected_at)", sql)

    def test_execution_preflight_rejects_expired_or_changed_publication(self) -> None:
        with patch(
            "zavant.orchestration.publication.read_publication",
            return_value=states()["stats_api"],
        ):
            for config, message in (
                (PublicationConfig(processing_cycle="2026-09-15"), "expired"),
                (
                    PublicationConfig(expected_publications={"stats_api": "old"}),
                    "changed",
                ),
            ):
                with (
                    self.subTest(message=message),
                    self.assertRaisesRegex(dg.Failure, message),
                ):
                    require_publications((STATS_API,), config, ATHENA, EVIDENCE, NOW)

    def test_execution_preflight_rejects_partially_published_glue_run(self) -> None:
        current = states()
        with (
            patch(
                "zavant.orchestration.publication.read_publication",
                side_effect=[
                    current["stats_api"],
                    replace(current["savant"], publication_id="older-run"),
                ],
            ),
            self.assertRaisesRegex(dg.Failure, "same Glue run"),
        ):
            require_publications(
                PUBLICATION_SOURCES, PublicationConfig(), ATHENA, EVIDENCE, NOW
            )

    def test_local_cycle_tracks_dst_not_a_fixed_utc_hour(self) -> None:
        for month, utc_hour in ((1, 8), (7, 7)):
            start = cycle_start(datetime(2026, month, 15, 16, tzinfo=timezone.utc))
            self.assertEqual(start.astimezone(timezone.utc).hour, utc_hour)
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            cycle_start(datetime(2026, 9, 16))


class BranchPlanningTests(unittest.TestCase):
    def test_stats_runs_without_savant_then_combined_waits_for_both_dbt_branches(
        self,
    ) -> None:
        first = plan_dbt_runs(states(savant_ready=False), [], CYCLE)
        self.assertEqual(
            {r.tags[BRANCH_TAG] for r in first}, {"independent", "stats_api"}
        )
        runs = [finished_run(request) for request in first]
        second = plan_dbt_runs(states(), runs, CYCLE)
        self.assertEqual([r.tags[BRANCH_TAG] for r in second], ["savant"])
        runs.extend(finished_run(request) for request in second)
        third = plan_dbt_runs(states(), runs, CYCLE)
        self.assertEqual([r.tags[BRANCH_TAG] for r in third], ["savant_and_stats_api"])
        runs.extend(finished_run(request) for request in third)
        self.assertEqual(plan_dbt_runs(states(), runs, CYCLE), [])
        for request in [*first, *second, *third]:
            branch = next(b for b in DBT_BRANCHES if b.name == request.tags[BRANCH_TAG])
            self.assertEqual(set(request.asset_selection or []), branch.keys)

    def test_failed_attempt_is_not_automatically_retried_or_used_as_ready_parent(
        self,
    ) -> None:
        requests = plan_dbt_runs(states(), [], CYCLE)
        runs = [
            finished_run(request, dg.DagsterRunStatus.FAILURE) for request in requests
        ]
        self.assertEqual(plan_dbt_runs(states(), runs, CYCLE), [])
        # A successful UI reexecution keeps the original identity tags.
        runs.extend(finished_run(request) for request in requests)
        self.assertEqual(
            [r.tags[BRANCH_TAG] for r in plan_dbt_runs(states(), runs, CYCLE)],
            ["savant_and_stats_api"],
        )

    def test_new_publication_rebuilds_parents_before_combining_models(self) -> None:
        old_requests = plan_dbt_runs(states(), [], CYCLE)
        runs = [finished_run(request) for request in old_requests]
        current = {
            name: replace(state, publication_id="projection-2")
            for name, state in states().items()
        }
        requests = plan_dbt_runs(current, runs, CYCLE)
        self.assertEqual(
            {r.tags[BRANCH_TAG] for r in requests}, {"stats_api", "savant"}
        )

    def test_mixed_publication_ids_never_request_combined_models(self) -> None:
        current = states()
        current["savant"] = replace(current["savant"], publication_id="older-run")
        parents = plan_dbt_runs(current, [], CYCLE)
        self.assertEqual(
            plan_dbt_runs(current, [finished_run(r) for r in parents], CYCLE), []
        )

    def test_active_run_blocks_new_requests_even_from_previous_cycle(self) -> None:
        run = dg.DagsterRun(job_name="build_dbt", status=dg.DagsterRunStatus.STARTED)
        self.assertEqual(plan_dbt_runs(states(), [run], CYCLE), [])

    def test_external_events_are_deduplicated_but_cursor_is_not_run_success(
        self,
    ) -> None:
        first = evaluate_publications(states(savant_ready=False), [], None, NOW)
        self.assertTrue(first.asset_events)
        self.assertTrue(
            all(
                event.metadata["source_family"].value == "stats_api"
                for event in first.asset_events
            )
        )
        second = evaluate_publications(
            states(savant_ready=False), [], first.cursor, NOW
        )
        self.assertEqual(second.asset_events, [])
        self.assertEqual(
            [r.run_key for r in first.run_requests or []],
            [r.run_key for r in second.run_requests or []],
        )
        third = evaluate_publications(
            states(),
            [finished_run(r) for r in first.run_requests or []],
            first.cursor,
            NOW,
        )
        self.assertTrue(third.asset_events)
        self.assertTrue(
            all(
                event.metadata["source_family"].value == "savant"
                for event in third.asset_events
            )
        )

    def test_neither_source_ready_does_not_launch_even_independent_work(self) -> None:
        current = {
            name: replace(state, ready=False) for name, state in states().items()
        }
        result = evaluate_publications(current, [], None, NOW)
        self.assertFalse(result.run_requests)
        self.assertFalse(result.asset_events)
        self.assertIsNotNone(result.skip_reason)
