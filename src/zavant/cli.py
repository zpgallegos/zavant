"""Command-line entry point for local development and automation."""

import argparse
from datetime import date, datetime, timedelta
from importlib import import_module
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, Sequence, cast
from uuid import UUID, uuid4

from zavant._time import as_utc
from zavant.ingestion.mlb_stats_api.acquisition.bounded_games import BoundedGameAcquirer
from zavant.ingestion.baseball_savant.backfill import (
    BaseballSavantBackfillCoordinator,
    BaseballSavantBackfillMode,
)
from zavant.ingestion.mlb_stats_api.acquisition.game_changes import (
    GameChangesPoller,
    GameChangesPollingError,
)
from zavant.ingestion.mlb_stats_api.acquisition.season_backfill import (
    SeasonBackfillMode,
)
from zavant.ingestion.baseball_savant.application import (
    BaseballSavantBackfillStorage,
    local_backfill_storage as local_baseball_savant_backfill_storage,
    s3_backfill_storage as s3_baseball_savant_backfill_storage,
)
from zavant.ingestion.mlb_stats_api.application import (
    build_daily_coordinator,
    build_season_backfill_coordinator,
)
from zavant.ingestion.baseball_savant.client import (
    DEFAULT_TIMEOUT_SECONDS as SAVANT_DEFAULT_TIMEOUT_SECONDS,
    BaseballSavantClient,
    BaseballSavantError,
)
from zavant.ingestion.baseball_savant.settings import BaseballSavantSettings
from zavant.ingestion.mlb_stats_api.client import (
    DEFAULT_TIMEOUT_SECONDS,
    MlbStatsApiClient,
    MlbStatsApiError,
)
from zavant.ingestion.mlb_stats_api.settings import MlbStatsApiSettings
from zavant.ingestion.http import RetryPolicy
from zavant.ingestion.mlb_stats_api.contracts.game_changes import (
    GameChangesContractError,
)
from zavant.ingestion.mlb_stats_api.contracts.schedule import ScheduleContractError
from zavant.settings import Settings
from zavant.ingestion.mlb_stats_api.storage.errors import (
    DailyRunConflictError,
    GameChangesConflictError,
    GameChangesWatermarkConflictError,
    ScheduleConflictError,
    SeasonBackfillConflictError,
)
from zavant.ingestion.mlb_stats_api.storage.bundles import (
    AcquisitionStorage,
    local_acquisition_storage,
    s3_acquisition_storage,
)
from zavant.storage.s3_objects import S3Client
from zavant.ingestion.baseball_savant.storage import BaseballSavantStorageError


class StsClient(Protocol):
    """AWS identity operation used to guard explicit S3 backfills."""

    def get_caller_identity(self) -> Dict[str, Any]: ...


def parse_iso_date(value: str) -> date:
    """Parse an ISO-8601 calendar date.

    Args:
        value: Date in `YYYY-MM-DD` format.

    Returns:
        Parsed calendar date.

    Raises:
        argparse.ArgumentTypeError: If the value is not a valid ISO date.
    """

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD format") from exc


def parse_utc_datetime(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and normalize it to UTC.

    Args:
        value: Timestamp containing an explicit UTC offset or `Z` suffix.

    Returns:
        A timezone-aware UTC timestamp.

    Raises:
        argparse.ArgumentTypeError: If the value is malformed or timezone-naive.
    """

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timestamp must use ISO-8601 format") from exc
    try:
        return as_utc(parsed, "timestamp")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _add_http_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument("--max-attempts", type=int, default=3)


def _add_data_dir_option(
    parser: argparse.ArgumentParser,
    help_text: str = "override ZAVANT_DATA_DIR for this invocation",
) -> None:
    parser.add_argument("--data-dir", type=Path, help=help_text)


def _build_api_client(args: Any) -> MlbStatsApiClient:
    return MlbStatsApiClient(
        base_url=MlbStatsApiSettings.from_environment().base_url,
        timeout_seconds=args.timeout_seconds,
        retry_policy=RetryPolicy(max_attempts=args.max_attempts),
    )


def _build_savant_client(args: Any) -> BaseballSavantClient:
    return BaseballSavantClient(
        base_url=BaseballSavantSettings.from_environment().base_url,
        timeout_seconds=args.timeout_seconds,
        retry_policy=RetryPolicy(max_attempts=args.max_attempts),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="zavant", description="Develop and operate the Zavant data platform"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    acquire_games = subparsers.add_parser(
        "acquire-games",
        help="download eligible MLB games from a bounded schedule",
    )
    acquire_games.add_argument("--start-date", required=True, type=parse_iso_date)
    acquire_games.add_argument("--end-date", required=True, type=parse_iso_date)
    acquire_games.add_argument("--sport-id", type=int, default=1)
    acquire_games.add_argument("--run-id", type=UUID, default=None)
    acquire_games.add_argument("--requested-at", type=parse_utc_datetime, default=None)
    _add_http_options(acquire_games)
    _add_data_dir_option(acquire_games)

    daily = subparsers.add_parser(
        "run-daily",
        help="discover new and corrected games through the complete local workflow",
    )
    daily.add_argument("--initial-schedule-date", type=parse_iso_date)
    daily.add_argument(
        "--initial-correction-watermark",
        type=parse_utc_datetime,
    )
    daily.add_argument("--through-date", type=parse_iso_date)
    daily.add_argument("--schedule-lookback-days", type=int, default=7)
    daily.add_argument("--sport-id", type=int, default=1)
    daily.add_argument("--correction-limit", type=int, default=1000)
    daily.add_argument("--correction-overlap-seconds", type=float, default=300.0)
    daily.add_argument("--correction-max-pages", type=int, default=100)
    _add_http_options(daily)
    _add_data_dir_option(daily)

    poll_changes = subparsers.add_parser(
        "poll-game-changes",
        help="land corrected-game pages and advance the durable watermark",
    )
    poll_changes.add_argument(
        "--initial-watermark",
        type=parse_utc_datetime,
        help="required only for the first poll; omit after state is initialized",
    )
    poll_changes.add_argument("--sport-id", type=int, default=1)
    poll_changes.add_argument("--limit", type=int, default=1000)
    poll_changes.add_argument("--overlap-seconds", type=float, default=300.0)
    poll_changes.add_argument("--max-pages", type=int, default=100)
    _add_http_options(poll_changes)
    _add_data_dir_option(poll_changes)

    stats_api_backfill = subparsers.add_parser(
        "backfill-statsapi",
        help="reconcile every eligible game in one or more MLB seasons",
    )
    stats_api_backfill.add_argument("seasons", nargs="+", type=int)
    stats_api_backfill.add_argument(
        "--mode",
        choices=tuple(mode.value for mode in SeasonBackfillMode),
        default=SeasonBackfillMode.RECONCILE.value,
    )
    stats_api_backfill.add_argument("--dry-run", action="store_true")
    stats_api_backfill.add_argument("--sport-id", type=int, default=1)
    stats_api_backfill.add_argument("--correction-limit", type=int, default=1000)
    stats_api_backfill.add_argument(
        "--correction-overlap-seconds", type=float, default=300.0
    )
    stats_api_backfill.add_argument("--correction-max-pages", type=int, default=100)
    stats_api_backfill.add_argument("--run-id", type=UUID, default=None)
    stats_api_backfill.add_argument(
        "--started-at", type=parse_utc_datetime, default=None
    )
    stats_api_backfill.add_argument(
        "--storage",
        choices=("local", "s3"),
        default="local",
        help="local is the safe default; S3 must be selected explicitly",
    )
    stats_api_backfill.add_argument("--bucket", help="override ZAVANT_S3_BUCKET")
    stats_api_backfill.add_argument("--prefix", help="override ZAVANT_S3_PREFIX")
    _add_http_options(stats_api_backfill)
    _add_data_dir_option(
        stats_api_backfill, "override ZAVANT_DATA_DIR for local storage"
    )

    savant_backfill = subparsers.add_parser(
        "backfill-savant",
        help="backfill Baseball Savant CSV exports by exact date",
    )
    savant_backfill.add_argument("--start-date", required=True, type=parse_iso_date)
    savant_backfill.add_argument("--end-date", required=True, type=parse_iso_date)
    savant_backfill.add_argument(
        "--mode",
        choices=tuple(mode.value for mode in BaseballSavantBackfillMode),
        default=BaseballSavantBackfillMode.MISSING.value,
    )
    savant_backfill.add_argument("--dry-run", action="store_true")
    savant_backfill.add_argument(
        "--request-delay-seconds",
        type=float,
        default=0.5,
        help="delay between source requests; defaults to 0.5 seconds",
    )
    savant_backfill.add_argument("--run-id", type=UUID, default=None)
    savant_backfill.add_argument("--started-at", type=parse_utc_datetime, default=None)
    savant_backfill.add_argument(
        "--timeout-seconds",
        type=float,
        default=SAVANT_DEFAULT_TIMEOUT_SECONDS,
    )
    savant_backfill.add_argument("--max-attempts", type=int, default=3)
    savant_backfill.add_argument(
        "--storage",
        choices=("local", "s3"),
        default="local",
        help="local is the safe default; S3 must be selected explicitly",
    )
    savant_backfill.add_argument("--bucket", help="override ZAVANT_S3_BUCKET")
    savant_backfill.add_argument("--prefix", help="override ZAVANT_S3_PREFIX")
    _add_data_dir_option(savant_backfill, "override ZAVANT_DATA_DIR for local storage")

    project_local = subparsers.add_parser(
        "project-local",
        help="project all local game revisions into inspectable Parquet tables",
    )
    project_local.add_argument(
        "--season",
        action="append",
        dest="seasons",
        type=int,
        help="limit projection to a season; repeat to select multiple seasons",
    )
    project_local.add_argument("--run-id", type=UUID)
    project_local.add_argument("--projected-at", type=parse_utc_datetime)
    project_local.add_argument(
        "--output-dir",
        type=Path,
        help="new output directory; defaults to a run below the local lake",
    )
    _add_data_dir_option(project_local)

    project_savant_local = subparsers.add_parser(
        "project-savant-local",
        help="project local Savant revisions into inspectable Parquet tables",
    )
    project_savant_local.add_argument("--start-date", type=parse_iso_date)
    project_savant_local.add_argument("--end-date", type=parse_iso_date)
    project_savant_local.add_argument("--run-id", type=UUID)
    project_savant_local.add_argument("--projected-at", type=parse_utc_datetime)
    project_savant_local.add_argument(
        "--output-dir",
        type=Path,
        help="new output directory; defaults to a run below the local lake",
    )
    _add_data_dir_option(project_savant_local)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_environment()
    data_dir = args.data_dir or settings.data_dir

    if args.command == "project-local":
        from zavant.projection.mlb_stats_api.local import run_local_projection

        run_id = args.run_id or uuid4()
        output_dir = args.output_dir or (
            data_dir / "analytical" / "projection_runs" / f"run_id={run_id}"
        )
        try:
            result = run_local_projection(
                data_dir=data_dir,
                output_dir=output_dir,
                run_id=run_id,
                projected_at=args.projected_at,
                seasons=args.seasons,
            )
        except (OSError, ValueError) as exc:
            parser.error(str(exc))
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "project-savant-local":
        from zavant.projection.baseball_savant.local import (
            run_local_statcast_projection,
        )

        run_id = args.run_id or uuid4()
        output_dir = args.output_dir or (
            data_dir / "analytical" / "statcast_projection_runs" / f"run_id={run_id}"
        )
        try:
            result = run_local_statcast_projection(
                data_dir=data_dir,
                output_dir=output_dir,
                run_id=run_id,
                projected_at=args.projected_at,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        except (OSError, ValueError) as exc:
            parser.error(str(exc))
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "acquire-games":
        if (args.run_id is None) != (args.requested_at is None):
            parser.error(
                "--run-id and --requested-at must be supplied together for resumption"
            )
        try:
            client = _build_api_client(args)
            storage = local_acquisition_storage(data_dir)
            acquisition = BoundedGameAcquirer(
                api=client,
                schedule_store=storage.schedules,
                game_store=storage.raw_games,
            ).acquire(
                start_date=args.start_date,
                end_date=args.end_date,
                sport_id=args.sport_id,
                run_id=args.run_id,
                requested_at=args.requested_at,
            )
        except (
            MlbStatsApiError,
            OSError,
            ScheduleContractError,
            ScheduleConflictError,
            ValueError,
        ) as exc:
            parser.error(str(exc))

        print(json.dumps(acquisition.as_dict(), indent=2, sort_keys=True))
        return 0 if acquisition.successful else 1

    if args.command == "run-daily":
        try:
            client = _build_api_client(args)
            storage = local_acquisition_storage(data_dir)
            daily_result = build_daily_coordinator(client, storage).run(
                initial_schedule_date=args.initial_schedule_date,
                initial_correction_watermark=args.initial_correction_watermark,
                through_date=args.through_date,
                schedule_lookback_days=args.schedule_lookback_days,
                correction_overlap=timedelta(seconds=args.correction_overlap_seconds),
                correction_limit=args.correction_limit,
                correction_max_pages=args.correction_max_pages,
                sport_id=args.sport_id,
            )
        except (DailyRunConflictError, OSError, ValueError) as exc:
            parser.error(str(exc))

        print(json.dumps(daily_result.as_dict(), indent=2, sort_keys=True))
        return 0 if daily_result.successful else 1

    if args.command == "poll-game-changes":
        try:
            client = _build_api_client(args)
            storage = local_acquisition_storage(data_dir)
            poll = GameChangesPoller(
                api=client,
                changes_store=storage.game_changes,
                watermark_store=storage.game_changes_watermark,
            ).poll(
                initial_watermark=args.initial_watermark,
                sport_id=args.sport_id,
                limit=args.limit,
                overlap=timedelta(seconds=args.overlap_seconds),
                max_pages=args.max_pages,
            )
        except (
            GameChangesContractError,
            GameChangesConflictError,
            GameChangesPollingError,
            GameChangesWatermarkConflictError,
            MlbStatsApiError,
            OSError,
            ValueError,
        ) as exc:
            parser.error(str(exc))

        print(json.dumps(poll.as_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "backfill-statsapi":
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )
        if (args.run_id is None) != (args.started_at is None):
            parser.error("--run-id and --started-at must be supplied together")
        try:
            backfill_storage = _stats_api_backfill_storage(args, settings, data_dir)
            client = _build_api_client(args)
            result = build_season_backfill_coordinator(client, backfill_storage).run(
                seasons=tuple(args.seasons),
                mode=SeasonBackfillMode(args.mode),
                dry_run=args.dry_run,
                sport_id=args.sport_id,
                correction_limit=args.correction_limit,
                correction_overlap=timedelta(seconds=args.correction_overlap_seconds),
                correction_max_pages=args.correction_max_pages,
                run_id=args.run_id,
                started_at=args.started_at,
            )
        except (
            MlbStatsApiError,
            OSError,
            SeasonBackfillConflictError,
            ValueError,
        ) as exc:
            parser.error(str(exc))

        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
        return 0 if result.successful else 1

    if args.command == "backfill-savant":
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )
        if (args.run_id is None) != (args.started_at is None):
            parser.error("--run-id and --started-at must be supplied together")
        try:
            client = _build_savant_client(args)
            storage = _savant_backfill_storage(args, settings, data_dir)
            result = BaseballSavantBackfillCoordinator(
                api=client,
                raw_store=storage.raw,
                backfill_store=storage.runs,
            ).run(
                start_date=args.start_date,
                end_date=args.end_date,
                mode=BaseballSavantBackfillMode(args.mode),
                dry_run=args.dry_run,
                request_delay_seconds=args.request_delay_seconds,
                run_id=args.run_id,
                started_at=args.started_at,
            )
        except (
            BaseballSavantError,
            BaseballSavantStorageError,
            OSError,
            ValueError,
        ) as exc:
            parser.error(str(exc))

        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
        return 0 if result.successful else 1

    parser.error(f"unsupported command: {args.command}")
    return 2


def _stats_api_backfill_storage(
    args: Any,
    settings: Settings,
    data_dir: Path,
) -> AcquisitionStorage:
    if args.storage != "s3":
        return local_acquisition_storage(data_dir)
    client, bucket, prefix = _s3_backfill_target(args, settings)
    return s3_acquisition_storage(
        client=client,
        bucket=bucket,
        prefix=prefix,
    )


def _savant_backfill_storage(
    args: Any,
    settings: Settings,
    data_dir: Path,
) -> BaseballSavantBackfillStorage:
    if args.storage != "s3":
        return local_baseball_savant_backfill_storage(data_dir)
    client, bucket, prefix = _s3_backfill_target(args, settings)
    return s3_baseball_savant_backfill_storage(
        client=client,
        bucket=bucket,
        prefix=prefix,
    )


def _s3_backfill_target(
    args: Any,
    settings: Settings,
) -> tuple[S3Client, str, str]:
    bucket = args.bucket or settings.s3_bucket
    if not bucket:
        raise ValueError("S3 backfill storage requires --bucket or ZAVANT_S3_BUCKET")
    expected_account_id = settings.expected_aws_account_id
    if not expected_account_id:
        raise ValueError("S3 backfill storage requires ZAVANT_AWS_ACCOUNT_ID")
    boto3 = import_module("boto3")
    client_factory = boto3.client
    sts_client = cast(StsClient, client_factory("sts"))
    try:
        identity = sts_client.get_caller_identity()
    except Exception as exc:
        raise OSError("failed to verify the active AWS account") from exc
    actual_account_id = identity.get("Account")
    if actual_account_id != expected_account_id:
        raise ValueError(
            f"refusing S3 backfill: expected AWS account {expected_account_id}, "
            f"received {actual_account_id}"
        )
    return (
        cast(S3Client, client_factory("s3")),
        bucket,
        args.prefix if args.prefix is not None else settings.s3_prefix,
    )
