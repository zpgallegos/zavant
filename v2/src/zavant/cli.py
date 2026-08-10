"""Command-line entry point for local development and automation."""

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Optional, Sequence
from uuid import UUID, uuid4

from zavant.acquisition.bounded_games import BoundedGameAcquirer
from zavant.acquisition.corrected_games import CorrectedGameProcessor
from zavant.acquisition.daily import DailyAcquisitionCoordinator
from zavant.acquisition.game_changes import (
    GameChangesPoller,
    GameChangesPollingError,
)
from zavant.acquisition.schedule_discovery import ScheduleDiscoverer
from zavant.clients.mlb_stats_api import (
    DEFAULT_TIMEOUT_SECONDS,
    MlbStatsApiClient,
    MlbStatsApiError,
    RetryPolicy,
)
from zavant.contracts.game_changes import (
    GameChangesContractError,
    GameChangesRequest,
    GameChangesResponse,
)
from zavant.contracts.raw_game import RawGameContractError, RawGameResponse
from zavant.contracts.schedule import (
    ScheduleContractError,
    ScheduleRequest,
    ScheduleResponse,
)
from zavant.settings import Settings
from zavant.storage.local_game_changes import (
    GameChangesConflictError,
    LocalGameChangesStore,
)
from zavant.storage.local_game_changes_watermark import (
    GameChangesWatermarkConflictError,
    LocalGameChangesWatermarkStore,
)
from zavant.storage.local_daily_runs import DailyRunConflictError, LocalDailyRunStore
from zavant.storage.local_raw import LocalRawGameStore, RawGameConflictError
from zavant.storage.local_schedule import LocalScheduleStore, ScheduleConflictError
from zavant.storage.local_schedule_watermark import LocalScheduleWatermarkStore


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
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    """Build the Zavant command-line parser.

    Returns:
        The configured top-level argument parser.
    """

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
    acquire_games.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    acquire_games.add_argument("--max-attempts", type=int, default=3)
    acquire_games.add_argument(
        "--data-dir",
        type=Path,
        help="override ZAVANT_DATA_DIR for this invocation",
    )

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
    daily.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    daily.add_argument("--max-attempts", type=int, default=3)
    daily.add_argument(
        "--data-dir",
        type=Path,
        help="override ZAVANT_DATA_DIR for this invocation",
    )

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
    poll_changes.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    poll_changes.add_argument("--max-attempts", type=int, default=3)
    poll_changes.add_argument(
        "--data-dir",
        type=Path,
        help="override ZAVANT_DATA_DIR for this invocation",
    )

    land_game = subparsers.add_parser(
        "land-game-file", help="validate and land an MLB live-game JSON file"
    )
    land_game.add_argument("path", type=Path)
    land_game.add_argument(
        "--data-dir",
        type=Path,
        help="override ZAVANT_DATA_DIR for this invocation",
    )
    land_game.add_argument(
        "--source-uri",
        help="record the payload's source; defaults to the input file URI",
    )
    land_game.add_argument(
        "--trigger",
        default="manual",
        choices=("initial", "game_changes", "manual", "reconciliation"),
        help="record why the game was retrieved",
    )

    land_schedule = subparsers.add_parser(
        "land-schedule-file",
        help="validate and land one recorded schedule response",
    )
    land_schedule.add_argument("path", type=Path)
    land_schedule.add_argument("--start-date", required=True, type=parse_iso_date)
    land_schedule.add_argument("--end-date", required=True, type=parse_iso_date)
    land_schedule.add_argument(
        "--requested-at",
        required=True,
        type=parse_utc_datetime,
    )
    land_schedule.add_argument("--sport-id", type=int, default=1)
    land_schedule.add_argument("--run-id", type=UUID, default=None)
    land_schedule.add_argument(
        "--data-dir",
        type=Path,
        help="override ZAVANT_DATA_DIR for this invocation",
    )
    land_schedule.add_argument(
        "--source-uri",
        help="record the payload's source; defaults to the input file URI",
    )

    land_changes = subparsers.add_parser(
        "land-changes-file",
        help="validate and land one recorded game-changes response page",
    )
    land_changes.add_argument("path", type=Path)
    land_changes.add_argument("--updated-since", required=True, type=parse_utc_datetime)
    land_changes.add_argument("--window-end", required=True, type=parse_utc_datetime)
    land_changes.add_argument("--run-id", type=UUID, default=None)
    land_changes.add_argument("--page-number", type=int, default=0)
    land_changes.add_argument("--limit", type=int, default=1000)
    land_changes.add_argument("--offset", type=int, default=0)
    land_changes.add_argument(
        "--data-dir",
        type=Path,
        help="override ZAVANT_DATA_DIR for this invocation",
    )
    land_changes.add_argument(
        "--source-uri",
        help="record the payload's source; defaults to the input file URI",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run a Zavant command.

    Args:
        argv: Optional arguments excluding the executable name. Defaults to
            process arguments when omitted.

    Returns:
        The process exit status.
    """

    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_environment()
    data_dir = args.data_dir or settings.data_dir

    if args.command == "acquire-games":
        if (args.run_id is None) != (args.requested_at is None):
            parser.error(
                "--run-id and --requested-at must be supplied together for resumption"
            )
        try:
            client = MlbStatsApiClient(
                base_url=settings.mlb_api_base_url,
                timeout_seconds=args.timeout_seconds,
                retry_policy=RetryPolicy(max_attempts=args.max_attempts),
            )
            acquisition = BoundedGameAcquirer(
                api=client,
                schedule_store=LocalScheduleStore(data_dir),
                game_store=LocalRawGameStore(data_dir),
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
            client = MlbStatsApiClient(
                base_url=settings.mlb_api_base_url,
                timeout_seconds=args.timeout_seconds,
                retry_policy=RetryPolicy(max_attempts=args.max_attempts),
            )
            schedule_store = LocalScheduleStore(data_dir)
            changes_store = LocalGameChangesStore(data_dir)
            game_store = LocalRawGameStore(data_dir)
            daily_result = DailyAcquisitionCoordinator(
                changes_poller=GameChangesPoller(
                    api=client,
                    changes_store=changes_store,
                    watermark_store=LocalGameChangesWatermarkStore(data_dir),
                ),
                corrected_game_processor=CorrectedGameProcessor(
                    api=client,
                    changes_store=changes_store,
                    game_store=game_store,
                ),
                schedule_discoverer=ScheduleDiscoverer(
                    acquirer=BoundedGameAcquirer(
                        api=client,
                        schedule_store=schedule_store,
                        game_store=game_store,
                    ),
                    watermark_store=LocalScheduleWatermarkStore(data_dir),
                ),
                run_store=LocalDailyRunStore(data_dir),
            ).run(
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
            client = MlbStatsApiClient(
                base_url=settings.mlb_api_base_url,
                timeout_seconds=args.timeout_seconds,
                retry_policy=RetryPolicy(max_attempts=args.max_attempts),
            )
            poll = GameChangesPoller(
                api=client,
                changes_store=LocalGameChangesStore(data_dir),
                watermark_store=LocalGameChangesWatermarkStore(data_dir),
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

    source_path = args.path.resolve()
    source_uri = args.source_uri or source_path.as_uri()

    if args.command == "land-game-file":
        try:
            raw = source_path.read_bytes()
            game = RawGameResponse.from_bytes(raw)
            landed_game = LocalRawGameStore(data_dir).land(
                game=game,
                raw=raw,
                source_uri=source_uri,
                trigger=args.trigger,
            )
        except (OSError, RawGameContractError, RawGameConflictError) as exc:
            parser.error(str(exc))

        print(json.dumps(landed_game.as_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "land-schedule-file":
        try:
            raw = source_path.read_bytes()
            schedule = ScheduleResponse.from_bytes(raw)
            request = ScheduleRequest(
                start_date=args.start_date,
                end_date=args.end_date,
                sport_id=args.sport_id,
                requested_at=args.requested_at,
                source_uri=source_uri,
            )
            landed_schedule = LocalScheduleStore(data_dir).land(
                schedule=schedule,
                request=request,
                raw=raw,
                run_id=args.run_id or uuid4(),
            )
        except (
            OSError,
            ScheduleContractError,
            ScheduleConflictError,
            ValueError,
        ) as exc:
            parser.error(str(exc))

        print(json.dumps(landed_schedule.as_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "land-changes-file":
        try:
            raw = source_path.read_bytes()
            changes = GameChangesResponse.from_bytes(raw)
            request = GameChangesRequest(
                updated_since=args.updated_since,
                window_end=args.window_end,
                page_number=args.page_number,
                limit=args.limit,
                offset=args.offset,
                source_uri=source_uri,
            )
            landed_changes = LocalGameChangesStore(data_dir).land_page(
                changes=changes,
                request=request,
                raw=raw,
                run_id=args.run_id or uuid4(),
            )
        except (
            OSError,
            GameChangesContractError,
            GameChangesConflictError,
            ValueError,
        ) as exc:
            parser.error(str(exc))

        print(json.dumps(landed_changes.as_dict(), indent=2, sort_keys=True))
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2
