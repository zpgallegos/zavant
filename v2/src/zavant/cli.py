"""Command-line entry point for local development and automation."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Optional, Sequence
from uuid import UUID, uuid4

from zavant.contracts.game_changes import (
    GameChangesContractError,
    GameChangesRequest,
    GameChangesResponse,
)
from zavant.contracts.raw_game import RawGame, RawGameContractError
from zavant.settings import Settings
from zavant.storage.local_game_changes import (
    GameChangesConflictError,
    LocalGameChangesStore,
)
from zavant.storage.local_raw import LocalRawGameStore, RawGameConflictError


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
        raise argparse.ArgumentTypeError(
            "timestamp must use ISO-8601 format"
        ) from exc
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
    source_path = args.path.resolve()
    source_uri = args.source_uri or source_path.as_uri()

    if args.command == "land-game-file":
        try:
            raw = source_path.read_bytes()
            game = RawGame.from_bytes(raw)
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
