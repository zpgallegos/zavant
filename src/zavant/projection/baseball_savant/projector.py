"""Pure projection of one Savant date revision into analytical rows."""

import csv
from datetime import datetime, timezone
from io import StringIO
from typing import Dict, Optional
from uuid import UUID

from zavant.ingestion.baseball_savant.contract import StatcastCsvResponse
from zavant.projection.baseball_savant.contracts import (
    STATCAST_BATTING_EVENTS_CONTRACT,
    STATCAST_DATES_CONTRACT,
    STATCAST_HISTORY_CONTRACTS,
    STATCAST_PROJECTION_CONTRACT_VERSION,
)
from zavant.projection.baseball_savant.models import (
    StatcastDateProjection,
    StatcastProjectionSource,
)
from zavant.projection.contracts import ProjectionContractError, ProjectionRow


def project_statcast_date(
    source: StatcastProjectionSource,
    run_id: UUID,
    projected_at: datetime,
) -> StatcastDateProjection:
    """Project terminal batting outcomes from one exact-date CSV revision.

    Savant exports one row per pitch. Expected batting values belong to the
    terminal row identified by a non-empty ``events`` value, so non-terminal
    pitches are intentionally excluded from this analytical slice.
    """

    if projected_at.utcoffset() is None:
        raise ValueError("projected_at must be timezone-aware")
    if source.observed_at.utcoffset() is None:
        raise ValueError("source observed_at must be timezone-aware")
    projected_at_utc = projected_at.astimezone(timezone.utc)
    response = StatcastCsvResponse.from_bytes(source.raw, source.game_date)
    identity: ProjectionRow = {
        "game_date": source.game_date,
        "season": source.game_date.year,
        "source_revision_id": source.revision_id,
        "projection_contract_version": STATCAST_PROJECTION_CONTRACT_VERSION,
        "projection_run_id": str(run_id),
        "projected_at": projected_at_utc,
    }
    batting_events = []
    reader = csv.DictReader(StringIO(source.raw.decode("utf-8-sig"), newline=""))
    for row_number, row in enumerate(reader, start=2):
        event = row.get("events")
        if not event:
            continue
        batting_events.append(
            {
                **identity,
                "game_pk": _integer(row, "game_pk", row_number),
                "at_bat_number": _integer(row, "at_bat_number", row_number),
                "pitch_number": _integer(row, "pitch_number", row_number),
                "batter_id": _integer(row, "batter", row_number),
                "pitcher_id": _integer(row, "pitcher", row_number),
                "event": event,
                "launch_speed": _optional_float(row, "launch_speed", row_number),
                "launch_angle": _optional_float(row, "launch_angle", row_number),
                "estimated_ba_using_speedangle": _optional_float(
                    row, "estimated_ba_using_speedangle", row_number
                ),
                "estimated_slg_using_speedangle": _optional_float(
                    row, "estimated_slg_using_speedangle", row_number
                ),
                "estimated_woba_using_speedangle": _optional_float(
                    row, "estimated_woba_using_speedangle", row_number
                ),
                "woba_value": _optional_float(row, "woba_value", row_number),
                "woba_denom": _optional_float(row, "woba_denom", row_number),
            }
        )
    if len(batting_events) != response.terminal_row_count:
        raise ProjectionContractError(
            "projected terminal row count does not match the validated CSV"
        )

    date_rows = (
        {
            **identity,
            "source_observed_at": source.observed_at.astimezone(timezone.utc),
            "source_uri": source.source_uri,
            "raw_object_uri": source.raw_object_uri,
            "row_count": response.row_count,
            "terminal_row_count": response.terminal_row_count,
        },
    )
    table_rows = {
        STATCAST_BATTING_EVENTS_CONTRACT.name: tuple(batting_events),
        STATCAST_DATES_CONTRACT.name: date_rows,
    }
    for contract in STATCAST_HISTORY_CONTRACTS:
        contract.validate(table_rows[contract.name])
    return StatcastDateProjection(table_rows)


def _integer(row: Dict[str, str | None], column: str, row_number: int) -> int:
    value = row.get(column)
    try:
        parsed = int(value) if value is not None else 0
    except ValueError as exc:
        raise ProjectionContractError(
            f"Statcast CSV row {row_number} has invalid {column}"
        ) from exc
    if parsed <= 0:
        raise ProjectionContractError(
            f"Statcast CSV row {row_number} has invalid {column}"
        )
    return parsed


def _optional_float(
    row: Dict[str, str | None],
    column: str,
    row_number: int,
) -> Optional[float]:
    value = row.get(column)
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ProjectionContractError(
            f"Statcast CSV row {row_number} has invalid {column}"
        ) from exc
