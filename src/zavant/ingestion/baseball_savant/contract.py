"""Validation contract for Baseball Savant Statcast Search CSV responses."""

import csv
from dataclasses import dataclass
from datetime import date
from io import StringIO
from typing import Dict, Tuple


OBSERVED_EXPORT_ROW_LIMIT = 25_000
REQUIRED_COLUMNS = (
    "game_date",
    "game_type",
    "game_pk",
    "at_bat_number",
    "pitch_number",
    "batter",
    "pitcher",
    "events",
    "launch_speed",
    "launch_angle",
    "launch_speed_angle",
    "estimated_ba_using_speedangle",
    "estimated_slg_using_speedangle",
    "estimated_woba_using_speedangle",
    "woba_value",
    "woba_denom",
)


class BaseballSavantContractError(ValueError):
    """Raised when a Statcast Search export violates its expected contract."""


@dataclass(frozen=True)
class StatcastCsvResponse:
    """Validated metadata for one exact-date, regular-season CSV export."""

    game_date: date
    columns: Tuple[str, ...]
    row_count: int
    terminal_row_count: int

    @classmethod
    def from_bytes(cls, raw: bytes, game_date: date) -> "StatcastCsvResponse":
        """Validate one all-player CSV response without retaining parsed rows."""

        if type(game_date) is not date:
            raise ValueError("game_date must be a date")
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise BaseballSavantContractError(
                "Statcast CSV must be UTF-8 encoded"
            ) from exc
        try:
            reader = csv.DictReader(StringIO(text, newline=""))
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise BaseballSavantContractError("Statcast CSV has no header")
            columns = tuple(fieldnames)
            if len(columns) != len(set(columns)):
                raise BaseballSavantContractError(
                    "Statcast CSV contains duplicate columns"
                )
            missing = tuple(
                column for column in REQUIRED_COLUMNS if column not in columns
            )
            if missing:
                raise BaseballSavantContractError(
                    f"Statcast CSV is missing required columns: {', '.join(missing)}"
                )

            row_count = 0
            terminal_row_count = 0
            for row_number, row in enumerate(reader, start=2):
                cls._validate_row(row, game_date, row_number)
                row_count += 1
                terminal_row_count += int(bool(row["events"]))
        except csv.Error as exc:
            raise BaseballSavantContractError("Statcast CSV is malformed") from exc

        if row_count >= OBSERVED_EXPORT_ROW_LIMIT:
            raise BaseballSavantContractError(
                "Statcast CSV reached the observed 25,000-row export limit"
            )
        return cls(
            game_date=game_date,
            columns=columns,
            row_count=row_count,
            terminal_row_count=terminal_row_count,
        )

    @staticmethod
    def _validate_row(
        row: Dict[str, str | None],
        expected_date: date,
        row_number: int,
    ) -> None:
        if None in row:
            raise BaseballSavantContractError(
                f"Statcast CSV row {row_number} has more values than columns"
            )
        structurally_missing = tuple(
            column for column in REQUIRED_COLUMNS if row[column] is None
        )
        if structurally_missing:
            raise BaseballSavantContractError(
                f"Statcast CSV row {row_number} has fewer values than columns; "
                "missing required fields: "
                f"{', '.join(structurally_missing)}"
            )
        if row.get("game_date") != expected_date.isoformat():
            raise BaseballSavantContractError(
                f"Statcast CSV row {row_number} is outside the requested date"
            )
        if row.get("game_type") != "R":
            raise BaseballSavantContractError(
                f"Statcast CSV row {row_number} is not a regular-season event"
            )
        for column in (
            "game_pk",
            "at_bat_number",
            "pitch_number",
            "batter",
            "pitcher",
        ):
            value = row.get(column)
            try:
                parsed = int(value) if value is not None else 0
            except ValueError as exc:
                raise BaseballSavantContractError(
                    f"Statcast CSV row {row_number} has invalid {column}"
                ) from exc
            if parsed <= 0:
                raise BaseballSavantContractError(
                    f"Statcast CSV row {row_number} has invalid {column}"
                )
