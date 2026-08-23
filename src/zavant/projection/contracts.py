"""Source-neutral analytical row and table contract primitives.

Both Stats API and Baseball Savant projectors validate their rows through this
boundary before local Parquet or production Iceberg publication.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Literal, Sequence, Tuple, Union


ColumnKind = Literal["boolean", "date", "float64", "int32", "int64", "string", "timestamp"]
Scalar = Union[bool, date, datetime, float, int, str, None]
ProjectionRow = Dict[str, Scalar]


class ProjectionContractError(ValueError):
    """Raised when a source value or projected row violates its contract."""


@dataclass(frozen=True)
class Column:
    """Name, logical type, and nullability for one analytical column."""

    name: str
    kind: ColumnKind
    nullable: bool = True


@dataclass(frozen=True)
class TableContract:
    """Source-neutral schema and natural key for an analytical table."""

    name: str
    columns: Tuple[Column, ...]
    primary_key: Tuple[str, ...]

    def validate(self, rows: Sequence[ProjectionRow]) -> None:
        """Reject malformed rows and duplicate natural keys within a batch."""

        column_names = tuple(column.name for column in self.columns)
        expected_names = set(column_names)
        if not set(self.primary_key).issubset(expected_names):
            raise ProjectionContractError(f"{self.name} primary key is not in schema")

        observed_keys = set()
        for row_number, row in enumerate(rows):
            if set(row) != expected_names:
                missing = sorted(expected_names - set(row))
                extra = sorted(set(row) - expected_names)
                raise ProjectionContractError(
                    f"{self.name} row {row_number} has missing={missing} extra={extra}"
                )
            for column in self.columns:
                value = row[column.name]
                if value is None:
                    if not column.nullable:
                        raise ProjectionContractError(
                            f"{self.name}.{column.name} must not be null"
                        )
                    continue
                if not _matches_kind(value, column.kind):
                    raise ProjectionContractError(
                        f"{self.name}.{column.name} has invalid type "
                        f"{type(value).__name__}"
                    )
            natural_key = tuple(row[name] for name in self.primary_key)
            if any(value is None for value in natural_key):
                raise ProjectionContractError(
                    f"{self.name} primary key must not contain null"
                )
            if natural_key in observed_keys:
                raise ProjectionContractError(
                    f"{self.name} contains duplicate primary key {natural_key}"
                )
            observed_keys.add(natural_key)


def _matches_kind(value: Scalar, kind: ColumnKind) -> bool:
    if kind == "boolean":
        return isinstance(value, bool)
    if kind in {"int32", "int64"}:
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "float64":
        return isinstance(value, (float, int)) and not isinstance(value, bool)
    if kind == "string":
        return isinstance(value, str)
    if kind == "date":
        return isinstance(value, date) and not isinstance(value, datetime)
    if kind == "timestamp":
        return isinstance(value, datetime) and value.utcoffset() is not None
    return False
