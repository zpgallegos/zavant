"""Analytical contracts for projected Baseball Savant batting outcomes."""

from zavant.projection.contracts import Column, TableContract


STATCAST_PROJECTION_CONTRACT_VERSION = "zavant-analytical-statcast-projection/v2"

STATCAST_IDENTITY_COLUMNS = (
    Column("game_date", "date", False),
    Column("season", "int32", False),
    Column("source_revision_id", "string", False),
    Column("projection_contract_version", "string", False),
    Column("projection_run_id", "string", False),
    Column("projected_at", "timestamp", False),
)

STATCAST_BATTING_EVENTS_CONTRACT = TableContract(
    name="statcast_batting_events",
    columns=STATCAST_IDENTITY_COLUMNS
    + (
        Column("game_pk", "int64", False),
        Column("at_bat_number", "int32", False),
        Column("pitch_number", "int32", False),
        Column("batter_id", "int64", False),
        Column("pitcher_id", "int64", False),
        Column("event", "string", False),
        Column("launch_speed", "float64"),
        Column("launch_angle", "float64"),
        Column("launch_speed_angle", "int32"),
        Column("estimated_ba_using_speedangle", "float64"),
        Column("estimated_slg_using_speedangle", "float64"),
        Column("estimated_woba_using_speedangle", "float64"),
        Column("woba_value", "float64"),
        Column("woba_denom", "float64"),
    ),
    primary_key=(
        "game_date",
        "source_revision_id",
        "game_pk",
        "at_bat_number",
    ),
)

STATCAST_DATES_CONTRACT = TableContract(
    name="statcast_dates",
    columns=STATCAST_IDENTITY_COLUMNS
    + (
        Column("source_observed_at", "timestamp", False),
        Column("source_uri", "string", False),
        Column("raw_object_uri", "string", False),
        Column("row_count", "int64", False),
        Column("terminal_row_count", "int64", False),
    ),
    primary_key=("game_date", "source_revision_id"),
)

CURRENT_STATCAST_DATE_REVISIONS_CONTRACT = TableContract(
    name="current_statcast_date_revisions",
    columns=(
        Column("game_date", "date", False),
        Column("season", "int32", False),
        Column("source_revision_id", "string", False),
        Column("projection_contract_version", "string", False),
        Column("projection_run_id", "string", False),
        Column("reconciled_at", "timestamp", False),
        Column("raw_object_uri", "string", False),
    ),
    primary_key=("game_date",),
)

# ``statcast_dates`` is intentionally last: its row is the durable completion
# marker proving every batting event for the date revision was merged.
STATCAST_HISTORY_CONTRACTS = (
    STATCAST_BATTING_EVENTS_CONTRACT,
    STATCAST_DATES_CONTRACT,
)
STATCAST_ICEBERG_CONTRACTS = (
    *STATCAST_HISTORY_CONTRACTS,
    CURRENT_STATCAST_DATE_REVISIONS_CONTRACT,
)
