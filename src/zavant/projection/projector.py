"""Composition and integrity checks for one game projection."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from zavant.projection.boxscore import project_boxscore
from zavant.projection.contracts import (
    PROJECTION_CONTRACT_VERSION,
    ProjectionContractError,
    ProjectionRow,
    TABLE_CONTRACTS,
)
from zavant.projection.game import project_game_row
from zavant.projection.game_support import project_game_support
from zavant.projection.models import GameProjection, ProjectionSource
from zavant.projection.play_extensions import project_play_extensions
from zavant.projection.plays import project_play_data


def project_game(
    source: ProjectionSource,
    run_id: UUID,
    projected_at: datetime,
) -> GameProjection:
    """Project one raw revision and validate all table relationships."""

    if projected_at.utcoffset() is None:
        raise ValueError("projected_at must be timezone-aware")
    projected_at_utc = projected_at.astimezone(timezone.utc)
    identity: ProjectionRow = {
        "game_pk": source.game.game_pk,
        "season": source.game.season,
        "official_date": source.game.official_date,
        "source_revision_id": source.revision_id,
        "projection_contract_version": PROJECTION_CONTRACT_VERSION,
        "projection_run_id": str(run_id),
        "projected_at": projected_at_utc,
    }
    try:
        game_row = project_game_row(source, identity)
        plays, events, pitches, batted_balls, event_kind_counts = project_play_data(
            source.game.payload,
            identity,
        )
        table_rows = {
            "games": (game_row,),
            "plays": tuple(plays),
            "play_events": tuple(events),
            "pitches": tuple(pitches),
            "batted_balls": tuple(batted_balls),
        }
        for projected_group in (
            project_play_extensions(source.game.payload, identity),
            project_game_support(source.game.payload, identity),
            project_boxscore(source.game.payload, identity),
        ):
            overlap = set(table_rows).intersection(projected_group)
            if overlap:
                raise ProjectionContractError(
                    f"projection table produced more than once: {sorted(overlap)}"
                )
            table_rows.update(
                {name: tuple(rows) for name, rows in projected_group.items()}
            )
    except ProjectionContractError as exc:
        raise ProjectionContractError(
            f"game {source.game.game_pk} revision {source.revision_id}: {exc}"
        ) from exc
    projection = GameProjection(
        table_rows=table_rows,
        event_kind_counts=event_kind_counts,
    )
    _validate_projection(projection)
    return projection


def _validate_projection(projection: GameProjection) -> None:
    tables = projection.tables()
    if set(tables) != set(TABLE_CONTRACTS):
        raise ProjectionContractError(
            "projected tables do not match the registered contracts"
        )
    for name, rows in tables.items():
        TABLE_CONTRACTS[name].validate(rows)

    event_keys = {_event_key(row) for row in projection.play_events}
    play_keys = {row["at_bat_index"] for row in projection.plays}
    pitch_keys = {_event_key(row) for row in projection.pitches}
    batted_ball_keys = {_event_key(row) for row in projection.batted_balls}
    if not pitch_keys.issubset(event_keys):
        raise ProjectionContractError("pitches must reference projected events")
    if not batted_ball_keys.issubset(pitch_keys):
        raise ProjectionContractError("batted balls must reference projected pitches")
    if sum(projection.event_kind_counts.values()) != len(projection.play_events):
        raise ProjectionContractError("event-kind counts do not match projected events")
    if projection.event_kind_counts.get("pitch", 0) != len(projection.pitches):
        raise ProjectionContractError("pitch count does not match pitch events")

    for table_name in (
        "actions",
        "substitutions",
        "disengagements",
        "non_pitch_calls",
        "rule_violations",
    ):
        if not {_event_key(row) for row in tables[table_name]}.issubset(event_keys):
            raise ProjectionContractError(
                f"{table_name} must reference projected events"
            )
    for review in tables["reviews"]:
        if review["at_bat_index"] not in play_keys:
            raise ProjectionContractError("reviews must reference projected plays")
        event_index = review["event_index"]
        if event_index is not None and (
            review["at_bat_index"], event_index
        ) not in event_keys:
            raise ProjectionContractError("event reviews must reference projected events")
    runner_keys = {
        (row["at_bat_index"], row["runner_index"])
        for row in tables["runner_movements"]
    }
    if not {
        (row["at_bat_index"], row["runner_index"])
        for row in tables["fielding_credits"]
    }.issubset(runner_keys):
        raise ProjectionContractError(
            "fielding credits must reference projected runner movements"
        )
    if not {row["at_bat_index"] for row in tables["runner_movements"]}.issubset(
        play_keys
    ):
        raise ProjectionContractError("runner movements must reference projected plays")

    player_keys = {
        (row["player_id"], row["team_id"]) for row in tables["players"]
    }
    for table_name in (
        "player_positions",
        "player_batting",
        "player_pitching",
        "player_fielding",
    ):
        referenced_players = {
            (row["player_id"], row["team_id"]) for row in tables[table_name]
        }
        if not referenced_players.issubset(player_keys):
            raise ProjectionContractError(
                f"{table_name} must reference projected player-team rows"
            )
    team_ids = {row["team_id"] for row in tables["game_teams"]}
    if not {row["team_id"] for row in tables["players"]}.issubset(team_ids):
        raise ProjectionContractError("players must reference projected game teams")
    for table_name in ("team_batting", "team_pitching", "team_fielding"):
        if {row["team_id"] for row in tables[table_name]} != team_ids:
            raise ProjectionContractError(
                f"{table_name} must contain both projected game teams"
            )


def _event_key(row: ProjectionRow) -> tuple[object, object]:
    return row["at_bat_index"], row["event_index"]
