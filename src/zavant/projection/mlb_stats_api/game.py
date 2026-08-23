"""Projection of Stats API game-level metadata."""

from __future__ import annotations

from typing import Any, Dict

from zavant.projection.mlb_stats_api._values import (
    boolean_value,
    date_value,
    integer_value,
    numeric_string_integer,
    object_value,
    string_value,
    timestamp_value,
)
from zavant.projection.contracts import ProjectionRow
from zavant.projection.mlb_stats_api.models import ProjectionSource


def project_game_row(
    source: ProjectionSource,
    identity: ProjectionRow,
) -> ProjectionRow:
    """Project one game-revision row from stable game and live-data fields."""

    payload = source.game.payload
    game_data = object_value(payload.get("gameData"), "gameData", required=True)
    live_data = object_value(payload.get("liveData"), "liveData", required=True)
    game = object_value(game_data.get("game"), "gameData.game", required=True)
    game_datetime = object_value(
        game_data.get("datetime"), "gameData.datetime", required=True
    )
    status = object_value(game_data.get("status"), "gameData.status", required=True)
    teams = object_value(game_data.get("teams"), "gameData.teams", required=True)
    away_team = object_value(teams.get("away"), "gameData.teams.away", required=True)
    home_team = object_value(teams.get("home"), "gameData.teams.home", required=True)
    game_info = object_value(game_data.get("gameInfo"), "gameData.gameInfo")
    venue = object_value(game_data.get("venue"), "gameData.venue")
    weather = object_value(game_data.get("weather"), "gameData.weather")
    flags = object_value(game_data.get("flags"), "gameData.flags")
    replay = object_value(game_data.get("review"), "gameData.review")
    abs_challenges = object_value(
        game_data.get("absChallenges"), "gameData.absChallenges"
    )
    mound_visits = object_value(
        game_data.get("moundVisits"), "gameData.moundVisits"
    )
    linescore = object_value(live_data.get("linescore"), "liveData.linescore")
    line_teams = object_value(linescore.get("teams"), "liveData.linescore.teams")

    row: Dict[str, Any] = dict(identity)
    row.update(
        {
            "raw_object_uri": source.raw_object_uri,
            "source_uri": source.source_uri or None,
            "source_observed_at": source.observed_at,
            "feed_timecode": source.game.feed_timecode,
            "game_type": string_value(
                game.get("type"), "gameData.game.type", required=True
            ),
            "game_id": string_value(game.get("id"), "gameData.game.id"),
            "double_header": string_value(
                game.get("doubleHeader"), "gameData.game.doubleHeader"
            ),
            "gameday_type": string_value(
                game.get("gamedayType"), "gameData.game.gamedayType"
            ),
            "tiebreaker": string_value(
                game.get("tiebreaker"), "gameData.game.tiebreaker"
            ),
            "game_number": integer_value(
                game.get("gameNumber"), "gameData.game.gameNumber"
            ),
            "calendar_event_id": string_value(
                game.get("calendarEventID"), "gameData.game.calendarEventID"
            ),
            "scheduled_start_at": timestamp_value(
                game_datetime.get("dateTime"), "gameData.datetime.dateTime"
            ),
            "original_date": date_value(
                game_datetime.get("originalDate"), "gameData.datetime.originalDate"
            ),
            "resume_date": date_value(
                game_datetime.get("resumeDate"), "gameData.datetime.resumeDate"
            ),
            "resumed_from_date": date_value(
                game_datetime.get("resumedFromDate"),
                "gameData.datetime.resumedFromDate",
            ),
            "day_night": string_value(
                game_datetime.get("dayNight"), "gameData.datetime.dayNight"
            ),
            "abstract_game_state": string_value(
                status.get("abstractGameState"), "gameData.status.abstractGameState"
            ),
            "coded_game_state": string_value(
                status.get("codedGameState"), "gameData.status.codedGameState"
            ),
            "detailed_state": string_value(
                status.get("detailedState"), "gameData.status.detailedState"
            ),
            "status_code": string_value(
                status.get("statusCode"), "gameData.status.statusCode"
            ),
            "start_time_tbd": boolean_value(
                status.get("startTimeTBD"), "gameData.status.startTimeTBD"
            ),
            "away_team_id": integer_value(
                away_team.get("id"), "gameData.teams.away.id", required=True
            ),
            "home_team_id": integer_value(
                home_team.get("id"), "gameData.teams.home.id", required=True
            ),
            "away_score": _line_score(line_teams, "away"),
            "home_score": _line_score(line_teams, "home"),
            "venue_id": integer_value(venue.get("id"), "gameData.venue.id"),
            "venue_name": string_value(venue.get("name"), "gameData.venue.name"),
            "attendance": integer_value(
                game_info.get("attendance"), "gameData.gameInfo.attendance"
            ),
            "first_pitch_at": timestamp_value(
                game_info.get("firstPitch"), "gameData.gameInfo.firstPitch"
            ),
            "game_duration_minutes": integer_value(
                game_info.get("gameDurationMinutes"),
                "gameData.gameInfo.gameDurationMinutes",
            ),
            "delay_duration_minutes": integer_value(
                game_info.get("delayDurationMinutes"),
                "gameData.gameInfo.delayDurationMinutes",
            ),
            "scheduled_innings": integer_value(
                linescore.get("scheduledInnings"),
                "liveData.linescore.scheduledInnings",
            ),
            "weather_condition": string_value(
                weather.get("condition"), "gameData.weather.condition"
            ),
            "temperature_fahrenheit": numeric_string_integer(
                weather.get("temp"), "gameData.weather.temp"
            ),
            "wind": string_value(weather.get("wind"), "gameData.weather.wind"),
            "no_hitter": boolean_value(flags.get("noHitter"), "gameData.flags.noHitter"),
            "perfect_game": boolean_value(
                flags.get("perfectGame"), "gameData.flags.perfectGame"
            ),
            "away_team_no_hitter": boolean_value(
                flags.get("awayTeamNoHitter"), "gameData.flags.awayTeamNoHitter"
            ),
            "away_team_perfect_game": boolean_value(
                flags.get("awayTeamPerfectGame"),
                "gameData.flags.awayTeamPerfectGame",
            ),
            "home_team_no_hitter": boolean_value(
                flags.get("homeTeamNoHitter"), "gameData.flags.homeTeamNoHitter"
            ),
            "home_team_perfect_game": boolean_value(
                flags.get("homeTeamPerfectGame"),
                "gameData.flags.homeTeamPerfectGame",
            ),
            **_challenge_fields(replay, "replay"),
            **_abs_challenge_fields(abs_challenges),
            **_mound_visit_fields(mound_visits),
        }
    )
    return row


def _line_score(line_teams: Dict[str, Any], side: str) -> Any:
    team = object_value(line_teams.get(side), f"liveData.linescore.teams.{side}")
    return integer_value(team.get("runs"), f"liveData.linescore.teams.{side}.runs")


def _challenge_fields(review: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    away = object_value(review.get("away"), "gameData.review.away")
    home = object_value(review.get("home"), "gameData.review.home")
    return {
        f"{prefix}_has_challenges": boolean_value(
            review.get("hasChallenges"), "gameData.review.hasChallenges"
        ),
        "away_replay_challenges_used": integer_value(
            away.get("used"), "gameData.review.away.used"
        ),
        "away_replay_challenges_remaining": integer_value(
            away.get("remaining"), "gameData.review.away.remaining"
        ),
        "home_replay_challenges_used": integer_value(
            home.get("used"), "gameData.review.home.used"
        ),
        "home_replay_challenges_remaining": integer_value(
            home.get("remaining"), "gameData.review.home.remaining"
        ),
    }


def _abs_challenge_fields(challenges: Dict[str, Any]) -> Dict[str, Any]:
    away = object_value(challenges.get("away"), "gameData.absChallenges.away")
    home = object_value(challenges.get("home"), "gameData.absChallenges.home")
    return {
        "abs_has_challenges": boolean_value(
            challenges.get("hasChallenges"), "gameData.absChallenges.hasChallenges"
        ),
        "away_abs_challenges_successful": integer_value(
            away.get("usedSuccessful"), "gameData.absChallenges.away.usedSuccessful"
        ),
        "away_abs_challenges_failed": integer_value(
            away.get("usedFailed"), "gameData.absChallenges.away.usedFailed"
        ),
        "away_abs_challenges_remaining": integer_value(
            away.get("remaining"), "gameData.absChallenges.away.remaining"
        ),
        "home_abs_challenges_successful": integer_value(
            home.get("usedSuccessful"), "gameData.absChallenges.home.usedSuccessful"
        ),
        "home_abs_challenges_failed": integer_value(
            home.get("usedFailed"), "gameData.absChallenges.home.usedFailed"
        ),
        "home_abs_challenges_remaining": integer_value(
            home.get("remaining"), "gameData.absChallenges.home.remaining"
        ),
    }


def _mound_visit_fields(visits: Dict[str, Any]) -> Dict[str, Any]:
    away = object_value(visits.get("away"), "gameData.moundVisits.away")
    home = object_value(visits.get("home"), "gameData.moundVisits.home")
    return {
        "away_mound_visits_used": integer_value(
            away.get("used"), "gameData.moundVisits.away.used"
        ),
        "away_mound_visits_remaining": integer_value(
            away.get("remaining"), "gameData.moundVisits.away.remaining"
        ),
        "home_mound_visits_used": integer_value(
            home.get("used"), "gameData.moundVisits.home.used"
        ),
        "home_mound_visits_remaining": integer_value(
            home.get("remaining"), "gameData.moundVisits.home.remaining"
        ),
    }
