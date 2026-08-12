"""Source-to-column mappings for boxscore game statistics."""

from __future__ import annotations

from typing import Literal, Tuple


StatKind = Literal["int32", "string"]
StatField = Tuple[str, str, StatKind]


def _integers(*names: Tuple[str, str]) -> Tuple[StatField, ...]:
    return tuple((output, source, "int32") for output, source in names)


def _strings(*names: Tuple[str, str]) -> Tuple[StatField, ...]:
    return tuple((output, source, "string") for output, source in names)


BATTING_FIELDS = _integers(
    ("games_played", "gamesPlayed"),
    ("plate_appearances", "plateAppearances"),
    ("at_bats", "atBats"),
    ("runs", "runs"),
    ("hits", "hits"),
    ("doubles", "doubles"),
    ("triples", "triples"),
    ("home_runs", "homeRuns"),
    ("rbi", "rbi"),
    ("total_bases", "totalBases"),
    ("strike_outs", "strikeOuts"),
    ("base_on_balls", "baseOnBalls"),
    ("intentional_walks", "intentionalWalks"),
    ("hit_by_pitch", "hitByPitch"),
    ("stolen_bases", "stolenBases"),
    ("caught_stealing", "caughtStealing"),
    ("ground_into_double_play", "groundIntoDoublePlay"),
    ("ground_into_triple_play", "groundIntoTriplePlay"),
    ("left_on_base", "leftOnBase"),
    ("sac_bunts", "sacBunts"),
    ("sac_flies", "sacFlies"),
    ("catchers_interference", "catchersInterference"),
    ("pickoffs", "pickoffs"),
    ("fly_outs", "flyOuts"),
    ("ground_outs", "groundOuts"),
    ("air_outs", "airOuts"),
    ("pop_outs", "popOuts"),
    ("line_outs", "lineOuts"),
) + _strings(
    ("average", "avg"),
    ("on_base_percentage", "obp"),
    ("slugging_percentage", "slg"),
    ("on_base_plus_slugging", "ops"),
    ("stolen_base_percentage", "stolenBasePercentage"),
    ("at_bats_per_home_run", "atBatsPerHomeRun"),
    ("summary", "summary"),
    ("note", "note"),
)

PITCHING_FIELDS = _integers(
    ("games_played", "gamesPlayed"),
    ("games_pitched", "gamesPitched"),
    ("games_started", "gamesStarted"),
    ("games_finished", "gamesFinished"),
    ("complete_games", "completeGames"),
    ("shutouts", "shutouts"),
    ("wins", "wins"),
    ("losses", "losses"),
    ("saves", "saves"),
    ("save_opportunities", "saveOpportunities"),
    ("holds", "holds"),
    ("blown_saves", "blownSaves"),
    ("batters_faced", "battersFaced"),
    ("at_bats", "atBats"),
    ("outs", "outs"),
    ("runs", "runs"),
    ("earned_runs", "earnedRuns"),
    ("hits", "hits"),
    ("doubles", "doubles"),
    ("triples", "triples"),
    ("home_runs", "homeRuns"),
    ("strike_outs", "strikeOuts"),
    ("base_on_balls", "baseOnBalls"),
    ("intentional_walks", "intentionalWalks"),
    ("hit_by_pitch", "hitByPitch"),
    ("hit_batsmen", "hitBatsmen"),
    ("balks", "balks"),
    ("wild_pitches", "wildPitches"),
    ("passed_balls", "passedBall"),
    ("pickoffs", "pickoffs"),
    ("stolen_bases", "stolenBases"),
    ("caught_stealing", "caughtStealing"),
    ("inherited_runners", "inheritedRunners"),
    ("inherited_runners_scored", "inheritedRunnersScored"),
    ("number_of_pitches", "numberOfPitches"),
    ("pitches_thrown", "pitchesThrown"),
    ("balls", "balls"),
    ("strikes", "strikes"),
    ("rbi", "rbi"),
    ("sac_bunts", "sacBunts"),
    ("sac_flies", "sacFlies"),
    ("catchers_interference", "catchersInterference"),
    ("fly_outs", "flyOuts"),
    ("ground_outs", "groundOuts"),
    ("air_outs", "airOuts"),
    ("pop_outs", "popOuts"),
    ("line_outs", "lineOuts"),
) + _strings(
    ("innings_pitched", "inningsPitched"),
    ("earned_run_average", "era"),
    ("walks_hits_per_inning", "whip"),
    ("strike_percentage", "strikePercentage"),
    ("stolen_base_percentage", "stolenBasePercentage"),
    ("caught_stealing_percentage", "caughtStealingPercentage"),
    ("ground_outs_to_air_outs", "groundOutsToAirouts"),
    ("pitches_per_inning", "pitchesPerInning"),
    ("runs_scored_per_nine", "runsScoredPer9"),
    ("home_runs_per_nine", "homeRunsPer9"),
    ("summary", "summary"),
    ("note", "note"),
)

FIELDING_FIELDS = _integers(
    ("games_started", "gamesStarted"),
    ("assists", "assists"),
    ("put_outs", "putOuts"),
    ("errors", "errors"),
    ("chances", "chances"),
    ("passed_balls", "passedBall"),
    ("pickoffs", "pickoffs"),
    ("stolen_bases", "stolenBases"),
    ("caught_stealing", "caughtStealing"),
) + _strings(
    ("fielding_percentage", "fielding"),
    ("stolen_base_percentage", "stolenBasePercentage"),
    ("caught_stealing_percentage", "caughtStealingPercentage"),
)
