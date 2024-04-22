import warnings
import argparse
import pandas as pd

warnings.filterwarnings("ignore")  # ignore UserWarning for regex in pandas contains()

game_data_cols = [
    "game_pk",
    "game_season",
]

game_teams_cols = [
    "game_pk",
    "team_id",
    "abbreviation",
    "league_name",
]

play_info_cols = [
    "game_pk",
    "play_idx",
    "about_isComplete",
    "count_outs",
    "matchup_batter_id",
    "matchup_batter_fullName",
    "offense_team_id",
    "result_eventType",
    "result_rbi",
]

play_runners_cols = [
    "game_pk",
    "play_idx",
    "details_eventType",
    "details_movementReason",
    "details_runner_id",
    "details_runner_fullName",
    "movement_end",
]

game_boxscore_cols = [
    "game_pk",
    "team_id",
    "person_id",
    "person_fullName",
    "gameStatus_isOnBench",
]

cols = {
    "game_data": game_data_cols,
    "game_teams": game_teams_cols,
    "play_info": play_info_cols,
    "play_runners": play_runners_cols,
    "game_boxscore": game_boxscore_cols,
}


def read(dname):
    return pd.read_parquet(f"pandas/{dname}.parquet", columns=cols[dname])


# group stats by...
grp = [
    "player_id",
    "game_season",
    "team_name",
    "league_name",
]
player_grp = ["player_id", "game_season"]
team_grp = ["team_name", "game_season"]
perc_grp = ["game_season", "league_name"]

if __name__ == "__main__":
    # game/team data
    teams = (
        read("game_data")
        .merge(read("game_teams"), on="game_pk")
        .rename(columns={"abbreviation": "team_name"})
    )
    teams["league_name"] = teams.league_name.apply(
        lambda x: "NL" if x == "National League" else "AL"
    )

    # boxscore. only needed for games played
    box = read("game_boxscore").rename(
        columns={"person_id": "player_id", "person_fullName": "player_name"}
    )
    box = teams.merge(box, on=["game_pk", "team_id"])

    # play info. needed for most of the stats
    info = read("play_info").rename(
        columns={
            "matchup_batter_id": "player_id",
            "matchup_batter_fullName": "player_name",
            "offense_team_id": "team_id",
        }
    )
    info = teams.merge(info, on=["game_pk", "team_id"])

    # runner movement data. needed for runs and stolen bases
    run = read("play_runners").rename(
        columns={
            "details_runner_id": "player_id",
            "details_runner_fullName": "player_name",
        }
    )
    run = info[
        ["game_pk", "play_idx", "game_season", "team_name", "league_name"]
    ].merge(run, on=["game_pk", "play_idx"])

    # games played
    # select where not on bench. allPositions seems to be null in this case
    g = box.loc[~box.gameStatus_isOnBench].groupby(grp).size().reset_index(name="G")

    # events whose counts constitute a stat on their own
    # or are needed for calculation of another stat
    stat_events = {
        "walk": "walk",
        "single": "1B",
        "double": "2B",
        "triple": "3B",
        "home_run": "HR",
        "intent_walk": "IBB",
        "hit_by_pitch": "HBP",
        "sac_bunt": "SAC",
        "sac_fly": "SF",
        "catcher_interf": "catcher_interf",
        "grounded_into_double_play": "GDP",
    }
    stat_set = set(stat_events)

    stats = (
        info[info.result_eventType.isin(stat_set)]
        .groupby(grp + ["result_eventType"])
        .size()
        .reset_index()
        .pivot(index=grp, columns="result_eventType", values=0)
        .reset_index()
        .fillna(0)
        .rename(columns=stat_events)
    )

    # PA = plate appearances
    # need to exclude plays where a result between the batter/pitcher is not obtained
    # these are mainly:
    # - a runner getting thrown out for the 3rd out: pickoff, caught stealing, other_out=rundown
    # - the game ending on a wild pitch/passed ball/balk for the winning run

    # helper column to identify last play of the game
    info["is_last_play"] = info.groupby("game_pk").play_idx.transform(
        lambda x: x == x.max()
    )

    # count nulls in result_eventType by game_pk
    nulls = info.groupby("game_pk")

    pa = (
        info.loc[
            (info.about_isComplete)  # False when game ends with a game delay
            & ~(
                (info.count_outs == 3)
                & info.result_eventType.str.contains("^(pickoff|caught|other_out)")
            )
            & ~(
                (info["is_last_play"])
                & info.result_eventType.str.contains("^(wild_pitch|passed_ball|balk)")
            )
        ]
        .groupby(grp)
        .size()
        .reset_index(name="PA")
    )

    # strikeouts
    # e.g., strikeout, strikeout_double_play, etc.
    so = (
        info.loc[info.result_eventType.str.contains("^strikeout")]
        .groupby(grp)
        .size()
        .reset_index(name="SO")
    )

    # rbis
    rbi = info.groupby(grp).result_rbi.sum().reset_index(name="RBI")

    # runs
    runs = (
        run.loc[run["movement_end"] == "score"]
        .groupby(grp)
        .size()
        .reset_index(name="R")
    )

    # stolen bases
    sb = (
        run.loc[
            (run.details_movementReason.notnull())
            & (run.details_movementReason.str.contains("^r_stolen_base"))
        ]
        .groupby(grp)
        .size()
        .reset_index(name="SB")
    )

    # caught stealing
    cs = (
        run.loc[
            run.details_eventType.str.contains(
                "^(caught_stealing|pickoff_caught_stealing)"
            )
        ]
        .groupby(grp)
        .size()
        .reset_index(name="CS")
    )

    # merge
    merge = lambda x, y: x.merge(y, on=grp, how="left")

    std = merge(g, stats)
    std = merge(std, pa)
    std = merge(std, so)
    std = merge(std, rbi)
    std = merge(std, runs)
    std = merge(std, sb)
    std = merge(std, cs)
    std = std.fillna(0)

    # handle players that played for more than one team in the same season
    # need to add a row for their aggregated stats for the season
    # and only the aggregated row should be used for percentile calculations
    std["n_teams"] = std.groupby(player_grp).team_name.transform(lambda x: len(x))
    std["is_partial"] = std.n_teams > 1
    std["is_agg"] = False

    part = std[std.is_partial].copy()
    part["team_name"] = part.n_teams.astype(str) + " Teams"
    part["league_name"] = ""

    part_comb = part.groupby(grp).sum().reset_index()
    part_comb["is_partial"] = False
    part_comb["is_agg"] = True

    std = (
        pd.concat([std, part_comb], ignore_index=True)
        .sort_values(grp)
        .reset_index(drop=True)
    )

    # stats that are a linear function of other stats
    std["BB"] = std["walk"] + std["IBB"]
    std["AB"] = (
        std["PA"]
        - std["BB"]
        - std["HBP"]
        - std["SAC"]
        - std["SF"]
        - std["catcher_interf"]
    )
    std["H"] = std["1B"] + std["2B"] + std["3B"] + std["HR"]
    std["TB"] = std["1B"] + 2 * std["2B"] + 3 * std["3B"] + 4 * std["HR"]
    std["OBP"] = (std["H"] + std["BB"] + std["HBP"]) / (
        std["AB"] + std["BB"] + std["HBP"] + std["SF"]
    )
    std["BA"] = std["H"] / std["AB"]
    std["SLG"] = std["TB"] / std["AB"]
    std["OPS"] = std["OBP"] + std["SLG"]
    std = std.fillna(0)

    # count distinct games per team per season, needed for the percentile qualifier
    # players with partials will use the average of their team's games
    team_games = (
        teams.groupby(team_grp).game_pk.nunique().reset_index(name="team_games")
    )

    std = std.merge(team_games, on=team_grp, how="left")
    part_games = (
        std.groupby(player_grp).team_games.mean().reset_index(name="team_avg_games")
    )
    std = std.merge(part_games, on=player_grp, how="left")
    std["team_games"] = std.team_games.fillna(std.team_avg_games).astype(int)
    std = std.drop(columns=["team_avg_games"])

    # percentile qualifier: at least 3.1 plate appearances per team game
    std["perc_qual"] = (~std.is_partial) & ((std.PA / std.team_games) >= 3.1)

    # calculate percentiles for each stat
    stat_cols = {
        "player_id": int,
        "game_season": str,
        "team_name": str,
        "league_name": str,
        "G": int,
        "PA": int,
        "AB": int,
        "R": int,
        "H": int,
        "2B": int,
        "3B": int,
        "HR": int,
        "RBI": int,
        "BB": int,
        "SO": int,
        "SB": int,
        "CS": int,
        "HBP": int,
        "BA": float,
        "OBP": float,
        "SLG": float,
        "OPS": float,
    }

    perc_cols = {
        f"{col}_perc": float for col in stat_cols if col not in grp and col != "G"
    }  # don't calculate a games percentile, who the fuck knows who's included in that

    perc = std[std.perc_qual].copy()
    for perc_col in perc_cols:
        col, _ = perc_col.split("_")
        perc[perc_col] = perc.groupby("game_season")[col].rank(pct=True, method="max")

    perc = perc[perc_cols.keys()]
    std = std.merge(perc, left_index=True, right_index=True, how="left")

    # fill NAs in the partial rows with the aggregated row from the same season
    # this is so that the partial rows inherit the percentile values from the whole season
    std = std.merge(
        std.loc[std.is_agg], how="left", on=player_grp, suffixes=("", "_agg")
    )

    for col in perc_cols:
        agg_col = f"{col}_agg"
        std[col] = std[col].fillna(std[agg_col])
        std = std.drop(agg_col, axis=1)

    # invert percentiles for stats where lower is better
    for col in ["SO", "CS"]:
        perc_col = f"{col}_perc"
        std[perc_col] = 1 - std[perc_col]

    # done, write out
    keep = list(stat_cols.keys()) + list(perc_cols.keys()) + ["perc_qual"]
    out = (
        std[keep + ["is_agg"]]
        .sort_values(["player_id", "game_season", "is_agg"])
        .drop("is_agg", axis=1)
    )
    out.to_parquet("output/mlb_std_batting.parquet", index=False)
