import os
import pandas as pd

from pyspark.sql import SparkSession, functions as F, Window
from zavant_py import sort_keys

DATA_DIR = "/Users/zpgallegos/Documents/zavant/local/data"

os.environ[
    "SPARK_HOME"
] = "/Users/zpgallegos/opt/anaconda3/envs/zavant/lib/python3.12/site-packages/pyspark"


game_data_schema = """
    game_pk INT,
    game_season STRING
"""

game_teams_schema = """
    game_pk INT,
    team_id INT,
    abbreviation STRING,
    league_name STRING
"""

play_info_schema = """
    game_pk INT,
    play_idx INT,
    about_isComplete BOOLEAN,
    count_outs INT,
    matchup_batter_id INT,
    matchup_batter_fullName STRING,
    offense_team_id INT,
    result_eventType STRING,
    result_rbi INT
"""

play_runners_schema = """
    game_pk INT,
    play_idx INT,
    details_eventType STRING,
    details_runner_id INT,
    details_runner_fullName STRING,
    movement_end STRING
"""

box_schema = """
    game_pk INT,
    team_id INT,
    person_id INT,
    person_fullName STRING,
    gameStatus_isOnBench BOOLEAN
"""

schemas = {
    "game_data": game_data_schema,
    "game_teams": game_teams_schema,
    "play_info": play_info_schema,
    "play_runners": play_runners_schema,
    "game_boxscore": box_schema,
}

read = (
    lambda dname: spark.read.format("json")
    .schema(schemas[dname])
    .load(f"json/{dname}/*/*/*/*.json")
)


def count_null(df):
    return df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    ).show()


def shape(df):
    return (df.count(), len(df.columns))


if __name__ == "__main__":
    spark = SparkSession.builder.appName("standard_batting").getOrCreate()

    """
    read in data and do some initial joins to get season/team/league info
    many players change teams in the middle of the season, stats so need to be grouped by team
    also need to do some renaming to facilitate smooth grouping across all data sources
    """

    # game/team data
    teams = (
        read("game_data")
        .join(read("game_teams"), on="game_pk")
        .withColumnRenamed("abbreviation", "team_name")
        .withColumn(
            "league_name",
            F.when(F.col("league_name") == "National League", "NL").otherwise("AL"),
        )
    )

    # boxscore. only used for games played
    box = (
        read("game_boxscore")
        .withColumnRenamed("person_id", "player_id")
        .withColumnRenamed("person_fullName", "player_fullName")
    )
    box = teams.join(box, on=["game_pk", "team_id"])

    # fill missing player names with name from record with same player_id
    box = box.withColumn(
        "player_fullName",
        F.when(
            F.col("player_fullName").isNull(),
            F.first("player_fullName", ignorenulls=True).over(
                Window.partitionBy("player_id").orderBy("game_pk")
            ),
        ).otherwise(F.col("player_fullName")),
    )

    # play info. needed for most of the stats
    info = (
        read("play_info")
        .withColumnRenamed("matchup_batter_id", "player_id")
        .withColumnRenamed("matchup_batter_fullName", "player_fullName")
        .withColumnRenamed("offense_team_id", "team_id")
    )
    info = teams.join(info, on=["game_pk", "team_id"])

    # runner movement data. needed for runs and stolen bases
    run = (
        read("play_runners")
        .withColumnRenamed("details_runner_id", "player_id")
        .withColumnRenamed("details_runner_fullName", "player_fullName")
    )
    run = info.select(
        "game_pk", "play_idx", "game_season", "team_name", "league_name"
    ).join(run, on=["game_pk", "play_idx"])

    """ calculate: MLB standard batting stats """

    # group stats by...
    grp = [
        "player_id",
        "player_fullName",
        "game_season",
        "team_name",
        "league_name",
    ]

    # games played
    # select where not on bench. allPositions seems to be null in this case
    g = (
        box.where(~F.col("gameStatus_isOnBench"))
        .groupBy(*grp)
        .agg(F.countDistinct("game_pk").alias("G"))
    )

    # events whose counts constitute a stat on their own
    # or are needs for calculation of another stat

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
        info.where(F.col("result_eventType").isin(stat_set))
        .groupBy(*grp, "result_eventType")
        .count()
        .groupBy(*grp)
        .pivot("result_eventType")
        .sum("count")
        .fillna(0)
    )

    for key, val in stat_events.items():
        stats = stats.withColumnRenamed(key, val)

    # PA = plate appearances
    # need to exclude plays where a result between the batter/pitcher is not obtained
    # these are mainly:
    # - a runner getting thrown out for the 3rd out: pickoff, caught stealing, other_out=rundown
    # - the game ending on a wild pitch/passed ball/balk for the winning run

    # helper column to identify last play of the game
    info = info.withColumn(
        "is_last_play",
        F.row_number().over(Window.partitionBy("game_pk").orderBy(F.desc("play_idx")))
        == 1,
    )

    pa = (
        info.where(
            (F.col("about_isComplete"))  # False when game ends with a game delay
            & ~(
                (F.col("count_outs") == 3)
                & (F.col("result_eventType").rlike("^(pickoff|caught|other_out)"))
            )
            & ~(
                (F.col("is_last_play"))
                & F.col("result_eventType").rlike("(wild_pitch|passed_ball|balk)")
            )
        )
        .groupBy(*grp)
        .count()
        .withColumnRenamed("count", "PA")
    )

    # strikeouts
    # e.g., strikeout, strikeout_double_play, etc.

    so = (
        info.where(F.col("result_eventType").rlike("^strikeout"))
        .groupBy(*grp)
        .count()
        .withColumnRenamed("count", "SO")
    )

    # rbis

    rbi = info.groupby(grp).agg(F.sum("result_rbi").alias("RBI"))

    # runs

    r = (
        run.where(F.col("movement_end") == "score")
        .groupBy(*grp)
        .count()
        .withColumnRenamed("count", "R")
    )

    # stolen bases

    sb = (
        run.where(F.col("details_eventType").rlike("^stolen_base"))
        .groupBy(*grp)
        .count()
        .withColumnRenamed("count", "SB")
    )

    # caught stealing

    cs = (
        run.where(
            F.col("details_eventType").rlike(
                "^(caught_stealing|pickoff_caught_stealing)"
            )
        )
        .groupBy(*grp)
        .count()
        .withColumnRenamed("count", "CS")
    )

    # merge

    std = (
        g.join(stats, on=grp, how="left")
        .join(pa, on=grp, how="left")
        .join(so, on=grp, how="left")
        .join(rbi, on=grp, how="left")
        .join(r, on=grp, how="left")
        .join(sb, on=grp, how="left")
        .join(cs, on=grp, how="left")
        .fillna(0)
    )

    # calculated stats

    std = (
        std.withColumn("BB", F.col("walk") + F.col("IBB"))
        .withColumn(
            "AB",
            F.col("PA")
            - F.col("BB")
            - F.col("HBP")
            - F.col("SAC")
            - F.col("SF")
            - F.col("catcher_interf"),
        )
        .withColumn("H", F.col("1B") + F.col("2B") + F.col("3B") + F.col("HR"))
        .withColumn("BA", F.col("H") / F.col("AB"))
        .withColumn(
            "OBP",
            (F.col("H") + F.col("BB") + F.col("HBP"))
            / (F.col("AB") + F.col("BB") + F.col("HBP") + F.col("SF")),
        )
        .withColumn(
            "TB", F.col("1B") + 2 * F.col("2B") + 3 * F.col("3B") + 4 * F.col("HR")
        )
        .withColumn("SLG", F.col("TB") / F.col("AB"))
        .withColumn("OPS", F.col("OBP") + F.col("SLG"))
        .fillna(0)
    )

    order = [
        "game_season",
        "player_id",
        "player_fullName",
        "team_name",
        "league_name",
        "G",
        "PA",
        "AB",
        "R",
        "H",
        "2B",
        "3B",
        "HR",
        "RBI",
        "BB",
        "SO",
        "SB",
        "CS",
        "HBP",
        "BA",
        "OBP",
        "SLG",
        "OPS",
    ]

    std = std.select(*order)

    # standard stats done, write out
    std.write.parquet(
        os.path.join("output", "standard-mlb-batting-stats"), mode="overwrite"
    )

    with pd.ExcelWriter("std.xlsx") as wrt:
        std.toPandas().sort_values(["player_id", "game_season"]).to_excel(
            wrt, index=False
        )

    # advanced stats


