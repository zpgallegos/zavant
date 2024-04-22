import sys

from functools import reduce

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def read(tbl):
    return glueContext.create_dynamic_frame.from_catalog(
        database="zavant", table_name=tbl
    ).toDF()


# player info, used for player name
players = read("d_player_info")

# game teams, used for team name and league name
teams = read("f_game_teams").select("team_id", "abbreviation", "league_name").distinct()
assert teams.groupBy("team_id").count().filter(F.col("count") > 1).count() == 0

# boxscore. only used for games played
box = read("f_game_boxscore").withColumnRenamed("person_id", "player_id")

# play info. used for most stats
plays = read("f_play_info")
for src, dst in [
    ("matchup_batter_id", "player_id"),
    ("matchup_batter_fullName", "player_fullName"),
    ("offense_team_id", "team_id"),
]:
    plays = plays.withColumnRenamed(src, dst)

runners = read("f_play_runners").withColumnRenamed("details_runner_id", "player_id")
runners = runners.join(
    plays.select("game_pk", "play_idx", "team_id"), on=["game_pk", "play_idx"]
)

grp = ["partition_0", "player_id", "team_id"]
stats = []

# games played
g = (
    box.filter(~F.col("gameStatus_isOnBench"))
    .groupBy(*grp)
    .agg(F.countDistinct("game_pk").alias("value"))
    .withColumn("stat", F.lit("G"))
)
stats.append(g)

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

stat = (
    plays.filter(F.col("result_eventType").isin(stat_set))
    .groupBy(*grp, "result_eventType")
    .count()
    .withColumnRenamed("result_eventType", "stat")
    .withColumnRenamed("count", "value")
    .select(g.columns)
)
stats.append(stat)

# PA = plate appearances
# need to exclude plays where a result between the batter/pitcher is not obtained
# these are mainly:
# - a runner getting thrown out for the 3rd out: pickoff, caught stealing, other_out=rundown
# - the game ending on a wild pitch/passed ball/balk for the winning run

plays = plays.withColumn(
    "is_game_last_play",
    F.row_number().over(Window.partitionBy("game_pk").orderBy(F.desc("play_idx"))) == 1,
)

pa = (
    plays.where(
        (F.col("about_isComplete"))  # False when game ends with a game delay
        & ~(
            (F.col("count_outs") == 3)
            & (F.col("result_eventType").rlike("^(pickoff|caught|other_out)"))
        )
        & ~(
            (F.col("is_game_last_play"))
            & F.col("result_eventType").rlike("(wild_pitch|passed_ball|balk)")
        )
    )
    .groupBy(*grp)
    .count()
    .withColumnRenamed("count", "value")
    .withColumn("stat", F.lit("PA"))
)
stats.append(pa)

# strikeouts
# e.g., strikeout, strikeout_double_play, etc.

so = (
    plays.filter(F.col("result_eventType").rlike("^strikeout"))
    .groupBy(*grp)
    .count()
    .withColumnRenamed("count", "value")
    .withColumn("stat", F.lit("SO"))
)
stats.append(so)

# RBIs

rbi = (
    plays.groupBy(*grp)
    .agg(F.sum("result_rbi").alias("value"))
    .withColumn("stat", F.lit("RBI"))
)
stats.append(rbi)

# runs

runs = (
    runners.filter(F.col("movement_end") == "score")
    .groupBy(*grp)
    .count()
    .withColumnRenamed("count", "value")
    .withColumn("stat", F.lit("R"))
)
stats.append(runs)

# stolen bases

sb = (
    runners.filter(F.col("details_eventType").rlike("^stolen_base"))
    .groupBy(*grp)
    .count()
    .withColumnRenamed("count", "value")
    .withColumn("stat", F.lit("SB"))
)
stats.append(sb)

# caught stealing

cs = (
    runners.filter(
        F.col("details_eventType").rlike("^(caught_stealing|pickoff_caught_stealing)")
    )
    .groupBy(*grp)
    .count()
    .withColumnRenamed("count", "value")
    .withColumn("stat", F.lit("CS"))
)
stats.append(cs)

# union, pivot
std = (
    reduce(lambda a, b: a.union(b), stats)
    .groupBy(*grp)
    .pivot("stat")
    .agg(F.first("value"))
    .fillna(0)
)

for src, dst in stat_events.items():
    std = std.withColumnRenamed(src, dst)

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
    .withColumn("TB", F.col("1B") + 2 * F.col("2B") + 3 * F.col("3B") + 4 * F.col("HR"))
    .withColumn("SLG", F.col("TB") / F.col("AB"))
    .withColumn("OPS", F.col("OBP") + F.col("SLG"))
    .fillna(0)
)

std = std.join(players.select("player_id", "firstLastName"), on="player_id").join(
    teams, on="team_id"
)

rename = [
    ("partition_0", "game_season"),
    ("abbreviation", "team_name"),
    ("firstLastName", "player_fullName"),
]

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

for src, dst in rename:
    std = std.withColumnRenamed(src, dst)

std = std.select(*order).orderBy("player_id", "game_season", "team_name")

s3output = glueContext.getSink(
    path="s3://zavant-intermediate/standard_batting",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
)
s3output.setCatalogInfo(catalogDatabase="zavant", catalogTableName="standard_batting")
s3output.setFormat("glueparquet", compression="snappy")
s3output.writeDataFrame(std, glueContext)

job.commit()