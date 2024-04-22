import os

from pyspark.sql import SparkSession, functions as F, Window

os.environ["SPARK_HOME"] = (
    "/Users/zpgallegos/opt/anaconda3/envs/zavant/lib/python3.12/site-packages/pyspark"
)

LOCAL = "/Users/zpgallegos/Documents/zavant/local/data"
DATA_DIR = f"{LOCAL}/zavant-processed/game-players"
OUT_DIR = f"{LOCAL}/zavant-datamart/d_player_info"

D_PLAYER_INFO_COLS = [
    "player_id",
    "active",
    "batSide_code",
    "batSide_description",
    "birthCity",
    "birthCountry",
    "birthDate",
    "boxscoreName",
    "currentAge",
    "draftYear",
    "firstLastName",
    "firstName",
    "height",
    "lastName",
    "lastPlayedDate",
    "link",
    "middleName",
    "mlbDebutDate",
    "nameSlug",
    "pitchHand_code",
    "pitchHand_description",
    "primaryNumber",
    "primaryPosition_abbreviation",
    "primaryPosition_code",
    "primaryPosition_name",
    "primaryPosition_type",
    "weight",
]


if __name__ == "__main__":

    spark = SparkSession.builder.appName("load-d-player-info").getOrCreate()

    df = spark.read.format("json").load(f"{DATA_DIR}/*/*.json")
    df = df.withColumn(
        "max_game_pk", F.max("game_pk").over(Window.partitionBy("player_id"))
    )
    df = df.filter(F.col("game_pk") == F.col("max_game_pk")).drop("max_game_pk")
    df = df.select(D_PLAYER_INFO_COLS).orderBy("player_id")

    df.write.parquet(OUT_DIR, mode="overwrite")
