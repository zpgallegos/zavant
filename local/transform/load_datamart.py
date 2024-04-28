import os
import logging

from pyspark.sql import SparkSession, functions as F, Window as W

os.environ["SPARK_HOME"] = (
    "/Users/zpgallegos/opt/anaconda3/envs/zavant/lib/python3.12/site-packages/pyspark"
)

DATA_PATH = "/Users/zpgallegos/Documents/zavant/local/data"
IN_PATH = f"{DATA_PATH}/zavant-flattened-json"
OUT_PATH = f"{DATA_PATH}/zavant-datamart"

CFG_PATH = os.path.join(DATA_PATH, "zavant-config", "load_datamart")
SCHEMAS_PATH = f"{CFG_PATH}/schemas"
TRACKING_PATH = f"{CFG_PATH}/tracking"


def get_schema(dname: str) -> str:
    """
    get the schema for the given @dname

    :param dname: name of the data set
    :return: str, schema as a string
    """
    return open(f"{SCHEMAS_PATH}/{dname}.txt").read()


def get_already_loaded(dname: str) -> list[str]:
    """
    get the list of files that have already been loaded for the given @dname

    :param dname: name of the data set
    :return: list of file paths
    """
    tracking_file = f"{TRACKING_PATH}/{dname}.txt"
    if os.path.exists(tracking_file):
        loaded = [file.strip() for file in open(tracking_file)]
    else:
        loaded = []
    return tracking_file, loaded


def list_all_infiles(dname: str, pref: str = "file://") -> list[str]:
    """
    list all the processed files that are available for transformation
    this is used to check the files beforehand against those that have already been processed

    :param pref: prefix to add to the file path. if necessary
        default is "file://", which spark's input_file_name() function adds for local files
        may need to be "s3://" for files in s3
    :return: list of file paths
    """
    return [
        os.path.join(f"{pref}{root}", file)
        for root, _, files in os.walk(f"{IN_PATH}/{dname}")
        for file in files
    ]


def write_tracking_file(tracking_file, prior, new):
    """
    write the new tracking file after new data has been loaded
    overwrite instead of append is intentional to mimic procedure on s3

    :param tracking_file: path to the tracking file
    :param prior: list of files that were already loaded at start of procedure
    :param new: list of newly loaded files
    :return: None
    """
    with open(tracking_file, "w") as f:
        for file in prior + new:
            f.write(f"{file}\n")


DNAMES = {
    "game_info": "d_game_info",
    "game_players": "d_players",
    "game_teams": "d_game_teams",
    "game_boxscore": "d_game_boxscore",
    "play_info": "f_play_info",
    "play_events": "f_play_events",
    "play_runners": "f_play_runners",
}


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    spark = SparkSession.builder.appName("load_datamart").getOrCreate()

    for dname, tbl in DNAMES.items():
        schema = get_schema(dname)
        out_path = f"{OUT_PATH}/{tbl}"

        if dname == "game_players":
            # source data for the players dimension table
            # this data is player attributes for the most recent game they played in
            # no partitioning, prior load logic tracking doesn't apply

            load_path = f"{IN_PATH}/{dname}/*/*.json"
            df = spark.read.format("json").schema(schema).load(load_path)
            df = df.withColumn(
                "max_game_pk", F.max("game_pk").over(W.partitionBy("player_id"))
            )
            df = df.filter(F.col("game_pk") == F.col("max_game_pk")).drop("max_game_pk")

            df.write.format("parquet").mode("overwrite").save(out_path)

            continue

        infiles = list_all_infiles(dname)
        tracking_file, prior = get_already_loaded(dname)
        to_load = list(set(infiles) - set(prior))

        if not to_load:
            logging.info(f"No new data to process for {dname}, skipping...")
            continue

        df = spark.read.format("json").schema(schema).load(to_load)
        df = df.withColumn("file", F.input_file_name())
        df = df.withColumn(
            "partition_0",
            F.regexp_extract(F.col("file"), r"/(\d{4})/[^/]*\.json$", 1),
        )

        df.write.format("parquet").partitionBy("partition_0").mode("append").save(
            out_path
        )

        new = df.select("file").distinct().rdd.map(lambda row: row.file).collect()
        write_tracking_file(tracking_file, prior, new)
