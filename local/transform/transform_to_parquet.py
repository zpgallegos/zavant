import os
import logging

from pyspark.sql import SparkSession, functions as F

os.environ["SPARK_HOME"] = (
    "/Users/zpgallegos/opt/anaconda3/envs/zavant/lib/python3.12/site-packages/pyspark"
)

# path containing schemas for each dname to use on read. keys are `dname`.txt
SCHEMA_PATH = (
    "/Users/zpgallegos/Documents/zavant/local/data/zavant-config/processed-schemas"
)

# path containing `dname`.txt files used to track which files have already been read
TRANSFORMED_PATH = (
    "/Users/zpgallegos/Documents/zavant/local/data/zavant-config/transformed-tracking"
)

# path containing processed files for each dname. dname is the prefix
IN_PATH = "/Users/zpgallegos/Documents/zavant/local/data/zavant-processed"

# path to write the output (parquet), prefix defined by map `DNAMES`
OUT_PATH = "/Users/zpgallegos/Documents/zavant/local/data/zavant-datamart"

DNAMES = {
    "game_boxscore": "f_game_boxscore",
    "game_info": "f_game_info",
    "game_players": "f_game_players",
    "game_teams": "f_game_teams",
    "play_events": "f_play_events",
    "play_info": "f_play_info",
    "play_runners": "f_play_runners",
}


def get_schema(dname: str) -> str:
    return open(f"{SCHEMA_PATH}/{dname}.txt").read()


def get_already_transformed(dname: str) -> list[str]:
    tracking_file = f"{TRANSFORMED_PATH}/{dname}.txt"
    if os.path.exists(tracking_file):
        trans = [file.strip() for file in open(tracking_file)]
    else:
        trans = []
    return tracking_file, trans


def list_all_infiles(dname: str, pref: str = "file://") -> list[str]:
    """
    list all the processed files that are available for transformation
    this is used to check the files beforehand against those that have already been processed

    :param pref: prefix to add to the file path. if necessary
        default is "file://", which spark's input_file_name() function adds for local files
    :return: list of file paths
    """
    return [
        os.path.join(f"{pref}{root}", file)
        for root, _, files in os.walk(f"{IN_PATH}/{dname}")
        for file in files
    ]


def get_out_path(dname):
    return f"{OUT_PATH}/{DNAMES[dname]}"


def write_tracking_file(tracking_file, trans, new):
    with open(tracking_file, "w") as f:
        for file in trans + new:
            f.write(f"{file}\n")


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    spark = SparkSession.builder.appName(f"transform_to_parquet").getOrCreate()

    for dname in DNAMES:
        infiles = list_all_infiles(dname)
        tracking_file, trans = get_already_transformed(dname)
        to_transform = list(set(infiles) - set(trans))

        if not to_transform:
            logging.info(f"No new data to process for {dname}, skipping...")
            continue

        schema = get_schema(dname)

        d = spark.read.format("json").schema(schema).load(to_transform)
        d = d.withColumn("file", F.input_file_name())
        d = d.withColumn(
            "partition_0",
            F.regexp_extract(F.col("file"), r"/(\d{4})/[^/]*\.json$", 1),
        )

        d.groupBy("partition_0").count().orderBy("partition_0").show()  ##

        d.write.format("parquet").partitionBy("partition_0").mode("append").save(
            get_out_path(dname)
        )

        new = d.select("file").distinct().rdd.map(lambda row: row.file).collect()
        write_tracking_file(tracking_file, trans, new)
