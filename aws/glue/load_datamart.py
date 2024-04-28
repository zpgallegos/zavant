import sys
import boto3
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, functions as F, Window as W

logging.basicConfig(level=logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")

IN_BUCKET = "zavant-flattened-json"
OUT_BUCKET = "zavant-datamart"
CFG_BUCKET = "zavant-config"


def get_schema(dname: str) -> str:
    """
    get the schema for the given @dname

    :param dname: name of the data set
    :return: str, schema as a string
    """
    obj = s3.get_object(Bucket=CFG_BUCKET, Key=f"load_datamart/schemas/{dname}.txt")
    return obj["Body"].read().decode("utf-8")


def get_already_loaded(dname: str) -> list[str]:
    """
    get the list of files that have already been loaded for the given @dname

    :param dname: name of the data set
    :return: tuple, (tracking file arguments, list of file paths)
        tracking file is returned for use later in overwriting after new data is added
    """
    tracking_file = {
        "Bucket": CFG_BUCKET,
        "Key": f"load_datamart/tracking/{dname}.txt",
    }
    try:
        obj = s3.get_object(**tracking_file)
        loaded = obj["Body"].read().decode("utf-8").splitlines()
    except s3.exceptions.NoSuchKey:
        loaded = []

    return tracking_file, loaded


def list_all_infiles(dname: str) -> list[str]:
    """
    list all the processed files that are available for transformation
    this is used to check the files beforehand against those that have already been processed

    :param dname: name of the data set
    :return: list of file paths
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=IN_BUCKET, Prefix=f"{dname}/")

    files = []
    for page in pages:
        files.extend(obj["Key"] for obj in page["Contents"])

    return [f"s3://{IN_BUCKET}/{file}" for file in files]


def write_tracking_file(tracking_file: dict, prior: list[str], new: list[str]) -> None:
    """
    write the new tracking file after new data has been loaded

    :param tracking_file: object with bucket and key for the tracking file
    :param prior: list of files that were already loaded at start of procedure
    :param new: list of newly loaded files
    :return: None
    """
    text = "\n".join(prior + new).encode("utf-8")
    s3.put_object(**tracking_file, Body=text)


DNAMES = {
    "game_info": "d_game_info",
    # "game_players": "d_players",
    "game_teams": "d_game_teams",
    "game_boxscore": "d_game_boxscore",
    "play_info": "f_play_info",
    "play_events": "f_play_events",
    "play_runners": "f_play_runners",
}


for dname, tbl in DNAMES.items():
    logging.info(f"processing {dname}...")

    schema = get_schema(dname)
    out_path = f"s3://{OUT_BUCKET}/{tbl}"

    if dname == "game_players":
        # source data for the players dimension table
        # this data is player attributes for the most recent game they played in
        # no partitioning, prior load logic tracking doesn't apply

        load_path = f"s3://{IN_BUCKET}/{dname}/*/*.json"
        df = spark.read.format("json").schema(schema).load(load_path)
        df = df.withColumn(
            "max_game_pk", F.max("game_pk").over(W.partitionBy("player_id"))
        )
        df = df.filter(F.col("game_pk") == F.col("max_game_pk")).drop(
            "max_game_pk", "game_pk"
        )

        df.write.format("parquet").mode("overwrite").save(out_path)

        continue

    infiles = list_all_infiles(dname)
    tracking_file, prior = get_already_loaded(dname)
    to_load = list(set(infiles) - set(prior))

    if not to_load:
        logging.info(f"no new data to process for {dname}, skipping...")
        continue

    logging.info(f"loading {len(to_load)} new files for {dname}...")

    df = spark.read.format("json").schema(schema).load(to_load)
    df = df.withColumn("file", F.input_file_name())
    df = df.withColumn(
        "partition_0",
        F.regexp_extract(F.col("file"), r"/(\d{4})/[^/]*\.json$", 1),
    )

    df.write.format("parquet").partitionBy("partition_0").mode("append").save(out_path)

    new = df.select("file").distinct().rdd.map(lambda row: row.file).collect()
    write_tracking_file(tracking_file, prior, new)

    logging.info(f"successfully loaded {len(new)} new files for {dname}")

job.commit()
