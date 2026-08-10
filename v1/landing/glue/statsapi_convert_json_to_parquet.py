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

FLAT_BUCKET = "zavant-statsapi-flat"
IN_PREF = "json"
OUT_PREF = "parquet"
RESOURCE_BUCKET = "zavant-resources"
RESOURCE_PREF = "statsapi_convert_json_to_parquet"


def get_schema(dname: str) -> str:
    obj = s3.get_object(
        Bucket=RESOURCE_BUCKET, Key=f"{RESOURCE_PREF}/schemas/{dname}.txt"
    )
    return obj["Body"].read().decode("utf-8")


def get_already_loaded(dname: str) -> list[str]:
    tracking_file = {
        "Bucket": RESOURCE_BUCKET,
        "Key": f"{RESOURCE_PREF}/tracking/{dname}.txt",
    }
    try:
        obj = s3.get_object(**tracking_file)
        loaded = obj["Body"].read().decode("utf-8").splitlines()
    except s3.exceptions.NoSuchKey:
        loaded = []

    return tracking_file, loaded


def list_all_infiles(dname: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=FLAT_BUCKET, Prefix=f"{IN_PREF}/{dname}/")

    files = []
    for page in pages:
        files.extend(obj["Key"] for obj in page["Contents"])

    return [f"s3://{FLAT_BUCKET}/{file}" for file in files]


def write_tracking_file(tracking_file: dict, prior: list[str], new: list[str]) -> None:
    text = "\n".join(prior + new).encode("utf-8")
    s3.put_object(**tracking_file, Body=text)


DNAMES = [
    "game_info",
    "game_players",
    "game_teams",
    "game_boxscore",
    "play_info",
    "play_events",
    "play_runners",
]

for dname in DNAMES:
    logging.info(f"processing {dname}...")

    schema = get_schema(dname)
    out_path = f"s3://{FLAT_BUCKET}/parquet/{dname}"

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
