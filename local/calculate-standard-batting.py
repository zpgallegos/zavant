import os
import pyspark

from pyspark.sql import SparkSession, functions as F, Window

os.environ[
    "SPARK_HOME"
] = "/Users/zpgallegos/opt/anaconda3/envs/zavant/lib/python3.12/site-packages/pyspark"


if __name__ == "__main__":

    spark = SparkSession.builder.appName("standard-batting").getOrCreate()

    df = spark.read.json("data/zavant-play-info/*/*/*/*.json")

    # value counts for event type
    