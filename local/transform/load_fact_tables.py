import os

from pyspark.sql import SparkSession, functions as F
from zavant_py import sort_keys

os.environ["SPARK_HOME"] = (
    "/Users/zpgallegos/opt/anaconda3/envs/zavant/lib/python3.12/site-packages/pyspark"
)

if __name__ == "__main__":

    os.chdir("/Users/zpgallegos/Documents/zavant/local/data")

    spark = SparkSession.builder.appName("create_fact_tables").getOrCreate()

    tups = [
        ("game-boxscore", "f_game_boxscore"),
        ("game-info", "f_game_info"),
        ("game-teams", "f_game_teams"),
        ("play-info", "f_play_info"),
        ("play-runners", "f_play_runners"),
    ]

    for src, dst in tups:
        df = spark.read.format("json").load(f"zavant-processed/{src}/*/*.json")

        drop = []
        for name, dtype in df.dtypes:
            if (
                dtype.startswith("array")
                or dtype.startswith("struct")
                or name.endswith("link")
            ):
                drop.append(name)

        df = df.drop(*drop)
        df = df.select(*sort_keys(df.columns))

        # partition by season
        df = df.withColumn(
            "partition_0",
            F.regexp_extract(F.input_file_name(), ".*/(\\d{4})/.*", 1).cast("string"),
        )

        df.write.partitionBy("partition_0").mode("overwrite").parquet(
            f"zavant-datamart/{dst}"
        )
