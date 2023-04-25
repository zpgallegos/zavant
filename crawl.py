import os

from pyspark.sql import SparkSession
from utils import sort_keys

DATA_DIR = "json"

os.environ[
    "SPARK_HOME"
] = "/Users/zpgallegos/opt/anaconda3/envs/baseball/lib/python3.10/site-packages/pyspark"


if __name__ == "__main__":
    spark = SparkSession.builder.appName("crawl_schemas").getOrCreate()

    for dname in os.listdir(DATA_DIR):
        if dname in [".DS_Store", "games"]:
            continue

        files = "*.json" if dname == "schedule" else "*/*/*/*.json"
        path = os.path.join(DATA_DIR, dname, files)

        df = (
            spark.read.format("json")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .load(path)
        )
        df = df.select(*sort_keys(df.schema.names))

        with open(os.path.join("schemas", f"{dname}.txt"), "w") as f:
            f.write(df._jdf.schema().treeString())