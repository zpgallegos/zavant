import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext

ID_COLS = [
    "game_pk",
    "team_id",
    "person_id",
    "player_id",
    "play_idx",
    "event_idx",
    "runner_idx",
]


def sort_keys(keys: list[str]):
    """
    convenience function to sort keys in a consistent order
    ID_COLS are first, then the rest are sorted alphabetically

    :param keys: list of keys
    :return: list of keys
    """
    id_keys = [key for key in ID_COLS if key in keys]
    other_keys = sorted(set(keys) - set(ID_COLS))
    return id_keys + other_keys


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

tups = [
    # ("game_boxscore", "f_game_boxscore"),
    # ("game-info", "f_game_info"),
    # ("game_teams", "f_game_teams"),
    # ("play_info", "f_play_info"),
    ("play_runners", "f_play_runners"),
]

for src, dst in tups:
    df = glueContext.create_dynamic_frame.from_catalog(
        database="zavant", table_name=src, transformation_ctx=src
    )
    dtypes = dict(df.toDF().dtypes)

    # drop unwanted fields
    drop = []
    for name, dtype in dtypes.items():
        if (
            dtype.startswith("array")
            or dtype.startswith("struct")
            or dtype == "void"
            or name.endswith("link")
        ):
            drop.append(name)

    dtypes = {key: value for key, value in dtypes.items() if key not in drop}
    dropped = DropFields.apply(
        frame=df, paths=drop, transformation_ctx=f"{src}_dropped"
    )

    # reorder columns
    columns = sort_keys(dtypes.keys())
    mappings = [(col, dtypes[col], col, dtypes[col]) for col in columns]
    reordered = ApplyMapping.apply(
        frame=dropped, mappings=mappings, transformation_ctx=f"{src}_reordered"
    )

    # done, write out
    out = glueContext.getSink(
        path=f"s3://zavant-datamart/{dst}",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        enableUpdateCatalog=True,
        partitionKeys=["partition_0"],
        transformation_ctx=f"{src}_out",
    )
    out.setCatalogInfo(catalogDatabase="zavant", catalogTableName=dst)
    out.setFormat("glueparquet", compression="snappy")
    out.writeFrame(reordered)

job.commit()
