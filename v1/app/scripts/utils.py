import numpy as np
import pandas as pd

from pyathena import connect

connect_athena = lambda: connect(
    region_name="us-east-1", s3_staging_dir="s3://zavant-dbt-dev/tables/zavant_dev"
)


def convert(obj):
    if isinstance(obj, pd.Series):
        return obj.tolist()
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, pd.Timedelta):
        return str(obj)
    if isinstance(obj, (pd.Int64Dtype, pd.Float64Dtype, pd.CategoricalDtype)):
        return obj.astype("object")
    if isinstance(obj, (np.int64, np.int32)):
        return int(obj)
    raise TypeError
