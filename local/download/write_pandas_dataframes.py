import os
import json
import pandas as pd

from zavant_py.utils import sort_keys

DATA_DIR = "/Users/zpgallegos/Documents/zavant/local/data"
PROC_DIR = f"{DATA_DIR}/zavant-flattened-json"
OUT_DIR = f"{DATA_DIR}/pandas"

if __name__ == "__main__":

    for dname in os.listdir(PROC_DIR):

        data = []
        for root, _, files in os.walk(os.path.join(PROC_DIR, dname)):
            for file in files:
                if not file.lower().endswith(".json"):
                    continue
                for line in open(os.path.join(root, file)).readlines():
                    d = json.loads(line)
                    d["partition_0"] = os.path.basename(root)
                    data.append(d)
        
        df = pd.DataFrame(data)
        cols = [k for k in sort_keys(df.columns) if k != "partition_0"] + ["partition_0"]
        df = df[cols]
        df.to_parquet(os.path.join(OUT_DIR, f"{dname}.parquet"))



