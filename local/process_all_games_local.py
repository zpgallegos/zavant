"""
local-only script to process raw game data into the zavant-processed tables
on AWS each file is processed on creation in the raw bucket via an S3 event trigger,
so no need for an equivalent script there
"""

import os
import subprocess
import multiprocessing

CPU = multiprocessing.cpu_count()
INTERP = "/Users/zpgallegos/opt/anaconda3/envs/zavant/bin/python3.12"
SCRIPT = "zavant-process-raw-game.py"
IN_DIR = "data/zavant-games-raw"


def to_cmd(key: str):
    """
    convert a key to a command line argument for the processing script
    """
    return f"{INTERP} {SCRIPT} --key={key}"


def process(key: str):
    """
    process a file in the raw bucket
    """
    subprocess.run(to_cmd(key), shell=True, check=True)


if __name__ == "__main__":

    # collect keys of all files in the raw bucket
    # processing script expects them in key="YYYY/MM/DD/game_pk.json" format
    keys = []
    for root, _, files in os.walk(IN_DIR):
        for file in files:
            if file.lower().endswith(".json"):
                _, prefix = root.split(IN_DIR + "/")
                key = os.path.join(prefix, file)
                keys.append(key)

    # process all files in parallel
    pool = multiprocessing.Pool(CPU)
    pool.map(process, keys)
    pool.close()
