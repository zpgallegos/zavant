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
DATA_DIR = "/Users/zpgallegos/Projects/zavant/data"
IN_DIR = os.path.join(DATA_DIR, "zavant-statsapi-raw")
OUT_DIR = os.path.join(DATA_DIR, "zavant-statsapi-flat", "json")
SCRIPT = "process_game.py"

DNAMES = [
    "game_boxscore",
    "game_info",
    "game_players",
    "game_teams",
    "play_events",
    "play_info",
    "play_runners",
]

SEASONS = range(2018, 2025)


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

    for dname in DNAMES:
        for season in SEASONS:
            out = os.path.join(OUT_DIR, dname, str(season))
            if not os.path.exists(out):
                os.makedirs(out)

    # collect keys of all files in the raw bucket
    # processing script expects them in key="YYYY/game_pk.json" format
    keys = []
    for root, _, files in os.walk(IN_DIR):
        for file in files:
            if file.lower().endswith(".json"):
                _, prefix = root.split(IN_DIR + "/")
                key = os.path.join(prefix, file)
                if os.path.exists(os.path.join(OUT_DIR, "play_runners", key)):
                    continue
                keys.append(key)

    # process all files in parallel
    pool = multiprocessing.Pool(CPU)
    pool.map(process, keys)
    pool.close()
