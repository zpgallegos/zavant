"""
verify that all game files are downloaded and the processing produced the expected tables
"""

import os
import json
import boto3
import requests

from datetime import datetime

from zavant_py.api import get_schedule_url
from zavant_py.utils import list_bucket_files, flatten

SEASONS = [
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
    2024,
]

RAW_BUCKET = "zavant-games-raw"
PROC_BUCKET = "zavant-flattened-json"

RAW_LOCAL = "/Users/zpgallegos/Documents/zavant/local/data/zavant-games-raw"
PROC_LOCAL = "/Users/zpgallegos/Documents/zavant/local/data/zavant-flattened-json"


def include_game(game: dict):
    """
    -> True if the game should be included in the dataset
    regular season games, finalized state

    :param game: game object
    :return: bool
    """
    state = game["status_codedGameState"]
    series = game["seriesDescription"]
    return state == "F" and series == "Regular Season"


def get_season_dates(seasons: list[str]):
    """
    yield start/end dates for each month in each year in @seasons
    ensures that the game limit isn't reached

    :param seasons: list of years
    :return: generator of (start, end) date tuples, pass to get_schedule_url
    """
    for year in seasons:
        for month in range(1, 13):
            end = 31
            while end > 28:
                try:
                    datetime(year, month, end)
                    break
                except ValueError:
                    end -= 1
            yield f"{year}-{month:02}-01", f"{year}-{month:02}-{end}"


def check(all_games, raw, proc):
    res = {"total_raw_files": len(raw), "not_downloaded": list(all_games - set(raw))}
    for bucket, files in proc.items():
        res.update(
            {
                bucket: {
                    "total_files": len(files),
                    "missing_files": list(set(raw) - set(files)),
                    "extra_files": list(set(files) - set(raw)),
                },
            }
        )
    return res


if __name__ == "__main__":

    # get all included games that are currently available from the API
    all_games = set()
    for start, end in get_season_dates(SEASONS):
        schedule = json.loads(requests.get(get_schedule_url(start, end)).text)
        for date in schedule["dates"]:
            for obj in date["games"]:
                season = obj["gameDate"][:4]
                game_pk = obj.pop("gamePk")
                game = flatten({"game_pk": game_pk, **obj})
                if include_game(game):
                    all_games.add(f"{season}/{game_pk}.json")

    s3 = boto3.client("s3")

    # get all the raw game files that have been downloaded
    raw = list_bucket_files(s3, RAW_BUCKET)

    # check the processed buckets
    proc_prefixed = list_bucket_files(s3, PROC_BUCKET)
    proc = {}
    for key in proc_prefixed:
        dname, file = key.split("/", 1)
        if dname not in proc:
            proc[dname] = [file]
        else:
            proc[dname].append(file)

    # get all local files that have been downloaded
    local_raw = []
    for root, _, files in os.walk(RAW_LOCAL):
        for file in files:
            if not file.endswith(".json"):
                continue
            dname, season = root.split("/")[-2:]
            fname = f"{season}/{file}"
            local_raw.append(fname)

    # check the local files
    local = {}
    for root, _, files in os.walk(PROC_LOCAL):
        for file in files:
            if not file.endswith(".json"):
                continue
            dname, season = root.split("/")[-2:]
            fname = f"{season}/{file}"
            if dname not in local:
                local[dname] = [fname]
            else:
                local[dname].append(fname)

    cloud_res = check(all_games, raw, proc)
    local_res = check(all_games, local_raw, local)

    print("Cloud Results")
    print(json.dumps(cloud_res, indent=4))

    print("\nLocal Results")
    print(json.dumps(local_res, indent=4))
