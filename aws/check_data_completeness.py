"""
verify that all game files are downloaded and the processing produced the expected tables

main game bucket:
    - zavant-games-raw
processed buckets:
    - zavant-game-data
    - zavant-game-teams
    - zavant-game-players
    - zavant-game-boxscore
    - zavant-play-info
    - zavant-play-events
    - zavant-play-runners
"""

import json
import boto3
import requests

from datetime import datetime

import sys
sys.path.append(r"/Users/zpgallegos/Documents/zavant/local")

from zavant_py.api import API_BASE, get_schedule_url
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
PROCESSED_BUCKETS = [
    "zavant-game-data",
    "zavant-game-teams",
    "zavant-game-players",
    "zavant-game-boxscore",
    "zavant-play-info",
    "zavant-play-events",
    "zavant-play-runners",
]

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


def check(raw, proc):
    res = {"total_raw_files": len(raw)}
    for bucket, files in proc.items():
        res.update(
            {
                bucket : {
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
                game_pk = obj.pop("gamePk")
                game = flatten({"game_pk": game_pk, **obj})
                if include_game(game):
                    all_games.add(game_pk)
    
    s3 = boto3.client("s3")

    # get all the raw game files that have been downloaded
    raw = list_bucket_files(s3, RAW_BUCKET)

    # check the processed buckets
    proc = {}
    for bucket in PROCESSED_BUCKETS:
        proc[bucket] = list_bucket_files(s3, bucket)

