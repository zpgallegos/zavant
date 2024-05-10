"""
local test
download raw game files from the MLB API

on AWS this is done via lambda on a nightly schedule, raw files saved to bucket zavant-games-raw
"""

import os
import json
import logging
import requests

from datetime import datetime

from zavant_py.api import API_BASE, get_schedule_url
from zavant_py.utils import sort_obj, flatten

# -> environmental/event
PAST_SEASONS = [2018, 2019, 2020, 2021, 2022, 2023]
CURRENT_SEASON = 2024
DATA_DIR = "/Users/zpgallegos/Documents/zavant/data/zavant-statsapi-raw"


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


def to_game_str(game: dict):
    """
    string representation of a game, just for logging

    :param game: game object
    :return: str
    """
    return (
        f"{game['officialDate']}: "
        f"{game['teams_home_team_name']} vs {game['teams_away_team_name']} "
        f"({game['teams_home_score']}-{game['teams_away_score']})"
    )


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


if __name__ == "__main__":
    games = []
    game_keys = set()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # get the list of seasons to download
    # on a daily basis, only an update to the current season is needed
    # include_history will download all seasons in the past for ad-hoc updates
    seasons = [CURRENT_SEASON] + PAST_SEASONS
    logging.info(f"Downloading seasons: {seasons}")

    # get the list of game_pks whose files are already in the bucket
    # these need to have their download skipped
    game_pks = set()
    for _, _, files in os.walk(DATA_DIR):
        for file in files:
            if file.lower().endswith(".json"):
                game_pks.add(int(os.path.splitext(file)[0]))

    for start, end in get_season_dates(seasons):
        schedule = json.loads(requests.get(get_schedule_url(start, end)).text)

        for date in schedule["dates"]:
            for obj in date["games"]:
                game_pk = obj.pop("gamePk")
                if game_pk in game_pks:  # skip if the game is already in the bucket
                    continue

                game = flatten({"game_pk": game_pk, **obj})
                game["downloaded"] = include_game(game)
                games.append(sort_obj(game))
                game_keys.update(game.keys())

                if not game["downloaded"]:
                    continue

                year, _, _ = game["officialDate"].split("-")
                date_dir = os.path.join(DATA_DIR, year)
                if not os.path.exists(date_dir):
                    os.makedirs(date_dir)

                # download the game json
                game_outfile = os.path.join(date_dir, f"{game_pk}.json")
                game_json = requests.get(f'{API_BASE}{game["link"]}').text
                with open(game_outfile, "w") as f:
                    f.write(game_json)

                print(f"Downloaded game: {to_game_str(game)}")
