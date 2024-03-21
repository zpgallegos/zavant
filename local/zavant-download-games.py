"""
download raw game files from the MLB API
seasons to include are defined in zavant/config.py

on AWS this is done via lambda on a nightly schedule, raw files saved to bucket zavant-games-raw
"""

import os
import json
import requests

from datetime import datetime

from zavant_py.api import API_BASE, get_schedule_url
from zavant_py.utils import sort_obj, flatten

# -> environmental
ALL_SEASONS = "2018,2019,2020,2021,2022,2023,2024"
DATA_DIR = "data/zavant-games-raw"


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

    seasons = list(map(int, ALL_SEASONS.split(",")))

    for start, end in get_season_dates(seasons):
        schedule = json.loads(requests.get(get_schedule_url(start, end)).text)

        for date in schedule["dates"]:
            for obj in date["games"]:
                game_pk = obj.pop("gamePk")
                game = flatten({"game_pk": game_pk, **obj})
                game["downloaded"] = include_game(game)
                games.append(sort_obj(game))
                game_keys.update(game.keys())

                if not game["downloaded"]:
                    continue

                year, month, day = game["officialDate"].split("-")
                date_dir = os.path.join(DATA_DIR, year, month, day)
                if not os.path.exists(date_dir):
                    os.makedirs(date_dir)
                
                # download the game json. if the file already exists, skip
                game_outfile = os.path.join(date_dir, f"{game_pk}.json")
                if os.path.exists(game_outfile):
                    continue

                game_json = requests.get(f'{API_BASE}{game["link"]}').text
                with open(game_outfile, "w") as f:
                    f.write(game_json)

                print(f"Downloaded game: {to_game_str(game)}")
