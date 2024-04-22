import os
import json
import boto3
import logging
import requests

from datetime import datetime

from zavant_py.api import API_BASE, get_schedule_url
from zavant_py.utils import sort_obj, flatten, list_bucket_files

s3 = boto3.client("s3")


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


def lambda_handler(event, context):
    """
    download games from the MLB API
    routine routines will only incrementally download games occurring in the current season
    use the "past_seasons" parameter to download games from past seasons

    :param event: dict: event data
        past_seasons: list[int] of seasons to download in addition to the current season
    :param context: object: context object
    :return: dict: response
    """
    games = []
    game_keys = set()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    bucket = os.environ["OUT_BUCKET"]

    # get the list of seasons to download
    # on a daily basis, only an update to the current season is needed
    # include_history will download all seasons in the past for ad-hoc updates
    seasons = [int(os.environ["CURRENT_SEASON"])]
    if "past_seasons" in event:
        seasons += event["past_seasons"]
    seasons = sorted(seasons)
    logging.info(f"Downloading seasons: {seasons}")

    # get the list of game_pks whose files are already in the bucket
    # these need to have their download skipped
    game_pks = set()
    for file in list_bucket_files(s3, bucket):
        if file.lower().endswith(".json"):
            game_pks.add(int(os.path.splitext(os.path.basename(file))[0]))

    # download the games
    dld = 0
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
                game_outfile = f"{year}/{game_pk}.json"

                # download the game and write it to the bucket
                game_json = requests.get(f'{API_BASE}{game["link"]}').text
                s3.put_object(Bucket=bucket, Key=game_outfile, Body=game_json)

                logging.info(f"Downloaded {game_pk}: {to_game_str(game)}")
                dld += 1

    return {"statusCode": 200, "body": json.dumps(f"Completed, downloaded {dld} games")}
