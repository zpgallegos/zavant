# https://github.com/toddrob99/MLB-StatsAPI/blob/master/statsapi/endpoints.py
# current roster = https://statsapi.mlb.com/api/v1/sports/1/players

import os
import json
import requests

from datetime import datetime
from utils import sort_obj, flatten

API_BASE = "https://statsapi.mlb.com"
SEASONS = [
    2020,
    2021,
    2022,
    2023,
]
DATA_DIR = os.path.join("json", "games")


def get_schedule_url(start, end):
    return f"{API_BASE}/api/v1/schedule?sportId=1&startDate={start}&endDate={end}"


def include_game(game):
    state = game["status_codedGameState"]
    series = game["seriesDescription"]
    return state == "F" and series == "Regular Season"


def to_game_str(game):
    return (
        f"{game['officialDate']}: "
        f"{game['teams_home_team_name']} vs {game['teams_away_team_name']} "
        f"({game['teams_home_score']}-{game['teams_away_score']})"
    )


def get_season_dates(seasons):
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

    for start, end in get_season_dates(SEASONS):
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

                game_outfile = os.path.join(date_dir, f"{game_pk}.json")
                if os.path.exists(game_outfile):
                    continue

                game_url = f'{API_BASE}{game["link"]}'
                game_json = requests.get(game_url).text
                with open(game_outfile, "w") as f:
                    f.write(game_json)

                print(f"Downloaded game: {to_game_str(game)}")

    with open(os.path.join("json", "schedule", "schedule.json"), "w") as f:
        json.dump(games, f)

    #track_schema("schedule", game_keys)
