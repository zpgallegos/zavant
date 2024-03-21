"""
lambda function to process a raw game json file when its uploaded to the zavant-games-raw bucket

tables (buckets):
- zavant-game-data: info about the game (start time, status, teams, etc.)
- zavant-game-teams: info about the teams in the game
- zavant-game-players: info about the players in the game
- zavant-game-boxscore: info about the boxscore of the game
- zavant-play-info: play types and outcomes
- zavant-play-events: events that constitute the plays. most stats are calculated with here
- zavant-play-runners: runner movement in the plays
"""

import os
import json
import boto3
import logging

from zavant_py.utils import flatten, sort_obj

OUT_BUCKET_PREFIX = os.environ["PROC_PREFIX"]

DNAMES = [
    "game-data",
    "game-teams",
    "game-players",
    "game-boxscore",
    "play-info",
    "play-events",
    "play-runners",
]


def write(client, data: dict | list[dict], dname: str, key: str):
    """
    write the flattened data @obj to its destination file
    bucket is determined by @dname

    :param obj: the flattened data
    :param dname: the destination bucket name, prefixed by "zavant-"
    :param key: the original file key from the trigger
    :return: None
    """
    assert dname in DNAMES, f"Invalid destination name: {dname}"

    if isinstance(data, dict):
        data = [data]

    line_delim = ""
    for obj in data:
        line_delim += json.dumps(sort_obj(obj)) + "\n"

    client.put_object(Bucket=f"{OUT_BUCKET_PREFIX}{dname}", Key=key, Body=line_delim)


def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]  # bucket name
    key = event["Records"][0]["s3"]["object"]["key"]  # file name

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(f"Processing {key} from {bucket}")

    s3 = boto3.client("s3")

    # get the object from s3
    obj = s3.get_object(Bucket=bucket, Key=key)

    game = json.loads(obj["Body"].read())

    game_pk = game["gamePk"]
    game_data = game["gameData"]
    live_data = game["liveData"]
    boxscore = live_data["boxscore"]
    game_date = game["gameData"]["datetime"]["officialDate"]
    plays = live_data["plays"]["allPlays"]

    # game data. players and teams are split off into their own tables
    players = game_data.pop("players")
    teams = game_data.pop("teams")
    write(s3, flatten(game_data), "game-data", key)

    # team info. this is better to union for easier joins
    game_teams = []
    for team_game_loc, team_obj in teams.items():
        team_obj["team_id"] = team_obj.pop("id")
        game_teams.append(
            {
                "game_pk": game_pk,
                "team_game_loc": team_game_loc,
                **flatten(team_obj),
            }
        )
    write(s3, game_teams, "game-teams", key)

    # players = info for the players that were in the game
    # not really useful stuff but height, weight, shit like that
    game_players = []
    for _, player_obj in players.items():
        p = {
            "game_pk": game_pk,
            **flatten(player_obj),
        }
        p["player_id"] = p.pop("id")
        game_players.append(p)
    write(s3, game_players, "game-players", key)

    # boxscore = ending stats for players that partipated in the game
    # append the appropriate team id for easier joins to the team data
    game_boxscore = []
    for team_game_loc, team_obj in boxscore["teams"].items():
        for player_obj in team_obj["players"].values():
            player_obj.pop("seasonStats")  # not interested, ignore
            game_boxscore.append(
                {
                    "game_pk": game_pk,
                    "team_id": teams[team_game_loc]["team_id"],
                    **flatten(player_obj),
                }
            )
    write(s3, game_boxscore, "game-boxscore", key)

    # play_info = information for the play itself (such as the outcome)
    play_info, play_events, play_runners = [], [], []
    for play_idx, play in enumerate(plays):

        # nested play data that's split off into their own tables
        events = play.pop("playEvents")
        runners = play.pop("runners")

        i = {
            "game_pk": game_pk,
            "play_idx": play_idx,
            **flatten(play),
        }
        i["offense_team_id"] = (
            teams["away"]["team_id"]
            if i["about_isTopInning"]
            else teams["home"]["team_id"]
        )
        i["defense_team_id"] = (
            teams["home"]["team_id"]
            if i["about_isTopInning"]
            else teams["away"]["team_id"]
        )
        play_info.append(i)

        # play_events = individual events composing the play (pitches, pickovers, etc.)
        for event_idx, event in enumerate(events):
            play_events.append(
                {
                    "game_pk": game_pk,
                    "play_idx": play_idx,
                    "event_idx": event_idx,
                    **flatten(event),
                }
            )

        # play_runners = movement of runners on the play
        for runner_idx, runner in enumerate(runners):
            play_runners.append(
                {
                    "game_pk": game_pk,
                    "play_idx": play_idx,
                    "runner_idx": runner_idx,
                    **flatten(runner),
                }
            )

    write(s3, play_info, "play-info", key)
    write(s3, play_events, "play-events", key)
    write(s3, play_runners, "play-runners", key)
