"""
lambda function to process a raw game json file when its uploaded to the zavant-games-raw bucket
"""

import json
import boto3
import logging

from zavant_py.utils import flatten, sort_obj

TRIGGER_BUCKET = "zavant-games-raw"
PROC_BUCKET = "zavant-processed"

s3 = boto3.client("s3")


def write(client, data: dict | list[dict], dname: str, key: str):
    """
    write the flattened data @obj to its destination file
    bucket is determined by @dname

    :param obj: the flattened data
    :param dname: the destination bucket name prefix
    :param key: the original file key from the trigger
    :return: None
    """
    if isinstance(data, dict):
        data = [data]

    line_delim = ""
    for obj in data:
        line_delim += json.dumps(sort_obj(obj)) + "\n"

    out_key = f"{dname}/{key}"
    client.put_object(Bucket=PROC_BUCKET, Key=out_key, Body=line_delim)

    logging.info(f"wrote {dname} data for {key}")


def lambda_handler(event, context):
    if "Records" in event:  # triggered by s3 upload
        bucket = event["Records"][0]["s3"]["bucket"]["name"]  # bucket name
        key = event["Records"][0]["s3"]["object"]["key"]  # file name
    else:
        key = event.get("key")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(f"Processing {key} from {TRIGGER_BUCKET}")

    # get the object from s3
    obj = s3.get_object(Bucket=TRIGGER_BUCKET, Key=key)

    game = json.loads(obj["Body"].read())

    game_pk = game["gamePk"]
    game_data = game["gameData"]
    live_data = game["liveData"]
    boxscore = live_data["boxscore"]
    plays = live_data["plays"]["allPlays"]

    # game data. players and teams are split off into their own tables
    players = game_data.pop("players")
    teams = game_data.pop("teams")
    write(s3, flatten(game_data), "game_info", key)

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
    write(s3, game_teams, "game_teams", key)

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
    write(s3, game_players, "game_players", key)

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
    write(s3, game_boxscore, "game_boxscore", key)

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

    write(s3, play_info, "play_info", key)
    write(s3, play_events, "play_events", key)
    write(s3, play_runners, "play_runners", key)
