"""
script to process the heavily nested raw games data file into a series of flat structures
several ids are spread to provide for easier joins later

on AWS this is done with Lambda via an S3 event trigger, so this script accepts a file 
path as a command line argument to work in a similar manner

out tables:
* game-info: info about the game (start time, status, teams, etc.)
* game-teams: info about the teams in the game
* game-players: info about the players in the game
* game-boxscore: info about the boxscore of the game
* play-info: play types and outcomes
* play-events: events that constitute the plays. most stats are calculated with here
* play-runners: runner movement in the plays
"""

import os
import json
import argparse

from zavant_py.utils import flatten, sort_obj

# these both have equivalent S3 buckets
DATA_DIR = "/Users/zpgallegos/Projects/zavant/data"
IN_DIR = os.path.join(DATA_DIR, "zavant-statsapi-raw")
OUT_DIR = os.path.join(DATA_DIR, "zavant-statsapi-flat", "json")


def write(data: dict | list[dict], dname: str, key: str):
    """
    write a flattened data @data to its destination file
    its table is determined by the @dname parameter
    @key should be the original file key with the date prefix

    :param data: dict or list[dict], the flattened data to write
    :param dname: str: the table name
    :param key: str: the original file key
    :return: None
    """
    season, file = os.path.dirname(key), os.path.basename(key)

    # local only
    out_dir = os.path.join(OUT_DIR, dname, season)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    outfile = os.path.join(out_dir, file)

    # write line-delimited json
    # check if data ia a list or dict
    if isinstance(data, dict):
        data = [data]

    line_delim = ""
    for obj in data:
        line_delim += json.dumps(sort_obj(obj)) + "\n"

    with open(outfile, "w") as f:
        f.write(line_delim)


if __name__ == "__main__":

    ap = argparse.ArgumentParser()
    ap.add_argument("-k", "--key", required=True, help="file key (.json) to process")
    args = vars(ap.parse_args())

    key = args["key"]

    game = json.loads(open(os.path.join(IN_DIR, key)).read())

    game_pk = game["gamePk"]
    game_data = game["gameData"]
    live_data = game["liveData"]
    boxscore = live_data["boxscore"]
    plays = live_data["plays"]["allPlays"]

    # game info. players and teams are split off into their own tables
    players = game_data.pop("players")
    teams = game_data.pop("teams")
    write(flatten(game_data), "game_info", key)

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
    write(game_teams, "game_teams", key)

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
    write(game_players, "game_players", key)

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
    write(game_boxscore, "game_boxscore", key)

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

    write(play_info, "play_info", key)
    write(play_events, "play_events", key)
    write(play_runners, "play_runners", key)
