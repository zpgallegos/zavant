import os
import json
import pandas as pd

from utils import flatten, sort_keys, sort_obj

DATA_DIR = "json"
DNAMES = [
    "game_data",
    "game_teams",
    "game_players",
    "game_boxscore",
    "play_info",
    "play_events",
    "play_runners",
]


def init_tracker():
    return {dname: [] for dname in DNAMES}


def append_to_tracker(tracker, dname, obj):
    tracker[dname].append(sort_obj(obj))


def write_game_data(tracker, game_pk, game_date):
    year, month, day = game_date.split("-")
    for dname, objs in tracker.items():
        out_dir = os.path.join(DATA_DIR, dname, year, month, day)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        with open(os.path.join(out_dir, f"{game_pk}.json"), "w") as f:
            json.dump(objs, f)


if __name__ == "__main__":
    # tracker to store data for an aggregated pandas dataframe
    all_data = init_tracker()

    for root, dirs, files in os.walk(os.path.join(DATA_DIR, "games")):
        for file in files:
            if not file.endswith(".json"):
                continue

            # tracker to store the data for this game
            data = init_tracker()

            game = json.loads(open(os.path.join(root, file)).read())

            game_pk = game["gamePk"]
            game_data = game["gameData"]
            live_data = game["liveData"]
            boxscore = live_data["boxscore"]
            game_date = game["gameData"]["datetime"]["officialDate"]
            plays = live_data["plays"]["allPlays"]

            # game data = info about the game (start time, status, teams, etc.)
            # players and teams are split off into their own tables
            players = game_data.pop("players")
            teams = game_data.pop("teams")

            g = flatten(game_data)
            append_to_tracker(data, "game_data", g)

            # team info. this is better to union for easier joins
            for team_game_loc, team_obj in teams.items():
                team_obj["team_id"] = team_obj.pop("id")
                t = {
                    "game_pk": game_pk,
                    "team_game_loc": team_game_loc,
                    **flatten(team_obj),
                }
                append_to_tracker(data, "game_teams", t)

            # players = info for the players that were in the game
            # not really useful stuff but height, weight, shit like that
            for _, player_obj in players.items():
                p = {
                    "game_pk": game_pk,
                    **flatten(player_obj),
                }
                p["player_id"] = p.pop("id")
                append_to_tracker(data, "game_players", p)

            # boxscore = ending stats for players that partipated in the game
            # append the appropriate team id for easier joins to the team data
            for team_game_loc, team_obj in boxscore["teams"].items():
                for player_obj in team_obj["players"].values():
                    player_obj.pop("seasonStats")  # not interested, ignore
                    b = {
                        "game_pk": game_pk,
                        "team_id": teams[team_game_loc]["team_id"],
                        **flatten(player_obj),
                    }
                    append_to_tracker(data, "game_boxscore", b)

            # play_info = information for the play itself (such as the outcome)
            for play_idx, play in enumerate(plays):
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
                append_to_tracker(data, "play_info", i)

                # play_events = individual events composing the play (pitches, pickovers, etc.)
                for event_idx, event in enumerate(events):
                    e = {
                        "game_pk": game_pk,
                        "play_idx": play_idx,
                        "event_idx": event_idx,
                        **flatten(event),
                    }
                    append_to_tracker(data, "play_events", e)

                # play_runners = movement of runners on the play
                for runner_idx, runner in enumerate(runners):
                    r = {
                        "game_pk": game_pk,
                        "play_idx": play_idx,
                        "runner_idx": runner_idx,
                        **flatten(runner),
                    }
                    append_to_tracker(data, "play_runners", r)

            write_game_data(data, game_pk, game_date)

            for dname in DNAMES:
                all_data[dname] += data[dname]

    for dname, objs in all_data.items():
        df = pd.DataFrame(objs)
        df[sort_keys(df.columns)].to_parquet(
            os.path.join("pandas", f"{dname}.parquet"), index=False
        )
