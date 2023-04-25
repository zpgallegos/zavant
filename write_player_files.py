import json
import numpy as np
import pandas as pd


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def to_obj(row, remove=["player_id"]):
    obj = {k: v for k, v in dict(row).items() if not pd.isnull(v)}
    for k in remove:
        obj.pop(k, None)
    return obj


if __name__ == "__main__":
    player_info = pd.read_parquet("output/player_info.parquet")
    std = pd.read_parquet("output/mlb_std_batting.parquet").sort_values(
        ["player_id", "game_season", "team_name"]
    )

    info = {}
    for player_id, grp in player_info.groupby("player_id"):
        info[player_id] = to_obj(grp.iloc[0])

    report = {}
    for player_id, grp in std.groupby("player_id"):
        report[player_id] = {
            "info": info[player_id],
            "std_batting": [to_obj(row) for _, row in grp.iterrows()],
        }

    for player_id, data in report.items():
        with open(f"report/{player_id}.json", "w") as f:
            json.dump(data, f, cls=NpEncoder)

    # temp, merge player name
    std = std.merge(player_info[["player_id", "player_name"]], on="player_id")
    cols = std.columns.tolist()
    cols.insert(1, cols.pop(cols.index("player_name")))
    std = std[cols]

    with pd.ExcelWriter("std.xlsx") as wrt:
        std.to_excel(wrt, index=False)