import pandas as pd

from datetime import datetime

cols = [
    "batSide_code",
    "currentAge",
    "draftYear",
    "firstLastName",
    "game_pk",
    "height",
    "id",
    "mlbDebutDate",
    "pitchHand_code",
    "primaryPosition_abbreviation",
    "weight",
]


if __name__ == "__main__":
    players = pd.read_parquet("pandas/game_players.parquet", columns=cols).rename(
        columns={"id": "player_id", "firstLastName": "player_name"}
    )

    # select records with max game_pk for each player
    out = players.loc[players.groupby("player_id").game_pk.idxmax()].drop(
        "game_pk", axis=1
    )

    for col, dtype in {
        "currentAge": int,
        "draftYear": "Int64",
        "weight": int,
    }.items():
        out[col] = out[col].astype(dtype)

    # change date format to MM/DD/YYYY
    out["mlbDebutDate"] = out["mlbDebutDate"].apply(
        lambda x: x
        if pd.isnull(x)
        else datetime.strptime(x, "%Y-%m-%d").strftime("%m/%d/%Y")
    )

    # done, write
    out.to_parquet("output/player_info.parquet", index=False)

