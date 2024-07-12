import json
import numpy as np
import pandas as pd

from utils import connect_athena, convert

conn = connect_athena()

OUTFILE = "../public/data/rosters/included_players.json"

PLAYER_VARS = [
    "player_id",
    "fullname",
    "pos_abbr",
    "pos_code",
    "pos_name",
    "pos_type",
]


if __name__ == "__main__":

    d = pd.read_sql("select * from zavant_dev.included_players", conn)
    d = d.sort_values(["league_name", "division_name", "team_long", "pos_code"])

    out = {}
    for (lid, lname), lgrp in d.groupby(["league_id", "league_name"]):
        lid = int(lid)
        out[lid] = {"league_name": lname, "divisions": {}}

        for (did, dname), dgrp in lgrp.groupby(["division_id", "division_name"]):
            did = int(did)
            out[lid]["divisions"][did] = {"division_name": dname, "teams": {}}

            for (tid, tname), tgrp in dgrp.groupby(["team_id", "team_long"]):
                tid = int(tid)
                out[lid]["divisions"][did]["teams"][tid] = {
                    "team_long": tname,
                    "team_short": tgrp["team_short"].iloc[0],
                    "players": [],
                }

                for _, row in tgrp.iterrows():
                    out[lid]["divisions"][did]["teams"][tid]["players"].append(
                        {var: row[var] for var in PLAYER_VARS}
                    )

    with open(OUTFILE, "w") as f:
        json.dump(out, f, default=convert, indent=2)
