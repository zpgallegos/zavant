import json
import numpy as np
import pandas as pd

from utils import connect_athena, convert

out_path = lambda player_id: f"../public/data/player_info/{player_id}.json"

conn = connect_athena()


if __name__ == "__main__":

    d = pd.read_sql("select * from zavant_dev.player_info_cards", conn)

    q = d.set_index("player_id").to_dict(orient="index")

    for player_id, obj in q.items():
        with open(out_path(player_id), "w") as f:
            json.dump(obj, f, default=convert, indent=2)
