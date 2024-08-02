import json
import pandas as pd

from utils import connect_athena, convert

out_path = lambda player_id: f"../public/data/ab_outcomes/{player_id}.json"

conn = connect_athena()


def build_hierarchy(data: pd.DataFrame) -> dict:
    root = {"name": "root", "children": []}

    for _, row in data.iterrows():
        parts = row.seq.split("-")

        curr = root
        for j, node_name in enumerate(parts):
            children = curr["children"]
            child_node = None

            if j + 1 < len(parts):
                found_child = False

                for k, child_node in enumerate(children):
                    if child_node["name"] == node_name:
                        child_node = children[k]
                        found_child = True
                        break

                if not found_child:
                    child_node = {"name": node_name, "children": []}
                    children.append(child_node)

                curr = child_node

            else:
                child_node = {"name": node_name, "value": row.cnt}
                children.append(child_node)

    return root


if __name__ == "__main__":

    d = pd.read_sql("select * from zavant_dev.ab_outcomes_hierarchy", conn)
    d = d.sort_values(["batter_id", "cnt"], ascending=[True, False])

    for batter_id, grp in d.groupby("batter_id"):
        with open(out_path(batter_id), "w") as f:
            json.dump(build_hierarchy(grp), f, default=convert, indent=2)
