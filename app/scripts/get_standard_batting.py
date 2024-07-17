import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from utils import connect_athena, convert

out_path = lambda player_id: f"../public/data/standard_batting/{player_id}.json"
tooltip_path = "../public/data/standard_batting/tooltip.json"

conn = connect_athena()

INFO_COLS = [
    "player_id",
    "season",
    "complete",
    "team_id",
    "team_short",
    "league_name_short",
]

LOWER_IS_BETTER = [
    "CS",
    "SO",
    "GIDP",
]

FLOATS = [
    "BA",
    "OBP",
    "SLG",
    "OPS",
]


def int_formatter(left, right):
    return f"[{left:.0f}, {right:.0f}"


def float_formatter(left, right):
    return f"[{left:.2f}, {right:.2f}"


def perc_formatter(perc):
    perc = perc * 100
    if perc < 1:
        return f"{perc:.2f}%"
    return f"{perc:.1f}%"


if __name__ == "__main__":

    d = pd.read_sql("select * from zavant_dev.standard_batting_included", conn)
    d = d.sort_values(
        ["player_id", "season", "team_id"], ascending=[True, True, False]
    ).reset_index(drop=True)
    STAT_COLS = [c.upper() for c in d.columns if c not in INFO_COLS]
    d.columns = INFO_COLS + STAT_COLS

    # append percentiles. need to leave out incomplete rows

    def ranking_pct(x, lower_is_better=False):
        # not exactly a percentile, want this to be the percentage of players
        # who each player is better than in the given statistic, excluding himself
        # such that best player gets a 100%, worst player gets a 0%
        if lower_is_better:
            x = -x
        return (x.rank(method="min") - 1) / (x.count() - 1)

    c = d[d.complete == 1].copy()
    for stat in STAT_COLS:
        lib = stat in LOWER_IS_BETTER
        eq = ">" if not lib else "<"
        ranks = c.groupby("season")[stat].transform(ranking_pct, lower_is_better=lib)
        c[f"{stat}_perc"] = ranks.apply(lambda x: eq + perc_formatter(x))

    c = c[[k for k in c.columns if k.endswith("_perc")]]
    c = c.reindex(d.index).fillna("")
    d = d.merge(c, left_index=True, right_index=True)

    # write individual player standard batting data
    for player_id, grp in d.groupby("player_id"):
        grp = grp.drop(["player_id", "team_id"], axis=1)
        rows = [row.to_dict() for _, row in grp.iterrows()]

        with open(out_path(player_id), "w") as f:
            json.dump(rows, f, default=convert, indent=2)

    # tooltip data for when metrics are moused over
    int_range = lambda x: np.arange(x.min(), x.max() + 1)
    binfs = {
        "G": lambda x: np.hstack([np.arange(0, 159, 10), [162]]),
        **{stat: int_range for stat in ["3B", "CS", "SH", "SF", "IBB"]},
    }
    default_bins = lambda x: 15

    out = {stat: {} for stat in STAT_COLS}
    for season, grp in d.groupby("season"):
        for stat in STAT_COLS:
            vals = grp[stat]
            bins = binfs.get(stat, default_bins)(vals)
            cnts, bins, _ = plt.hist(grp[stat], bins)

            if stat in FLOATS:
                formatter = float_formatter
            else:
                formatter = int_formatter

            objs = []
            for i, cnt in enumerate(cnts):
                left, right = bins[i], bins[i + 1]
                objs.append(
                    {
                        "bin_left": left,
                        "bin_right": right,
                        "bin_mid": (left + right) / 2,
                        "bin_label": formatter(left, right)
                        + ("]" if i == len(cnts) - 1 else ")"),
                        "count": cnt,
                    }
                )

            out[stat][season] = objs

    with open(tooltip_path, "w") as f:
        json.dump(out, f, indent=2)
