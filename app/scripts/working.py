import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyathena import connect


if __name__ == "__main__":

    conn = connect(
        region_name="us-east-1", s3_staging_dir="s3://zavant-dbt-dev/tables/zavant_dev"
    )

    d = pd.read_sql(
        **{
            "sql": f"""
                select *
                from zavant_dev.int_ab_outcomes__temp
            """,
            "con": conn,
        }
    )
    d = d.sort_values(["game_pk", "play_idx", "event_idx"])

    s = (
        d.groupby("play_id")
        .pitch_result.apply(lambda x: "-".join(x))
        .to_frame()
        .rename(columns={"pitch_result": "seq"})
    )
    s["seq_len"] = s.seq.apply(lambda x: len(x.split("-")))

    q = pd.read_sql("select * from zavant_dev.ab_outcomes_hierarchy where batter_id=521692", conn)
