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
                from zavant_dev.pitch_model__temp
            """,
            "con": conn,
        }
    )

    d = d.sort_values(["hit_hardness", "hit_trajectory", "det_cnt"], ascending=[True, True, False])
                    


    batter = 605141
    ss = pd.read_sql(
        **{
            "sql": f"""
                select * 
                from zavant_dev.stg_statsapi__strikezones 
                where batter_id = {batter}
                order by zone""",
            "con": conn,
        }
    )

    d = pd.read_sql(
        **{
            "sql": f"""
                select * 
                from zavant_dev.statcast_pitches 
                where batter_id = {batter} and season = '2024'
                order by pitch_id
                """,
            "con": conn,
        }
    )
    d["zone"] = pd.Categorical(
        d.zone, categories=sorted(ss.zone.unique()), ordered=True
    )

    d["is_ground_ball"] = d.hit_trajectory == "ground_ball"

    fig, ax = plt.subplots(figsize=(10, 10))
    sns.scatterplot(
        ax=ax,
        data=d,
        x="coord_px",
        y="coord_pz",
        style="pitch_type_desc",
        hue="is_ground_ball",
    )
    ax.set_xlim(-3, 3)
    ax.set_ylim(0, 5)

    for row in ss.itertuples():
        if row.zone_type == "outside":
            continue
        l, r, t, b = (
            getattr(row, k)
            for k in ["zone_left", "zone_right", "zone_top", "zone_bottom"]
        )
        for args in (
            ([l, r], [t, t]),
            ([l, r], [b, b]),
            ([l, l], [t, b]),
            ([r, r], [t, b]),
        ):
            ax.plot(*args, color="black")

        ax.text(
            (l + r) / 2,
            (t + b) / 2,
            row.zone,
            color="black",
            fontsize=8,
            ha="center",
            va="center",
        )

    plt.show()
