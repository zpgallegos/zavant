import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


class StrikeZone:
    """
    define landmarks of the strike zone (catcher's view)
    - width is constant at the width of the plate (17 inches)
        - in Statcast, this corresponds to a value of ~0.83 * 2 in whatever units they use for px/pz
        - => the sides of the plate are at -/+ 0.83
    - height depends on the batter's height
        - Statcast gives the top and bottom of the strike zone on each pitch...multiple distinct per game
        - game info also provides a top/bottom for each batter

    :param top: top of the strike zone
    :param bottom: bottom of the strike zone
    :param n: number of rows/columns to divide the zone
    """

    conv = 0.09754885882352941  # 1 inch in Statcast units
    half_plate = conv * 8.5  # plate is 17 inches wide
    ball = conv * 2.9  # baseball is ~2.9 inches in diameter

    def __init__(self, top, bottom, batside_code, n=4):
        self.n = n
        self.top = top
        self.bottom = bottom
        self.batside_code = batside_code
        self.left = -self.half_plate
        self.right = self.half_plate

        # strike zone dims
        self.width = self.half_plate * 2
        self.height = self.top - self.bottom

        # width/height of zones inside the strike zone
        self.width_each = self.width / n
        self.height_each = self.height / n

        # for zones outside the strike zone
        # these are going to be defined as a constant two-ball width outside the strike zone
        self.outside_top = self.top + self.ball
        self.outside_bottom = self.bottom - self.ball
        self.outside_left = self.left - self.ball
        self.outside_right = self.right + self.ball

        # for pitches outside even the outside zones
        self.mid_width = 0  # middle of the plate
        self.mid_height = self.bottom + self.height / 2

        self.create_zones()

    @staticmethod
    def zone_contains(zone_obj: dict, coord_px: dict, coord_pz: dict):
        return (
            coord_px >= zone_obj["left"]
            and coord_px < zone_obj["right"]
            and coord_pz <= zone_obj["top"]
            and coord_pz > zone_obj["bottom"]
        )

    def is_outer(self, i):
        return i == 0 or i == self.n - 1

    def create_zones(self):
        self.zones = {}
        i = 1

        # inside zones
        for row in range(self.n):
            for col in range(self.n):
                obj = {
                    "left": self.left + self.width_each * col,
                    "top": self.top - self.height_each * row,
                }
                obj["right"] = obj["left"] + self.width_each
                obj["bottom"] = obj["top"] - self.height_each

                self.zones[str(i)] = obj
                i += 1

        # outside zones
        outside_zones = [
            {
                "left": self.left,
                "top": self.outside_top,
                "right": self.right,
                "bottom": self.top,
            },
            {
                "left": self.left,
                "top": self.bottom,
                "right": self.right,
                "bottom": self.outside_bottom,
            },
            {
                "left": self.outside_left,
                "top": self.outside_top,
                "right": self.left,
                "bottom": self.outside_bottom,
            },
            {
                "left": self.right,
                "top": self.outside_top,
                "right": self.outside_right,
                "bottom": self.outside_bottom,
            },

            {
                "left": self.outside_left,
                "top": 999,
                "right": self.outside_right,
                "bottom": self.outside_top,
            },
            {
                "left": self.outside_left,
                "top": self.outside_bottom,
                "right": self.outside_right,
                "bottom": -999,
            },
            {
                "left": -999,
                "top": 999,
                "right": self.outside_left,
                "bottom": -999,
            },
            {
                "left": self.outside_right,
                "top": 999,
                "right": 999,
                "bottom": -999,
            },
        ]

        zones = [
            "high",
            "low",
            "inside" if self.batside_code == "R" else "outside",
            "outside" if self.batside_code == "R" else "inside",
            "up",
            "down",
            "in" if self.batside_code == "R" else "away",
            "away" if self.batside_code == "R" else "in",
        ]

        for obj, zone in zip(outside_zones, zones):
            self.zones[zone] = obj

    def cast_categorical(self, x: pd.Series) -> pd.Series:
        return pd.Categorical(x, categories=self.zones.keys(), ordered=True)

    def assign_zone(self, coord_px: float, coord_pz: float) -> tuple:
        for zone, obj in self.zones.items():
            if self.zone_contains(obj, coord_px, coord_pz):
                return zone

        return None

    def draw_zones(self, ax):
        for i, obj in self.zones.items():
            l, r, t, b = (obj[k] for k in ["left", "right", "top", "bottom"])
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
                i,
                color="black",
                fontsize=8,
                ha="center",
                va="center",
            )


if __name__ == "__main__":

    d = pd.read_parquet("s3://zavant-dbt-dev/tables/zavant_dev/int_pitches__temp/")

    zone = StrikeZone(3.49, 1.65, "L", n=3)

    sub = d.copy()
    sub["zone"] = zone.cast_categorical(
        sub.apply(lambda x: zone.assign_zone(x.coord_px, x.coord_pz), axis=1)
    )
    # sub = sub[sub.zone <= zone.max_zone]

    fig, ax = plt.subplots(figsize=(10, 10))
    sns.scatterplot(data=sub, x="coord_px", y="coord_pz", hue="zone")
    ax.set_xlim(-3, 3)
    ax.set_ylim(0, 5)
    zone.draw_zones(ax)
    plt.show()
