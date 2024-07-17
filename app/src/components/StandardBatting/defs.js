export const STATS = {
    G: "Games Played",
    PA: "Plate Appearances",
    AB: "At Bats",
    R: "Runs Scored",
    H: "Hits",
    "2B": "Doubles",
    "3B": "Triples",
    HR: "Home Runs",
    RBI: "Runs Batted In",
    SB: "Stolen Bases",
    CS: "Caught Stealing",
    BB: "Walks",
    SO: "Strikeouts",
    BA: "Batting Average",
    OBP: "On-Base Percentage",
    SLG: "Slugging",
    OPS: "On-Base Plus Slugging",
    TB: "Total Bases",
    GIDP: "Grounded Into Double Play",
    HBP: "Hit By Pitch",
    SH: "Sacrifice Bunts",
    SF: "Sacrifice Flies",
    IBB: "Intentional Walks",
};

export const FLOATS = ["BA", "OBP", "SLG", "OPS"]

export const formatStat = (obj, key) => {
    if (FLOATS.includes(key)) {
        return obj[key].toFixed(3);
    }
    return obj[key];
};