IDS = [
    "game_pk",
    "team_id",
    "person_id",
    "player_id",
    "play_idx",
    "event_idx",
    "runner_idx",
]


def flatten(obj, pref=""):
    ret = {}
    for key, value in obj.items():
        key = f"{pref}{key}"
        if isinstance(value, dict):
            ret.update(flatten(value, f"{key}_"))
        elif isinstance(value, list):
            ret[key] = [
                flatten(item) if isinstance(item, dict) else item for item in value
            ]
        else:
            ret[key] = value
    return ret


def sort_keys(keys):
    out = []
    for id in IDS:
        if id in keys:
            out.append(id)
    return out + sorted(set(keys) - set(IDS))


def sort_obj(obj):
    return {key: obj[key] for key in sort_keys(obj.keys())}
