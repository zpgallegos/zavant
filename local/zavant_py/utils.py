import boto3

ID_COLS = [
    "game_pk",
    "team_id",
    "person_id",
    "player_id",
    "play_idx",
    "event_idx",
    "runner_idx",
]


def flatten(obj: dict, pref: str = ""):
    """
    recursively flatten a nested dict, with keys prefixed by @pref

    :param obj: dict
    :param pref: string
    :return: dict
    """
    ret = {}
    for key, value in obj.items():
        key = f"{pref}{key}"
        if isinstance(value, dict):
            # update the return dict with the nested dict, prefixed by @key
            ret.update(flatten(value, f"{key}_"))
        elif isinstance(value, list):
            # if the value is a list, flatten each item in the list
            ret[key] = [
                flatten(item) if isinstance(item, dict) else item for item in value
            ]
        else:
            ret[key] = value
    return ret


def sort_keys(keys: list[str]):
    """
    convenience function to sort keys in a consistent order
    ID_COLS are first, then the rest are sorted alphabetically

    :param keys: list of keys
    :return: list of keys
    """
    out = []
    for id in ID_COLS:
        if id in keys:
            out.append(id)
    return out + sorted(set(keys) - set(ID_COLS))


def sort_obj(obj: dict):
    """
    sort a dict by its keys

    :param obj: dict
    :return: dict
    """
    return {key: obj[key] for key in sort_keys(obj.keys())}


def list_bucket_files(client: boto3.client, bucket: str):
    """
    get the list of existing files in the bucket

    :param client: s3 client
    :param bucket: bucket name
    :return: list of keys
    """
    files = []
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket)
    for page in pages:
        files.extend([obj["Key"] for obj in page["Contents"]])
    return files
