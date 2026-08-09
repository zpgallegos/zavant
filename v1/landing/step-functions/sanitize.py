import re
import json

PIPELINE_FILE = "zavant-update-pipeline.json"
SANITIZED_FILE = PIPELINE_FILE.replace(".json", "-sanitized.json")

acct_re = re.compile(r"\d{12}")
vpc_re = re.compile(r"((?:subnet|sg)-)([a-z0-9]+)")


def sanitize_string(x: str) -> str:
    x = acct_re.sub("<ACCT_ID>", x)

    m = vpc_re.match(x)
    if m:
        x = f"{m.group(1)}<id>"

    return x


def sanitize(val):
    if isinstance(val, str):
        return sanitize_string(val)
    return val


def sanitize_json(d: dict) -> dict:
    restrict_keys = ["Cluster", "TaskDefinition"]

    for key, val in d.items():
        if isinstance(val, dict):  # recurse
            d[key] = sanitize_json(val)
        elif isinstance(val, list):
            d[key] = [sanitize(x) for x in val]
        elif key in restrict_keys:
            d[key] = f"<{key.lower()}>"
        else:
            d[key] = sanitize(val)
    return d


if __name__ == "__main__":

    with open(PIPELINE_FILE) as f:
        d = json.load(f)

    with open(SANITIZED_FILE, "w") as f:
        json.dump(sanitize_json(d), f, indent=2)
