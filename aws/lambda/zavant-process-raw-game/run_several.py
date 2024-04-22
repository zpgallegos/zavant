import subprocess

SCRIPT = "run_single_key.zsh"

keys = [
    "2024/745438.json",
    "2024/746815.json",
    "2024/745115.json",
    "2024/745602.json",
    "2024/745035.json",
    "2024/746090.json",
    "2024/746739.json",
    "2024/745845.json",
    "2024/746332.json",
    "2024/745683.json",
    "2024/746166.json",
    "2024/746413.json",
    "2024/747222.json",
    "2024/747062.json",
    "2024/745279.json",
]


if __name__ == "__main__":
    for key in keys:
        subprocess.call([f"./{SCRIPT}", key])
