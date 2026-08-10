import boto3
import logging

from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = "zavant-statsapi-flat"

PREFIXES = [
    "game_boxscore",
    "game_info",
    "game_players",
    "game_teams",
    "play_events",
    "play_info",
    "play_runners",
]

SEASONS = list(map(str, range(datetime.now().year, 2017, -1)))


def check_file_exists(game_pk: int, prefix: str) -> bool:
    for season in SEASONS:
        key = f"json/{prefix}/{season}/{game_pk}.json"
        try:
            s3.head_object(Bucket=BUCKET, Key=key)
            return True
        except:
            return False


def lambda_handler(event, context):
    """Report whether every downloaded game has all flattened datasets.

    Args:
        event: Lambda event containing `downloaded_games`.
        context: AWS Lambda invocation context.

    Returns:
        Readiness state for the next pipeline step.
    """

    downloaded_games = event["downloaded_games"]

    logger.info(f"checking for flattened files for games: {downloaded_games}")

    if not downloaded_games:
        logger.info("no new games to check, ready...")
        return {"ready": True}

    ready = True
    for game_pk in downloaded_games:
        logger.info(f"checking game: {game_pk}")
        game_ready = True

        for prefix in PREFIXES:
            if not check_file_exists(game_pk, prefix):
                logger.warning(f"Missing: {game_pk} - {prefix}")
                ready = False
                game_ready = False

        logger.info(f"{game_pk} ready: {game_ready}")

    return {"ready": ready}
