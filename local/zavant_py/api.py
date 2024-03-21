API_BASE = "https://statsapi.mlb.com"


def get_schedule_url(start: str, end: str):
    """
    get the game schedule between @start and @end
    needed to get the gamePks for the game endpoint

    :param start: start date in the format "YYYY-MM-DD"
    :param end: end date in the format "YYYY-MM-DD"
    :return: url for the schedule endpoint to hit
    """
    return f"{API_BASE}/api/v1/schedule?sportId=1&startDate={start}&endDate={end}"
