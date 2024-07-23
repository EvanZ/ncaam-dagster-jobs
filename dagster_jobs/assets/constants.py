import os

DAILY_SCOREBOARD_TEMPLATE_PATH = lambda date: os.path.join("data", "raw", f"daily_scoreboard_{date}.json")

DATE_FORMAT = "%Y%m%d"
START_DATE = "2024-03-21"
END_DATE = "2024-03-25"