SCOREBOARD_URL_TEMPLATE = lambda date_str: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50&limit=150"
GAME_SUMMARY_URL_TEMPLATE = lambda id: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={id}"

DATE_FORMAT = "%Y%m%d"

