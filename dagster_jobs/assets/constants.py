SCOREBOARD_URL_TEMPLATE = lambda date_str: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50&limit=150"
GAME_SUMMARY_URL_TEMPLATE = lambda id: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={id}"
TEAMS_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?region=us&lang=en&contentorigin=espn&limit=400&groups=50&groupType=conference&enable=groups"
CONFERENCES_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard/conferences?groups=50"
ROSTER_URL = lambda id, season: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{id}?enable=roster&season={season}"

DATE_FORMAT = "%Y%m%d"

# compute kinds
PYTHON = "python"
DUCKDB = "duckdb"
SKLEARN = "scikitlearn"

# group names
DAILY = "update_daily"
MANUAL = "update_manual"
MODELS = "update_model"
SEASONAL = "update_seasonal"
REPORT = "create_report"