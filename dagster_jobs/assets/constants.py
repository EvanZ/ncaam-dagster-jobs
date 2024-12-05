SCOREBOARD_URL_TEMPLATE = lambda date_str: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50&limit=250"
GAME_SUMMARY_URL_TEMPLATE = lambda id: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={id}"
TEAMS_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?region=us&lang=en&contentorigin=espn&limit=400&groups=50&groupType=conference&enable=groups"
CONFERENCES_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard/conferences?groups=50"
ROSTER_URL = lambda id, season: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{id}?enable=roster&season={season}"

# NCAAW versions
TEAMS_URL_WOMEN = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams?region=us&lang=en&contentorigin=espn&limit=400&groups=50&groupType=conference&enable=groups"
CONFERENCES_URL_WOMEN = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard/conferences?groups=50"
ROSTER_URL_WOMEN = lambda id, season: f"https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{id}?enable=roster&season={season}"
SCOREBOARD_URL_TEMPLATE_WOMEN = lambda date_str: f"https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?dates={date_str}&groups=50&limit=250"
GAME_SUMMARY_URL_TEMPLATE_WOMEN = lambda id: f"https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary?event={id}"

DATE_FORMAT = "%Y%m%d"

# compute kinds
PYTHON = "python"
DUCKDB = "duckdb"
SKLEARN = "scikitlearn"

# group names
DAILY = "update_daily"
DAILY_WOMEN = "update_daily_women"
MANUAL = "update_manual"
MODELS = "update_model"
MODELS_WOMEN = "update_models_women"
SEASONAL = "update_seasonal"
TOP_LINES = "top_lines"
TOP_LINES_WOMEN = "top_lines_women"
RANKINGS = "season_rankings"
RANKINGS_WOMEN = "season_rankings_women"