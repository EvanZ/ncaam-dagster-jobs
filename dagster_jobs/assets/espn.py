import datetime
import json
import os
import re
from glob import glob
from datetime import datetime
from typing import Generator
import warnings

import dagster
from dagster import (
    asset, 
    op, 
    Field,
    String,
    Int,
    AssetExecutionContext, 
    MaterializeResult, 
    MetadataValue, 
    AssetCheckResult, 
    AssetCheckSpec, 
    AssetsDefinition,
    Output, 
    RetryPolicy,
)
from dagster_duckdb import DuckDBResource
from duckdb import InvalidInputException
import fasteners
import numpy
import pandas
from jinja2 import Environment, FileSystemLoader

from . import queries
from .constants import (
    DATE_FORMAT, 
    ROSTER_URL, 
    SCOREBOARD_URL_TEMPLATE, 
    GAME_SUMMARY_URL_TEMPLATE, 
    TEAMS_URL, 
    CONFERENCES_URL,
    PYTHON,
    DUCKDB,
    DAILY,
    REPORT,
    SEASONAL,
    )
from ..partitions import daily_partition
from ..resources import LocalFileStorage, JinjaTemplates
from ..utils.utils import fetch_data

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)


@asset(
    group_name=SEASONAL,
    compute_kind=PYTHON
)
def teams_file(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    downloads the teams from the espn api
    """
    path = os.path.join(storage.filepath, "teams")
    os.makedirs(path, exist_ok=True)

    teams_info = fetch_data(TEAMS_URL, context)
    with open(os.path.join(path, "teams.json"), "w") as f:
        json.dump(teams_info, f)

    with database.get_connection() as conn:
        df = conn.execute(queries.teams_metadata(os.path.join(path, "teams.json"))).df()
        context.log.info(df.head(n=10))

    return MaterializeResult(
        metadata={
            "path": path,
            "num_teams": MetadataValue.int(len(df)),
            "sample": MetadataValue.md(df.sample(n=25)[['shortConference','id','displayName']].to_markdown())
        }
    )

@asset(
    group_name=SEASONAL,
    compute_kind=PYTHON
)
def conferences_file(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    downloads the teams from the espn api
    """
    path = os.path.join(storage.filepath, "conferences")
    os.makedirs(path, exist_ok=True)

    teams_info = fetch_data(CONFERENCES_URL, context)
    with open(os.path.join(path, "conferences.json"), "w") as f:
        json.dump(teams_info, f)

    with database.get_connection() as conn:
        df = conn.execute(queries.conferences_metadata(os.path.join(path, "conferences.json"))).df()
        context.log.info(df.head(n=10))

    return MaterializeResult(
        metadata={
            "path": path,
            "num_confs": MetadataValue.int(len(df)),
            "sample": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=[conferences_file],
    group_name=SEASONAL,
    compute_kind=DUCKDB 
)
def stage_conferences(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stages the conference data
    """
    path = os.path.join(storage.filepath, "conferences", "conferences.json")
    context.log.info(f'path: {path}')

    if not os.path.exists(path):
        raise Exception("path/to/conferences does not exist...aborting further materialization")
    
    with database.get_connection() as conn:
        conn.execute(queries.create_table_stage_conferences())
        context.log.info(queries.insert_table_stage_conferences(path))
        df = conn.execute(queries.insert_table_stage_conferences(path)).df()

    return MaterializeResult(
        metadata={
            "num_rows": len(df),
            "df": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=[conferences_file],
    group_name=SEASONAL,
    compute_kind=DUCKDB  
)
def stage_teams(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stages the teams data
    """
    path = os.path.join(storage.filepath, "teams", "teams.json")
    context.log.info(f'path: {path}')

    if not os.path.exists(path):
        raise Exception("path/to/teams does not exist...aborting further materialization")
    
    with database.get_connection() as conn:
        conn.execute("drop table if exists stage_teams;")
        conn.execute(queries.create_table_stage_teams())
        context.log.info(queries.insert_table_stage_teams(path))
        df = conn.execute(queries.insert_table_stage_teams(path)).df()

    return MaterializeResult(
        metadata={
            "num_rows": len(df),
            "df": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
        deps=[stage_teams],
        group_name=SEASONAL,
        config_schema={"season": int, "ids": [int]},
        compute_kind=PYTHON
)
def rosters_files(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    download the team rosters for a specific season
    if ids are specified in config then only use those, otherwise hit stage_teams table
    """
    invalid_urls = []
    valid_urls = []
    ids = context.op_config['ids']
    if not ids:
        with database.get_connection() as conn:
            df = conn.execute("select id from stage_teams;").df()
        ids = df['id']

    for id in ids:
        context.log.info(f'team id: {id}')
        roster_url = ROSTER_URL(id, context.op_config['season'])
        context.log.info(roster_url)
        try:
            roster = fetch_data(roster_url, context)
            path = os.path.join(storage.filepath, "rosters", str(id))
            os.makedirs(path, exist_ok=True)
            with open(os.path.join(path, "roster.json"), "w") as f:
                context.log.info(f"""
                                id: {id} team: {roster['team']['displayName']} players: {len(roster['team']['athletes'])}
                                """)
                json.dump(roster, f)
            valid_urls.append(roster_url)
        except ValueError as e:
            context.log.info(e)
            invalid_urls.append(roster_url)

    return MaterializeResult(
        metadata={
            "valid_urls": MetadataValue.md(pandas.DataFrame(valid_urls).to_markdown()),
            "invalid_urls": MetadataValue.md(pandas.DataFrame(invalid_urls).to_markdown())
        }
    )

@asset(
        group_name="udpate_annual",
        compute_kind=DUCKDB,
        deps=[rosters_files]
)
def stage_players(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage player info from rosters download
    """
    with database.get_connection() as conn:
        conn.execute("drop table if exists stage_players;")
        conn.execute(queries.create_table_stage_players())

    all_rosters_paths = glob(os.path.join(storage.filepath, "rosters", "*", "roster.json"))
    with database.get_connection() as conn:
        context.log.info(queries.insert_table_stage_players(all_rosters_paths))
        df = conn.execute(queries.insert_table_stage_players(all_rosters_paths)).df()

    return MaterializeResult(
        metadata={
            "players": MetadataValue.md(df.sample(n=100).to_markdown()),
            "num_players": len(df['id'])
        }
    )

@asset(
    group_name=SEASONAL,
    compute_kind=DUCKDB
)
def stage_rsci_rankings(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage rsci rankings from csv files
    """
    path = os.path.join(storage.filepath, "rsci", "*.csv")
    context.log.info(path)

    with database.get_connection() as conn:
        conn.execute("drop table if exists stage_rsci_rankings;")
        conn.execute(f"""
                     create table stage_rsci_rankings as (
                     from read_csv('{path}', header=true, filename=true)
                     );
                     """)
        schema = conn.execute("describe from stage_rsci_rankings limit 1;").df()
        df = conn.execute("from stage_rsci_rankings;").df()
        context.log.info(schema)
    
    return MaterializeResult(
        metadata={
            "schema": MetadataValue.md(schema.to_markdown()),
            "count": MetadataValue.int(len(df)),
            "sample": MetadataValue.md(df.sample(n=50).to_markdown())
        }
    )

@asset(
    group_name=DAILY,
    compute_kind=PYTHON,
    partitions_def=daily_partition,
    retry_policy=RetryPolicy(
            max_retries=5,        # Maximum number of retries
            delay=15              # Delay between retries in seconds
        ),
    check_specs=[AssetCheckSpec(name="has_at_least_one_game", asset="daily_scoreboard_file"),
                AssetCheckSpec(name="all_games_completed_is_true", asset="daily_scoreboard_file"),
                 AssetCheckSpec(name="all_game_ids_have_nine_digits", asset="daily_scoreboard_file")]
)
def daily_scoreboard_file(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> Generator:
    """
    downloads scoreboard of games partitioned by day and writes them to disk
    """
    partition_date_str = context.partition_key
    date_obj = datetime.strptime(partition_date_str, "%Y-%m-%d")
    date_str = date_obj.strftime(DATE_FORMAT)
    scoreboard_url_complete = SCOREBOARD_URL_TEMPLATE(date_str)
    context.log.info(f'scoreboard_url: {scoreboard_url_complete}')
    scoreboard_info = fetch_data(scoreboard_url_complete, context)
    path = os.path.join(storage.filepath, "scoreboard", partition_date_str)
    os.makedirs(path, exist_ok=True)
    scoreboard_file_path = os.path.join(path,"scoreboard.json")
    context.log.info(f'scoreboard_file_path: {scoreboard_file_path}')

    with open(scoreboard_file_path, "w") as f:
        json.dump(scoreboard_info, f)

    scoreboard_metadata = queries.scoreboard_metadata(scoreboard_file_path)
    context.log.info(scoreboard_metadata)

    with database.get_connection() as conn:
        df = conn.execute(scoreboard_metadata).df()
        context.log.info(df.head())

    has_at_least_one_game = bool(len(df['id']) > 0)
    all_games_completed = not any(~df['completed'])
    valid_ids = bool(df['id'].apply(lambda x: bool(re.match(r'^4\d{8}$', x))).all())

    yield AssetCheckResult(
                passed=has_at_least_one_game,
                check_name="has_at_least_one_game"
    )

    if not has_at_least_one_game:
        raise Exception("Empty schedule. Aborting further materialization.")
    
    yield AssetCheckResult(
                passed=all_games_completed,
                check_name="all_games_completed_is_true"
            )
    
    yield AssetCheckResult(
                passed=valid_ids,
                check_name="all_game_ids_have_nine_digits"
            )
    
    yield Output(
        value=df,
        metadata={
            "num_games": len(df['id']),
            "any_overtime": MetadataValue.bool(any(df['period']>2)),
            "any_postseason": MetadataValue.bool(any(df['slug']=="post-season")),
            "schedule": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    retry_policy=RetryPolicy(
            max_retries=5,        # Maximum number of retries
            delay=15              # Delay between retries in seconds
        ),
    group_name=DAILY,
    compute_kind=PYTHON,
    partitions_def=daily_partition
)
def game_summary_files(context: AssetExecutionContext, storage: LocalFileStorage, daily_scoreboard_file: pandas.DataFrame) -> MaterializeResult:
    """
    fetches json data from espn game summary api 
    """

    partition_date = context.partition_key

    ids = daily_scoreboard_file['id']

    for id in ids:
        download_games_path = os.path.join(storage.filepath, "games", partition_date, id)
        os.makedirs(download_games_path, exist_ok=True)
        context.log.info(f'download_games_path = {download_games_path}')
        with open(os.path.join(download_games_path, "game.json"), "w") as f:
            game_summary_url_complete = GAME_SUMMARY_URL_TEMPLATE(id)
            game_summary = fetch_data(game_summary_url_complete, context)
            json.dump(game_summary, f)

    return MaterializeResult(
        metadata={
            "num_games": len(ids),
            "ids": MetadataValue.md(ids.to_markdown())
        }
    )

@asset(
    deps=["daily_scoreboard_file"],
    group_name=DAILY,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_daily_scoreboard(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    loads scoreboard of events from downloaded files into duckdb | partitioned by day
    """

    partition_date = context.partition_key
    path = os.path.join(storage.filepath, "scoreboard", partition_date, "scoreboard.json")
    
    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            res = conn.execute(f"select length(events) as num_events from read_json('{path}');").fetchnumpy()
            if not res['num_events'] > 0:
                raise Exception("No games scheduled. Abort further materialization steps.")
            conn.execute(queries.create_table_stage_daily_scoreboard())
            context.log.info(queries.insert_table_stage_daily_scoreboard(path, partition_date))
            res = conn.execute(queries.insert_table_stage_daily_scoreboard(path, partition_date)).fetchnumpy()
            df = conn.execute(f"from stage_daily_scoreboard where date='{partition_date}';").df()

    return MaterializeResult(
        metadata={
            "num_games_inserted": len(res['game_id']),
            "summary": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=["game_summary_files", "stage_daily_scoreboard"],
    group_name=DAILY,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_game_logs(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    staging table for game logs containing box score totals for each team
    """

    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            context.log.info(queries.create_table_stage_game_logs())
            conn.execute(queries.create_table_stage_game_logs())
            context.log.info(queries.insert_table_stage_game_logs(files))
            res = conn.execute(queries.insert_table_stage_game_logs(files)).fetchnumpy()
            df = conn.execute(f"from stage_game_logs where date='{partition_date}';").df()

    return MaterializeResult(
        metadata={
            "num_games_inserted": len(res['game_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id', 'date','team_1_location','team_2_location']].to_markdown())
        }
    )

@asset(
    deps=["game_summary_files"],
    group_name=DAILY,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_player_lines(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage table for player lines (ie box score stats) for each game
    """

    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            conn.execute(queries.create_table_stage_player_lines())
            context.log.info(queries.insert_table_stage_player_lines(files, partition_date))
            res = conn.execute(queries.insert_table_stage_player_lines(files, partition_date)).fetchnumpy()
            df = conn.execute(f"from stage_player_lines where date='{partition_date}';").df()
    
    return MaterializeResult(
        metadata={
            "num_lines_inserted": len(res['player_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id','date','player_id','name','stats']].to_markdown())
        }
    )

@asset(
    deps=["game_summary_files"],
    group_name=DAILY,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_plays(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    insert plays into staging table
    """
    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            conn.execute(queries.create_table_stage_plays())
            context.log.info(queries.insert_table_stage_plays(files, partition_date))
            res = conn.execute(queries.insert_table_stage_plays(files, partition_date)).fetchnumpy()
            df = conn.execute(f"from stage_plays where date='{partition_date}' using sample reservoir(5%);").df()
    
    return MaterializeResult(
        metadata={
            "num_plays_inserted": len(res['play_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id','date','play_index','text','assist','period','game_clock']].to_markdown())
        }
    )

@asset(
    deps=["stage_plays"],
    group_name=DAILY,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_player_shots_by_game(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    partition_date = context.partition_key

    with database.get_connection() as conn:
        context.log.info(queries.create_table_stage_player_shots_by_game())
        conn.execute(queries.create_table_stage_player_shots_by_game())
        context.log.info(queries.insert_table_stage_player_shots_by_game(partition_date))
        res = conn.execute(queries.insert_table_stage_player_shots_by_game(partition_date)).fetchnumpy()
        df = conn.execute(f"from stage_player_shots_by_game where date='{partition_date}' limit 50;").df()

    return MaterializeResult(
        metadata={
            "num_players": MetadataValue.int(len(res['player_id'])),
            "sample": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=["stage_plays"],
    group_name=DAILY,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_player_assists_by_game(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    date = context.partition_key
    sql_query = queries.stage_player_assists_by_game(date)
    context.log.info(sql_query)

    with database.get_connection() as conn:
        context.log.info(queries.create_table_stage_player_assists_by_game())
        conn.execute(queries.create_table_stage_player_assists_by_game())
        df = conn.execute(sql_query).df()

    return MaterializeResult(
        metadata={
            "num_players": MetadataValue.int(len(df['player_id'])),
            "sample": MetadataValue.md(df.head(10).to_markdown())
        }
    )

@asset(
    deps=[
        "stage_player_lines", 
        "stage_player_shots_by_game", 
        "stage_players",
        "stage_player_assists_by_game"
    ],
    partitions_def=daily_partition,
    group_name=DAILY,
    compute_kind=DUCKDB,
    check_specs=[AssetCheckSpec(name="has_at_least_one_player", asset="stage_top_lines")],
)
def stage_top_lines(context: AssetExecutionContext, database: DuckDBResource) -> Generator:
    """
    stage table that will be turned into html report
    """
    partition_date = context.partition_key
    context.log.info(f'partition_date: {partition_date}')

    sql_query = queries.insert_table_stage_top_lines(date=partition_date)

    with database.get_connection() as conn:
        context.log.info(queries.create_table_stage_top_lines())
        conn.execute(queries.create_table_stage_top_lines())
        context.log.info(sql_query)
        df = conn.execute(sql_query).df()

    has_at_least_one_player = bool(len(df['player_id']) > 0)

    yield AssetCheckResult(
                passed=has_at_least_one_player,
                check_name="has_at_least_one_player"
    )

    # if not has_at_least_one_player:
    #     raise Exception("No players inserted into staging table. Check for empty schedule? Aborting further materialization.")
        
    yield Output(
        value=df,
        metadata={
            "query": MetadataValue.md(sql_query),
            "num_players": MetadataValue.int(len(df['player_id'])),
            "top_10": MetadataValue.md(df.head(10).to_markdown())
        }
    )

def build_top_lines_html_table(exp: list[int], name: str="report") -> AssetsDefinition:
    @asset(
        name=f"top_lines_html_table_for_{name}",
        deps=["stage_top_lines"],
        config_schema={
            "start_date": Field(String, is_required=True),
            "end_date": Field(String, is_required=True),
            "top_n": Field(Int, default_value=50, is_required=False)
        },
        group_name=REPORT,
        compute_kind=PYTHON
    )
    def _asset(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage, templates: JinjaTemplates) -> MaterializeResult:
        """
        create html report for newsletter
        """
        start_date = context.op_config['start_date']
        end_date = context.op_config['end_date']
        top_n = context.op_config['top_n']

        with database.get_connection() as conn:
            df = conn.execute(query=queries.top_lines_report_query(
                start_date=start_date,
                end_date=end_date,
                exp=exp,
                top_n=top_n
            )).df()

        # Set up Jinja2 environment to load templates from the current directory
        env = Environment(loader=FileSystemLoader(searchpath=templates.searchpath))
        player_card_template = env.get_template("player_cards_template.html")

        # Convert DataFrame to a list of dictionaries
        players = df.to_dict(orient="records")

        # Render the template with player data
        html_content = player_card_template.render(players=players)
        path = os.path.join(storage.filepath, "top_lines", end_date)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, f"{name}.html"), "w") as f:
            f.write(html_content)
        
        return MaterializeResult(
            metadata={
                "df": MetadataValue.md(df.to_markdown())
            }
        )
    
    return _asset

freshmen_tl = build_top_lines_html_table(exp=[0,1], name="freshmen")
sophomores_tl = build_top_lines_html_table(exp=[2], name="sophomores")
upperclassmen_tl = build_top_lines_html_table(exp=[3,4,5], name="upperclassmen")