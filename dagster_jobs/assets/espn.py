import datetime
import json
import os
import re
from glob import glob
from datetime import datetime
from typing import Generator


import dagster
from dagster import asset, op, AssetExecutionContext, MaterializeResult, MetadataValue, AssetCheckResult, AssetCheckSpec, Output, RetryPolicy
from dagster_duckdb import DuckDBResource
from duckdb import InvalidInputException, ColumnExpression
import fasteners
import numpy
import pandas
import warnings

from . import queries
from .constants import (
    DATE_FORMAT, 
    ROSTER_URL, 
    SCOREBOARD_URL_TEMPLATE, 
    GAME_SUMMARY_URL_TEMPLATE, 
    TEAMS_URL, 
    CONFERENCES_URL,
    KENPOM_URL
    )
from ..partitions import daily_partition
from ..resources import LocalFileStorage
from ..utils.utils import fetch_data

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

def create_drop_table_op(table_name: str):
    @op(name=f"drop_{table_name}_table")
    def drop_table_op(database: DuckDBResource) -> Output:
        """
        Drop a staging table and return metadata with the number of rows dropped.
        """
        with database.get_connection() as conn:
            try:
                # Check if the table exists and count rows
                result = conn.execute(f"SELECT COUNT(*) as row_count FROM {table_name}").fetchone()
                row_count = result[0] if result else 0
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            except InvalidInputException:
                row_count = 0

        return Output(
            value=row_count,
            metadata={
                "rows_dropped": MetadataValue.int(row_count)
            }
        )

    return drop_table_op

@asset(
    group_name="update_manually"
)
def stage_kenpom(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage latest kenpom rankings
    """
    path = os.path.join(storage.filepath, "kenpom", "kenpom.csv")
    context.log.info(path)

    with database.get_connection() as conn:
        conn.execute("""
                     create table if not exists stage_kenpom (
                        rank INT,
                        team STRING PRIMARY KEY,
                        ortg DOUBLE,
                        drtg DOUBLE,
                        year INT
                     );
                     """)
        df = conn.execute(queries.insert_table_stage_kenpom(path)).df()        
 
    return MaterializeResult(
        metadata={
            "path": path,
            "df": MetadataValue.md(df.to_markdown()),
            "num_teams": MetadataValue.int(len(df))
        }
    )

@asset(
    group_name="update_annual"
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
    group_name="update_annual"
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
    group_name="update_annual"    
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
    group_name="update_annual"    
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
        group_name="update_annual",
        config_schema={"season": int, "ids": [int]}
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
    group_name="update_daily",
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
        tags={
            "owner": "evan"
        },
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
    group_name="update_daily",
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
        group_name="udpate_annual",
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
    deps=[daily_scoreboard_file],
    group_name="update_daily",
    partitions_def=daily_partition,
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
    deps=[game_summary_files, stage_daily_scoreboard],
    group_name="update_daily",
    partitions_def=daily_partition,
)
def stage_game_logs(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    staging table for game logs containing box score totals for each team
    """

    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
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
    deps=[game_summary_files],
    group_name="update_daily",
    partitions_def=daily_partition,
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
    deps=[game_summary_files],
    group_name="update_daily",
    partitions_def=daily_partition,
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
