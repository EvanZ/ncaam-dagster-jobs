import datetime
import json
import os
import re
from datetime import datetime
from typing import Generator

import dagster
from dagster import asset, op, AssetExecutionContext, MaterializeResult, MetadataValue, AssetCheckResult, AssetCheckSpec, Output
from dagster_duckdb import DuckDBResource
import numpy
import pandas
import warnings
from duckdb import InvalidInputException

from . import queries
from .constants import DATE_FORMAT, \
    SCOREBOARD_URL_TEMPLATE, GAME_SUMMARY_URL_TEMPLATE
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
    group_name="download_files",
    partitions_def=daily_partition,
    check_specs=[AssetCheckSpec(name="all_games_completed_is_true", asset="daily_scoreboard_file"),
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
    scoreboard_info = fetch_data(scoreboard_url_complete)
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

    all_games_completed = not any(~df['completed'])
    valid_ids = bool(df['id'].apply(lambda x: bool(re.match(r'^4\d{8}$', x))).all())

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
            "any_postseason": MetadataValue.bool(any(df['abbreviation']=="post")),
            "schedule": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    group_name="download_files",
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
            game_summary = fetch_data(game_summary_url_complete)
            json.dump(game_summary, f)

    return MaterializeResult(
        metadata={
            "num_games": len(ids),
            "ids": MetadataValue.md(ids.to_markdown())
        }
    )

@asset(
    deps=[daily_scoreboard_file],
    group_name="staging_tables",
    partitions_def=daily_partition,
)
def stage_daily_scoreboard(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    loads scoreboard of events from downloaded files into duckdb | partitioned by day
    """

    partition_date = context.partition_key
    path = os.path.join(storage.filepath, "scoreboard", f"{partition_date}", "scoreboard.json")
    context.log.info(f'scoreboard filepath: {path}')
    
    with database.get_connection() as conn:
        conn.execute(queries.create_table_stage_daily_scoreboard())
        res = conn.execute(queries.insert_table_stage_daily_scoreboard(path, partition_date)).fetchnumpy()
        df = conn.execute(f"from stage_daily_scoreboard where date='{partition_date}'").df()

    return MaterializeResult(
        metadata={
            "num_games_inserted": len(res['game_id']),
            "summary": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    group_name="staging_tables",
    deps=[stage_daily_scoreboard, game_summary_files],
    partitions_def=daily_partition
)
def stage_game_logs(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    staging table for game logs containing box score totals for each team
    """

    partition_date_str = context.partition_key
    path = os.path.join(storage.filepath, "games", partition_date_str, "*.json")
    
    with database.get_connection() as conn:
        conn.execute(queries.create_table_stage_game_logs())
        res = conn.execute(queries.insert_table_stage_game_logs(path)).fetchnumpy()
        df = conn.execute(f"from stage_game_logs where date='{partition_date_str}';").df()

    return MaterializeResult(
        metadata={
            "num_games_inserted": len(res['game_id']),
            "summary": MetadataValue.md(df[['game_id', 'date','team_1_location','team_2_location']].to_markdown())
        }
    )

@asset(
    group_name="staging_tables",
    deps=[game_summary_files],
    partitions_def=daily_partition
)
def stage_player_lines(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage table for player lines (ie box score stats) for each game
    """

    partition_date = context.partition_key
    path = os.path.join(storage.filepath, "games", partition_date, "*.json")

    with database.get_connection() as conn:
        conn.execute(queries.create_table_stage_player_lines())
        res = conn.execute(queries.insert_table_stage_player_lines(path, partition_date)).fetchnumpy()
        df = conn.execute(f"from stage_player_lines where date='{partition_date}';").df()
    
    return MaterializeResult(
        metadata={
            "num_lines_inserted": len(res['player_id']),
            "summary": MetadataValue.md(df[['game_id','date','player_id','name','stats']].to_markdown())
        }
    )

@asset(
    group_name="staging_tables",
    deps=[game_summary_files],
    partitions_def=daily_partition
)
def stage_plays(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    insert plays into staging table
    """
    partition_date = context.partition_key
    path = os.path.join(storage.filepath, "games", partition_date, "*.json")

    with database.get_connection() as conn:
        conn.execute(queries.create_table_stage_plays())
        res = conn.execute(queries.insert_table_stage_plays(path, partition_date)).fetchnumpy()
        df = conn.execute(f"from stage_plays where date='{partition_date}' using sample 10%;").df()
    
    return MaterializeResult(
        metadata={
            "num_lines_inserted": len(res['play_id']),
            "summary": MetadataValue.md(df[['game_id','date','play_index','text','assisted','period','game_clock']].to_markdown())
        }
    )
