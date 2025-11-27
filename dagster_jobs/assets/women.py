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
    Bool,
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
    ROSTER_URL_WOMEN,
    SCOREBOARD_URL_TEMPLATE, 
    SCOREBOARD_URL_TEMPLATE_WOMEN,
    GAME_SUMMARY_URL_TEMPLATE_WOMEN, 
    TEAMS_URL, 
    TEAMS_URL_WOMEN,
    CONFERENCES_URL,
    CONFERENCES_URL_WOMEN,
    PYTHON,
    DUCKDB,
    DAILY,
    DAILY_WOMEN,
    RANKINGS_WOMEN,
    SEASONAL,
    TOP_LINES,
    TOP_LINES_WOMEN
    )
from ..partitions import daily_partition
from ..resources import LocalFileStorage, JinjaTemplates
from ..utils.utils import fetch_data

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

@asset(
    group_name=SEASONAL,
    compute_kind=DUCKDB,
    config_schema={
        "path": str
    }
)
def stage_hoopgurlz_rankings(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    """
    stage rsci rankings from csv files
    """
    path = os.path.join(context.op_config["path"], "*.csv")
    context.log.info(path)

    with database.get_connection() as conn:
        conn.execute("drop table if exists stage_hoopgurlz_rankings;")
        conn.execute(f"""
                     create table stage_hoopgurlz_rankings as (
                     from read_csv('{path}', header=true, filename=true)
                     );
                     """)
        schema = conn.execute("describe from stage_hoopgurlz_rankings limit 1;").df()
        df = conn.execute("from stage_hoopgurlz_rankings;").df()
        context.log.info(schema)
    
    return MaterializeResult(
        metadata={
            "schema": MetadataValue.md(schema.to_markdown()),
            "count": MetadataValue.int(len(df)),
            "sample": MetadataValue.md(df.sample(n=50).to_markdown())
        }
    )

@asset(
    group_name=DAILY_WOMEN,
    compute_kind=PYTHON,
    partitions_def=daily_partition,
    retry_policy=RetryPolicy(
            max_retries=5,        # Maximum number of retries
            delay=15              # Delay between retries in seconds
        ),
    check_specs=[AssetCheckSpec(name="has_at_least_one_game", asset="daily_scoreboard_file_women"),
                AssetCheckSpec(name="all_games_completed_is_true", asset="daily_scoreboard_file_women"),
                 AssetCheckSpec(name="all_game_ids_have_nine_digits", asset="daily_scoreboard_file_women")]
)
def daily_scoreboard_file_women(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> Generator:
    """
    downloads scoreboard of games partitioned by day and writes them to disk
    """
    partition_date_str = context.partition_key
    date_obj = datetime.strptime(partition_date_str, "%Y-%m-%d")
    date_str = date_obj.strftime(DATE_FORMAT)
    scoreboard_url_complete = SCOREBOARD_URL_TEMPLATE_WOMEN(date_str)
    context.log.info(f'scoreboard_url: {scoreboard_url_complete}')
    scoreboard_info = fetch_data(scoreboard_url_complete, context)
    path = os.path.join(storage.filepath, "women", "scoreboard", partition_date_str)
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
            delay=5              # Delay between retries in seconds
        ),
    group_name=DAILY_WOMEN,
    compute_kind=PYTHON,
    partitions_def=daily_partition
)
def game_summary_files_women(context: AssetExecutionContext, storage: LocalFileStorage, daily_scoreboard_file_women: pandas.DataFrame) -> MaterializeResult:
    """
    fetches json data from espn game summary api 
    """

    partition_date = context.partition_key

    ids = daily_scoreboard_file_women['id']

    for id in ids:
        download_games_path = os.path.join(storage.filepath, "women", "games", partition_date, id)
        os.makedirs(download_games_path, exist_ok=True)
        context.log.info(f'download_games_path = {download_games_path}')
        with open(os.path.join(download_games_path, "game.json"), "w") as f:
            game_summary_url_complete = GAME_SUMMARY_URL_TEMPLATE_WOMEN(id)
            game_summary = fetch_data(game_summary_url_complete, context)
            json.dump(game_summary, f)

    return MaterializeResult(
        metadata={
            "num_games": len(ids),
            "ids": MetadataValue.md(ids.to_markdown())
        }
    )

@asset(
    deps=["daily_scoreboard_file_women"],
    group_name=DAILY_WOMEN,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_daily_scoreboard_women(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    loads scoreboard of events from downloaded files into duckdb | partitioned by day
    """

    partition_date = context.partition_key
    path = os.path.join(storage.filepath, "women", "scoreboard", partition_date, "scoreboard.json")
    
    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            res = conn.execute(f"select length(events) as num_events from read_json('{path}');").fetchnumpy()
            if not res['num_events'] > 0:
                raise Exception("No games scheduled. Abort further materialization steps.")
            conn.execute(queries.create_table_stage_daily_scoreboard(women=True))
            insert_query = queries.insert_table_stage_daily_scoreboard(path, partition_date, women=True)
            context.log.info(insert_query)
            res = conn.execute(insert_query).fetchnumpy()
            df = conn.execute(f"from stage_daily_scoreboard_women where date='{partition_date}';").df()

    return MaterializeResult(
        metadata={
            "num_games_inserted": len(res['game_id']),
            "summary": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=["game_summary_files_women", "stage_daily_scoreboard_women"],
    group_name=DAILY_WOMEN,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_game_logs_women(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    staging table for game logs containing box score totals for each team
    """

    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "women", "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            create_query = queries.create_table_stage_game_logs(women=True)
            context.log.info(create_query)
            conn.execute(create_query)
            insert_query = queries.insert_table_stage_game_logs(files, women=True)
            context.log.info(insert_query)
            res = conn.execute(insert_query).fetchnumpy()
            df = conn.execute(f"from stage_game_logs_women where date='{partition_date}';").df()

    return MaterializeResult(
        metadata={
            "num_games_inserted": len(res['game_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id', 'date','team_1_location','team_2_location']].to_markdown())
        }
    )

@asset(
    deps=["game_summary_files_women"],
    group_name=DAILY_WOMEN,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_player_lines_women(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage table for player lines (ie box score stats) for each game
    """

    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "women", "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            create_query = queries.create_table_stage_player_lines(women=True)
            context.log.info(create_query)
            conn.execute(create_query)

            insert_query = queries.insert_table_stage_player_lines(files, partition_date, women=True)
            context.log.info(insert_query)
            res = conn.execute(insert_query).fetchnumpy()
            df = conn.execute(f"from stage_player_lines_women where date='{partition_date}';").df()
    
    return MaterializeResult(
        metadata={
            "num_lines_inserted": len(res['player_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id','date','player_id','name','stats']].to_markdown())
        }
    )

@asset(
    deps=["game_summary_files_women"],
    group_name=DAILY_WOMEN,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_plays_women(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    insert plays into staging table
    """
    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "women", "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            create_query = queries.create_table_stage_plays(women=True)
            context.log.info(create_query)
            conn.execute(create_query)

            insert_query = queries.insert_table_stage_plays(files, partition_date, women=True)
            context.log.info(insert_query)
            res = conn.execute(insert_query).fetchnumpy()
            df = conn.execute(f"from stage_plays_women where date='{partition_date}' using sample reservoir(5%);").df()
    
    return MaterializeResult(
        metadata={
            "num_plays_inserted": len(res['play_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id','date','play_index','text','assist','period','game_clock']].to_markdown())
        }
    )

@asset(
    deps=["stage_plays_women"],
    group_name=DAILY_WOMEN,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_player_shots_by_game_women(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    partition_date = context.partition_key

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            create_query = queries.create_table_stage_player_shots_by_game(women=True)
            context.log.info(create_query)
            conn.execute(create_query)

            # Delete existing records for this date to avoid duplicate key errors
            delete_query = f"delete from stage_player_shots_by_game_women where date='{partition_date}';"
            context.log.info(delete_query)
            conn.execute(delete_query)

            # Check for duplicates in the source query
            dup_check_query = f"""
            select 
                game_id,
                player_1_id as player_id,
                count(*) as cnt
            from (
                select 
                    game_id,
                    date,
                    team_id,
                    opp_id,
                    home,
                    player_1_id
                from stage_plays_women
                where type_id in (437, 558, 572, 574) and
                date='{partition_date}' and player_1_id is not null
                group by game_id, date, team_id, opp_id, home, player_1_id
            )
            group by game_id, player_1_id
            having count(*) > 1;
            """
            dup_df = conn.execute(dup_check_query).df()
            if len(dup_df) > 0:
                context.log.warning(f"Found {len(dup_df)} duplicate game_id/player_id combinations:")
                context.log.warning(dup_df.to_string())

            insert_query = queries.insert_table_stage_player_shots_by_game(partition_date, women=True)
            context.log.info(insert_query)
            res = conn.execute(insert_query).fetchnumpy()
            df = conn.execute(f"from stage_player_shots_by_game_women where date='{partition_date}' limit 50;").df()

    return MaterializeResult(
        metadata={
            "num_players": MetadataValue.int(len(res['player_id'])),
            "sample": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=["stage_plays_women"],
    group_name=DAILY_WOMEN,
    partitions_def=daily_partition,
    compute_kind=DUCKDB
)
def stage_player_assists_by_game_women(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    date = context.partition_key

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            create_query = queries.create_table_stage_player_assists_by_game(women=True)
            context.log.info(create_query)
            conn.execute(create_query)

            insert_query = queries.stage_player_assists_by_game(date, women=True)
            context.log.info(insert_query)
            df = conn.execute(insert_query).df()

    return MaterializeResult(
        metadata={
            "num_players": MetadataValue.int(len(df['player_id'])),
            "sample": MetadataValue.md(df.head(10).to_markdown())
        }
    )

@asset(
    deps=[
        "stage_player_lines_women", 
        "stage_player_shots_by_game_women", 
        "stage_players_women",
        "stage_player_assists_by_game_women",
        "stage_box_stat_adjustment_factors_women",
        "stage_shot_type_adjustment_factors_women"
    ],
    partitions_def=daily_partition,
    group_name=DAILY_WOMEN,
    compute_kind=DUCKDB,
    check_specs=[AssetCheckSpec(name="has_at_least_one_player", asset="stage_top_lines_women")],
)
def stage_top_lines_women(context: AssetExecutionContext, database: DuckDBResource) -> Generator:
    """
    stage table that will be turned into html report
    """
    partition_date = context.partition_key
    context.log.info(f'partition_date: {partition_date}')

    create_query = queries.create_table_stage_top_lines(women=True)
    context.log.info(create_query)
    
    insert_query = queries.insert_table_stage_top_lines(date=partition_date, women=True)
    context.log.info(insert_query)

    with database.get_connection() as conn:
        conn.execute(create_query)
        df = conn.execute(insert_query).df()

    has_at_least_one_player = bool(len(df['player_id']) > 0)

    yield AssetCheckResult(
                passed=has_at_least_one_player,
                check_name="has_at_least_one_player"
    )

    if not has_at_least_one_player:
        raise Exception("No players inserted into staging table. Check for empty schedule? Aborting further materialization.")
        
    yield Output(
        value=df,
        metadata={
            "num_players": MetadataValue.int(len(df['player_id'])),
            "top_10": MetadataValue.md(df.head(10).to_markdown())
        }
    )

def build_top_lines_html_table_women(exp: list[int], name: str="report") -> AssetsDefinition:
    @asset(
        name=f"top_lines_html_table_for_{name}_women",
        deps=["stage_top_lines_women"],
        config_schema={
            "start_date": Field(String, is_required=True),
            "end_date": Field(String, is_required=True),
            "top_n": Field(Int, default_value=50, is_required=False)
        },
        group_name=TOP_LINES_WOMEN,
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
                top_n=top_n,
                women=True
            )).df()

        # Set up Jinja2 environment to load templates from the current directory
        env = Environment(loader=FileSystemLoader(searchpath=templates.searchpath))
        player_card_template = env.get_template("player_cards_template_all_loop_tables_women.html")

        # Convert DataFrame to a list of dictionaries
        players = df.to_dict(orient="records")

        # Render the template with player data
        html_content = player_card_template.render(players=players, dates={
            'start': datetime.strptime(start_date, "%Y-%m-%d").strftime('%b. %d'),
            'end': datetime.strptime(end_date, "%Y-%m-%d").strftime('%b. %d')
        })
        path = os.path.join(storage.filepath, "women", "top_lines", end_date)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, f"{name}.html"), "w") as f:
            f.write(html_content)
        
        return MaterializeResult(
            metadata={
                "df": MetadataValue.md(df.to_markdown())
            }
        )
    
    return _asset

all_tl = build_top_lines_html_table_women(exp=[0,1,2,3,4,5], name="all")

def build_season_rankings_report_women(exp: list[int], name: str="report") -> AssetsDefinition:
    @asset(
        name=f"season_rankings_report_for_{name}_women",
        deps=["stage_top_lines_women", "stage_players_women", "stage_teams_women"],
        config_schema={
            "start_date": Field(String, is_required=True),
            "end_date": Field(String, is_required=True),
            "top_n": Field(Int, default_value=50, is_required=False)
        },
        group_name=RANKINGS_WOMEN,
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
            df = conn.execute(query=queries.prospect_rankings_report_query(
                start_date=start_date,
                end_date=end_date,
                exp=exp,
                top_n=top_n,
                women=True
            )).df()

        # Set up Jinja2 environment to load templates from the current directory
        env = Environment(loader=FileSystemLoader(searchpath=templates.searchpath))
        template = env.get_template("player_cards_template_season_rankings_women.html")

        # Convert DataFrame to a list of dictionaries
        players = df.to_dict(orient="records")

        # Render the template with player data
        html_content = template.render(players=players)
        path = os.path.join(storage.filepath, "women", "rankings", end_date)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, f"{name}.html"), "w") as f:
            f.write(html_content)
        
        return MaterializeResult(
            metadata={
                "df": MetadataValue.md(df.to_markdown())
            }
        )
    
    return _asset

prospect_rankings_report = build_season_rankings_report_women(exp=[0,1,2,3,4,5], name="all")
